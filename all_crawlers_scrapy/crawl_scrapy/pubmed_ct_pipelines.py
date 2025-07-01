from datetime import datetime
import pymongo
import json
from celery import Celery
from confluent_kafka import Producer
from .pipelines import MongoPipeline


class PubmedCTPipeline(MongoPipeline):
    """
    To store and update items in db
    """

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.supported_scrapers = ['pubmed', 'clinicaltrials']
        # Use celery
        self.celery = Celery()
        self.celery.config_from_object('settings')
        # self.producer = Producer({'bootstrap.servers': kafka_cluster})
        self.producer = Producer({'bootstrap.servers': '172.31.10.51:9192'})

    @classmethod
    def from_crawler(cls, crawler):
        try:
            mongo_uri = getattr(crawler.spider, "db_uri")
        except:
            mongo_uri = getattr(crawler.spider, "mongo_uri")
        return cls(
            mongo_uri=mongo_uri,
            mongo_db=getattr(crawler.spider, "db_name"),
        )

    def process_item(self, item, spider):
        item_dict = dict(item)
        # if item_dict['source'] == 'pubmed' or item_dict['source'] == 'clinicaltrials':
        if item_dict['type'] == 'article':
            source = item_dict.get('source')
            if source in self.supported_scrapers:
                client_id = item_dict['client_id']
                source = item_dict.get('source', '')
                item_dict['created_at'] = datetime.utcnow()
                media_entity_id=item_dict["media_entity_id"]
                article_id = item_dict['article_id']
                created_date = item_dict['created_date']
                item_dict['body'] = ''
                item_dict['description'] = ''

                data_to_set = {"$set": item_dict}
                # data_to_set = {"$setOnInsert": item_dict}
                print("Pipelines Debug: Pubmed/CT article is Saving to Database", media_entity_id)

                db_name = 'pubmed_ct_articles'
                pubmed_ct_client = pymongo.MongoClient(self.mongo_uri)
                db_name = pubmed_ct_client[db_name]

                db_name["media_pubmed_ct"].update_one(
                    {'client_id': client_id, 'source': source, 'media_entity_id': media_entity_id,
                     'article_id': article_id, 'created_date': created_date}, data_to_set, upsert=True)

        return item
