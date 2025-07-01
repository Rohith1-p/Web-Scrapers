from datetime import datetime
import pymongo
import json
from celery import Celery
from confluent_kafka import Producer
from .pipelines import MongoPipeline


class MongoDatabricksPipeline(MongoPipeline):
    """
    To store and update items in db
    """

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.media_supported_scrapers = ['onclive', 'targetedonc', 'cancernetwork', 'ascopost', 'ashclinicalnews', 'oncologytube', 'lymphomahub', 'vjhemonc', 'youtube', 'oncnet']

        self.article_supported_scrapers = ['pubmed', 'clinicaltrials']
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
        client_id = item_dict['client_id']
        source = item_dict.get('source', '')
        item_dict['created_at'] = datetime.utcnow()
        media_entity_id = item_dict["media_entity_id"]

        if source in (self.media_supported_scrapers + self.article_supported_scrapers):
            print("Pipelines Debug: Article/Video is flowing")
            article_id = item_dict['article_id']
            task_id = spider.task_id
            mongo_client = pymongo.MongoClient(self.mongo_uri)

            task_details_db = mongo_client['databricks_tasks']
            task_details_rows = task_details_db['task_details'].find({'task_id': task_id})

            db_name = None
            set_db_col = None
            is_databricks_input = False
            task_details_rows = list(task_details_rows)
            if len(task_details_rows) == 1:
                is_databricks_input = True
                spider.logger.info("task_id succesfully refered")

                set_host = task_details_rows[0]['host']
                set_port = task_details_rows[0]['port']
                set_db_name = task_details_rows[0]['db_name']
                set_db_col = task_details_rows[0]['db_col']
                source_client = pymongo.MongoClient(set_host, set_port)
                db_name = source_client[set_db_name]

            else:
                spider.logger.error("Not able to reference task id / going into Non-databricks Pipeline")

            if source in self.media_supported_scrapers:
                data_to_set = {"$setOnInsert": item_dict}
                print("Pipelines Debug: Pharma article/video is Saving to Database", media_entity_id)

                if is_databricks_input:
                    db_name[set_db_col].update_one(
                        {'client_id': client_id, 'source': source, 'media_entity_id': media_entity_id, 'article_id': article_id}, data_to_set, upsert=True)
                    topic_name = 'mfi_filter_pharma.filtered_media_pharma_demo_tableau'
                else:
                    db_name = 'twitter_articles'
                    pharma_client = pymongo.MongoClient(self.mongo_uri)
                    db_name = pharma_client[db_name]

                    db_name["media_pharma"].update_one(
                        {'client_id': client_id, 'source': source, 'media_entity_id': media_entity_id, 'article_id': article_id}, data_to_set, upsert=True)
                    topic_name = 'mfi_filter_pharma.filtered_media_pharma'
                self.queue_scraped_review_data(item_dict, topic_name)

            elif source in self.article_supported_scrapers:
                created_date = item_dict['created_date']
                item_dict['body'] = ''
                item_dict['description'] = ''

                data_to_set = {"$set": item_dict}
                print("Pipelines Debug: Pubmed/CT article is Saving to Database", media_entity_id)

                if is_databricks_input:
                    db_name[set_db_col].update_one(
                        {'client_id': client_id, 'source': source, 'media_entity_id': media_entity_id,
                         'article_id': article_id, 'created_date': created_date}, data_to_set, upsert=True)

                else:
                    db_name = 'pubmed_ct_articles'
                    pubmed_ct_client = pymongo.MongoClient(self.mongo_uri)
                    db_name = pubmed_ct_client[db_name]

                    db_name["media_pubmed_ct"].update_one(
                        {'client_id': client_id, 'source': source, 'media_entity_id': media_entity_id,
                         'article_id': article_id, 'created_date': created_date}, data_to_set, upsert=True)
            return item
        return item

    def queue_scraped_review_data(self, doc, topic_name):
        message_key = doc['client_id'].encode('utf-8')
        if doc:
            self.producer.poll(0)

            def delivery_report(err, msg):
                if err is not None:
                    print('Message delivery failed: {}'.format(err))
                else:
                    print('Message delivered to {} {} [{}]'.format(msg.topic(), msg.key(),
                                                                msg.partition()))

            self.producer.produce(topic_name, json.dumps(doc, default=MongoPipeline.default).encode('utf-8'),
                                  key=message_key, callback=delivery_report)
            self.producer.flush()
        print("Pipelines Debug: Pipeline Exit Here")
