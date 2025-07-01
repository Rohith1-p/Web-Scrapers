from datetime import datetime,date
import json
import pymongo
from celery import Celery
import requests
from confluent_kafka import Producer
from scrapy.utils.project import get_project_settings
from .pipelines import MongoPipeline

# To store and update items in db
class MongoELCPipeline(MongoPipeline):
    """
    To store and update items in db
    """

    def __init__(self, mongo_uri, mongo_db,reporting_url,logger):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.reporting_url = reporting_url
        self.supported_scrapers = ['sephora', 'ulta', 'nordstrom', 'macys', 'bloomingdales']
        self.logger = logger
        # Use celery
        self.celery = Celery()
        self.celery.config_from_object('settings')
        kafka_cluster = get_project_settings()["KAFKA_CLUSTER"]
        self.producer = Producer({'bootstrap.servers': kafka_cluster})

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        try:
            mongo_uri = getattr(crawler.spider, "db_uri")
        except:
            mongo_uri = getattr(crawler.spider, "mongo_uri")
        return cls(
            mongo_uri=mongo_uri,
            mongo_db=getattr(crawler.spider, "db_name"),
            reporting_url=settings.get('REPORTING_URL'),
            logger=getattr(crawler.spider, "logger"),
        )

    def process_item(self, item, spider):
        item_dict = dict(item)
        if item_dict['client_id'] == 'ELCskincarev1' or item_dict['client_id'] == 'ELCmakeupv1':
            self.logger.info('Processing item '+str(item))
            item.setdefault('extra_info', {})
            media_entity_id = item_dict.get('media_entity_id')
            item_dict['created_at'] = datetime.utcnow()
            media_source = item_dict.get('media_source', '')
            entity_source = item_dict.get('entity_source', '')

            if "creator_id" in item_dict or "creator_name" in item_dict:
                del item_dict['creator_id']
                del item_dict['creator_name']

            if media_source in self.supported_scrapers or entity_source in self.supported_scrapers:

                client_id = item_dict['client_id']
                if item_dict["type"] == 'entity':

                    self.db["media_entity"].update_one({"entity_source": item_dict["entity_source"], 'id': item_dict["id"]},
                                                       {"$setOnInsert": item_dict,

                                                        "$set": {"updated_at": datetime.utcnow()}}, upsert=True)
                elif item_dict["type"] == 'media':
                    self.logger.info('Processing media item ')
                    item_dict['created_at'] = datetime.utcnow()
                    media_sub_source = item_dict.get('media_sub_source', None)
                    if media_sub_source:
                        status = self.db["media"].find({"client_id": client_id, "media_sub_source": media_sub_source,
                                                     'id': item_dict["id"],'media_entity_id': media_entity_id},
                                                    ).count()

                        if status == 0:
                            self.db["media"].update_one({"client_id": client_id, "media_sub_source": media_sub_source,
                                                         'media_entity_id': media_entity_id, 'id': item_dict["id"]},
                                                        {"$setOnInsert": item_dict,
                                                         "$set": {"updated_at": datetime.utcnow()}}, upsert=True)

                            if item_dict['propagation'] in ['filtering', 'annotation',
                                                              'reporting'] and (item_dict[
                                                                                    'client_id'] == 'ELCskincarev1' or
                                                                                item_dict['client_id'] == 'ELCmakeupv1'):
                                self.queue_scraped_review_data(item_dict)

                            else:
                                print('Problem with propogation value')
                        else:
                            print("review already present")

                    else:
                        status = self.db["media"].find({"client_id": client_id, "media_source": media_source,
                                                     'id': item_dict["id"],'media_entity_id': media_entity_id},
                                                    ).count()

                        if status == 0:
                            self.db["media"].update_one({"client_id": client_id, "media_source": media_source,
                                                         'media_entity_id': media_entity_id, 'id': item_dict["id"]},
                                                        {"$setOnInsert": item_dict,
                                                         "$set": {"updated_at": datetime.utcnow()}}, upsert=True)

                            if item_dict['propagation'] in ['filtering', 'annotation',
                                                              'reporting'] and (item_dict[
                                                                                    'client_id'] == 'ELCskincarev1' or
                                                                                item_dict['client_id'] == 'ELCmakeupv1'):
                                self.queue_scraped_review_data(item_dict)

                            else:
                                print('Problem with propogation value')
                        else:
                            print("review already present")

                elif item_dict["type"] == 'comments':
                    item_dict['created_at'] = datetime.utcnow()
                    item_dict['updated_at'] = datetime.utcnow()
                    self.db["comments"].update_one({'media_source': item_dict["media_source"],
                                                    'parent_id': item_dict["parent_id"], "id": item_dict["id"]},
                                                   {"$set": item_dict}, upsert=True)

                elif item_dict["type"] == 'lifetime':

                    url = 'http://actv2.mineforinsights.com/admin/elc/taxonomy/lifetimesummary'

                    response = requests.post(url=url, json={'client_id': item_dict[
                    'client_id'], 'source': item_dict['media_source'],
                    'scraping_product_id':item_dict['media_entity_id'],
                    'average_rating':item_dict.get('average_ratings',0.0),
                    'review_count':item_dict.get('review_count',0),
                    'one_star_count':item_dict['ratings'].get('rating_1',0)
                    ,'two_star_count':item_dict['ratings'].get('rating_2',0),
                    'three_star_count':item_dict['ratings'].get('rating_3',0),
                    'four_star_count':item_dict['ratings'].get('rating_4',0),
                    'five_star_count':item_dict['ratings'].get('rating_5',0),
                    'recommended_percentage':item_dict.get('recommended_percentage','Null')
                    })

                    self.logger.info('response for item '+str(item_dict)+' is '
                    ''+response.text)
                    return item


                elif item_dict['type'] == 'closed_end_data':
                    self.logger.info('Sending closed end data')

                    url = self.reporting_url + "/taxonomy/additionalreviewinfo/"
                    response = requests.post(url=url, json={'age':item_dict['age'],
                    'eye_color':item_dict['eye_color'],'hair_color':item_dict[
                    'hair_color'],'skin_tone':item_dict['skin_tone'],'skin_type':
                    item_dict['skin_type'],'recommend_product':item_dict[
                    'recommended'],'uuid':item_dict['uuid'],
                    'received_free':item_dict['received_free'],
                    'scraping_product_id':item_dict['media_entity_id']})

                    self.logger.info('response for item ' + str(item_dict) + ' is '
                    '' + response.text)
                    return item

                elif item_dict['type'] == 'product_love_count':
                    self.logger.info('Sending product love count')
                    url = self.reporting_url + "/taxonomy/additionalproductinfo/"

                    response = requests.post(url=url, json={
                        'client_id':item_dict['client_id'],
                        'love_count':item_dict['product_love_count'],
                        'scraping_product_id':item_dict['media_entity_id']})

                    self.logger.info('response for item ' + str(item_dict) + ' is '+ response.text)

                    return item

        return item


    def queue_scraped_review_data(self, doc):
        if doc:
            self.logger.info('Before queuing message ')
            self.producer.poll(0)

            def delivery_report(err, msg):
                if err is not None:
                    print('Message delivery failed: {}'.format(err))
                else:
                    print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

            self.producer.produce('mfi_filter.filtered_media',
                                      json.dumps(doc, default=MongoPipeline.default)
                                      .encode('utf-8'), callback=delivery_report)

            self.logger.info('Before flushing')

            self.producer.flush()
            self.logger.info('Message queued')
