# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
from datetime import datetime, date
import csv

import pymongo
from celery import Celery
from confluent_kafka import Producer
from scrapy.utils.project import get_project_settings


# To store and update items in db
class MongoPipeline:
    """
    To store and update items in db
    """

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.file = open(get_project_settings()["LOG_DIR"] + "PersonalCare_pipeline_logs.csv", "a")
        self.supported_scrapers = ['amazon', 'amazon_pharmapacks', 'amazonprimepantry',
                                   'freshamazon', 'souqamazon', 'sephora', 'ulta', 'webhose',
                                   'nordstrom', 'macys', 'bloomingdales', 'facebook', 'lazada',
                                   'shopee', '11street', 'costco', 'samsclub', 'target', 'jet',
                                   'walgreens', 'cvs', 'walmart', 'ebay', 'homedepot', 'peapod',
                                   'yhd', 'redmart', 'suning', 'qoo10', 'blibli', 'jd', 'asda',
                                   'morrisons', 'ocado', 'sainsburys', 'cbdoil', 'healthyhempoil',
                                   'drogeriemarkt', 'rossmann', 'influenster', 'boots', 'medpex',
                                   'wilko', 'leafly', 'amazon-brand', 'utkonos', 'ozon',
                                   'superdrug', 'vitacost', 'vitaminshoppe', 'lohaco', 'catch',
                                   'bananarepublic', 'iceheadshop', 'trustpilot', 'marijuanabreak',
                                   'nuleafnaturals', 'greenroadsworld', 'tokopedia', 'bjs',
                                   'mercadolibre', 'allegro', 'allegro_durex', 'allegro_scholl',
                                   'hepsiburada', 'rakuten', 'rakuten_direct', 'rakuten_kenkocom',
                                   'newpharma', 'shoppingcbd', 'bol', 'emag', 'reviewsco',
                                   'hollandandbarrett', 'hktvmall', 'dangdang', 'gome', 'tmall',
                                   'aponeo', 'aporot', 'docmorris', 'apotal', 'mycare', 'sanicare',
                                   'shopapotheke', 'volksversand', 'eurapon', 'delmed', 'apodiscounter',
                                   'medikamente', 'chemistwarehouse', 'pubmed', 'clinicaltrials',
                                   'hktvmall_products', 'bukalapak', 'bukalapak_products',
                                   'tokopedia_products', 'shopee_products', 'bukalapak_level',
                                   'shopee_level', 'tokopedia_level', 'shopee_vn_products',
                                   'tiki_products', 'blibli_products', 'lazada_products',
                                   'jd_id_products', 'lazada_ph_products', 'lazada-category-scraper',
                                   'pet_lazada', 'aminer', 'europepmc', 'researchgate', 'wiley',
                                   'haematologica', 'clinicaltrialseu', 'gastro_level_1', 'gastro_level_two',
                                   'mdedge_lv1', 'gi_org_lv1', 'mdedge_lv2', 'gi_org_lv2', 'gastroendonews_level_one',
                                   'gastroendonews_level_two', 'haematologica', 'clinicaltrialseu',
                                   'shopee_vn_category', 'tiki_vn_category',
                                   'shopee_category', 'shopee_products', 'blibli_id_category',
                                   'lazada_category', 'tokopedia_id_category', 'jd_id_category',
                                   'bukalapak_category', 'hktvmall_category', 'scholar_google_handle',
                                   'semantic_scholar_handle', 'semantic_scholar_list', 'scholar_google_list','amazon_products']

        # Use celery
        self.celery = Celery()
        self.celery.config_from_object('settings')
        kafka_cluster = get_project_settings()["KAFKA_CLUSTER"]
        self.producer = Producer({'bootstrap.servers': kafka_cluster})

    @classmethod
    def from_crawler(cls, crawler):
        try:
            mongo_uri = getattr(crawler.spider, "db_uri")
        except:
            mongo_uri = getattr(crawler.spider, "mongo_uri")
        return cls(
            mongo_uri=mongo_uri,
            mongo_db=getattr(crawler.spider, "db_name")
        )

    def open_spider(self, spider):
        print(f"Pipelines Debug: Pipeline Spider Opened for {spider}")
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        print(f"Pipelines Debug: Pipeline Spider Closed for {spider}")
        self.client.close()

    def handle_social_media_source(self, item, spider):
        if item.get('media_source') == 'instagram' or item.get('entity_source') == 'instagram':
            item_dict = dict(item)

            if item_dict["type"] == "page":
                item['updated_at'] = datetime.utcnow()
                self.db["media_entity"].update_one(
                    {
                        'entity_source': item_dict["entity_source"],
                        "id": item_dict["id"]
                    },
                    {
                        "$set": item_dict,
                    }, upsert=True)
            else:
                keywords = item_dict["keywords"]
                del item_dict["keywords"]
                client_id = item_dict['client_id']
                del item_dict['client_id']
                if item_dict["type"] == "post" and 'filtered_for' in item_dict:
                    filtered_for = item_dict['filtered_for']
                    del item_dict['filtered_for']
                    interactions = item_dict['interactions']
                    del item_dict['interactions']
                    self.db["media"].update_one(
                        {
                            'client_id': client_id,
                            'media_source': item_dict["media_source"],
                            "id": item_dict["id"]
                        },
                        {
                            "$setOnInsert": item_dict,
                            '$set': {
                                "updated_at": datetime.utcnow(),
                                "interactions": interactions
                            },
                            "$addToSet": {
                                "keywords": {
                                    "$each": keywords
                                },
                                "filtered_for": filtered_for
                            }
                        }, upsert=True
                    )

                elif item_dict["type"] == "post" and 'filtered_for' not in item_dict:
                    interactions = item_dict['interactions']
                    del item_dict['interactions']
                    self.db["media"].update_one(
                        {
                            'client_id': client_id,
                            'media_source': item_dict["media_source"],
                            'id': item_dict["id"]
                        },
                        {
                            "$setOnInsert": item_dict,
                            '$set': {
                                "updated_at": datetime.utcnow(),
                                "interactions": interactions
                            },
                            "$addToSet": {
                                "keywords": {
                                    "$each": keywords
                                },
                            }
                        }, upsert=True)

                elif item_dict["type"] == "comment":
                    self.db["comments"].update_one(
                        {
                            'client_id': client_id,
                            'media_source': item_dict["media_source"],
                            "id": item_dict["id"]
                        },
                        {
                            "$setOnInsert": item_dict,
                            '$set': {
                                "updated_at": datetime.utcnow()
                            },
                            "$addToSet": {
                                "keywords": {
                                    "$each": keywords
                                },
                            }
                        }, upsert=True)

            if spider.scrapper_name == "instagram_scrapper_without_filtering":
                if item_dict.get('propagation', 'filtering') in \
                        ['filtering', 'annotation', 'reporting'] \
                        and item['type'] in ['post', 'comments']:
                    collection = "media" if item['type'] == "post" else "comments"
                    self.celery.send_task(
                        'filter_media.filter_instagram_media_without_rules',
                        (item["id"], item['media_source'], collection, client_id))

            return item

    def process_item(self, item, spider):
        item_dict = dict(item)
        if item_dict['client_id'] != 'ELCskincarev1' and item_dict['client_id'] != 'ELCmakeupv1' and \
                item_dict['client_id'] != 'madhuri_ccba' and item_dict['type'] != 'article':
            print("Pipelines Debug: Item is Processing")
            propagate_value = {'scraping': 0, 'filtering': 1, 'annotation': 2, 'reporting': 3}
            item.setdefault('extra_info', {})
            item_dict = dict(item)
            media_entity_id = item_dict.get('media_entity_id')
            media_source = item_dict.get('media_source', '')
            item_dict['created_at'] = datetime.utcnow()
            entity_source = item_dict.get('entity_source', '')
            if "creator_id" in item_dict or "creator_name" in item_dict:
                del item_dict['creator_id']
                del item_dict['creator_name']

            if media_source in self.supported_scrapers or entity_source in self.supported_scrapers:
                client_id = str(item_dict['client_id'])
                review_type = str(item_dict['type'])
                new_propagation = str(item_dict['propagation']).lower()
                if review_type in ['media', 'comments']:
                    data = self.db[review_type].find(
                        {'client_id': client_id, 'media_source': media_source,
                         'media_entity_id': media_entity_id, 'id': item_dict['id']})
                    data_count = data.count()
                    if data_count < 1:
                        self.save_to_database(item_dict, media_entity_id, media_source)
                        self.queue_scraped_review_data(item_dict)
                    if data_count >= 1:
                        for _data in data:
                            new_data_propagate_value = propagate_value.get \
                                (new_propagation, 'scraping')
                            old_data_propagate_value = propagate_value.get \
                                (str(_data['propagation']), 'scraping')
                            if new_data_propagate_value > old_data_propagate_value:
                                self.save_to_database(item_dict, media_entity_id, media_source)
                                self.queue_scraped_review_data(item_dict)
                else:
                    self.save_to_database(item_dict, media_entity_id, media_source)
                    self.queue_scraped_review_data(item_dict)
            else:
                self.handle_social_media_source(item, spider)

        return item

    def save_to_database(self, item_dict, media_entity_id, media_source):
        print("Pipelines Debug: Item is Saving to Database", media_entity_id)
        client_id = item_dict['client_id']
        item_dict['created_at'] = datetime.utcnow()
        if item_dict["type"] == 'lifetime':
            created_date = item_dict['created_date']
        item_dict.update({"updated_at": datetime.utcnow()})
        data_to_set = {"$set": item_dict}

        if item_dict["type"] == 'media':
            media_sub_source = item_dict.get('media_sub_source', None)
            if media_sub_source:
                self.db["media"].update_one(
                    {"client_id": client_id, "media_sub_source": media_sub_source,
                     'media_entity_id': media_entity_id, 'id': item_dict["id"]},
                    data_to_set, upsert=True)
            else:
                self.db["media"].update_one(
                    {"client_id": client_id, "media_source": media_source,
                     'media_entity_id': media_entity_id, 'id': item_dict["id"]},
                    data_to_set, upsert=True)

        elif item_dict["type"] == 'comments':
            self.db["comments"].update_one(
                {"client_id": client_id, 'media_source': item_dict["media_source"],
                 'parent_id': item_dict["parent_id"], "id": item_dict["id"]},
                data_to_set, upsert=True)

        elif item_dict["type"] == 'lifetime':
            self.db["lifetime"].update_one(
                {'client_id': item_dict['client_id'], 'media_source': item_dict[
                    "media_source"], "media_entity_id": item_dict["media_entity_id"],
                 "created_date": created_date}, data_to_set, upsert=True)

        elif item_dict["type"] == 'rank_info':
            created_date = item_dict['created_date']
            self.db["rank_info"].update_one(
                {'client_id': item_dict['client_id'], 'media_source': item_dict[
                    "media_source"], "media_entity_id": item_dict["media_entity_id"],
                 "created_date": created_date}, data_to_set, upsert=True)

        elif item_dict["type"] == 'product_details':
            created_date = item_dict['created_date']
            self.db["product_details"].update_one(
                {'client_id': item_dict['client_id'], 'media_source': item_dict[
                    "media_source"], "media_entity_id": item_dict["media_entity_id"],
                 "created_date": created_date}, data_to_set, upsert=True)

        elif item_dict["type"] == 'category_details':
            created_date = item_dict['created_date']
            self.db["category_details"].update_one(
                {'client_id': item_dict['client_id'], 'media_source': item_dict[
                    "media_source"], "media_entity_id": item_dict["media_entity_id"],
                 "created_date": created_date}, data_to_set, upsert=True)

    @staticmethod
    def default(o):
        if isinstance(o, (date, datetime)):
            return o.isoformat()

    def queue_scraped_review_data(self, doc):
        message_key = doc['client_id'].encode('utf-8')
        if doc:
            self.producer.poll(0)

            def delivery_report(err, msg):
                if err is not None:
                    print('Message delivery failed: {}'.format(err))
                else:
                    print('Message delivered to {} {} [{}]'.format(msg.topic(), msg.key(),
                                                                   msg.partition()))

            if doc['type'] != 'lifetime':
                if doc['client_id'] == 'PersonalCare':
                    print("Client_id", doc['client_id'], "Source", doc['media_source'], "Product_id",
                          doc['media_entity_id'], "Review_id", doc['id'], file=self.file)
                self.producer.produce('mfi_filter.filtered_media',
                                      value=json.dumps(doc, default=MongoPipeline.default)
                                      .encode('utf-8'), key=message_key,
                                      callback=delivery_report)
            else:
                self.producer.produce('mfi_filter.filtered_media_lifetime',
                                      json.dumps(doc, default=MongoPipeline.default)
                                      .encode('utf-8'), key=message_key,
                                      callback=delivery_report)
            self.producer.flush()
        print("Pipelines Debug: Pipeline Exit Here")
