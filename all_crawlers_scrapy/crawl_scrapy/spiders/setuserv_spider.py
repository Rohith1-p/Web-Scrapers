from logging.handlers import TimedRotatingFileHandler
import logging
import time

import datetime
import time
import os
import random
import string
import dateparser
import xlsxwriter
import pandas as pd
import pymongo
import requests

from bson import ObjectId
import scrapy
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.project import get_project_settings
from scrapy.xlib.pydispatch import dispatcher
from twisted.internet.error import DNSLookupError, TCPTimedOutError
from .email_handler import send_email, send_email_with_file
from .log_parser import log_parse_file
from confluent_kafka import Producer
import json
import sys

sys.path.append("/mfi_scrapers/setuserv_scrapy/setuserv_scrapy")
from ..utils import kafkaProducer
from ..utils import MFILogs
from utils import payment_gateway_api
from ..produce_monitor_logs import KafkaMonitorLogs
unique_url_page_dict = {'url': '','page_no':-1, 'url_page_no':''}

from scrapy.conf import settings
import configparser


class SetuservSpider(scrapy.Spider):
    review_count_map, unsupported_url_count_map = {}, {}
    status_urls_list = []
    review_id_map = {}
    no_response_ids = []
    settings = get_project_settings()

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id,
                 spider_name, env):
        # import pdb
        # pdb.set_trace()
        self.document_id = document_id
        self.mongo_client = pymongo.MongoClient(mongo_host, int(mongo_port))
        self.mongo_db = self.mongo_client[mongo_db]
        self.mongo_collection = self.mongo_db[mongo_collection]
        self.config_doc = self.mongo_collection.find_one({"_id": ObjectId(document_id)})
        self.start_urls = self.config_doc.get('start_urls', [])
        self.product_ids = self.config_doc.get('product_ids', [])
        self.client_id = self.config_doc['company_id']
        self.start_date = self.config_doc['since']
        self.end_date = self.config_doc['until']
        self.db_uri = self.config_doc['db_host']
        self.db_name = self.config_doc['db_name']
        self.source = self.config_doc['media_source']
        self.gsheet_id = self.config_doc['gsheet_id']
        self.propagation = self.config_doc.get('propagation', 'filtering')
        self.type = self.config_doc.get('review_type', 'media')
        self.env = env
        mongo_collection = self.mongo_db['tasks']
        task_doc = mongo_collection.find_one({"config_id": ObjectId(self.document_id)})
        self.task_id = task_doc.get('task_ID')
        self.__init__logger(spider_name)
        self.__init__stats(spider_name)
        dispatcher.connect(self.spider_opened, signals.spider_opened)
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.dump_time = str(time.time()).split('.')[0]
        self.failed_urls = list()
        self.product_issue = list()
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.setuserv/kafka.ini'))
        kafka_config = config['Kafka']
        kafka_server = kafka_config['KAFKA_SERVER']
        self.producer = Producer({'bootstrap.servers': kafka_server})
        self.media_entity_logs = {'client_id': self.client_id, 'gsheet_id': self.gsheet_id,
                                  'start_date': self.start_date, 'type_media': self.type}
        self.email_id = payment_gateway_api().get_email_id(self.client_id)

    def __init__logger(self, spider_name):
        logger = logging.getLogger()
        dir_path = get_project_settings()["LOG_DIR"] + self.client_id + '/' + self.source
        print("dir_path: ", dir_path)
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
            except:
                print("Inside except at line 95 in method __init__logger")
        formatter = logging.Formatter(f'%(asctime)s - %(relativeCreated)6d - %(threadName)s - %(process)d - '
                                      f'[{self.document_id}] - [{spider_name}] - '
                                      f'%(name)s - %(levelname)s - %(message)s')
        log_handler = TimedRotatingFileHandler(dir_path + '/scrapy.log', when='D',
                                               interval=1, encoding='utf-8')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)

    def __init__stats(self, spider_name):
        self.stats = logging.getLogger('stats')
        dir_path = get_project_settings()["LOG_DIR"] + "scraping_stats/" + self.source
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        formatter = logging.Formatter(f'%(asctime)s - %(relativeCreated)6d - %(threadName)s - %(process)d - '
                                      f'[{self.document_id}] - [{spider_name}] - [{self.client_id}] - [{self.task_id}] -'
                                      f'%(name)s - %(levelname)s - %(message)s')
        log_handler = TimedRotatingFileHandler(dir_path + '/scrapy.log', when='D',
                                               interval=1, encoding='utf-8')
        log_handler.setFormatter(formatter)
        self.stats.addHandler(log_handler)

    def delete_queue_task(self, task_id):
        info_collection = self.mongo_db['info_parent']

        for product_id in self.product_ids:
            if info_collection.find_one({'task_id': task_id}):
                if info_collection.find_one({'task_id': task_id})['update_frequency_days'] > 0:
                    info_collection.update_many({'task_id': task_id},
                                                {"$set": {'status': 'Periodic'}})
                else:
                    info_collection.delete_one({'task_id': task_id,
                                                'company_id': self.client_id,
                                                'media_source': self.source,
                                                'product_id': product_id})

    def connection_for_tasks(self, time_elapsed):
        mongo_collection = self.mongo_db['tasks']
        mongo_collection.update({"config_id": ObjectId(self.document_id)},
                                {"$set": {"status": True}})
        task_doc = mongo_collection.find_one({"config_id": ObjectId(self.document_id)})

        if task_doc is not None:
            task_id = task_doc.get('task_ID')
            self.delete_queue_task(task_id)
            email = task_doc.get('email')
            print("review_count_map.items", self.review_count_map.items(), type(self.review_count_map.items()))

            print("self.start_urls", self.start_urls, type(self.start_urls))

            for key, value in self.review_count_map.items():
                print("Keys & values", key, value, type(key), type(value))
                index = self.start_urls.index(key)
                product_id = self.product_ids[index]
                self.create_summary_tables(task_id, product_id, key, "success", value, '', email)

            print("unsupported_url_count_map.items", self.unsupported_url_count_map.items())

            for key, value in self.unsupported_url_count_map.items():
                if key not in SetuservSpider.review_count_map:
                    index = self.start_urls.index(key)
                    product_id = self.product_ids[index]
                    self.create_summary_tables(task_id, product_id, key, "failure", 0, value, email)

            review_plus_unsupported_map = {**self.review_count_map,
                                           **self.unsupported_url_count_map}

            print("review_count_map.items", review_plus_unsupported_map.items())

            for _url in self.start_urls:
                if _url not in review_plus_unsupported_map.keys():
                    SetuservSpider.no_response_ids.append(_url)
                    index = self.start_urls.index(_url)
                    product_id = self.product_ids[index]
                    self.create_summary_tables(task_id, product_id, _url, "No response", 0, '', email)

            # if self.source.startswith('amazon') or self.source.endswith('amazon'):
            #     if SetuservSpider.no_response_ids:
            #         self.no_response_data()

            update_status = True
            for data in mongo_collection.find({"task_ID": task_id}):
                update_status = update_status and data.get('status')
                if not update_status:
                    break

            if update_status:
                mongo_collection_for_summary = self.mongo_db['scrapy_summary']
                pd.set_option('max_colwidth', -1)
                _df = \
                    pd.DataFrame(list(mongo_collection_for_summary.find({'task_id': task_id})))
                df_new = _df.drop(['_id', 'task_id'], axis=1)

                df_new = df_new[['client_id', 'source', 'start_date', 'end_date', 'url',
                                 'product_id', 'review_count', 'review_type', 'error', 'status']]

                additional_info = f"Time elapsed: {time_elapsed}"

                arrays = [df_new['client_id'], df_new['source'], df_new['start_date'],
                          df_new['end_date'], df_new['url'], df_new['product_id'],
                          df_new['review_count'], df_new['review_type'], df_new['error'],
                          df_new['status']]

                tuples = list(zip(*arrays))
                index = pd.MultiIndex.from_tuples(tuples,
                                                  names=['client_id', 'source',
                                                         'start_date', 'end_date',
                                                         'url', 'product_id', 'review_count',
                                                         'review_type', 'error', 'status'])
                df_email = pd.DataFrame('', columns=[''], index=index)

                content = df_email.to_html()

                if email is not None:
                    send_email(email, f"Scraper status for {task_id}", additional_info,
                               content)
                    if len(self.failed_urls) > 0:
                        err_report = pd.DataFrame(self.failed_urls).to_html()
                        send_email(email, f"Knowl failed urls for {task_id}", additional_info,
                                   err_report)
                    if len(self.product_issue) > 0:
                        product_report = pd.DataFrame(self.product_issue).to_html()
                        send_email(email, f"Products/Reviews not found for {task_id}", additional_info,
                                   product_report)

            log_parse_file(task_id, self.client_id, self.source)

    def create_summary_tables(self, task_id, product_id, url, status, review_count,
                              error_type, email):

        db_details = 'ecommerce_reviews' + '__url_status'
        media = {
            "client_id": self.client_id,
            "product_id": product_id,
            "url": url,
            "type": "url_status",
            "db_details": db_details
        }
        self.publish_data_to_gateway(media)
        mfi_logs = MFILogs()
        mfi_logs.scraper_logs('review_type', url, self.start_date, 'page_no', self.gsheet_id, 'check_logs',self.client_id)

        summary = {'task_id': task_id,
                   'client_id': self.client_id,
                   'source': self.source,
                   'product_id': product_id,
                   'url': url,
                   'review_count': review_count,
                   'review_type': self.type,
                   'status': status,
                   'error': error_type,
                   'email': email,
                   'start_date': self.start_date,
                   'end_date': self.end_date
                   }

        self.mongo_db['scrapy_summary'].insert(summary)

        if status == 'success':
            self.logger.info(f"{[self.source]}Successfully scraped {review_count} "
                             f"reviews for product id {product_id}")
        if status == 'No response':
            self.logger.info(f"{[self.source]}Successfully opened {url} "
                             f"but fetched no reviews")
        if status == '':
            self.logger.info(f"{[self.source]}Url {url} not opening")

    def product_err(self, value, product_url, product_id):
        self.logger.info(f"Product/Review does not exist for {product_id}")
        products = str(value), str(product_url), str(product_id)
        self.product_issue.append(products)

    def err(self, failure):
        self.logger.error(f"Error {repr(failure)} occurred while opening the url")
        failed_url = failure.request.url
        failed_meta = failure.request.meta
        failed_prodid = failed_meta['media_entity']['id']
        try:
            status = str(failure.value.response.status)
        except:
            status = "Ignore Request"
        try:
            user_agent = failure.request.headers["User-Agent"].decode("utf-8")
        except:
            user_agent = "No Headers"
        failed_request = (
            str(failed_url), failed_prodid, self.client_id, self.source, self.start_date, self.end_date,
            str(failed_meta))
        self.stats.error(f"Request Failure:,{failed_prodid},{failed_url},{self.client_id},{self.source},"
                         f"{self.start_date},{self.end_date},{status},{self.task_id},{failed_meta},{user_agent}")
        self.failed_urls.append(failed_request)

        if failure.check(HttpError):
            response = failure.value.response
            self._unsupported_url_count(response.meta['media_entity']['id'],response.meta['media_entity']['url'],
                                        'HttpError')
            self.logger.error(f"Type of error is HttpError, "
                              f"Check the url - {response.url} again!")

        elif failure.check(DNSLookupError):
            request = failure.request
            self._unsupported_url_count(request.meta['media_entity']['id'], request.meta['media_entity']['url'],
                                        'DNSLookupError')
            self.logger.error(f"Type of error is DNSLookupError, "
                              f"Check the url {request.url} again!")

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self._unsupported_url_count(request.meta['media_entity']['id'], request.meta['media_entity']['url'],
                                        'TimeoutError/TCPTimedOutError')
            self.logger.error(f"Type of error is TimeoutError, "
                              f"Check the url {request.url} again!")

    def spider_opened(self, spider):
        self.logger.info(f"Setu Debug: Setuserv Spider {spider} Opened")
        spider.started_on = datetime.datetime.utcnow()
        monitor_log = f'Successfully {self.source} Opened'
        monitor_log = {"G_Sheet_ID": self.gsheet_id, "Client_ID": self.client_id, "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

    def spider_closed(self, spider):
        self.logger.info(f"Setu Debug: Setuserv Spider {spider} Closed")
        time_elapsed = datetime.datetime.utcnow() - spider.started_on

        monitor_log = f'Successfully {self.source} Closed'
        monitor_log = {"G_Sheet_ID": self.gsheet_id, "Client_ID": self.client_id, "Message": monitor_log}
        self.connection_for_tasks(time_elapsed)
        KafkaMonitorLogs.push_monitor_logs(monitor_log)


    def log_error(self, review_type, product_url, date_remove, page_no, gsheet_id, status, dump_name, response):
        self.dump(response, 'html', dump_name)
        mfi_logs = MFILogs()
        mfi_logs.scraper_logs(review_type, product_url, date_remove, str(page_no), gsheet_id, status,self.client_id)

    def yield_items(self, _id, review_date, title, body, rating, url, review_type, creator_id, creator_name, product_id,
                    page_no='',extra_info={}, product_id_ = ''):
        print("scraper_logs ##########################################################")
        assert _id is not None and _id != ''
        assert isinstance(review_date, datetime.datetime)
        assert body is not None
        assert product_id in self.product_ids
        assert isinstance(extra_info, dict)
        index = self.product_ids.index(product_id)
        product_url = self.start_urls[index]
        page_no = page_no
        print("******page_no****", page_no)
        if product_id_ != '':
            product_id = product_id_

        db_details = 'ecommerce_reviews' + '__review_data'

        if self.propagation == "api":
            media = {
                "parent_id": '',
                "id": str(_id),
                "created_date": review_date,
                "body": str(body),
                "rating": float(rating),
                "parent_type": 'media_entity',
                "url": url,
                "media_source": str(self.source),
                "type": review_type,
                "creator_id": str(creator_id),
                "creator_name": str(creator_name),
                "media_entity_id": str(product_id),
                "media_entity_url": product_url,
                "title": title,
                "client_id": str(self.client_id),
                "propagation": self.propagation,
                "extra_info": extra_info,
                "review_timestamp": int(review_date.timestamp()),
                "db_details": db_details
            }
            self.publish_data_to_gateway(media)
            self._add_product_count(product_url, _id)

        elif self.propagation == 'gsheet_scraping' or self.propagation == 'g_buzz_estimators' or self.propagation == 'Excel_scraping':
            media = {
                "parent_id": '',
                "Review ID": str(_id),
                "Review Date": review_date,
                "body": str(body),
                "rating": float(rating),
                "parent_type": 'media_entity',
                "Review url": url,
                "Source": str(self.source),
                "Scraper Type": "Consumer Posts",
                "creator_id": str(creator_id),
                "Reviewer_Name": str(creator_name),
                "Product ID": str(product_id),
                "Product URL": product_url,
                "title": title,
                "Title + Body": title +" "+ str(body),
                "client_id": str(self.client_id),
                "propagation": self.propagation,
                "extra_info": extra_info,
                "review_timestamp": int(review_date.timestamp()),
                "db_details": db_details,

            }

            # yield media
            print("******in yield setuserv spider**")
            self.publish_data_to_gateway(media)
            self._add_product_count(product_url, _id)
            status = 'Pass'
            date_remove = str(self.start_date)
            date_remove = date_remove[:11] + date_remove[12:]
            url_page_no = product_url+str(page_no)
            print("~~~~~~~~~~~~~~~~~~In yield_items unique_url_page_dict~~~~~~~~~~~~~~~~~~~~~",unique_url_page_dict)
            if  unique_url_page_dict['url_page_no'] != url_page_no:
                unique_url_page_dict['url_page_no'] = url_page_no
                print("~~~~~~~~~~~~~~~~~~In yield_items updated unique_url_page_dict~~~~~~~~~~~~~~~~~~~~~",unique_url_page_dict)
                payment_gateway_api().update_quota(self.email_id)
                mfi_logs = MFILogs()
                mfi_logs.scraper_logs(review_type, product_url, date_remove, str(page_no), self.gsheet_id, status,self.client_id)

                monitor_log = f'Successfully Scraped reviews for {product_url} and page no is {page_no}'
                monitor_log = {"G_Sheet_ID": self.gsheet_id, "Client_ID": self.client_id,
                                "Message": monitor_log}
                KafkaMonitorLogs.push_monitor_logs(monitor_log)

    def yield_items_comments(self, parent_id, _id, comment_date, title, body, rating,
                             url, review_type, creator_id, creator_name, product_id,review_date,
                             extra_info={} ):
        assert parent_id is not None and parent_id != ''
        assert _id is not None and _id != ''
        assert isinstance(comment_date, datetime.datetime)
        assert body is not None
        assert product_id in self.product_ids
        assert isinstance(extra_info, dict)
        index = self.product_ids.index(product_id)
        product_url = self.start_urls[index]

        db_details = 'ecommerce_reviews' + '__review_data'

        if self.propagation == "api":

            media = {
                "parent_id": str(parent_id),
                "id": str(_id),
                "created_date": comment_date,
                "body": str(body),
                "rating": rating,
                "parent_type": 'media_entity',
                "url": url,
                "media_source": str(self.source),
                "type": review_type,
                "media_entity_url": product_url,
                "creator_id": str(creator_id),
                "creator_name": str(creator_name),
                "media_entity_id": str(product_id),
                "title": title,
                "client_id": str(self.client_id),
                "propagation": self.propagation,
                "extra_info": extra_info,
                "review_timestamp": int(comment_date.timestamp()),
                "db_details": db_details
            }

            # yield media
            self.publish_data_to_gateway(media)
            self._add_product_count(product_id, _id)

        elif self.propagation == 'gsheet_scraping' or self.propagation == 'g_buzz_estimators' or self.propagation == 'Excel_scraping':
            media = {
                "parent_id": str(parent_id),
                "Review ID": str(_id),
                "Review Date": comment_date,
                "body": str(body),
                "rating": rating,
                "parent_type": 'media_entity',
                "Review url": url,
                "Source": str(self.source),
                "Scraper Type": "Consumer Posts",
                "Product URL": product_url,
                "creator_id": str(creator_id),
                "Reviewer_Name": str(creator_name),
                "Product ID": str(product_id),
                "title": title,
                "Title + Body": title +" "+ str(body),
                "client_id": str(self.client_id),
                "propagation": self.propagation,
                "extra_info": extra_info,
                "review_timestamp": int(comment_date.timestamp()),
                "db_details": db_details
            }

            # yield media
            self.publish_data_to_gateway(media)
            self._add_product_count(product_url, _id)

    def yield_lifetime_ratings(self, product_id, review_count, average_ratings, ratings):
        from datetime import datetime
        assert product_id in self.product_ids
        assert isinstance(ratings, dict)
        index = self.product_ids.index(product_id)
        product_url = self.start_urls[index]
        db_details = 'ecommerce_reviews' + '__product_lifetime'
        if self.propagation == "api":
            if review_count is not None:
                review_count = int(review_count)

            lifetime_rating = {
                "client_id": str(self.client_id),
                "media_source": str(self.source),
                "media_entity_id": str(product_id),
                "product_url": product_url,
                "type": "lifetime",
                "propagation": self.propagation,
                "review_count": review_count,
                "average_ratings": round(float(average_ratings), 2),
                "rating": ratings,
                "created_date": datetime.utcnow(),
                "db_details": db_details
            }
            # yield lifetime_rating
            self.publish_data_to_gateway(lifetime_rating)

        elif self.propagation == 'gsheet_scraping' or self.propagation == 'g_buzz_estimators' or self.propagation == 'Excel_scraping':
            lifetime_rating = {
                "client_id": str(self.client_id),
                "Source": str(self.source),
                "Product ID": str(product_id),
                "Product URL": product_url,
                "type": "lifetime",
                "propagation": self.propagation,
                "Ratings Count": int(review_count),
                "Average Rating": round(float(average_ratings), 2),
                "Rating Breakdown": ratings,
                "Scraper Run Date": datetime.utcnow(),
                "db_details": db_details
            }
            # yield lifetime_rating
            print("insede yiled_lifetime_ratings in setuserv_spider")
            print("lifetime rating is: ", lifetime_rating)
            print("waiting for 5 sec just to obeseve result")
            time.sleep(5)

            self.publish_data_to_gateway(lifetime_rating)

    def yield_rank_info(self, product_id, product_info, price, rank_info):
        index = self.product_ids.index(product_id)
        product_url = self.start_urls[index]

        db_details = 'ecommerce_reviews' + '__product_rank_info'
        rank_info = {
            "client_id": str(self.client_id),
            "media_source": str(self.source),
            "media_entity_id": str(product_id),
            "product_url": product_url,
            "product_info": product_info,
            "type": "rank_info",
            "propagation": self.propagation,
            "price": price,
            "rank": rank_info,
            "created_date": datetime.datetime.utcnow(),
            "db_details": db_details
        }
        self.publish_data_to_gateway(rank_info)

    def yield_article(self, article_id, product_id, created_date, username, description,
                      full_text, title, url, disease_area, medicines,
                      trial, views_count):
        assert product_id in self.product_ids

        db_details = 'media_sources_db_scraping'
        article = {
            "client_id": self.client_id,
            "source": str(self.source),
            "media_entity_id": product_id,
            "created_date": created_date,
            "article_id": article_id,
            "username": username,
            "reviewer_name": username,
            "description": description,
            "full_text": full_text,
            "url": url,
            "type": self.type,
            "title": title,
            "disease_area": disease_area,
            "medicines": medicines,
            "trial": trial,
            "views_count": views_count,
            "propagation": self.propagation,
            "review_timestamp": int(created_date.timestamp()),
            "db_details": db_details
        }
        # yield article
        self.publish_data_to_gateway(article)

    # def yield_pubmed_ct_article(self, url, author_name, article_title, article_link, created_date,
    #                   article_id, extra_info, product_id, full_text):
    #
    #     db_details = 'articles_scraping_db'
    #     pubmed_ct_article = {
    #         "client_id": self.client_id,
    #         "source": str(self.source),
    #         "media_entity_id": product_id,
    #         "created_date": created_date,
    #         "article_id": article_id,
    #         "url": url,
    #         "full_text": full_text,
    #         "type": "article",
    #         "article_title": article_title,
    #         "author_name": author_name,
    #         "article_link": article_link,
    #         "extra_info": extra_info,
    #         "propagation": self.propagation,
    #         "db_details": db_details,
    #         "review_timestamp": int(created_date.timestamp())
    #     }
    #     # yield pubmed_ct_article
    #     self.publish_data_to_gateway(pubmed_ct_article)

    def yield_product_details(self,product_url,product_id,product_name, brand_name,product_price,avg_rating,
                          seller_name,category_url='',
                          volume_or_weight='',additional_fields='',seller_url='', seller_avg_rating='',about='',
                          seller_no_of_ratings='',no_of_unites_sold='',total_reviews='',
                          seller_followers='', seller_no_of_unites_sold='',discount_price='', discount='',rating_count='',
                          best_seller_rating='',rating_map='',asin='', stock='',product_description='', product_information='',
                          product_specifications= " ", breadcrumb = " ",image_url = " ", highlights=" "):
        from datetime import datetime
        print("inside yield_product_details method")
        db_details = 'ecommerce_product' + '__product_details'

        media = {
            "client_id": str(self.client_id),
            "Source": str(self.source),
            "Product URL": product_url,
            "Product ID": product_id,
            "Product Name": product_name,
            "Brand Name": brand_name,
            "List Price": product_price,
            "About this Item": about,
            "Rating": avg_rating,
            "Units Sold": no_of_unites_sold,
            "Review Count": total_reviews,
            "Product Description": product_description,
            "Product Information": product_information,
            "Seller Name": seller_name,
            "Category URL": category_url,
            "Volume or Weight": volume_or_weight,
            "Additional Fields": additional_fields,
            "Seller URL": seller_url,
            "Seller Rating": seller_avg_rating,
            "Seller Rating Count": seller_no_of_ratings,
            "Seller Followers": seller_followers,
            "Seller Units Sold": seller_no_of_unites_sold,
            "Scraper Type": "Product Details",
            "Discounted Price": discount_price,
            "Discount Percentage": discount,
            "Rating Count": rating_count,
            "Best Sellers Rank": best_seller_rating,
            "Rating Breakdown": rating_map,
            "ASIN": asin,
            "Product Availability": stock,
            "Breadcrumb": breadcrumb,
            "Product Specifications": product_specifications,
            "propagation": self.propagation,
            "Scraper Run Date": datetime.utcnow(),
            "db_details": db_details,
            "product_specifications": product_specifications,
            "breadcrumb": breadcrumb,
            "Image":image_url,
            "Highlights": highlights
        }
        # yield media
        self.publish_data_to_gateway(media)
        self._add_product_count(product_url, product_url)
        status = 'Pass'
        mfi_logs = MFILogs()
        date_remove = str(self.start_date)
        date_remove = date_remove[:11] + date_remove[12:]
        mfi_logs.scraper_logs(self.type, product_url, date_remove, '1' , self.gsheet_id, status,self.client_id)
        #if unique_url_page_dict['url']!=product_url:
        #    unique_url_page_dict['url'] = product_url
        payment_gateway_api().update_quota(self.email_id)

        monitor_log = f'Successfully Scraped Product Info for {product_url}'
        monitor_log = {"G_Sheet_ID": self.gsheet_id, "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

    def yield_category_details(self, category_url, product_url,  page_no,extra_info='',asin='',product_id='',product_name='', rating='',
                              rating_count='', discounted_price='', actual_price='', discount_percentage='',
                              product_availability='', sponsored_listing='',product_description='',
                              category_level_1='', category_level_2='', category_level_3='',
                              category_level_4='',category_level_5='', category_level_6='',is_sponsored=' ',units_sold='',brand_name='', size='',
                               mall_type='', seller_name='', seller_id='', review_count='' ):
        from datetime import datetime
        db_details = 'ecommerce_product' + '__category_details'
        page_no = page_no
        print("***page_no***", page_no)

        media = {
            "client_id": str(self.client_id),
            "Source": str(self.source),
            "Source URL": category_url,
            "Product URL": product_url,
            "Product ID": product_id,
            "Scraper Type": "Category Products",
            "Scraper Run Date": datetime.utcnow(),
            'Product Name': product_name,
            'Rating': rating,
            'Rating Count': rating_count,
            'ASIN': asin,
            'Discounted Price': discounted_price,
            'List Price': actual_price,
            'Product description': product_description,
            'Discount Percentage': discount_percentage,
            'Product Availability': product_availability,
            'Sponsored Listing?': sponsored_listing,
            'Level 1 Category': category_level_1,
            'Level 2 Category': category_level_2,
            'Level 3 Category': category_level_3,
            'Level 4 Category': category_level_4,
            'Level 5 Category': category_level_5,
            'Level 6 Category': category_level_6,
            'Size':size,
            'Rank': '',
            'Units Sold':units_sold,
            "Brand":brand_name,
            "propagation": self.propagation,
            "extra_info": extra_info,
            "db_details": db_details,
            "is_sponsored": is_sponsored,
            "Mall Type": mall_type,
            "Seller Name": seller_name,
            "Seller ID": seller_id,
            "Review Count": review_count
        }
        # yield media
        self.publish_data_to_gateway(media)
        self._add_product_count(category_url, product_url)
        status = 'Pass'
        date_remove = str(self.start_date)
        date_remove = date_remove[:11] + date_remove[12:]
        print("category_url",category_url,"page_no",page_no)
        url_page_no = category_url+str(page_no)
        print("~~~~~~~~~~~~~~~~~~In yield_amazon_category_details unique_url_page_dict~~~~~~~~~~~~~~~~~~~~~",unique_url_page_dict)
        if  unique_url_page_dict['url_page_no'] != url_page_no:
            unique_url_page_dict['url_page_no'] = url_page_no
            print("~~~~~~~~~~~~~~~~~~In yield_amazon_category_details updated unique_url_page_dict~~~~~~~~~~~~~~~~~~~~~",unique_url_page_dict)
            payment_gateway_api().update_quota(self.email_id)
            mfi_logs = MFILogs()
            mfi_logs.scraper_logs(self.type, category_url, date_remove, str(page_no), self.gsheet_id, status,self.client_id)

            monitor_log = f'Successfully Scraped Category for {category_url} and page no {page_no}'
            monitor_log = {"G_Sheet_ID": self.gsheet_id, "Client_ID": self.client_id,
                            "Message": monitor_log}
            KafkaMonitorLogs.push_monitor_logs(monitor_log)

    def yield_amazon_category_details(self, category_url, product_url, product_id, product_name, rating,
                                      rating_count, discounted_price, actual_price, discount_percentage,
                                      product_availability, sponsored_listing,
                                      category_level_1, category_level_2, category_level_3, category_level_4,
                                      category_level_5, category_level_6, page_no,rank):
        from datetime import datetime
        db_details = 'ecommerce_product' + '__amazon_category_details'
        media = {
            "client_id": str(self.client_id),
            "Source": str(self.source),
            "category_url": category_url,
            "Source URL": category_url,
            "Product URL": product_url,
            "Product ID": product_id,
            "Scraper Type": "Category Products",
            "Scraper Run Date": datetime.utcnow(),
            'Product Name': product_name,
            'Rating': rating,
            'Rating Count': rating_count,
            'ASIN': product_id,
            'Discounted Price': discounted_price,
            'List Price': actual_price,
            'Discount Percentage': discount_percentage,
            'Product Availability': product_availability,
            'Sponsored Listing?': sponsored_listing,
            'Level 1 Category': category_level_1,
            'Level 2 Category': category_level_2,
            'Level 3 Category': category_level_3,
            'Level 4 Category': category_level_4,
            'Level 5 Category': category_level_5,
            'Level 6 Category': category_level_6,
            'Rank':rank,
            "propagation": self.propagation,
            "created_date": datetime.utcnow(),
            "extra_info": {},
            "db_details": db_details
        }
        self.publish_data_to_gateway(media)
        self._add_product_count(category_url, product_url)
        status = 'Pass'
        date_remove = str(self.start_date)
        date_remove = date_remove[:11] + date_remove[12:]
        print("category_url",category_url,"page_no",page_no)
        url_page_no = category_url+str(page_no)
        print("~~~~~~~~~~~~~~~~~~In yield_amazon_category_details unique_url_page_dict~~~~~~~~~~~~~~~~~~~~~",unique_url_page_dict)
        if  unique_url_page_dict['url_page_no'] != url_page_no:
            unique_url_page_dict['url_page_no'] = url_page_no
            print("~~~~~~~~~~~~~~~~~~In yield_amazon_category_details updated unique_url_page_dict~~~~~~~~~~~~~~~~~~~~~",unique_url_page_dict)
            payment_gateway_api().update_quota(self.email_id)
            mfi_logs = MFILogs()
            mfi_logs.scraper_logs(self.type, category_url, date_remove, str(page_no), self.gsheet_id, status,self.client_id)

            monitor_log = f'Successfully Scraped Category for {category_url} and page no {page_no}'
            monitor_log = {"G_Sheet_ID": self.gsheet_id, "Client_ID": self.client_id,
                            "Message": monitor_log}
            KafkaMonitorLogs.push_monitor_logs(monitor_log)

    def yield_product_variation_details(self, category_url, category_breadcrumb, product_url, product_id, product_name,
                                        brand_name, total_reviews, avg_rating, no_of_unites_sold, product_breadcrumb,
                                        product_price, discount_price, discount_percentage, sale_price, wholesale,
                                        product_description, product_info_selected, product_info_options,
                                        product_specifications, shop_vouchers, promotions, sku, offers, stock,
                                        shop_location, additional_fields):
        from datetime import datetime
        db_details = 'ecommerce_product' + '__product_variation_details'

        media = {
            "client_id": str(self.client_id),
            "media_source": str(self.source),
            "category_url": category_url,
            "category_breadcrumb": category_breadcrumb,
            "product_url": product_url,
            "media_entity_id": product_id,
            "product_name": product_name,
            "brand_name": brand_name,
            "total_reviews": total_reviews,
            "avg_rating": avg_rating,
            "no_of_unites_sold": no_of_unites_sold,
            "product_breadcrumb": product_breadcrumb,
            "product_price": product_price,
            "discount_price": discount_price,
            "discount_percentage": discount_percentage,
            "sale_price": sale_price,
            "wholesale": wholesale,
            "product_description": product_description,
            "product_info_selected": product_info_selected,
            "product_info_options": product_info_options,
            "product_specifications": product_specifications,
            "shop_vouchers": shop_vouchers,
            "promotions": promotions,
            "offers": offers,
            "stock": stock,
            "shop_location": shop_location,
            "additional_fields": additional_fields,
            "sku": sku,
            "type": "product_variation_details",
            "propagation": self.propagation,
            "created_date": datetime.utcnow(),
            "db_details": db_details
        }

        # yield media
        self.publish_data_to_gateway(media)

    def yield_nykaa_product_details(self, description, discount, listed_price, discounted_price, product_name,
                                    product_url, product_id, rating, rating_count, reviews_count, star_rating):
        from datetime import datetime
        db_details = 'ecommerce_product' + '__nykaa_product_details'

        media = {
            "client_id": str(self.client_id),
            "media_source": str(self.source),
            "media_entity_id": product_id,
            "product_url": product_url,
            "product_name": product_name,
            "product_description": description,
            "discount_on_product": discount,
            "listed_price": listed_price,
            "discounted_price": discounted_price,
            "average_rating": rating,
            "total_rating_count": rating_count,
            "total_reviews_count": reviews_count,
            "type": "nykaa_product_details",
            "propagation": self.propagation,
            "created_date": datetime.utcnow(),
            "star_ratings": star_rating,
            "db_details": db_details
        }
        # yield media
        self.publish_data_to_gateway(media)

    def yield_research_sources(self, article_id, product_id, created_date, author_name, description, full_text,
                               product_url,
                               title, url, article_link, extra_info):
        # assert product_id in self.product_ids

        db_details = 'articles_scraping_db'
        article = {
            "client_id": self.client_id,
            "source": str(self.source),
            "media_entity_id": product_id,
            "created_date": created_date,
            "article_id": article_id,
            "author_name": author_name,
            "description": description,
            "full_text": full_text,
            "media_entity_url": product_url,
            "url": url,
            "type": self.type,
            "title": title,
            "article_link": article_link,
            'extra_info': extra_info,
            "propagation": self.propagation,
            "product_id": str(product_id),
            "review_timestamp": int(created_date.timestamp()),
            "db_details": db_details
        }
        # yield article
        self.publish_data_to_gateway(article)

    def yield_hcp_profile_articles(self, product_url, product_id, article_id, article_url, ol_name, designation,
                                   publications_count, citations, citations_since_2016, h_index, h_index_since_2016,
                                   i10_index, i10_index_since_2016, citations_2014, citations_2015, citations_2016,
                                   citations_2017, citations_2018, citations_2019, citations_2020, citations_2021,
                                   authors_list, description, article_citations, published_date, publisher,
                                   article_title):
        from datetime import datetime
        db_details = 'hcp_profile_articles'
        media = {
            "client_id": str(self.client_id),
            "media_source": str(self.source),
            "product_url": product_url,
            "media_entity_id": product_id,
            "article_id": article_id,
            "article_url": article_url,
            "ol_name": ol_name,
            "designation": designation,
            "publications_count": publications_count,
            "citations": citations,
            "citations_since_2016": citations_since_2016,
            "h_index": h_index,
            "h_index_since_2016": h_index_since_2016,
            "i10_index": i10_index,
            "i10_index_since_2016": i10_index_since_2016,
            "citations_2014": citations_2014,
            "citations_2015": citations_2015,
            "citations_2016": citations_2016,
            "citations_2017": citations_2017,
            "citations_2018": citations_2018,
            "citations_2019": citations_2019,
            "citations_2020": citations_2020,
            "citations_2021": citations_2021,
            "authors_list": authors_list,
            "description": description,
            "article_citations": article_citations,
            "published_date": published_date,
            "publisher": publisher,
            "article_title": article_title,
            "type": "hcp_profile_articles",
            "propagation": self.propagation,
            "created_date": datetime.utcnow(),
            "db_details": db_details
        }
        # yield media
        self.publish_data_to_gateway(media)

    def yield_get_article_handle_names(self, product_id, product_url, handle_url, citation_count, paper_count,
                                       handle_name,
                                       bio):
        from datetime import datetime
        db_details = 'handle_names'
        media = {
            "client_id": str(self.client_id),
            "media_source": str(self.source),
            "media_entity_id": product_id,
            "product_url": product_url,
            "handle_url": handle_url,
            "citation_count": citation_count,
            "paper_count": paper_count,
            "type": "handle_names",
            "handle_name": handle_name,
            "bio": bio,
            "propagation": self.propagation,
            "created_at": datetime.utcnow(),
            "created_date": datetime.utcnow(),
            "db_details": db_details
        }
        # yield media
        self.publish_data_to_gateway(media)

    def yield_get_article_details(self, product_id, product_url, handle_url, handle_name, description, title,
                                  citation_count, paper_count):
        from datetime import datetime
        db_details = 'handle_data'
        media = {
            "client_id": str(self.client_id),
            "media_source": str(self.source),
            "media_entity_id": product_id,
            "product_url": product_url,
            "handle_url": handle_url,
            "type": "handle_data",
            "handle_name": handle_name,
            "description": description,
            "title": title,
            "citation_count": citation_count,
            "paper_count": paper_count,
            "propagation": self.propagation,
            "created_at": datetime.utcnow(),
            "created_date": datetime.utcnow(),
            "db_details": db_details
        }
        # yield media
        self.publish_data_to_gateway(media)

    def yield_products_list(self, sub_brand, product_url, product_id, product_name, review_count):
        from datetime import datetime
        db_details = 'products_list_scraping_db'
        media = {
            'client_id': str(self.client_id),
            'media_source': str(self.source),
            'sub_brand': sub_brand,
            'product_url': product_url,
            "media_entity_id": product_id,
            'product_name': product_name,
            'review_count': review_count,
            'created_date': datetime.utcnow(),
            'type': 'products_list',
            'propagation': self.propagation,
            'db_details': db_details
        }
        # yield media
        self.publish_data_to_gateway(media)

    def yield_quora(self, product_id, product_url, _id, questions, answers, links,
                    names, views, upvotes, answer_id, comments):
        from datetime import datetime
        db_details = 'quora_answers_scraping_db'
        media = {"client_id": self.client_id,
                 "media_entity_id": str(product_id),
                 "media_source": self.source,
                 "product_url": product_url,
                 "questions": str(questions),
                 "answers": answers,
                 "links": links,
                 "names": names,
                 "question_id": str(_id),
                 'type': 'quora_answers',
                 'created_date': datetime.utcnow(),
                 'propagation': self.propagation,
                 'db_details': db_details,
                 'views': views,
                 'upvotes': upvotes,
                 'answer_id': str(answer_id),
                 'comments': comments
                 }
        self.publish_data_to_gateway(media)

    def _add_product_count(self, product_url, review_id):
        if product_url in SetuservSpider.review_count_map:
            if review_id in SetuservSpider.review_id_map:
                pass
            else:
                self.review_count_map[product_url] += 1
                self.review_id_map.update({review_id: 1})
        else:
            self.review_count_map.update({product_url: 1})
            self.review_id_map.update({review_id: 1})

    def _unsupported_url_count(self, product_id, product_url, error_type):
        self.unsupported_url_count_map.update({product_url: error_type})

    def no_response_data(self):
        workbook = xlsxwriter.Workbook('noresponse_file', {'strings_to_urls': False})
        sheet1 = workbook.add_worksheet()
        sheet1.write(0, 0, 'client_id')
        sheet1.write(0, 1, 'source')
        sheet1.write(0, 2, 'review_type')
        sheet1.write(0, 3, 'url')
        sheet1.write(0, 4, 'product_id')
        sheet1.write(0, 5, 'start_date')
        sheet1.write(0, 6, 'end_date')
        sheet1.write(0, 7, 'date_flag')
        sheet1.write(0, 8, 'update_frequency_days')
        sheet1.write(0, 9, 'propagation')
        sheet1.name = "all_sources"

        row_number = 0
        data = self.mongo_db['scrapy_summary'].find({'status': 'No Response'})
        date_flag = 'yes'
        for _data in data:
            sheet1.write(row_number + 1, 0, _data['client_id'])
            sheet1.write(row_number + 1, 1, _data['source'])
            sheet1.write(row_number + 1, 2, _data['review_type'])
            sheet1.write(row_number + 1, 3, _data['url'])
            sheet1.write(row_number + 1, 4, _data['product_id'])
            sheet1.write(row_number + 1, 5, _data['start_date'].isoformat())
            sheet1.write(row_number + 1, 6, _data['end_date'].isoformat())
            sheet1.write(row_number + 1, 7, date_flag)
            sheet1.write(row_number + 1, 8, 0)
            sheet1.write(row_number + 1, 9, _data['propagation'])

            row_number += 1
        workbook.close()

        url = get_project_settings()['SCRAPY_SERVER'] + 'scraping_service/scraping'
        file_obj = os.getcwd() + '/noresponse_file'
        chars = string.ascii_uppercase + string.digits
        temporary_file_name = 'scraping-' + ''.join(random.choice(chars) for _ in range(10))
        temporary_file_name = temporary_file_name + ".xlsx"
        temp_file = '/tmp/' + temporary_file_name
        with open(temp_file, 'w') as _file:
            for line in file_obj:
                _file.write(line)

        file = {'upload_file': open(temp_file, 'rb')}
        data = {'email': get_project_settings()['CC_EMAIL']}
        response = requests.post(url, data=data, files=file)
        print(response)

    def dump(self, response, ext, *params):
        try:
            filename = '/tmp/' + "_".join(params) + '_' + self.dump_time
            f = open(filename + "." + ext, "w+")
            f.write(response.text)
            f.close()
            print("name of the dump file is: ", filename)
        except:
            self.logger.info("dumping html file failed")

    def publish_data_to_gateway(self, item):
        print('came to publish_data_to_gateway in setuserv_spider')
        print('ITEM IN PROCESS_ITEM', item)
        item_dict = dict(item)
        print("item_dict keys ", item_dict.keys())
        item_dict['created_at'] = datetime.datetime.utcnow()
        # item_dict['db_key'] = self.db_details

        client_id = ''
        if 'client_id' in item_dict:
            client_id = item_dict['client_id']
        topic_name = "setuserv_spider.processed_scraped_data"
        topic_name = topic_name + "_" + client_id + "_" + self.env

        print('item_dict from setu_spider -->', item_dict)
        print('topic_name -->', topic_name)
        SetuProducers = kafkaProducer()
        SetuProducers.setu_producer(item_dict, topic_name)
