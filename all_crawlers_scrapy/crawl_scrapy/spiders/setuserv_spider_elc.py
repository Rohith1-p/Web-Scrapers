from logging.handlers import TimedRotatingFileHandler
import logging
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
from scrapy.conf import settings
from bson import ObjectId
import scrapy
from scrapy import signals
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.project import get_project_settings
from scrapy.xlib.pydispatch import dispatcher
from twisted.internet.error import DNSLookupError, TCPTimedOutError
from .email_handler import send_email, send_email_with_file
from .log_parser import log_parse_file
from .setuserv_spider import SetuservSpider

class SetuservSpiderELC(SetuservSpider):

    review_sub_source_count_map = {}
    review_sub_source_id_map = {}
    lifetime_rating_map = {}
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,'
        'image/apng,*/*;q=0.8, application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'text/plain;charset=UTF-8',
        'User-Agent': 'Mozilla/4.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
    }

    def connection_for_tasks(self, time_elapsed):
        mongo_collection = self.mongo_db['tasks']
        mongo_collection.update({"config_id": ObjectId(self.document_id)},
                                {"$set": {"status": True}})
        task_doc = mongo_collection.find_one({"config_id": ObjectId(self.document_id)})

        if task_doc is not None:
            task_id = task_doc.get('task_ID')
            email = task_doc.get('email')
            for key, value in self.review_count_map.items():
                index = self.product_ids.index(key)
                url = self.start_urls[index]
                self.create_summary_tables(task_id, key, url, "success", value, '', email)

            for key, value in self.review_sub_source_count_map.items():
                index = self.product_ids.index(key[1])
                url = self.start_urls[index]
                self.create_classify_tables(task_id, key[0], key[1], url, value)

            for key, value in self.lifetime_rating_map.items():
                index = self.product_ids.index(key)
                url = self.start_urls[index]
                self.create_lifetime_summary_tables(task_id, key, url, "success", value, '', email)

            for key, value in self.unsupported_url_count_map.items():
                if key not in SetuservSpider.review_count_map:
                    index = self.product_ids.index(key)
                    url = self.start_urls[index]
                    self.create_summary_tables(task_id, key, url, "failure", 0, value, email)

            review_plus_unsupported_map = {**self.review_count_map,
                                           **self.unsupported_url_count_map}

            lifetime_plus_unsupported_map = {**self.lifetime_rating_map,
                                             **self.unsupported_url_count_map}
            for _id in self.product_ids:
                if _id not in review_plus_unsupported_map.keys():
                    SetuservSpider.no_response_ids.append(_id)
                    index = self.product_ids.index(_id)
                    url = self.start_urls[index]
                    self.create_summary_tables(task_id, _id, url, "No response", 0, '', email)

            no_response_file = ''
            if SetuservSpider.no_response_ids:
                no_response_file = self.no_response_data(task_id)

            for _id in self.product_ids:
                if _id not in lifetime_plus_unsupported_map.keys():
                    SetuservSpider.no_response_ids.append(_id)
                    index = self.product_ids.index(_id)
                    url = self.start_urls[index]
                    self.create_lifetime_summary_tables(task_id, _id, url, "No response", 0, '', email)


            update_status = True
            for data in mongo_collection.find({"task_ID": task_id}):
                update_status = update_status and data.get('status')
                if not update_status:
                    break

            if update_status:
                additional_info, df_review = self.create_review_df(task_id, time_elapsed)
                summary_df = self.get_review_summary(task_id)
                classify_df = self.create_classify_df(task_id)
                df_life_time = self.create_lifetime_df(task_id)
                life_time_summary_df = self.get_lifetime_summary(task_id)

                review_report = f'review_report_task_id_{task_id}.xlsx'
                self.result_to_excel(review_report, [summary_df, life_time_summary_df,
                                                     df_review, df_life_time, classify_df], ['Review Summary',
                                                                                           'Lifetime Summary',
                                                                                           'Review Details',
                                                                                           'Lifetime Details',
                                                                                           'Classify Details'])
                file_list = [review_report]
                if no_response_file:
                    file_list.append(no_response_file)
                if email is not None:
                    send_email_with_file(email, f"[{task_id}]Scraper status of {self.client_id}", additional_info, file_list)

            log_parse_file(task_id, self.client_id, self.source)

    def yield_items_elc(self, review_id, media_sub_source, review_date, title, body, rating, url, review_type, creator_id, creator_name, media_entity_id, extra_info={}):
        assert review_id is not None and review_id != ''
        assert isinstance(review_date, datetime.datetime)
        assert body is not None
        assert media_entity_id in self.product_ids
        assert isinstance(extra_info, dict)
        index = self.product_ids.index(media_entity_id)
        media_entity_url = self.start_urls[index]

        media = dict()

        if media_sub_source:
            media["media_sub_source"] = str(media_sub_source)

        media["id"] = review_id
        media["media_source"] = str(self.source)
        media["body"] = str(body)
        media["created_at"] = datetime.datetime.utcnow()
        media["created_date"] = review_date
        media["creator_id"] = str(creator_id)
        media["creator_name"] = str(creator_name)
        media["media_entity_id"] = str(media_entity_id)
        media["parent_type"] = "entity"
        media["rating"] = float(rating)
        media["review_url"] = url
        media["title"] = str(title)
        media["type"] = review_type
        media["url"] = media_entity_url
        media['client_id'] = str(self.client_id)
        media['propagation'] = self.propagation
        media["review_timestamp"] = int(review_date.timestamp())

        yield media
        self._add_product_count(media_entity_id, review_id)
        self._add_sub_source_reviews(media_sub_source, media_entity_id, review_id)

    def yield_lifetime_ratings_elc(self, product_id, total_review_count, average_ratings, rating_map, recommended_percentage='0.0'):
        assert product_id in self.product_ids
        assert isinstance(rating_map, dict)
        lifetime_rating = {'client_id': self.client_id, 'media_source': self.source, 'media_entity_id': product_id,
                     'type': 'lifetime', 'propagation': self.propagation, 'review_count': total_review_count,
                     'average_ratings': round(float(average_ratings), 2), 'ratings': rating_map,
                           "recommended_percentage" : recommended_percentage}

        yield lifetime_rating
        self._add_lifetime_ratings(product_id, total_review_count)

    def no_response_data(self, task_id):
        file_name = f"no_response_task_id_{task_id}.xlsx"
        workbook = xlsxwriter.Workbook(file_name, {'strings_to_urls': False})
        sheet1 = workbook.add_worksheet('all_sources')
        sheet1.write(0, 0, 'client_id')
        sheet1.write(0, 1, 'source')
        sheet1.write(0, 2, 'type')
        sheet1.write(0, 3, 'url')
        sheet1.write(0, 4, 'product_id')
        sheet1.write(0, 5, 'start_date')
        sheet1.write(0, 6, 'end_date')
        sheet1.write(0, 7, 'update_frequency_days')
        sheet1.write(0, 8, 'propagation')

        row_number = 0
        data = self.mongo_db['scrapy_summary'].find({'task_id': task_id, 'status': 'No response'})
        for _data in data:
            sheet1.write(row_number + 1, 0, _data['client_id'])
            sheet1.write(row_number + 1, 1, _data['source'])
            sheet1.write(row_number + 1, 2, _data['review_type'])
            sheet1.write(row_number + 1, 3, _data['url'])
            sheet1.write(row_number + 1, 4, _data['product_id'])
            sheet1.write(row_number + 1, 5, _data['start_date'].isoformat())
            sheet1.write(row_number + 1, 6, _data['end_date'].isoformat())
            sheet1.write(row_number + 1, 7, 0)
            sheet1.write(row_number + 1, 8, self.propagation)

            row_number += 1
        workbook.close()

        return file_name

    def err(self, failure):
        self.logger.error(f"Error {repr(failure)} occurred while opening the url")

        if failure.check(HttpError):
            response = failure.value.response
            self._unsupported_url_count(response.meta['media_entity']['id'],
                                        'HttpError')
            self.logger.error(f"Type of error is HttpError, "
                              f"Check the url - {response.url} again!, status - {response.status}")


        elif failure.check(DNSLookupError):
            request = failure.request
            self._unsupported_url_count(request.meta['media_entity']['id'],
                                        'DNSLookupError')
            self.logger.error(f"Type of error is DNSLookupError, "
                              f"Check the url {request.url} again!")

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self._unsupported_url_count(request.meta['media_entity']['id'],
                                        'TimeoutError/TCPTimedOutError')
            self.logger.error(f"Type of error is TimeoutError, "
                              f"Check the url {request.url} again!")


    def create_review_df(self, task_id, time_elapsed):
        mongo_collection_for_summary = self.mongo_db['scrapy_summary']
        _df = pd.DataFrame(list(mongo_collection_for_summary.find({'task_id': task_id})))
        df_new = _df.drop(['_id', 'task_id'], axis=1)
        df_new = df_new[['client_id', 'source', 'start_date', 'end_date', 'url',
                         'product_id', 'review_count', 'review_type', 'error', 'status']]
        additional_info = f"Time elapsed: {str(time_elapsed)}"
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
        df_email = pd.DataFrame(index=index)
        return additional_info, df_email

    def create_classify_df(self, task_id):
        mongo_collection_for_summary = self.mongo_db['scrapy_classify_summary']
        _df = pd.DataFrame(list(mongo_collection_for_summary.find({'task_id': task_id})))
        df_new = _df.drop(['_id', 'task_id'], axis=1)
        df_new = df_new[['client_id', 'source', 'start_date', 'end_date', 'url',
                         'product_id', 'sub_source', 'review_count', 'review_type']]

        arrays = [df_new['client_id'], df_new['source'], df_new['start_date'],
                  df_new['end_date'], df_new['url'], df_new['product_id'], df_new['sub_source'],
                  df_new['review_count'], df_new['review_type']]
        tuples = list(zip(*arrays))
        index = pd.MultiIndex.from_tuples(tuples,
                                          names=['client_id', 'source',
                                                 'start_date', 'end_date',
                                                 'url', 'product_id', 'sub_source', 'review_count',
                                                 'review_type'])
        summary_df = pd.DataFrame(index=index)
        return summary_df

    def create_lifetime_df(self, task_id):
        mongo_collection_for_summary = self.mongo_db['lifetime_rating_info']
        _df = pd.DataFrame(list(mongo_collection_for_summary.find({'task_id': task_id})))
        df_new = _df.drop(['_id', 'task_id'], axis=1)
        df_new = df_new[['client_id', 'source', 'url',
                         'product_id', 'review_count', 'error', 'status']]

        arrays = [df_new['client_id'], df_new['source'],
                  df_new['url'], df_new['product_id'],
                  df_new['review_count'], df_new['error'],
                  df_new['status']]
        tuples = list(zip(*arrays))
        index = pd.MultiIndex.from_tuples(tuples,
                                          names=['client_id', 'source',
                                                 'url', 'product_id', 'review_count',
                                                 'error', 'status'])
        df = pd.DataFrame(index=index)
        return df

    def create_classify_tables(self, task_id, sub_source, product_id, url, review_count):
        summary = {'task_id': task_id,
                   'client_id': self.client_id,
                   'source': self.source,
                   'product_id': product_id,
                   'sub_source': sub_source,
                   'url': url,
                   'review_count': review_count,
                   'review_type': self.type,
                   'start_date': self.start_date,
                   'end_date': self.end_date
                   }

        self.mongo_db['scrapy_classify_summary'].insert(summary)

    def create_lifetime_summary_tables(self, task_id, product_id, url, status, review_count,
                              error_type, email):
        summary = {'task_id': task_id,
                   'client_id': self.client_id,
                   'source': self.source,
                   'product_id': product_id,
                   'url': url,
                   'review_count': review_count,
                   'status': status,
                   'error': error_type,
                   'email': email
                   }

        self.mongo_db['lifetime_rating_info'].insert(summary)

    def _add_sub_source_reviews(self, sub_source, product_id, review_id):
        if sub_source:
            if (sub_source, product_id) in self.review_sub_source_count_map:
                if review_id in self.review_sub_source_id_map:
                    pass
                else:
                    self.review_sub_source_count_map[(sub_source, product_id)] += 1
                    self.review_sub_source_id_map.update({review_id: 1})
            else:
                self.review_sub_source_count_map.update({(sub_source, product_id): 1})
                self.review_sub_source_id_map.update({review_id: 1})

    def _add_lifetime_ratings(self, product_id, total_review_count):
        self.lifetime_rating_map[product_id] = total_review_count

    def get_review_summary(self, task_id):
        sources = self.mongo_db['scrapy_summary'].distinct('source', {'task_id': task_id})
        tuples = list()

        for source in sources:
            total_count = self.mongo_db['scrapy_summary'].count({'task_id': task_id, 'source' : source})
            success_count = self.mongo_db['scrapy_summary'].count({'task_id': task_id, 'source' : source,
                                                                   'status': 'success'})
            failed_count = total_count - success_count
            review_count = self.mongo_db['scrapy_summary'].find({'task_id': task_id, 'source' : source,
                                                                 'status': 'success',},
                                                                     {'review_count': 1})
            review_count = sum(int(x.get('review_count', 0)) for x in review_count)
            tuples.append((self.client_id, source, total_count, success_count, failed_count, review_count))

        index = pd.MultiIndex.from_tuples(tuples,
                                          names=['client_id', 'source',
                                                 'Total URLs', 'Total Success', 'Total Failed', 'Total Reviews'])
        summary_df = pd.DataFrame(index=index)


        return summary_df

    def get_lifetime_summary(self, task_id):
        sources = self.mongo_db['scrapy_summary'].distinct('source', {'task_id': task_id})
        tuples = list()

        for source in sources:
            total_count = self.mongo_db['lifetime_rating_info'].count({'task_id': task_id, 'source': source})
            success_count = self.mongo_db['lifetime_rating_info'].count({'task_id': task_id, 'source': source,
                                                                   'status': 'success'})
            failed_count = total_count - success_count
            tuples.append((self.client_id, source, total_count, success_count, failed_count))

        index = pd.MultiIndex.from_tuples(tuples,
                                          names=['client_id', 'source',
                                                 'Total URLs', 'Total Success', 'Total Failed'])
        summary_df = pd.DataFrame(index=index)

        return summary_df

    def get_headers(self):
        return self.headers

    @staticmethod
    def result_to_excel(filename, dataframes_list, sheet_names_list):
        writer_report = pd.ExcelWriter(filename, engine='xlsxwriter')
        workbook = writer_report.book
        for i, dataframe in enumerate(dataframes_list):
            sheet_name = sheet_names_list[i]
            dataframe.to_excel(writer_report, sheet_name=sheet_name)
            cell_format = workbook.add_format({
                'bold': False,
                'border': 0})
            worksheet = writer_report.sheets[sheet_name]
            worksheet.set_column(0, 10, 20)
            for col_num, col_name in enumerate(dataframe.columns.values):
                worksheet.write(0, col_num, col_name, cell_format)

        writer_report.save()
        writer_report.close()
