import datetime
import json
import re

import requests
import scrapy
from scrapy.utils.project import get_project_settings
from setuserv_scrapy.items import WebhoseItems

from .setuserv_spider import SetuservSpider


class WebhoseSpider(SetuservSpider):
    name = 'webhose-product-reviews'
    HOST = 'http://webhose.io'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        # override self.start_urls because we are getting query string as input
        self.query = self.start_urls[0]
        self.start_urls = [f'{self.HOST}/filterWebContent?token={get_project_settings()["WEBHOSE_TOKEN"]}&format=json&sort=published&q={self.query}']
        assert (self.source == 'webhose')


    def utc_datetime(self, datetimestamp):
        datetime_str, tz_hour, tz_minutes = re.findall('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3})([+\-]{1}\d{2}):(\d{2})', datetimestamp)[0]
        utc_timestamp = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%f') - datetime.timedelta(hours=int(tz_hour), minutes=int(tz_minutes))
        return utc_timestamp

    def get_review_url(self, webhose_url):
        resp = requests.get(webhose_url)
        target_url = re.findall(r'url=(http.*)" />', resp.text)
        target_url = target_url[0] if target_url else None
        return target_url

    def parse(self, response):
        # inspect_response(response, self)
        resp = json.loads(response.body)
        reviews = resp['posts']
        paginate = True
        for review in reviews:
            created_date = review['published']
            created_date = self.utc_datetime(created_date)
            if self.start_date <= created_date <= self.end_date:

                fields = {
                    'client_id': self.client_id,
                    'id': review['uuid'],
                    'created_date': created_date, # timezone handled
                    'body': review['text'],
                    'rating': review['rating'],
                    'parent_type': 'media_entity', # we are considering datum as media
                    'url': self.get_review_url(review['url']), # This is making scrapy very slow
                    'media_source': 'webhose',
                    'type': 'media', # we are considering datum as media
                    'creator_id': review['author'], # We dont see anything uniqe so using author name as id
                    'creator_name': review['author'],
                    'media_entity_id': self.query,
                    'title': review['title'],
                    'propagation': self.propagation
                }
                media_item = WebhoseItems(**fields)
                yield media_item
            elif created_date > self.end_date:
                continue
            else:
                paginate = False
                break

        # Handle pagination for next page reviews
        requests_left = resp['requestsLeft']
        more_results = resp['moreResultsAvailable']
        if requests_left:
            next_page_url = resp['next'] if (paginate and more_results) else None
            if next_page_url:
                url = self.HOST + next_page_url
                print('paginating webhose...')
                yield scrapy.Request(url, callback=self.parse)
        else:
            self.logger.critical('quota over for more requests')
