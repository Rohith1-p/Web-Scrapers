# -*- coding: utf-8 -*-
import json
from datetime import datetime
from datetime import timedelta

import scrapy
from scrapy.conf import settings

from .setuserv_spider_elc import SetuservSpiderELC
from .utils import re_classify_review_source

# settings.overrides['DOWNLOADER_MIDDLEWARES'] = {
#     'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
#     'setuserv_scrapy.smartproxy.ProxyMiddleware': 100
# }

class UltaSpider(SetuservSpiderELC):
    HOST = 'https://www.ulta.com'
    name = 'ulta-product-reviews'
    review_count = 0

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Ulta process start")
        assert (self.source == 'ulta')

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page_count = 0
            yield scrapy.Request(self.get_review_url(media_entity['id'], page_count), callback=self.parse_reviews,
                                 meta={'product_id': product_id, 'page_count': page_count,
                                       'media_entity': media_entity, 'lifetime': True},
                                 errback=self.err, dont_filter=True)
            self.logger.info(f"Processing for product_id {product_id}")
            self.logger.info(f"Generating reviews for {product_url}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        media_entity_id = str(media_entity["id"])
        lifetime = response.meta['lifetime']
        product_id = media_entity['id']
        res = json.loads(response.text)
        results = res['results'][0]['reviews']

        for item in results:
            details = item['details']
            details['nickname'] = ""

        if lifetime:
            self.create_lifetime_ratings(product_id, response)

        self.create_reviews(results, media_entity, media_entity_id, response)

    def create_reviews(self, items, media_entity, media_entity_id, response):
        res = json.loads(response.text)
        current_page_number = res['paging']['current_page_number']
        created_date = self.start_date - timedelta(days=1)
        for review in items:
            details = review['details']
            created_date = datetime.utcfromtimestamp(details['created_date'] / 1000)
            sub_source = None
            if 'brand_name' in details:
                sub_source = details['brand_name'].lower()
            elif 'Brand_name' in details:
                sub_source = details['Brand_name'].lower()
            allow_review, media_sub_source = re_classify_review_source(self.source, sub_source)
            if allow_review:
                if self.start_date <= created_date <= self.end_date:
                    details['comments'] = '' if details['comments'] is None else details['comments']
                    details['headline'] = '' if details['headline'] is None else details['headline']

                    review_id = str(sub_source + str(review["review_id"]) if sub_source else review["review_id"])
                    body = str(details['comments'])
                    creator_id = str(details['nickname'])
                    creator_name = str(details['nickname'])
                    rating = float(review['metrics']['rating'])
                    title = str(details['headline'])

                    self.yield_items_elc(review_id, media_sub_source, created_date, title, body, rating,
                                                response.url, 'media', creator_id, creator_name,
                                                media_entity['id'])

                    UltaSpider.review_count += 1
                    self.logger.info(f"Successfully scraped {UltaSpider.review_count} reviews")

            else:
                self.logger.info('dropping review with id ' + review['review_id'] + ' '
                                                                                    'with content ' + details[
                                     'comments'] + ' for source ' + self.source
                                 + ' and created date ' + str(created_date))

        if created_date >= self.start_date:
            page_count = current_page_number * 10
            yield scrapy.Request(self.get_review_url(media_entity_id, page_count),
                                 callback=self.parse_reviews,
                                 meta={'media_entity': media_entity, 'lifetime': False, 'page_count': page_count},
                                 errback=self.err, dont_filter=True)

        if created_date <= self.start_date:
                self.logger.info(f"Finished scraping reviews for url with product_id: {media_entity_id}")

    def create_lifetime_ratings(self, product_id, response):
        data = json.loads(response.text)
        review_stats = {}
        if 'rollup' in data['results'][0]:
            review_stats = data['results'][0]['rollup']
        else:
            review_stats['review_count'] = 0
            review_stats['average_rating'] = 0.0
            review_stats['rating_histogram'] = []
        total_review_count = review_stats['review_count']
        average_ratings = review_stats['average_rating']
        ratings = review_stats['rating_histogram']
        rating_map = {}
        for idx, val in enumerate(ratings):
            rating_map['rating_' + str(idx + 1)] = val

        self.yield_lifetime_ratings_elc(product_id, total_review_count, round(float(average_ratings), 1),
                                               rating_map)

    @staticmethod
    def get_review_url(product_id, start):
        url = f'https://display.powerreviews.com/m/6406/l/en_US/product/{product_id}/reviews?paging.from={start}&paging.size=10&filters=&search=&sort=Newest&apikey=daa0f241-c242-4483-afb7-4449942d1a2b'
        return url
