import hashlib
import math
from datetime import timedelta
import dateparser
import json
from urllib.parse import urlparse

import scrapy
from .setuserv_spider import SetuservSpider


class LeaflySpider(SetuservSpider):
    name = 'leafly-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Leafly process start")
        assert self.source == 'leafly'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            yield scrapy.Request(url=self.get_review_url(product_url, page),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        if 'name="robots"' in response.text or 'class="content blocking"' in response.text:
            yield scrapy.Request(url=self.get_review_url(product_url, page),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            return
        res = json.loads(response.text)
        review_date = self.start_date
        total_count = res['metadata']['totalCount']

        if res['data']:
            for item in res['data']:
                if item:
                    extra_info = {"product_name": item['product']['name'],
                                  "brand_name": item['product']['brand']['name']}
                    _id = item["id"]
                    review_date = dateparser.parse(item["created"]).replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item['text']
                                if body:
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item['rating'],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date and page < total_count:
                if page == 0:
                    page += 3
                else:
                    page += 6
                yield scrapy.Request(url=self.get_review_url(product_url, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity,
                                           'page': page})

        else:
            if '"data":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_review_url(product_url, page):
        url_path = urlparse(product_url).path.split('/')[-1]
        if page == 0:
            url = f"https://consumer-api.leafly.com/api/product_reviews/v1/{url_path}?take=3&skip=0"
        else:
            url = f"https://consumer-api.leafly.com/api/product_reviews/v1/{url_path}?take=6&skip={page}"
        return url
