import re
import json
import dateparser
import scrapy
from .setuserv_spider import SetuservSpider


class ShopapothekeSpider(SetuservSpider):
    name = 'shopapotheke-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopapotheke process start")
        assert self.source == 'shopapotheke'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=self.get_review_url(product_id, 1), callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        lifetime = response.meta['lifetime']
        product_id = media_entity["id"]

        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        page = 1
        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity,
                                   'page': page})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        res = json.loads(response.text)
        review_date = self.start_date

        if res['reviews']:
            for item in res['reviews']:
                if item:
                    try:
                        product_name = item['productName']
                    except:
                        product_name = ''
                    extra_info = {"product_name": product_name, "brand_name": ""}
                    review_date = dateparser.parse(str(item['submissionDate']).split('T')[0])
                    _id = item['reviewId']
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item['message']
                                if body:
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item['title'],
                                         body=body,
                                         rating=item['rating'],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

            if review_date >= self.start_date:
                page += 1
                yield scrapy.Request(url=self.get_review_url(product_id, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity,
                                           'page': page})

    @staticmethod
    def get_review_url(product_id, page):
        url = f'https://www.shop-apotheke.com/webclient/api/product-review/v1/de/com/variants/' \
              f'{product_id}/reviews?page={page}&pageSize='
        return url

    def get_lifetime_ratings(self, product_id, response):
        try:
            res = json.loads(response.text)
            res = res['summary']
            review_count = res['ratingCount']
            average_ratings = res['averageRating']
            ratings = {
                "rating_1" : res['starsFive'],
                "rating_2" : res['starsFour'],
                "rating_3" : res['starsThree'],
                "rating_4" : res['starsTwo'],
                "rating_5" : res['starsOne'],
            }

            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=round(float(average_ratings), 1),
                 ratings=ratings)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
