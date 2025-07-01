import json
import dateparser

import scrapy
from .setuserv_spider import SetuservSpider


class OcadoSpider(SetuservSpider):
    name = 'ocado-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("ocado process start")
        assert self.source == 'ocado'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Processing for product_url {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        try:
            product_name = response.css('title[data-react-helmet="true"]::text').extract_first()
            product_name = product_name.split('|')[0]
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        offset = 0
        yield scrapy.Request(url=self.get_review_url(product_id, offset),
                             callback=self.parse_reviews, errback=self.err,
                             dont_filter=True,
                             meta={'offset': offset, 'media_entity': media_entity,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity['url']
        extra_info = response.meta['extra_info']
        offset = response.meta["offset"]
        res = json.loads(response.text)
        review_date = self.start_date
        if res['reviews']:
            for item in res['reviews']:
                if item:
                    review_date = dateparser.parse(item['creationDate'])
                    if self.start_date <= review_date <= self.end_date:
                        _id = item['reviewId']
                        try:
                            if self.type == 'media':
                                body = item['text']
                                if body:
                                    self.yield_items \
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
                            self.logger.warning("Body is not their for review {}".format(_id))
            if review_date >= self.start_date:
                offset += 10
                yield scrapy.Request(url=self.get_review_url(product_id, offset),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'offset': offset, 'media_entity': media_entity,
                                           'extra_info': extra_info, 'dont_proxy': True})
        else:
            if '"reviews":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))

    @staticmethod
    def get_review_url(product_id, offset):
        url = f"https://www.ocado.com/webshop/api/v1/products/{product_id}/reviews" \
              f"?sortOrder=MOST_RECENT&limit=10&offset={offset}"
        return url
