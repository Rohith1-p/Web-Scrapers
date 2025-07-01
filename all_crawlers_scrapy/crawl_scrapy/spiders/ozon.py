import json
import math
import dateparser
import time

import scrapy
from .setuserv_spider import SetuservSpider


class OzonSpider(SetuservSpider):
    name = 'ozon-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Ozon process start")
        assert self.source == 'ozon'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, method="POST", callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'dont_proxy': True},
                                 headers=self.get_headers(product_url))
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        try:
            product_name = response.css('script[type="application/ld+json"]'
                                        '::text').extract_first()
            product_name = product_name.split('"name":"')[1].split('","brand')[0]\
                .replace("\\", '').split(',')[0]
        except:
            product_name = ''
        try:
            brand = response.css('script[type="application/ld+json"]::text'
                                 ).extract_first().split('brand":"')[1].split('",')[0]
        except:
            brand = ''
        extra_info = {"product_name": product_name, "brand_name": brand}
        page = 1
        yield scrapy.Request(url=self.get_review_url(), method="POST",
                             callback=self.parse_reviews,
                             errback=self.err,
                             dont_filter=True,
                             body=json.dumps(self.get_payload(product_id, page)),
                             headers=self.get_headers(product_url),
                             meta={'media_entity': media_entity, 'extra_info': extra_info,
                                   'page': page, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        extra_info = response.meta["extra_info"]
        page = response.meta["page"]

        if 'Request unsuccessful' in response.text:
            self.logger.info(f"Request is blocked & Sending request again for {product_url} and {product_id}")
            time.sleep(1)
            yield scrapy.Request(url=self.get_review_url(), method="POST",
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 dont_filter=True,
                                 body=json.dumps(self.get_payload(product_id, page)),
                                 headers=self.get_headers(product_url),
                                 meta={'media_entity': media_entity, 'extra_info': extra_info,
                                       'page': page, 'dont_proxy': True})
            return

        res = json.loads(response.text)
        count = res['state']['paging']['total']
        total_pages = math.ceil(count / 10)
        review_date = self.start_date
        self.logger.info(f"Reviews are fetching for {product_id}")

        if res['state']['reviews']:
            for item in res['state']['reviews']:
                if item:
                    _id = item["id"]
                    review_date = dateparser.parse(str(item['createdAt']))
                    review_date = review_date.replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["content"]['comment']
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item['content']["score"],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date and page <= total_pages:
                page += 1
                yield scrapy.Request(url=self.get_review_url(), method="POST",
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     dont_filter=True,
                                     body=json.dumps(self.get_payload(product_id, page)),
                                     headers=self.get_headers(product_url),
                                     meta={'media_entity': media_entity, 'extra_info': extra_info,
                                           'page': page, 'dont_proxy': True})
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_payload(product_id, page):
        payload = {'asyncData': 'eyJ1cmwiOiIvY29udGV4dC9kZXRhaWwvaWQvMTc5NzY0NzQvIiwiY'
                                '2kiOnsiaWQiOjIwNzg1MywibmFtZSI6Imxpc3RSZXZpZXdzRGVza3'
                                'RvcCIsInZlcnRpY2FsIjoicnBQcm9kdWN0IiwidmVyc2lvbiI6MSw'
                                'icGFyYW1zIjpbeyJuYW1lIjoicGFnaW5hdGlvblR5cGUiLCJ0ZXh0'
                                'IjoibG9hZE1vcmVCdXR0b24ifV19fQ==',
                   'componentName': 'listReviewsDesktop',
                   'extraBody': {},
                   'url': f"/context/detail/id/{product_id}/?page={page}"
                   f"&sort=created_at_desc"}
        return payload

    @staticmethod
    def get_review_url():
        url = 'https://www.ozon.ru/api/composer-api.bx/widget/json/v2'
        return url

    @staticmethod
    def get_headers(product_url):
        return {'Accept': 'application/json',
                'Content-Type': 'application/json',
                'path': '/api/composer-api.bx/widget/json/v2',
                'Origin': 'https://www.ozon.ru',
                'Referer': product_url,
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5)'
                              ' AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/75.0.3770.100 Safari/537.36'}
