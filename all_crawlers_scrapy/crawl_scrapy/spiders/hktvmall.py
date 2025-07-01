import json
from datetime import timedelta
import math
import dateparser
import scrapy

from .setuserv_spider import SetuservSpider


class HktvmallSpider(SetuservSpider):
    name = 'hktvmall-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Hktvmall process start")
        assert self.source == 'hktvmall'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        try:
            product_name = response.css('h1.last::text').extract_first()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ""}
        page = 0
        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page, 'media_entity': media_entity,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        extra_info = response.meta['extra_info']
        res = json.loads(response.text)
        total_pages = math.ceil(int(res['pagination']['total'])/10)
        review_date = self.start_date

        if res['data']:
            for item in res['data']:
                if item:
                    _id = item["_id"]
                    review_date = dateparser.parse(item["date"]).replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["comment"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item["rating"],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                                    self.parse_comments(item, _id, review_date,
                                                                   product_url, product_id,
                                                                   extra_info)
                            if self.type == 'comments':
                                self.parse_comments(item, _id, review_date,
                                                               product_url, product_id,
                                                               extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

            if review_date >= self.start_date and page <= total_pages:
                page += 1
                yield scrapy.Request(url=self.get_review_url(product_id, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page': page, 'media_entity': media_entity,
                                           'extra_info': extra_info, 'dont_proxy': True})
        else:
            if '"data":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            if item['replies']:
                for comment in item['replies']:
                    comment_date = dateparser.parse(comment['date'])
                    comment_date = comment_date.replace(tzinfo=None)
                    comment_date_until = review_date + \
                                         timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:

                        if comment['comment']:
                            self.yield_items_comments \
                                (parent_id=_id,
                                 _id=comment['_id'],
                                 comment_date=comment_date,
                                 title='',
                                 body=comment['comment'],
                                 rating='',
                                 url=product_url,
                                 review_type='comments',
                                 creator_id='',
                                 creator_name='',
                                 product_id=product_id,
                                 extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} on "
                             f"review {_id}")

    @staticmethod
    def get_review_url(product_id, page):
        product_id = product_id.replace('_S_', '')
        url = f'https://ucapi.comms.hktvmall.com/hktvmall/products/{product_id}/reviews/?' \
              f'current_page={page}&has_image=false&has_reply=false&page_size=10'
        return url
