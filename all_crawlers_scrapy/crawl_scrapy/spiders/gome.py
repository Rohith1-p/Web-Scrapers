import json
import hashlib
from datetime import timedelta
import dateparser
import scrapy

from .setuserv_spider import SetuservSpider


class GomeSpider(SetuservSpider):
    name = 'gome-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Gome process start")
        assert self.source == 'gome'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]

        try:
            product_name = response.css('div.hgroup h1::text').extract_first()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        page_count = 1
        yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page_count': page_count,
                                   'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_id = media_entity['id']
        product_url = media_entity['url']
        page_count = response.meta['page_count']
        res = json.loads(response.text.split("all(")[1].replace('"})', '"}'))
        review_date = self.start_date

        if res['evaList']['Evalist']:
            for item in res['evaList']['Evalist']:
                if item:
                    _id = item["appraiseId"]
                    review_date = dateparser.parse(item["post_time"]).replace(hour=0, minute=0, second=0)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["appraiseElSum"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item["appraiseEleTITLE"],
                                         body=body,
                                         rating=item["mscore"],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                                    self.parse_comments \
                                        (item, _id, review_date, product_url,
                                         product_id, extra_info)
                            if self.type == 'comments':
                                self.parse_comments \
                                    (item, _id, review_date, product_url,
                                     product_id, extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

            if review_date >= self.start_date:
                page_count += 1
                yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page_count': page_count,
                                           'media_entity': media_entity,
                                           'extra_info': extra_info})

        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            comment = item['gomereply']['text']
        except:
            comment = ''
        try:
            comment_date = \
                dateparser.parse(item['gomereply']['time']).replace(tzinfo=None)
        except:
            comment_date = ''
        try:
            comment_date_until = review_date + \
                                 timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:
                if comment:
                    comment_id = _id + \
                                 hashlib.sha512(comment.encode('utf-8')).hexdigest()

                    self.yield_items_comments \
                        (parent_id=_id,
                         _id=comment_id,
                         comment_date=comment_date,
                         title='',
                         body=comment,
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
    def get_review_url(product_id, page_count):
        url = f'https://ss.gome.com.cn/item/v1/prdevajsonp/appraiseNew/' \
              f'{product_id}/{page_count}/all/1/14693/flag/appraise/all'
        return url
