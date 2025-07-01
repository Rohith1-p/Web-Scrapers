import json
import hashlib
import dateparser
import scrapy
from .setuserv_spider import SetuservSpider


class SanicareSpider(SetuservSpider):
    name = 'sanicare-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Sanicare process start")
        assert self.source == 'sanicare'

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
            product_name = response.css('h1[itemprop="name"]::text').extract_first().strip()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        page = 0
        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page,
                                   'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_id = media_entity['id']
        product_url = media_entity['url']
        page = response.meta['page']
        res = json.loads(response.text)
        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    body = item['review_text']
                    if body:
                        _review_date = item['review_date']
                        _id = _review_date + hashlib.sha512(body.encode('utf-8')).hexdigest()
                        review_date = dateparser.parse(str(_review_date)).replace(hour=0, minute=0, second=0)
                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item['review_rating'],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                            except:
                                self.logger.warning(f"Body is not their for review {_id}")

            if review_date >= self.start_date:
                page += 10
                yield scrapy.Request(url=self.get_review_url(product_id, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page': page,
                                           'media_entity': media_entity,
                                           'extra_info': extra_info})

    @staticmethod
    def get_review_url(product_id, page):
        url = f'https://widgets.ekomi.com/get-more-reviews?widgetToken=sf259165cac6952c3168&crossDomain=true' \
              f'&dataType=jsonp&lastShown={page}&required=10&productId={product_id}'
        return url
