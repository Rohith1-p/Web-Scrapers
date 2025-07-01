import re
import json
import hashlib
import scrapy
import datetime
from .setuserv_spider import SetuservSpider


class EuraponSpider(SetuservSpider):
    name = 'eurapon-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Eurapon process start")
        assert self.source == 'eurapon'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        try:
            product_name = response.css('h1.product--title::text').extract_first().strip()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ""}
        res = response.css('section.usercomment__item')

        if res:
            for item in res:
                if item:
                    body = item.css('div.usercomment__itemcomment p::text').extract_first()
                    if body:
                        _review_date = item.css('time[itemprop="datePublished"]::attr(datetime)').extract_first().split(' ')[0]
                        _id = _review_date + hashlib.sha512(body.encode('utf-8')).hexdigest()
                        review_date = datetime.datetime.strptime(_review_date, '%Y-%m-%d')

                        if self.start_date <= review_date <= self.end_date:
                            _rating = item.css('a[href="#productComment"]').extract_first()
                            _rating = re.findall(r'class="eurapon--star-filled"', str(_rating))
                            rating = _rating.count('class="eurapon--star-filled"')
                            try:
                                title = item.css('h3::text').extract()[-1].strip()
                            except:
                                title = ''
                            try:
                                if self.type == 'media':
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body.strip(),
                                         rating=rating,
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                            except:
                                self.logger.warning(f"Body is not their for review {_id}")
