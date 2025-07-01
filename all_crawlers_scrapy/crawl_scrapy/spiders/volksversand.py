import re
import hashlib
import scrapy
import datetime
from .setuserv_spider import SetuservSpider


class VolksversandSpider(SetuservSpider):
    name = 'volksversand-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Volksversand process start")
        assert self.source == 'volksversand'

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
        res = response.css('div.review--entry')

        if res:
            for item in res:
                if item:
                    body = item.css('p[class="content--box review--content"]::text').extract_first().strip()
                    if body:
                        _review_date = item.css('span.content--field::text').extract()[-1]
                        _id = _review_date + hashlib.sha512(body.encode('utf-8')).hexdigest()
                        review_date = datetime.datetime.strptime(_review_date, '%d.%m.%Y')

                        if self.start_date <= review_date <= self.end_date:
                            _rating = item.css('span.product--rating').extract_first()
                            _rating = re.findall(r'class="icon--star"', str(_rating))
                            rating = _rating.count('class="icon--star"')
                            try:
                                title = item.css('h4.content--title::text').extract_first().strip()
                            except:
                                title = ''
                            try:
                                if self.type == 'media':
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=rating,
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                            except:
                                self.logger.warning(f"Body is not their for review {_id}")
