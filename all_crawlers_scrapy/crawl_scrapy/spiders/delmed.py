import re
import hashlib
import scrapy
import datetime
from .setuserv_spider import SetuservSpider


class DelmedSpider(SetuservSpider):
    name = 'delmed-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Delmed process start")
        assert self.source == 'delmed'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        try:
            _product_name = response.css('h1[itemprop="name"]::text').extract_first()
            product_name = "".join(_product_name.split("  ")).replace('\n', '').replace('\xa0', '')
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ""}
        url = response.css('a[class="ml-auto mr-0 text-primary"]::attr(href)').extract_first()
        yield scrapy.Request(url=url,
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta['extra_info']
        res = response.css('ul[class="p-0"] li')

        if res:
            for item in res:
                if item:
                    _body = item.css('p.m-0::text').extract()
                    body = ''
                    for _, __body in enumerate(_body):
                        body += __body
                    if body:
                        _review_date = item.css('div::text').extract()[-2].strip().split(' ')[-1]
                        _id = _review_date + hashlib.sha512(body.encode('utf-8')).hexdigest()
                        review_date = datetime.datetime.strptime(_review_date, '%d.%m.%Y')
                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    _rating = item.css('rating-stars').extract_first()
                                    rating = re.findall(r'\d+', _rating)[0]
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
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
