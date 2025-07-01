import hashlib
import scrapy
import datetime
from .setuserv_spider import SetuservSpider


class AporotSpider(SetuservSpider):
    name = 'aporot-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Aporot process start")
        assert self.source == 'aporot'

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
            _product_name = response.css('h1[id="product-title"]::text').extract_first()
            product_name = "".join(_product_name.split("  ")).replace('\n', '')
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        yield scrapy.Request(url=self.get_review_url(product_id),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity, 'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta['extra_info']
        res = response.css('div[class=" col-xs-12 margin-vert-smaller border padding-vert-half"]')

        if res:
            for item in res:
                if item:
                    body = item.css('div[class="row"] div[class="col-xs-12"]::text').extract_first()
                    if body:
                        _review_date = item.css('div[class="date col-xs-6 text-right"]::text').extract_first().split(' ')[0]
                        _id = _review_date + hashlib.sha512(body.encode('utf-8')).hexdigest()
                        review_date = datetime.datetime.strptime(_review_date, '%d.%m.%Y')

                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item.css('div[class="rateit"]::attr(data-rateit-value)').extract_first(),
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                            except:
                                self.logger.warning(f"Body is not their for review {_id}")

    @staticmethod
    def get_review_url(product_id):
        url = f'https://www.apo-rot.de/include/content/details/rating-artikel.html;jsessionid=' \
              f'F82C6A26E10FCAA0CBB8299E963D245C.tcn-8?artnr={product_id}'
        return url
