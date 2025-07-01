import json
import dateparser
import scrapy
from .setuserv_spider import SetuservSpider


class PeapodSpider(SetuservSpider):
    name = 'peapod-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Peapod process start")
        assert self.source == 'peapod'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            offset = 0
            yield scrapy.Request(url=self.get_review_url(product_id, offset),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'offset': offset, 'media_entity': media_entity,
                                       'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        res = json.loads(response.text)
        offset = res['Offset']
        total_count = res['TotalResults']
        review_date = self.start_date

        if res['Includes']:
            _product_id = product_id + '-PPDALL'
            try:
                product_name = res['Includes']['Products'][_product_id]['Name']
            except:
                product_name = ''
            try:
                brand = res['Includes']['Products'][_product_id]['Brand']['Name']
            except:
                brand = ''

            extra_info = {"product_name": product_name, "brand_name": brand}

        if res['Results']:
            for item in res['Results']:
                if item:
                    _id = item["Id"]
                    review_date = dateparser.parse(item['SubmissionTime']).replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["ReviewText"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item['Title'],
                                         body=body,
                                         rating=item["Rating"],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date and offset <= total_count:
                offset = response.meta["offset"] + 10
                yield scrapy.Request(url=self.get_review_url(product_id, offset),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'offset': offset, 'media_entity': media_entity,
                                           'dont_proxy': True})
        else:
            if '"Results":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))

    @staticmethod
    def get_review_url(product_id, offset):
        url = "https://api.bazaarvoice.com/data/reviews.json?" \
              "apiVersion=5.4&filter=ProductId:{}-PPDALL&include=Products&limit=10&" \
              "method=reviews.json&offset={}&passKey=74f52k0udzrawbn3mlh3r8z0m" \
              "&sort=SubmissionTime:desc&stats=Reviews".format(product_id, offset)
        return url
