import dateparser
import json
import scrapy
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class MedikamenteSpider(SetuservSpider):
    name = 'medikamente-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Medikamente process start")
        assert self.source == 'medikamente per klick'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")
            print("Sourcename Modifed")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        try:
            product_name = response.css('h1[itemprop="name"]::text').extract_first().strip()
        except:
            product_name = ''
        try:
            brand_name = response.css('dd[itemprop="brand"]::text').extract_first().strip()
        except:
            brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}
        page = 0
        yield FormRequest(url=self.get_review_url(),
                          callback=self.parse_reviews,
                          errback=self.err,
                          dont_filter=True,
                          method="POST",
                          headers = self.get_headers(),
                          formdata=self.get_payload(product_id, page),
                          meta={'media_entity': media_entity, 'page': page,
                                'extra_info': extra_info})
        self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta["page"]
        extra_info = response.meta['extra_info']
        res = json.loads(response.text)
        review_date = self.start_date

        if res['results']:
            for item in res['results']:
                if item:
                    _id = item["productReviewId"]
                    review_date = dateparser.parse(item["postedDateTime"])
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["productReview"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item["title"],
                                         body=body,
                                         rating=item['productRating'],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

            if review_date >= self.start_date:
                page += 1
                yield FormRequest(url=self.get_review_url(),
                                  callback=self.parse_reviews,
                                  errback=self.err,
                                  dont_filter=True,
                                  method="POST",
                                  formdata=self.get_payload(product_id, page),
                                  meta={'media_entity': media_entity, 'page': page,
                                        'extra_info': extra_info})
    @staticmethod
    def get_headers():
        headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}

    @staticmethod
    def get_payload(product_id, page):
        payload = {"productId": str(product_id),
                   "viewIndex": str(page),
                   "viewSize": str(5),
                   "fromShop": "Y"}
        return payload

    @staticmethod
    def get_review_url():
        url = 'https://www.medikamente-per-klick.de/getApprovedReviews'
        return url
