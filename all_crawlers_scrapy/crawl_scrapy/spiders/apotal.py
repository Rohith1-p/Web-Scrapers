import dateparser
import json
import scrapy
import requests
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class ApotalSpider(SetuservSpider):
    name = 'apotal-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Apotal process start")
        assert self.source == 'apotal'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        lifetime = response.meta["lifetime"]

        if lifetime:
            self.get_lifetime_ratings(product_id, response)

        if self.type == 'media':
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
            try:
                review_api_id =response.css('input[name="prodId"]::attr(value)').extract_first().split(',')[0].split('[')[1]
            except:
                review_api_id =response.css('input[name="prodId"]::attr(value)').extract_first()
            review_url = self.get_review_url()
            payload = self.get_payload(review_api_id, page)
            response = requests.post(review_url, data=payload)
            self.parse_reviews(media_entity, page, extra_info, review_api_id, response)

        else:
            self.logger.info(f"Type is not media for {product_id}")

    def parse_reviews(self, media_entity, page, extra_info, review_api_id, response):
        product_url = media_entity['url']
        product_id = media_entity['id']
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
                review_url = self.get_review_url()
                payload = self.get_payload(review_api_id, page)
                response = requests.post(review_url, data=payload)
                self.parse_reviews(media_entity, page, extra_info, review_api_id, response)
                self.logger.info(f"Scraping for {product_url} and page {page}")

        else:
            if '"results":[]' in response.text:
                self.logger.info(f"No reviews / Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_payload(product_id, page):
        payload = {"productId": str(product_id),
                   "viewIndex": str(page),
                   "viewSize": str(5),
                   "fromShop": "Y"}
        return payload

    @staticmethod
    def get_review_url():
        url = 'https://shop.apotal.de/getApprovedReviews'
        return url

    def get_lifetime_ratings(self, product_id, response):
        try:
            review_count =  response.css('span[itemprop="reviewCount"]::text').extract_first()
        except:
            review_count = 0

        self.yield_lifetime_ratings \
            (product_id=product_id,
             review_count=review_count,
             average_ratings=0,
             ratings={})
