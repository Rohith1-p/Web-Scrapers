import scrapy
import json
import requests
import dateparser

from .setuserv_spider import SetuservSpider


class NuleafnaturalsSpider(SetuservSpider):
    name = 'nuleafnaturals-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Nuleafnaturals process start")
        assert self.source == 'nuleafnaturals'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url='https://www.google.com/',
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        page = 1
        review_url = self.get_review_url()
        payload = self.get_payload(product_id, page)
        response = requests.post(review_url, data=payload)
        self.parse_reviews(media_entity, page, response)

    def parse_reviews(self, media_entity, page, response):
        product_url = media_entity['url']
        product_id = media_entity['id']
        res = json.loads(response.text)
        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    extra_info = {"product_name": item['product_title'],
                                  "brand_name": ""}
                    _id = item["id"]
                    review_date = dateparser.parse(item["date"])
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["content"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item["title"],
                                         body=body,
                                         rating=item['rating'],
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
                payload = self.get_payload(product_id, page)
                response = requests.post(review_url, data=payload)
                self.logger.info(f"Scraping for {product_url} and page {page}")
                self.parse_reviews(media_entity, page, response)
        else:
            if '[]' in response.text:
                self.logger.info(f"No reviews / Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_payload(product_id, page):
        payload = {
            "action": "get_yotpo_product_reviews",
            "page": page,
            "pid": product_id
        }
        return payload

    @staticmethod
    def get_review_url():
        url = 'https://nuleafnaturals.com/wp-admin/admin-ajax.php'
        return url
