import scrapy
import json
import math

from .setuserv_spider import SetuservSpider


class ProductListWalgreens(SetuservSpider):
    name = 'walgreens-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("walgreens_product_list scraping process start")
        assert self.source == 'walgreens_product_list'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            query_url = self.get_product_url()
            print("Query URL", self.source, query_url)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 method='POST',
                                 body=json.dumps(self.get_payload(product_id, page)),
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        page = response.meta["page"]

        res = json.loads(response.text)
        total_page_count = int(res['summary']['totalNumPages'])

        if res['products']:
            for item in res['products']:
                if item:
                    product_id = item['productInfo']['prodId']
                    product_url = 'https://www.walgreens.com/' + item['productInfo']['productURL']
                    product_name = item['productInfo']['productName']
                    try:
                        review_count = item['productInfo']['reviewCount']
                    except:
                        review_count = 0

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=int(review_count))

            if page < total_page_count:
                page += 1
                query_url = self.get_product_url()
                print("Query URL", self.source, query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     errback=self.err,
                                     method='POST',
                                     body=json.dumps(self.get_payload(sub_brand, page)),
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page})
    @staticmethod
    def get_product_url():
        url = 'https://www.walgreens.com/retailsearch/products/search'
        return url

    @staticmethod
    def get_payload(sub_brand, page):
        payload = {"p": str(page), "s": "72", "view": "allView", "geoTargetEnabled": "false", "deviceType": "desktop",
                   "q": sub_brand, "requestType": "search", "user_token": "bf34aaf5qz6ur1e3u94zqlq6",
                   "sort": "relevance", "searchTerm": sub_brand}
        return payload

