import scrapy
import json
import math

from .setuserv_spider import SetuservSpider


class ProductListSamsclub(SetuservSpider):
    name = 'samsclub-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("samsclub_product_list scraping process start")
        assert self.source == 'samsclub_product_list'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            query_url = self.get_product_url(product_id, page)
            print("Query URL", self.source, query_url)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'query_url': query_url})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        page = response.meta["page"]
        query_url = response.meta["query_url"]

        if '"status":"OK"' not in response.text:
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'query_url': query_url})
            return

        res = json.loads(response.text)
        total_page_count = res['payload']['totalRecords']

        if res['payload']['records']:
            for item in res['payload']['records']:
                if item:
                    product_id = item['productId']
                    product_url = 'https://www.samsclub.com/p/' + product_id
                    product_name = item['descriptors']['name']
                    try:
                        review_count = item['reviewsAndRatings']['numReviews']
                    except:
                        review_count = 0

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=int(review_count))

            if page < total_page_count:
                page += 48
                query_url = self.get_product_url(sub_brand, page)
                print("Query URL", self.source, query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'query_url': query_url})
    @staticmethod
    def get_product_url(sub_brand, page):
        url = f"https://www.samsclub.com/api/node/vivaldi/browse/v2/products/search?sourceType=1&limit=48" \
              f"&clubId=&offset={page}&searchTerm={sub_brand}&br=true&sponsored=1&secondaryResults=2&sba=" \
              f"true&includeOptical=true&wmsba=true"
        return url
