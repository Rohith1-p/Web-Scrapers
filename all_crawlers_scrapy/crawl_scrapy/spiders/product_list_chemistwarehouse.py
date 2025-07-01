import scrapy
import json
import math
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class ProductListChemistwarehouse(SetuservSpider):
    name = 'chemistwarehouse-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("chemistwarehouse_product_list scraping process start")
        assert self.source == 'chemistwarehouse_product_list'

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
                                       'page': page})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        page = response.meta["page"]
        res = json.loads(response.text)

        total_page_count = math.ceil(int(res['universes']['universe'][0]['items-section']['results']['total-items']))
        print("total_page_count", total_page_count)

        if res['universes']['universe'][0]['items-section']['items']['item']:
            for item in res['universes']['universe'][0]['items-section']['items']['item']:
                if item:
                    product_id = item['id']
                    product_url = f'https://www.chemistwarehouse.com.au/buy/{product_id}'
                    product_name = item['attribute'][2]['value'][0]['value']
                    # print(str(item).split('bv_total_votes')[0])
                    review_count = "Couldn't Scrape review count"

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=review_count)

            if page < total_page_count:
                page += 48
                query_url = self.get_product_url(sub_brand, page)
                print("Query URL", self.source, query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page})

    @staticmethod
    def get_product_url(sub_brand, page):
        url = f"https://www.chemistwarehouse.com.au/searchapi/webapi/search/terms?" \
              f"category=catalog01_chemau&term={sub_brand}&index={page}&sort"
        return url

