import json
from bs4 import BeautifulSoup
import scrapy

from .setuserv_spider import SetuservSpider


class HktvmallProductsSpider(SetuservSpider):
    name = 'hktvmall-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Hktvmall Products Scraping starts")
        assert self.source == 'hktvmall_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            yield scrapy.Request(url=self.get_category_url(product_id, page),
                                 callback=self.parse_category,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating products for {product_url} category ")

    def parse_category(self, response):
        media_entity = response.meta["media_entity"]
        category_id = media_entity['id']
        category_url = media_entity['url']
        page = response.meta["page"]
        res = json.loads(response.text)
        total_pages = int(res['pagination']['numberOfPages'])

        if res['products']:
            for item in res['products']:
                if item:
                    media = {'category_url': category_url,
                             'product_url': "https://www.hktvmall.com/hktv/zh/" + item["url"],
                             "product_id": item["baseProduct"],
                             "product_name": item["name"],
                             "brand_name": item["brandName"]
                             }
                    yield self.yield_category_details(category_url=media['category_url'],
                                                      product_url=media['product_url'],
                                                      product_id=media['product_id'],
                                                      extra_info='')
            if page <= total_pages:
                page += 1
                yield scrapy.Request(url=self.get_category_url(category_id, page),
                                     callback=self.parse_category,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity, 'page': page})
                self.logger.info(f"{page} is going")

    @staticmethod
    def get_category_url(product_id, page):
        url = f"https://www.hktvmall.com/hktv/zh/ajax/search_products?query=%22%22%3Arelevance%3Acategory%3AAA13600500000%3Azone%3Apersonalcarenhealth%3Astreet%3Amain%3A&currentPage={page}&pageSize=60"
        return url
