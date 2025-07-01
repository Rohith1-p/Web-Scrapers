import json
import datetime
from datetime import datetime
import scrapy

from .setuserv_spider import SetuservSpider


class ShopeePHCategorySpider(SetuservSpider):
    name = 'shopee-ph-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee PH category Scraping starts")
        assert self.source == 'shopee_ph_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        for category_url, category_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': category_url, 'id': category_id}
            page = 0
            yield scrapy.Request(url=self.get_category_url(category_id, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(self.get_category_url(category_id, page)),
                                 meta={'media_entity': media_entity,
                                       'page': page})
            self.logger.info(f"Generating reviews for {category_url} and {category_id}")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        category_url = media_entity["url"]
        category_id = media_entity["id"]
        res = json.loads(response.text)

        if res['items']:
            for item in res['items']:
                item_id = item['item_basic']["itemid"]
                shop_id = item['item_basic']["shopid"]
                category_url = category_url
                product_id = str(shop_id) + "." + str(item_id)
                product_url = f"https://shopee.ph/--i.{shop_id}.{item_id}"

                if item:
                    media = {
                        "category_url": category_url,
                        "product_url": product_url,
                        "media_entity_id": product_id,
                    }
                    self.yield_category_details(category_url=media['category_url'],
                                                      product_url=media['product_url'],
                                                      product_id=media['media_entity_id'],
                                                      extra_info='')

            page += 60
            yield scrapy.Request(url=self.get_category_url(category_id, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(self.get_category_url(category_id, page)),
                                 meta={'media_entity': media_entity, 'page': page})
        else:
            self.logger.info(f"Pages exhausted for {category_url} and {category_id}")

    @staticmethod
    def get_category_url(product_id, page):
        url = f"https://shopee.ph/api/v4/search/search_items?by=relevancy&limit=60&match_id={product_id}&newest={page}" \
              f"&order=desc&page_type=search&scenario=PAGE_CATEGORY&version=2"
        return url

    @staticmethod
    def get_headers(category_api_url):
        headers = {
            'authority': 'shopee.ph',
            'scheme': 'https',
            'accept': 'application/json',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'path': category_api_url.split('https://shopee.ph')[1],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
            'x-api-source': 'pc',
            'x-requested-with': 'XMLHttpRequest',
            'x-shopee-language': 'en'
        }

        return headers
