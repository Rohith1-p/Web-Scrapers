import json
import datetime
from datetime import datetime
import scrapy
from scrapy.conf import settings

from .setuserv_spider import SetuservSpider


class ShopeeVnCategorySpider(SetuservSpider):
    name = 'shopee-vn-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee Vn category Scraping starts")
        assert self.source == 'shopee_vn_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            if "keyword" in product_url:
                product_id = (product_url.split("keyword=")[1]).split("&")[0]
            else:
            	product_id = ((product_url.split('.'))[-1]).split('?')[0]
            page = 0
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            print(self.get_category_url(product_url,product_id, page))
            yield scrapy.Request(url=self.get_category_url(product_url,product_id, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(self.get_category_url(product_url,product_id, page)),
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating products for {product_url} category ")

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
                product_url = f"https://shopee.vn/--i.{shop_id}.{item_id}"

                if item:
                    media = {
                        "client_id": str(self.client_id),
                        "media_source": str(self.source),
                        "category_url": category_url,
                        "product_url": product_url,
                        "media_entity_id": product_id,
                        "product_name": item['item_basic']["name"],
                        "brand_name": item['item_basic']["brand"],
                        "type": "product_details",
                        "propagation": self.propagation,
                        "created_date": datetime.utcnow()
                    }
                    yield self.yield_category_details(category_url=media['category_url'],
                                                      product_url=media['product_url'],
                                                      product_id=media['media_entity_id'],
                                                      extra_info='')

            page += 60

            yield scrapy.Request(url=self.get_category_url(category_url,category_id, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(self.get_category_url(category_url,category_id, page)),
                                 meta={'media_entity': media_entity, 'page': page})

    @staticmethod
    def get_category_url(prod_url, id, page):
        if "keyword" in prod_url:
            url = f"https://shopee.vn/api/v4/search/search_items?by=ctime&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2"
        else:
            url = f"https://shopee.vn/api/v4/search/search_items?by=relevancy&limit=60&match_id={id}&newest={page}&order=desc&page_type=collection&scenario=PAGE_COLLECTION&version=2"
        
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'authority': 'shopee.vn',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'path': product_url.split('https://shopee.vn')[1],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
            'x-api-source': 'pc',
            'x-requested-with': 'XMLHttpRequest',
            'x-shopee-language': 'vi'
        }

        return headers
