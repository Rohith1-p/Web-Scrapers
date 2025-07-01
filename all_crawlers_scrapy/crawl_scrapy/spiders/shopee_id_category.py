import json
import datetime
from datetime import datetime
import scrapy
from scrapy.conf import settings
from urllib.parse import urlsplit
from .setuserv_spider import SetuservSpider


class ShopeeIdCategorySpider(SetuservSpider):
    name = 'shopee-id-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee id category Scraping starts now")
        assert self.source == 'shopee_category' or 'shopee_vn_category'

    def start_requests(self):
        self.logger.info("Starting requests")

        for product_url, product_id in zip(self.start_urls, self.product_ids):
            if "keyword" in product_url:
                product_id = (product_url.split("keyword=")[1]).split("&")[0]
            else:
            	product_id = ((product_url.split('.'))[-1]).split('?')[0]
            media_entity = {'url': product_url, 'id': product_id}
            country_code = urlsplit(product_url).netloc[-2:]
            media_entity["country_code"] = country_code
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            yield scrapy.Request(url=self.get_category_url(product_url,product_id, page,country_code),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(self.get_category_url(product_url,product_id, page,country_code)),
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating products for {product_url} category ")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        category_url = media_entity["url"]
        category_id = media_entity["id"]
        country_code = media_entity["country_code"]
        res = json.loads(response.text)

        try:
            if res['items']:
                for item in res['items']:
                    item_id = item['item_basic']["itemid"]
                    shop_id = item['item_basic']["shopid"]
                    product_name: item['item_basic']["name"]
                    brand_name: item['item_basic']["brand"]
                    category_url = category_url
                    product_id =  str(shop_id)+"_"+str(item_id)
                    product_url = self.get_product_url(shop_id, item_id, country_code)

                    yield self.yield_category_details(category_url=category_url,
                                                          product_url=product_url,
                                                          product_id=product_id,
                                                          page_no = (page % 60) + 1,
                                                          extra_info='')

                page += 60
                yield scrapy.Request(url=self.get_category_url(category_url,category_id, page,country_code),
                                     callback=self.parse_products,
                                     errback=self.err, dont_filter=True,
                                     headers=self.get_headers(self.get_category_url(category_url,category_id, page,country_code)),
                                     meta={'media_entity': media_entity, 'page': page})
        except:
            if res["data"]["sections"][0]["data"]['item']:
                for item in res["data"]["sections"][0]["data"]['item']:
                    item_id = item["itemid"]
                    shop_id = item["shopid"]
                    product_name: item["name"]
                    brand_name: item["brand"]
                    category_url = category_url
                    product_id =  str(shop_id)+"_"+str(item_id)
                    product_url = self.get_product_url(shop_id, item_id, country_code)

                    yield self.yield_category_details(category_url=category_url,
                                                              product_url=product_url,
                                                              product_id=product_id,
                                                          page_no = (page % 60) + 1,
                                                          extra_info='')

                page += 60
                yield scrapy.Request(url=self.get_category_url(category_url,category_id, page,country_code),
                                     callback=self.parse_products,
                                     errback=self.err, dont_filter=True,
                                     headers=self.get_headers(self.get_category_url(category_url,category_id, page,country_code)),
                                     meta={'media_entity': media_entity, 'page': page})

    @staticmethod
    def get_product_url(shop_id, item_id, country_code):
        if country_code:
            if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th', 'tw'}:
                return {
                    'my': f"https://shopee.com.my/--i.{shop_id}.{item_id}",
                    'vn': f"https://shopee.vn/--i.{shop_id}.{item_id}",
                    'ph': f"https://shopee.ph/--i.{shop_id}.{item_id}",
                    'id': f"https://shopee.co.id/--i.{shop_id}.{item_id}",
                    'sg': f"https://shopee.sg/--i.{shop_id}.{item_id}",
                    'th': f"https://shopee.co.th/--i.{shop_id}.{item_id}",
                    'tw': f"https://shopee.tw/--i.{shop_id}.{item_id}"
                }[country_code]
            else:
                self.logger.error(f"Country code {country_code} for product - "
                                  f"{shop_id}.{item_id} is wrong, "
                                  f"Please check again")
        return url

    @staticmethod
    def get_category_url(prod_url, id, page, country_code):
        if country_code:
            if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th', 'tw'}:
                if "keyword" in prod_url:
                    return {
                        'my': f"https://shopee.com.my/api/v4/search/search_items?by=relevency&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2",
                        'vn': f"https://shopee.vn/api/v4/search/search_items?by=relevency&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2",
                        'ph': f"https://shopee.ph/api/v4/search/search_items?by=relevancy&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2",
                        'id': f"https://shopee.co.id/api/v4/search/search_items?by=relevancy&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2",
                        'sg': f"https://shopee.sg/api/v4/search/search_items?by=relevancy&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2",
                        'th': f"https://shopee.co.th/api/v4/search/search_items?by=relevancy&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2",
                        'tw': f"https://shopee.tw/api/v4/search/search_items?by=relevancy&keyword={id}&limit=60&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2"
                    }[country_code]
                else:
                    return {
                        'my': f"https://shopee.com.my/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id}&limit=60&offset={page}",
                        'vn': f"https://shopee.vn/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id}&limit=60&offset={page}",
                        'ph': f"https://shopee.ph/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id}&limit=60&offset={page}",
                        'id': f"https://shopee.co.id/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id}&limit=60&offset={page}",
                        'sg': f"https://shopee.sg/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id}&limit=60&offset={page}",
                        'th': f"https://shopee.co.th/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id}&limit=60&offset={page}",
                        'tw': f"https://shopee.tw/api/v4/recommend/recommend?bundle=category_landing_page&cat_level=1&catid={id}&limit=60&offset={page}",
                        }[country_code]

        else:
            self.logger.error(f"Country code {country_code} for product - "
                              f"{shop_id}.{item_id} is wrong, "
                              f"Please check again")

        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'authority': 'shopee.co.id',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            #'path': product_url.split('https://shopee.co.id')[1],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
            'x-api-source': 'pc',
            'x-requested-with': 'XMLHttpRequest',
            'x-shopee-language': 'id'
        }

        return headers
