import json
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider
from datetime import datetime


class JdIdCategorySpider(SetuservSpider):
    name = 'jd-category-id-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("JD Indonesia category Scraping starts")
        assert self.source == 'jd_id_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        for category_url, category_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': category_url, 'id': category_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            category_api = self.get_category_api(page)
            yield scrapy.Request(url=category_api,
                                 callback=self.parse_category,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating products for category {category_url} ")

    def parse_category(self, response):
        media_entity = response.meta["media_entity"]
        category_id = media_entity['id']
        category_url = media_entity['url']
        page = response.meta["page"]
        response = response.text.split("window.crumb")[0].split("window.pageModel =")[1].split(';')[0].strip()
        res = json.loads(response)

        if res['data']['paragraphs']:
            for item in res['data']['paragraphs']:
                if item:
                    product_name = item['Content']['title'].strip()
                    brand_name = product_name.split(' ')[0]
                    warehouse_id = item['skuid']
                    product_id = item['spuid']
                    extra_info = item['commentscore'] + "|" + item['commentcount'] + "|" + item['Content'][
                        'price'] + "|" + product_name + "|" + brand_name
                    media = {

                        "client_id": str(self.client_id),
                        "media_source": str(self.source),
                        "category_url": category_url,
                        "product_url": f"https://www.jd.id/product/_{product_id}/{warehouse_id}.html",
                        "media_entity_id": product_id,
                        "type": "product_details",
                        "extra_info": extra_info,
                        "propagation": self.propagation,
                        "created_date": datetime.utcnow()
                    }
                    yield self.yield_category_details(category_url=media['category_url'],
                                                      product_url=media['product_url'],
                                                      product_id=media['media_entity_id'],
                                                      extra_info=media['extra_info'])

            page += 1
            category_api = self.get_category_api(page)
            yield scrapy.Request(url=category_api,
                                 callback=self.parse_category,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    @staticmethod
    def get_category_api(page):
        url = f"https://www.jd.id/category/jual-oli-pelumas-mobil-875061674.html?page={page}"
        return url

    @staticmethod
    def get_headers(category_url):
        headers = {
            'authority': 'color.jd.id',
            'method': 'GET',
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'path': category_url.split('https://color.jd.id')[1],
            'path': '/jdid_pc_website/search_pc/1.0?category=875061815&pvid=b434dbb746c849808df8aee82b988b1a&page=2&pagesize=60',
            'lang': 'id',
            'origin': 'https://www.jd.id',
            'referer': 'https://www.jd.id',
            'x-api-lang': 'id',
            'x-api-platform': 'PC',
            'x-api-timestamp': '1653806362858',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.106 Safari/537.36'
        }

        return headers
