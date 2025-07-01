import json
from bs4 import BeautifulSoup
from scrapy.conf import settings
# settings.overrides['DOWNLOADER_MIDDLEWARES'] = {}
import scrapy
from .setuserv_spider import SetuservSpider


class JdIdProductsSpider(SetuservSpider):
    name = 'jd-id-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("JD Indonesia Products Scraping starts")
        assert self.source == 'jd_id_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating products for category {product_url} ")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        product_name = response.css('meta[name="twitter:title"]::attr(content)').extract_first()
        product_description = response.css('meta[key="description"]::attr(content)').extract_first()

        media = {
            "category_url": "https://www.jd.id/category/jual-oli-pelumas-motor-875061815.html?page=2",
            "product_url": product_url,
            "media_entity_id": product_id,
            "product_name": product_name,
            "brand_name": product_name.split(' ')[0],
            "product_description": product_description,
        }

        yield self.yield_product_details \
            (category_url=media['category_url'],
             product_url=media['product_url'],
             product_id=media['media_entity_id'],
             product_name=media['product_name'],
             brand_name=media['brand_name'],
             product_price='',
             no_of_unites_sold="",
             avg_rating='',
             total_reviews='',
             product_description=media['product_description'],
             volume_or_weight="",
             additional_fields="",
             seller_name="",
             seller_url="",
             seller_avg_rating="",
             seller_no_of_ratings="",
             seller_followers="",
             seller_no_of_unites_sold="")

    @staticmethod
    def get_products_api(product_id, warehouse_id):
        url = f"https://color.jd.id/jdid_pc_website/sea_item_ware_description/1.0?skuId={warehouse_id}" \
              f"&wareId={product_id}&venderType=-1"
        return url

    @staticmethod
    def get_headers(category_url):
        headers = {
            'authority': 'color.jd.id',
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'path': category_url.split('https://color.jd.id')[1],
            'lang': 'id',
            'origin': 'https://www.jd.id',
            'referer': 'https://www.jd.id',
            'x-api-lang': 'id',
            'x-api-platform': 'PC',
            'x-api-timestamp': '1624527974413',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.106 Safari/537.36'
        }

        return headers
