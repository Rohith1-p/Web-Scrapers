import json
import datetime
from datetime import datetime
import scrapy
from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs


class LazadaScrapingSpider(SetuservSpider):
    name = 'lazada-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Lazada Category Scraping starts")
        assert self.source == 'lazada-category-scraper'

    def start_requests(self):
        self.logger.info("Starting requests")
        monitor_log = 'Successfully Called Lazada Category Products Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=self.products_api(product_url, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating products for {product_url} category ")

    @staticmethod
    def get_headers(product_url, url):
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'path': url.split('https://www.lazada.com.ph/')[1],
            'referer': product_url,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/72.0.3626.121 Safari/537.36'
        }
        return headers

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity["url"]
        page = response.meta["page"]

        # print(response.text)
        if '"https://www.lazada.com.ph:443//' in response.text:
            yield scrapy.Request(url=self.products_api(product_url, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            return

        if 'mods' not in response.text:
            yield scrapy.Request(url=self.products_api(product_url, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            return

        res = json.loads(response.text)
        print("Page is printing", page)

        if res['mods']['listItems']:
            for item in res['mods']['listItems']:
                if item:
                    self.yield_product_variation_details(
                        category_url=product_url,
                        category_breadcrumb="",
                        product_url='https:' + item["productUrl"],
                        product_id=item['nid'],
                        product_name="",
                        brand_name="",
                        total_reviews="",
                        avg_rating="",
                        no_of_unites_sold="",
                        product_breadcrumb="",
                        product_price="",
                        discount_price="",
                        discount_percentage="",
                        sale_price="",
                        wholesale="",
                        product_description="",
                        product_info_selected="",
                        product_info_options="",
                        product_specifications="",
                        shop_vouchers="",
                        promotions="",
                        sku="",
                        offers="",
                        stock="",
                        shop_location="",
                        additional_fields="")

            page += 1
            yield scrapy.Request(url=self.products_api(product_url, page),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    def products_api(self, product_url, page):

        '''Popularity'''
        # url = f'{product_url}?ajax=true&page={page}&sort=popularity'

        '''Genearl'''
        url = f'{product_url}?ajax=true&page={page}'

        # '''ASEC'''
        # url = f'{product_url}?ajax=true&page={page}&sort=priceasc'

        '''DSEC'''
        # url = f'{product_url}?ajax=true&page={page}&sort=pricedesc'

        return url
