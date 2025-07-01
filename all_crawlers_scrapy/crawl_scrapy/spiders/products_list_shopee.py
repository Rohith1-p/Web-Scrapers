import scrapy
import json
import time

from .setuserv_spider import SetuservSpider


class ProductListShopee(SetuservSpider):
    name = 'shopee-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("shopee products list scraping process start")
        assert self.source == 'shopee_id' or 'shopee_my' or 'shopee_vn' or 'shopee_ph' or 'shopee_th' or 'shopee_sg'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            country_code = self.get_country_code(self.source.split('_')[1])
            query_url = self.get_product_url(product_id, page, country_code)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'country_code': country_code})
            self.logger.info(f"Generating products list for {query_url} and {page}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        country_code = response.meta["country_code"]
        page = response.meta["page"]
        print(response.text)

        res = json.loads(response.text)
        total_page_count = res['total_count']

        if res['items']:
            for item in res['items']:
                if item:
                    product_id = str(item['shopid']) + '.' + str(item['itemid'])
                    product_url = f'{country_code}/--i.' + str(product_id)
                    product_name = item['item_basic']['name']
                    review_count = item['item_basic']['cmt_count']

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=review_count)

            if page < total_page_count:
                page += 60
                time.sleep(0.5)
                country_code = self.get_country_code(self.source.split('_')[1])
                query_url = self.get_product_url(sub_brand, page, country_code)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'country_code': country_code})
                self.logger.info(f"Scraping products list for {query_url} and {page}")

    @staticmethod
    def get_product_url(sub_brand, page, country_code):
        url = f"{country_code}/api/v4/search/search_items?by=relevancy&keyword={sub_brand}&limit=60" \
              f"&newest={page}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2"
        return url

    @staticmethod
    def get_country_code(source):
        if source in {'my', 'vn', 'ph', 'id', 'sg', 'th'}:
            return {
                'my': "https://shopee.com.my",
                'vn': "https://shopee.vn",
                'ph': "https://shopee.ph",
                'id': "https://shopee.co.id",
                'sg': "https://shopee.sg",
                'th': "https://shopee.co.th",
            }[source]
