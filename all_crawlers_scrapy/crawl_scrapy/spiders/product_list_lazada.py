import scrapy
import json
import time
import math

from .setuserv_spider import SetuservSpider


class ProductListLazada(SetuservSpider):
    name = 'lazada-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("lazada products list scraping process start")
        assert self.source == 'lazada_id' or 'lazada_my' or 'lazada_vn' or 'lazada_ph' or 'lazada_th' or 'lazada_sg'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            country_code = self.get_country_code(self.source.split('_')[1])
            query_url = ProductListLazada.get_product_url(product_id, page, country_code)
            print("Query URL", self.source, country_code, query_url)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'country_code': country_code,
                                       'query_url': query_url})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        country_code = response.meta["country_code"]
        page = response.meta["page"]
        query_url = response.meta["query_url"]

        if '443//catalog/_____tmd_____/punish?' in response.text \
                or 'RGV587_ERROR' in response.text:
            time.sleep(0.5)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'country_code': country_code,
                                       'query_url': query_url})
            return

        if '"HOST": "my.lazada' in response.text or '"action": "captcha"' in response.text \
                or 'CONTENT="NO-CACHE"' in response.text:
            time.sleep(0.5)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'country_code': country_code,
                                       'query_url': query_url})
            return

        res = json.loads(response.text)
        total_page_count = math.ceil(int(res['mainInfo']['totalResults'])/40)
        print('total_page_count', total_page_count)

        if res['mods']['listItems']:
            for item in res['mods']['listItems']:
                if item:
                    product_id = item['itemId']
                    product_url = "https:" + item['productUrl']
                    product_name = item['name']
                    review_count = item['review']
                    if len(review_count) == 0 :
                        review_count = 0

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=int(review_count))

            if page <= total_page_count and page < 120:
                page += 1
                time.sleep(0.5)
                query_url = self.get_product_url(sub_brand, page, country_code)
                print("Query URL", self.source, country_code, query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'country_code': country_code,
                                           'query_url': query_url})

    @staticmethod
    def get_product_url(sub_brand, page, country_code):
        url = f"{country_code}/catalog/?_keyori=ss&ajax=true&from=input&page={page}&q={sub_brand}"
        return url

    @staticmethod
    def get_country_code(source):
        if source in {'my', 'vn', 'ph', 'id', 'sg', 'th'}:
            return {
                'my': "https://www.lazada.com.my",
                'vn': "https://www.lazada.vn",
                'ph': "https://www.lazada.com.ph",
                'id': "https://www.lazada.co.id",
                'sg': "https://www.lazada.sg",
                'th': "https://www.lazada.co.th",
            }[source]

