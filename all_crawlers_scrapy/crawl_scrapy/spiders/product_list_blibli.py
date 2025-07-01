import scrapy
import json
import time

from .setuserv_spider import SetuservSpider


class ProductListBlibli(SetuservSpider):
    name = 'blibli-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("blibli_product_list scraping process start")
        assert self.source == 'blibli_product_list'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            query_url = self.get_product_url(product_id, page)
            print("Query URL", self.source, query_url)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 headers=self.get_headers(),
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
        total_page_count = res['data']['paging']['total_page']

        if res['data']['products']:
            for item in res['data']['products']:
                if item:
                    product_id = item['id']
                    product_url = 'https://www.blibli.com/' + item['url']
                    if '?' in response.text:
                        product_url = product_url.split('?')[0]
                    product_name = item['name']
                    review_count = item['review']['count']

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=review_count)

            if page < total_page_count:
                page += 1
                time.sleep(0.5)
                query_url = self.get_product_url(sub_brand, page)
                print("Query URL", self.source, query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     headers=self.get_headers(),
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page})

    @staticmethod
    def get_product_url(sub_brand, page):
        url = f"https://www.blibli.com/backend/search/products?sort=&page={page}&start={(int(page)-1)*40}" \
              f"&searchTerm={sub_brand}&intent=false&merchantSearch=true&multiCategory=true&customUrl=&" \
              f"&channelId=mobile-web&showFacet=false"
        return url

    @staticmethod
    def get_headers():
        headers = {
            'authority': 'www.blibli.com',
            'method': 'GET',
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cache-control': 'no-cache',
            'channelid': 'mobile-web',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/'
                          '537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
        }
        return headers