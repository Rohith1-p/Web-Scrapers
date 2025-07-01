import scrapy
import json
import time
import math

from .setuserv_spider import SetuservSpider


class ProductList11Street(SetuservSpider):
    name = '11street-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("11street_product_list scraping process start")
        assert self.source == '11street_product_list'

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
        total_page_count = res['totalCount']
        total_page_count = math.ceil(total_page_count / 60)
        print('total_page_count, ', total_page_count)

        if res['productList']:
            for item in res['productList']:
                if item:
                    product_id = item['prdNo']
                    product_url = 'https://www.prestomall.com/productdetail/' + product_id
                    product_name = item['prdNm']
                    review_count = item['prdEvlTotQty']

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
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page})

    @staticmethod
    def get_product_url(sub_brand, page):
        url = f"https://www.prestomall.com/totalsearch/TotalSearchAction/new/getProductSearchAjax.do?" \
              f"&kwd={sub_brand}&pageSize=60&pageNum={page}"
        return url
