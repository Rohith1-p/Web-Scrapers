import scrapy
import json
import time
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class ProductListSuning(SetuservSpider):
    name = 'suning-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("suning_product_list scraping process start")
        assert self.source == 'suning_product_list'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            query_url = self.get_product_url(product_id)
            print("Query URL", self.source, query_url)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'query_url': query_url})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        page = response.meta["page"]
        query_url = response.meta["query_url"]

        _res = BeautifulSoup(response.text, 'html.parser')
        res = _res.findAll("li", {"doctype": "1"})

        if res:
            for item in res:
                if item:
                    try:
                        product_url = "https:" + str(item.find("div", {"class": "title-selling-point"}).find('a').get('href'))
                        product_id = product_url.split('/')[-1]
                        product_name = item.find("div", {"class": "title-selling-point"}).find('a').text.strip()
                        try:
                            review_count = item.find("div", {"class": "info-evaluate"}).find('i').text
                        except:
                            review_count = 0

                        self.yield_products_list \
                            (sub_brand=sub_brand,
                             product_url=product_url,
                             product_id=product_id,
                             product_name=product_name,
                             review_count=review_count)
                    except:
                        print(f'Error in {self.source}, {sub_brand}')

            if 'id="nextPage"' in response.text:
                page += 1
                if page == 1:
                    query_url = 'https://search.suning.com/' + _res.find("a", {"pagenum": str(page+1)}).get('href')
                else:
                    print("Error comes in Query", query_url)
                    query_url = query_url.split('&cp=')[0] + f'&cp={page}'
                print('next_page_query_url', query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'query_url': query_url})

    @staticmethod
    def get_product_url(sub_brand):
        url = f"https://search.suning.com/{sub_brand}/"
        return url

    @staticmethod
    def get_headers():
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,'
                      'image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
        }
        return headers

