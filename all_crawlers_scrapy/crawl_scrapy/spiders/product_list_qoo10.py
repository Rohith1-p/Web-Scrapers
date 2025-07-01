import scrapy
import json
import math
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class ProductListQoo10(SetuservSpider):
    name = 'qoo10-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("qoo10_product_list scraping process start")
        assert self.source == 'qoo10_product_list'

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
                                 headers=self.get_headers(query_url),
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

        if 'There is too much traffic and the server is delaying processing' in response.text:
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 headers=self.get_headers(query_url),
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'query_url': query_url})
            return
        print(response.text)
        _res = BeautifulSoup(response.text, 'html.parser')
        try:
            total_results = _res.find("div", {"id": "div_item_title"}).find('strong').text
            total_page_count = math.ceil(int(total_results)/50)
        except:
            total_page_count = 0
            print(f'No Results Found for {sub_brand}')
        print(total_page_count)
        res = _res.findAll('tr')

        if res:
            for item in res:
                if item:
                    product_id = item['goodscode']
                    product_url = item.find("a", {"data-type": "goods_url"}).get('href')
                    product_name = item.find("a", {"data-type": "goods_url"}).get('title')
                    try:
                        review_count = item.find("a", {"class": "lnk_rcm"}).find('strong').text
                    except:
                        review_count = 0

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=review_count)

            if page < total_page_count:
                page += 1
                query_url = self.get_product_url(sub_brand, page)
                print("Query URL", self.source, query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     headers=self.get_headers(query_url),
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'query_url': query_url})
    @staticmethod
    def get_product_url(sub_brand, page):
        url = f"https://www.qoo10.sg/gmkt.inc/Search/DefaultAjaxAppend.aspx?search_keyword={sub_brand}" \
              f"&search_type=SearchItems&f=&st=IN&s=r&v=lt&p={page}"
        return url

    @staticmethod
    def get_headers(query_url):
        headers = {
            'authority': 'www.qoo10.sg',
            'method': 'POST',
            'path': query_url.split('https://www.qoo10.sg')[1],
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'origin': 'https://www.qoo10.sg',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
        }

        return headers