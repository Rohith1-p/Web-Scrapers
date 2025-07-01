import scrapy
import re
import math
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class ProductListProductreviews(SetuservSpider):
    name = 'productreviews-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("productreviews_product_list scraping process start")
        assert self.source == 'productreviews_product_list'

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

        _res = BeautifulSoup(response.text, 'html.parser')
        try:
            total_results = _res.find("span", {"class": "mb-0__MF flex-grow-1_iy8 d-block__Gv h2"}).text
            total_page_count = re.findall(r'\d+', total_results)[0]
            total_page_count = math.ceil(int(total_page_count)/ 20)
        except:
            total_page_count = 0
        print("total_results", total_page_count)
        res = _res.findAll("div", {"class": "mb-0__MF relative___C overflow-hidden_Yg5 "
                                            "card_UzP card-full_soR card-full-md_tUp"})

        if res:
            for item in res:
                if item:
                    product_url = "https://www.productreview.com.au/" + \
                                  item.find("h3", {"class": "mb-0__MF d-inline_r8U"}).find("a").get("href")
                    product_id = product_url
                    product_name = item.find("h3", {"class": "mb-0__MF d-inline_r8U"}).find("a").text.strip()
                    review_count = item.find("span", {"class": "text-body_Ts5"}).text.split(' ')[0]
                    if review_count == 'No':
                        review_count = 0

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=review_count)

            if 'class="text-nowrap_KtG">Next' in response.text and page < total_page_count:
                page += 1
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
        url = f"https://www.productreview.com.au/search?q={sub_brand}&page={page}#search-results"
        return url
