import dateparser
import scrapy
from bs4 import BeautifulSoup
from datetime import datetime
from .setuserv_spider import SetuservSpider
import ast


class NykaaSpider(SetuservSpider):
    name = 'nykaa-review-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Nykaa process start")
        assert (self.source == 'nykaa_reviews')

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            page_count = 1
            yield scrapy.Request(url=self.get_review_url(product_id, page_count), callback=self.parse_reviews,
                                 meta={'page_count': page_count, 'media_entity': media_entity,
                                       'dont_proxy': True}, errback=self.err, dont_filter=True)
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        print(response.text)
        page_count = response.meta["page_count"]
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        if 'Access denied' in response.text:
            self.logger.info(f"Captcha Found for {product_id}")
            yield scrapy.Request(url=self.get_review_url(product_id, page_count), callback=self.parse_reviews,
                                 meta={'page_count': page_count, 'media_entity': media_entity,
                                       'dont_proxy': True}, errback=self.err, dont_filter=True)
            return
        result = response.text.replace("true", "True")
        result = result.replace("false","False")
        res = ast.literal_eval(result)
        print(res)
        if res["response"]["reviewData"]:
            # print(res["response"]["reviewData"])
            result_1 = res["response"]["reviewData"]
            print(result_1)
            for item in result_1:
                    review_date_ = item["createdOn"].split()[0]
                    print("*********************************", review_date_)
                    review_date = datetime.strptime(review_date_,'%Y-%m-%d')
                    print(type(review_date))
                    if review_date >= self.start_date and review_date <= self.end_date:
                         self.yield_items\
                         (_id = item["childId"],
                         review_date = review_date,
                         title= item["title"],
                         body= item["description"],
                         rating = item["rating"],
                         url = product_url,
                         review_type = 'media',
                         creator_id = item["id"],
                         creator_name = item["name"],
                         product_id = product_id,
                         extra_info={}
                      )

        page_count = response.meta['page_count'] + 1
        if review_date >= self.start_date and review_date <= self.end_date:
            yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                 callback=self.parse_reviews,
                                 meta={'media_entity': media_entity,
                                       'page_count': page_count, 'dont_proxy': True})

    def get_review_url(self, product_id, page_count):
        url = "https://www.nykaa.com/gateway-api/products/{}/reviews?pageNo={}&sort=MOST_RECENT&filters=DEFAULT&domain=nykaa".format(
            product_id, page_count)

        print(url)
        return url
