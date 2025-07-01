import json
import datetime
from datetime import datetime
import scrapy
from scrapy.conf import settings
import requests
from urllib.parse import quote_plus
import ast
from .setuserv_spider import SetuservSpider


class TokopediaCategorySpider(SetuservSpider):
    name = 'tokopedia-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Tokopedia category Scraping startss")
        assert self.source == 'tokopedia_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        print("Starting requests   ########")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            if "page=" in product_url:
                page_no = int(product_url.split("page=")[1])
            else:
                page_no = 1
            media_entity["page_no"] = page_no
            yield scrapy.Request(url=self.get_category_url(product_url, page_no),
                                 headers=self.get_headers(product_url),
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True, meta={'media_entity': media_entity})

    def parse_info(self, response):
        print("in parse info******")
        media_entity = response.meta["media_entity"]
        category_url = media_entity["url"]
        page_no = media_entity["page_no"]
        if '"statusCode":404' in response.text:
            return
        if '"products":' not in response.text:
            yield scrapy.Request(url=self.get_category_url(category_url, page_no),
                                 headers=self.get_headers(category_url),
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True, meta={'media_entity': media_entity})
            return
        products = response.text.split('"products":')[1]
        products_list = products.split('related')[0]
        products_list = products_list.replace("false", "False")
        products_list= products_list[:-2]
        products_list= ast.literal_eval(products_list)
        last_product =products_list[-1]["id"]
        split_val =  last_product+'.'
        final_dict = products.split(split_val)[0]
        type_name = products_list[0]["typename"]

        if type_name == "AceSearchProduct":
            final_dict = final_dict + '"}]}'
            final_dict = final_dict.split('AceSearchProductData"},')[1]
        else:
            print("6. in elseee")
            final_dict = final_dict + '"}]}'
            final_dict = final_dict.split('"AceSearchUnifyData"},')[1]
        final_dict = "{"+final_dict + "}"
        final_dict = final_dict.replace("false", "False")
        final_dict = final_dict.replace("true", "True")
        final_dict= ast.literal_eval(final_dict)

        for item in products_list:
            id_ = item['id']
            product_id = final_dict[id_]["id"]
            url_ = final_dict[id_]["url"]
            url_ = url_.replace("\u002F", "/")
            media = {
                "client_id": str(self.client_id),
                "media_source": str(self.source),
                "category_url": category_url,
                "product_url": url_,
                "media_entity_id": product_id,
                "type": "product_details",
                "propagation": self.propagation,
                "created_date": datetime.utcnow()
            }
            self.yield_category_details(category_url=media['category_url'],
                                        product_url=media['product_url'],
                                        product_id=media['media_entity_id'],
                                        page_no = page_no,
                                        extra_info='')

        page_no =  page_no + 1
        media_entity["page_no"] = page_no
        if '"statusCode":404' not in response.text:
            yield scrapy.Request(url=self.get_category_url(category_url, page_no),
                                 headers=self.get_headers(category_url),
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True, meta={'media_entity': media_entity})
        else:
            self.logger.info(f"No more pages for the best seller category {category_url}")

    @staticmethod
    def get_category_url(url, page_no):
        if "q=" in url:
            split_url = (url.split("q=")[1]).split("&")[0]
            url = "https://www.tokopedia.com/find/" + split_url + "?page={}".format(page_no)
            return url
        if "page=" in url:
            # url = url.split("page=")[0]
            # url = url + "page={}".format(page_no)
            url = url2.split("page=")
            cat = (url[1].split("&"))
            cat = ("&".join(cat)[1:])
            url = url[0] + "page={}".format(str(1)) + cat
        else:
            try:
                url_1 = url.split("?")
                url = url_1[0]+"?page{}&"+url_1[1]
                print("***url", url)
            except:
                url = url + "?page={}".format(str(1))
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'referer': product_url,
            'origin': 'www.tokopedia.com',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/75.0.3770.100 Safari/537.36'
        }

        return headers
