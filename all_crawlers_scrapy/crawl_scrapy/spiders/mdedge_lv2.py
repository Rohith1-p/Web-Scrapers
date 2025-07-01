import scrapy
import math
import hashlib
import re
import datetime
from datetime import timedelta
from urllib.parse import urlparse
import dateparser
from dateparser import search as datesearch
# from scrapy.conf import settings

import requests
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider

from .setuserv_spider import SetuservSpider


class MdedgeLv2Spider(SetuservSpider):
    name = 'mdedge-lv2-article'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id,env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name,env)
        assert self.source == 'mdedge_lv2'

    def start_requests(self):
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'id': product_id, 'url': product_url}
            yield scrapy.Request(url=product_url, callback=self.parse_response,
                                 dont_filter=True, errback=self.err,
                                 meta={'media_entity': media_entity, 'lifetime': True},
                                 )

    def parse_response(self, response):
        product_id = response.meta['media_entity']['id']
        product_url = response.meta['media_entity']['url']
        try:
            title = response.css("#page-title::text").extract_first().strip()
        except:
            title = ""
        try:
            created_date = response.css(".byline::text").extract_first()
            created_date = dateparser.parse(created_date)
        except:
            created_date = ""
        try:
            author = response.css(".inline a::text").extract()
        except:
            author = ""
        try:
            information = response.css(".field-label-hidden h2::text , .field-type-text-long p::text").extract()
            information_list = []
            for item in information:
                item = item.strip()
                information_list.append(item)
            information = " ".join(information_list)
        except:
            information = ""

        self.yield_article(
            article_id="",
            product_id=product_id,
            created_date=created_date,
            username=author,
            description="",
            full_text=information,
            title=title,
            url=product_url,
            disease_area="",
            medicines="",
            trial="",
            views_count="")
