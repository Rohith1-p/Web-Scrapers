import math
import hashlib
import re
import datetime
from datetime import timedelta
from urllib.parse import urlparse
import dateparser
from dateparser import search as datesearch
import scrapy
# from scrapy.conf import settings

import requests
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


# settings.overrides['CONCURRENT_REQUESTS'] = 20
# settings.overrides['CONCURRENT_REQUESTS_PER_DOMAIN'] = 20


class MdedgeSpider(SetuservSpider):
    name = 'mdedge-lv1-article-links'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id,env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name,env)
        assert self.source == 'mdedge_lv1'

    def start_requests(self):
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'id': product_id, 'url': product_url}
            page = 0
            yield scrapy.Request(url=product_url, callback=self.parse_response,
                                 dont_filter=True, errback=self.err,
                                 meta={'media_entity': media_entity, 'lifetime': True, 'page': page},
                                 )

    def parse_response(self, response):
        product_id = response.meta['media_entity']['id']
        product_url = response.meta['media_entity']['url']
        page = response.meta['page']
        media_entity = response.meta['media_entity']
        articles = response.css(".node-view-mode-teaser")
        if articles:
            for item in articles:
                link = item.css('h2 a::attr(href)').extract_first()
                link = 'https://www.mdedge.com' + link
                posted_date = item.css('div.byline::text').extract_first()
                posted_date = dateparser.parse(posted_date)
                title = item.css("h2 a::text").extract_first()
                title = title.strip()
                author = item.css("dt+ .field-label-hidden::text").extract_first()
                try:
                    author = author.strip()
                except:
                    author = ""



                self.yield_get_article_handle_names(
                    product_url=product_url,
                    product_id=product_id,
                    handle_url=link,
                    citation_count=posted_date,
                    paper_count="",
                    handle_name=author,
                    bio=title)

            page += 1

            next_url = str(product_url) + '?page=' + str(page)
            print("next_url ", next_url)
            yield scrapy.Request(url=next_url, callback=self.parse_response,
                                 dont_filter=True, errback=self.err,
                                 meta={'media_entity': media_entity, 'lifetime': True, 'page': page},
                                 )
