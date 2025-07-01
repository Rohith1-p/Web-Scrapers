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


class Gi_orgLv2Spider(SetuservSpider):
    name = 'gi_org_lv2-article'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id,env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name,env)
        assert self.source == 'gi_org_lv2'

    def start_requests(self):
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'id': product_id, 'url': product_url}
            yield scrapy.Request(url=product_url, callback=self.parse_response,
                                 dont_filter=True, errback=self.err,
                                 meta={'media_entity': media_entity, 'lifetime': True,},
                                 )

    def parse_response(self, response):
        product_url = response.meta['media_entity']['url']
        product_id =  response.meta['media_entity']['id']

        if 'video' in product_url:
            information = response.css("p::text , h2::text").extract()
            information = ' '.join(information)
            if '\xa0' in information:
                information = information.replace(u'\xa0',u' ')
            try:
                author = response.css('.wp-block-image+ .has-text-align-center::text').extract()[0]
            except:
                author = response.css('h2:nth-child(8)::text').extract_first()
            created_date = response.css('.updated::text').extract_first()
            created_date= dateparser.parse(created_date)
            title = response.css('.main-title::text').extract_first()

        else:
            information = response.css("h2 a::text , p::text").extract()
            information = " ".join(information)
            if "\xa0" in information:
                information = information.replace(u'\xa0', u' ')
            created_date = response.css('.updated::text').extract_first()
            created_date = dateparser.parse(created_date)
            author = ''
            title = response.css('.main-title::text').extract_first()

        self.yield_article(
            article_id = "",
            product_id = product_id ,
            created_date = created_date,
            username = author,
            description= "",
            full_text = information,
            title = title,
            url = product_url,
            disease_area= "",
            medicines= "",
            trial = "",
            views_count="")
