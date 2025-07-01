import scrapy
import json
import re
import math
import dateparser
import dateutil.parser as dparser
from dateparser import search as datesearch
from urllib.parse import urlparse
import base64
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class HaematologicaSpider(SetuservSpider):
    name = 'haematologica-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("haematologica process start")
        assert self.source == 'haematologica'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            url = product_url + f'&searchPage={page}#results'
            yield scrapy.Request(url=url,
                                 callback=self.parse_response,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       "page": page})
            self.logger.info(f"Generating Articles for product_url {product_url} and {url}")

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        created_date = self.start_date

        if 'DB Error: Duplicate entry' in response.text:
            self.logger.info(f"Captcha Found for {product_url}")
            url = product_url + f'&searchPage={page}#results'
            yield scrapy.Request(url=url,
                                 callback=self.parse_response,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       "page": page})
            return

        total_pages = response.css('div.cmp_pagination::text').extract_first().strip().split('of')[1]
        total_pages = int(re.findall(r'\d+', total_pages)[0])
        total_pages = math.ceil(total_pages / 50)
        articles = response.css('div[class="search_results"] div[class="card-body"]')

        if articles:
            for item in articles:
                if item:
                    title = item.css('h4[class="issue-article-title card-title"] ::text')[1].extract().strip()
                    article_url = item.css(
                        'h4[class="issue-article-title card-title"] a::attr(href)').extract_first().strip()
                    author_name = item.css('p[class="issue-auth card-text"] ::text').extract_first().strip()
                    article_id = article_url.split('/')[-1]

                    yield scrapy.Request(url=article_url,
                                         callback=self.parse_article,
                                         errback=self.err,
                                         dont_filter=True,
                                         meta={'media_entity': media_entity,
                                               'article_id': article_id,
                                               'title': title,
                                               'article_url': article_url,
                                               'author_name': author_name})
                    self.logger.info(f"Generating articles for {article_id}")

            if page < total_pages:
                page += 1
                url = product_url + f'&searchPage={page}#results'
                yield scrapy.Request(url=url,
                                     callback=self.parse_response,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           "page": page})
                self.logger.info(f"Request is going for page {page} and product_url {url} ")

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        article_id = response.meta['article_id']
        article_url = response.meta['article_url']
        title = response.meta['title']
        author_name = response.meta['author_name']

        if 'DB Error: Duplicate entry' in response.text:
            self.logger.info(f"Captcha Found for {article_url}")
            yield scrapy.Request(url=article_url,
                                 callback=self.parse_article,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'article_id': article_id,
                                       'title': title,
                                       'article_url': article_url,
                                       'author_name': author_name})
            return

        _created_date = response.css('span[class="galley-issue-id"]::text').extract_first()
        if 'Haematologica' in _created_date:
            created_date = datesearch.search_dates(_created_date)[0][1]
        else:
            _created_date = _created_date.split(':')[-1].strip()
        _created_date = '01 ' + str(_created_date)
        _created_date = dparser.parse(_created_date, fuzzy=True)
        created_date = dateparser.parse(str(_created_date))
        description = ''
        abstract = response.css('div[class="article-fulltext"] ::text').extract()
        abstract = " ".join(str(x) for x in abstract)

        extra_info = ''
        if self.start_date <= created_date <= self.end_date:
            self.yield_research_sources(
                article_id=article_id,
                product_id=product_id,
                created_date=created_date,
                author_name=str(author_name),
                description=description,
                full_text=abstract,
                product_url=product_url,
                title=title,
                url=article_url,
                article_link=article_url,
                extra_info=str(extra_info)
            )
