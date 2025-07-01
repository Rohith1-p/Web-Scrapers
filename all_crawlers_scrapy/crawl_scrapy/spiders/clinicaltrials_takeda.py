import scrapy
from urllib.parse import urlparse
import base64
import dateparser
import dateutil.parser as dparser
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class ClinicaltrialseuSpider(SetuservSpider):
    name = 'clinicaltrialseu-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("clinicaltrialsregister process start")
        assert self.source == 'clinicaltrialsregister'


    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            urls = product_url + '&page=' + str(page) + '&dateFrom=2019-01-01&dateTo=2023-01-01'
            yield scrapy.Request(url=urls,
                                 callback=self.parse_response,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        created_date = self.start_date
        articles = response.css('div[class="results grid_8plus"] table[class="result"]')

        if articles:
            count = 0
            for item in articles:
                if item:
                    _created_date = item.css('table[class="result"] tr td ')[2]
                    _created_date = _created_date.css('td')[-1].extract()
                    _created_date = _created_date.split('>')[-2].split('<')[0].strip()
                    created_date = dateparser.parse(str(_created_date))
                    author_name = item.css('table[class="result"] tr')[1]
                    author_name = author_name.css('td ::text')[-1].extract().strip()
                    title = item.css('table[class="result"] tr')[2]
                    title = title.css('td ::text')[-1].extract().strip()
                    article_url = item.css('table[class="result"] tr')[-2]
                    article_url = article_url.css('a::attr(href)').extract_first()
                    article_url = "https://www.clinicaltrialsregister.eu" + str(article_url).strip()
                    article_id = article_url.split('/')[-2]
                    if created_date:
                        if self.start_date <= created_date <= self.end_date:
                            yield scrapy.Request(url=article_url,
                                                 callback=self.parse_article,
                                                 errback=self.err,
                                                 dont_filter=True,
                                                 meta={'media_entity': media_entity,
                                                       'created_date': created_date,
                                                       'author_name': author_name,
                                                       'title': title,
                                                       'article_url': article_url,
                                                       'article_id': article_id})
                    else:
                        self.logger.warning("Created date is not present in article")

            if 'accesskey="n"' in response.text:
                page += 1
                urls = product_url + '&page=' + str(page) + '&dateFrom=2019-01-01&dateTo=2023-01-01'
                yield scrapy.Request(url=urls,
                                     callback=self.parse_response,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page})

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        article_id = response.meta['article_id']
        article_url = response.meta['article_url']
        title = response.meta['title']
        author_name = response.meta['author_name']
        created_date = response.meta['created_date']

        description = response.css('tr[class="tricell"]')[2]
        description = description.css('td[class="third"] ::text').extract()
        description = " ".join(str(x) for x in description)
        abstract = response.css('tr[class="tricell"]')[3]
        abstract = abstract.css('td[class="third"] ::text').extract()
        abstract = " ".join(str(x) for x in abstract)
        if len(abstract) <= 10:
            abstract = description
        extra_info = ''
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
