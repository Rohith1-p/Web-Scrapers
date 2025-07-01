import scrapy
import json
import dateparser
from urllib.parse import urlparse
import base64
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class OnlinelibrarySpider(SetuservSpider):
    name = 'wiley-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("wiley process start")
        assert self.source == 'wiley'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            # url = 'https://onlinelibrary.wiley.com/action/doSearch?SeriesKey=16000609&sortBy=Earliest&startPage=0&pageSize=20'
            url = product_url + '&sortBy=Earliest&startPage=' + str(page) + '&pageSize=20'
            yield scrapy.Request(url=url, callback=self.parse_response,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, "page": page})
            self.logger.info(f"Generating Articles for product_url {product_url} and {product_id}")

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        created_date = self.start_date

        articles = response.css('ul[class="rlist search-result__body items-results"] div[class="item__body"]')

        if articles:
            for item in articles:
                if item:
                    title = item.css('a[class="publication_title visitable"] ::text').extract_first()
                    article_url = item.css('a[class="publication_title visitable"] ::attr(href)').extract_first()
                    article_url = 'https://onlinelibrary.wiley.com' + article_url
                    author_name = item.css('a[class="publication_contrib_author"] ::text').extract()
                    article_id = article_url.split('/')[-1]
                    _created_date = item.css('p[class="meta__epubDate"] ::text')[-1].extract()
                    created_date = dateparser.parse(str(_created_date))

                    if self.start_date <= created_date <= self.end_date:
                        yield scrapy.Request(url=article_url, callback=self.parse_article,
                                             errback=self.err, dont_filter=True,
                                             meta={'media_entity': media_entity, 'created_date': created_date,
                                                   'article_id': article_id, 'title': title, 'article_url': article_url,
                                                   'author_name': author_name})
                        self.logger.info(f"Generating articles for {article_id}")

            if created_date >= self.start_date:
                page += 1
                url = product_url + '&sortBy=Earliest&startPage=' + str(page) + '&pageSize=20'
                yield scrapy.Request(url=url, callback=self.parse_response,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity, "page": page})
                self.logger.info(f"Request is going for page {page} and product_url {product_url} ")

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        created_date = response.meta['created_date']
        article_id = response.meta['article_id']
        article_url = response.meta['article_url']
        title = response.meta['title']
        author_name = response.meta['author_name']
        extra_info = ' '
        description = response.css('div[class="article-header__references-container no-truncate"] p::text').extract()
        description = "".join(str(x) for x in description)
        description = description.rstrip()
        abstract = response.css('div[class="article__body "] ::text').extract()
        abstract = "".join(str(x) for x in abstract)
        abstract = abstract.rstrip()
        if len(abstract) <= 10:
            abstract = description
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
