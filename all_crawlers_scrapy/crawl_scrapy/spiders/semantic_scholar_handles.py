import dateparser
import json
import re
from bs4 import BeautifulSoup
import scrapy
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class SemanticScholarHandleSpider(SetuservSpider):
    name = 'semantic-scholar-articles-handle'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Semantic-Scholar process start")
        assert self.source == 'semantic_scholar_handle'

    '''Commenting Handle names scraping Script'''

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            url = "https://www.semanticscholar.org/api/1/search"
            headers = {
                'Content-Type': 'application/json',
                'Host': 'www.semanticscholar.org',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
            }
            payload = {"queryString": product_url, "page": 1, "pageSize": 10, "sort": "relevance",
                       "authors": [], "coAuthors": [], "venues": []}
            yield scrapy.Request(url=url,
                                 callback=self.get_handle_names,
                                 method="POST",
                                 dont_filter=True,
                                 headers=headers,
                                 body=json.dumps(payload),
                                 meta={'media_entity': media_entity})

    def get_handle_names(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        res = json.loads(response.text)

        if res['matchedAuthors']:
            for item in res['matchedAuthors']:
                if item:
                    handle_name = item['name']
                    citation_count = item['citationCount']
                    paper_count = item['paperCount']
                    # handle_name_list.append(handle_name)
                    media = {
                        "client_id": self.client_id,
                        "media_source": self.source,
                        "product_url": product_url,
                        "type": "handle_names",
                        "propagation": self.propagation,
                        "handle_url": f"https://www.semanticscholar.org/author/{item['slug']}/{item['id']}",
                        "citation_count": citation_count,
                        "paper_count": paper_count,
                        "handle_name": handle_name,
                    }
                    yield scrapy.Request(url=media['handle_url'],
                                         callback=self.parse_bio,
                                         errback=self.err, dont_filter=True,
                                         meta={'media_entity': media_entity, 'media': media})

    def parse_bio(self, response):
        media_entity = response.meta["media_entity"]
        media = response.meta["media"]
        product_id = media_entity["id"]
        fieldsOfStudy = response.css('span[class="cl-paper-fos"]::text').extract_first()
        bio = response.css('meta[name="twitter:description"]::attr(content)').extract_first()
        bio = bio + ' fieldsOfStudy: ' + fieldsOfStudy

        yield self.yield_get_article_handle_names(product_url=media['product_url'],
                                                  product_id=product_id,
                                                  handle_url=media['handle_url'],
                                                  citation_count=media['citation_count'],
                                                  paper_count=media['paper_count'],
                                                  handle_name=media['handle_name'],
                                                  bio=bio)
