import dateparser
import json
import re
from bs4 import BeautifulSoup
import scrapy
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class SemanticScholarListSpider(SetuservSpider):
    name = 'semantic-scholar-articles-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Semantic-Scholar process start")
        assert self.source == 'semantic_scholar_list'

    '''Commenting get_topic_names scraping Script'''

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=product_url,
                                 callback=self.get_authors_list,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})

    def get_authors_list(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]

        articles_list = response.css('div[class="cl-paper-row author-page-details__paper-row paper-row-normal"]')
        print("articles_list", articles_list)
        if articles_list[0:5]:
            for item in articles_list[0:5]:
                if item:
                    article_url = 'https://www.semanticscholar.org' + \
                                  item.css('a[data-selenium-selector="title-link"]::attr(href)').extract_first()
                    print("article_url ---->>>>>>>>>>", article_url)
                    yield scrapy.Request(url=article_url,
                                         callback=self.get_topics_list,
                                         errback=self.err, dont_filter=True,
                                         meta={'media_entity': media_entity,
                                               'article_url': article_url})
        # print(values)
        # import requests
        # resp = requests.get(url="https://www.semanticscholar.org/paper/Phenotypic-characterization-of-human-colorectal-Dalerba-Dylla/54ceb2f5601b1c7672153bcab70e4cc01aee11c1#extracted")
        # res = BeautifulSoup(response.text, 'html.parser')
        # topic_names = res.find("span", {"class": "preview-box__target"}).text

    def get_topics_list(self, response):
        media_entity = response.meta["media_entity"]
        article_url = response.meta["article_url"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        # print('res_text', response.text)
        title = response.css('meta[name="twitter:title"]::attr(content)').extract_first()
        description = response.css('meta[name="twitter:description"]::attr(content)').extract_first()
        citation_count = response.css('span[class="scorecard-stat__headline__dark"]::text').extract_first()
        # topic_names = response.css('span.preview-box__target a::text').extract()
        # print("topic_names ------->>>>>", topic_names)
        if description:
            media = {
                "client_id": self.client_id,
                "media_source": self.source,
                "media_entity_id": product_id,
                "product_url": product_url,
                "type": "topic_names",
                "propagation": self.propagation,
                "article_url": article_url,
                "title": title,
                "description": description,
                "citation_count": citation_count,
            }

            yield self.yield_get_article_details(product_url=media['product_url'],
                                                 product_id=media['media_entity_id'],
                                                 handle_url=media['article_url'],
                                                 handle_name=media['media_entity_id'],
                                                 title=media['title'],
                                                 description=media['description'],
                                                 citation_count=media['citation_count'],
                                                 paper_count=''
                                                 )
