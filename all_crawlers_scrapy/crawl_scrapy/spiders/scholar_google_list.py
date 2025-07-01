import dateparser
from datetime import datetime
import re
import time
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider


class ScholarGoogleListSpider(SetuservSpider):
    name = 'scholar-google-articles-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Scholar-google process start")
        assert self.source == 'scholar_google_list'

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


        articles_list = response.css('tr.gsc_a_tr')
        if articles_list[0:5]:
            for item in articles_list[0:5]:
                if item:
                    article_url = 'https://scholar.google.co.in/' + \
                                  item.css('a[class="gsc_a_at"]::attr(href)').extract_first()
                    yield scrapy.Request(url=article_url,
                                         callback=self.get_description,
                                         errback=self.err, dont_filter=True,
                                         meta={'media_entity': media_entity,
                                               'article_url': article_url})

    def get_description(self, response):
        media_entity = response.meta["media_entity"]
        article_url = response.meta["article_url"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        raw_data = response.css('div.gs_scl')
        info_dict = {}

        for item in raw_data:
            key = item.css('div.gsc_oci_field::text').extract_first()
            val = item.css('div.gsc_oci_value').extract_first()
            if key == 'Total citations':
                val = item.css('div.gsc_oci_value a').extract_first()
            soup = BeautifulSoup(val, 'html.parser')
            for s in soup(['script', 'style']):
                s.decompose()
            val = ' '.join(soup.stripped_strings)
            info_dict.update({key: val})

        description = info_dict.get('Description')
        title = response.css('meta[property="og:title"]::attr(content)').extract_first()
        print("description ------->>>>>", description)
        citation_count = response.css('meta[name="description"]::attr(content)'
                                      ).extract_first().split('Cited by')[1].strip().split(' ')[0]
        citation_count = citation_count.strip()
        media = {
            "client_id": self.client_id,
            "media_source": self.source,
            "media_entity_id": product_id,
            "handle_url": product_url,
            "type": "topic_names",
            "propagation": self.propagation,
            "article_url": article_url,
            "title": title,
            "description": description
        }

        yield self.yield_get_article_details(product_url=media['handle_url'],
                                             product_id=media['media_entity_id'],
                                             handle_url=media['article_url'],
                                             handle_name=media['media_entity_id'],
                                             title=media['title'],
                                             description=media['description'],
                                             citation_count=citation_count,
                                             paper_count='')
