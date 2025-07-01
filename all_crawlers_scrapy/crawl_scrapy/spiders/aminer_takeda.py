import scrapy
from urllib.parse import urlparse
import base64
import dateparser
import dateutil.parser as dparser
from bs4 import BeautifulSoup
import json

from .setuserv_spider import SetuservSpider


class AminerSpider(SetuservSpider):
    name = 'aminer-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("aminer process start")
        assert self.source == 'aminer'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            product_id = media_entity['id']
            product_ids = product_id.split('_')[0]
            urls = "https://apiv2.aminer.cn/n?a=SEARCH__searchapi.SearchPubsCommon___"
            page = 0
            payload = [{"action": "searchapi.SearchPubsCommon",
                        "parameters": {"offset": page, "size": 20, "searchType": "all", "switches": ["lang_zh"],
                                       "aggregation": ["year", "author_year", "keywords", "org"],
                                       "query": str(product_ids),
                                       "es_search_condition": {"include_and": {"conditions": [
                                           {"search_type": "range", "field": "year",
                                            "origin": {"gte": "2011", "lte": "2023"}}]}},
                                       "year_interval": 1, "search_tab": "latest"}, "schema": {
                    "publication": ["id", "year", "title", "title_zh", "abstract", "abstract_zh", "authors",
                                    "authors._id", "authors.name", "keywords", "authors.name_zh", "num_citation",
                                    "num_viewed", "num_starred", "num_upvoted", "is_starring", "is_upvoted",
                                    "is_downvoted", "venue.info.name", "venue.volume", "venue.info.name_zh",
                                    "venue.info.publisher", "venue.issue", "pages.start", "pages.end", "lang", "pdf",
                                    "ppt", "doi", "urls", "flags", "resources", "issn"]}}]

            yield scrapy.Request(url=urls,
                                 body=json.dumps(payload),
                                 callback=self.parse_response,
                                 errback=self.err,
                                 dont_filter=True,
                                 method="POST",
                                 meta={'media_entity': media_entity,
                                       "page": page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        product_url = media_entity['url']
        created_date = self.start_date
        page = response.meta['page']
        print('page', page)

        data_dict = json.loads(response.text)
        articles = data_dict['data'][0]['items']
        if articles:
            for item in articles:
                if item:
                    _created_dates = item['year']
                    _created_date = "01-01-" + str(_created_dates)
                    created_date = dateparser.parse(str(_created_date))
                    if self.start_date <= created_date <= self.end_date:
                        article_id = item['id']
                        title = item['title']
                        url = product_url
                        product_id = product_id
                        author_name = item['authors']
                        author_name = [x['name'] for x in author_name]
                        article_link = 'https://www.aminer.org/pub/' + article_id
                        extra_info = ''
                        description = ' '
                        if 'abstract' in item:
                            abstract = item['abstract']
                        else:
                            abstract = ''
                        self.yield_research_sources(
                            article_id=article_id,
                            product_id=product_id,
                            created_date=created_date,
                            author_name=str(author_name),
                            description=description,
                            full_text=abstract,
                            product_url=product_url,
                            title=title,
                            url=article_link,
                            article_link=article_link,
                            extra_info=str(extra_info)
                        )

            if created_date >= self.start_date:
                page += 20
                product_ids = product_id.split('_')[0]
                payload = [{"action": "searchapi.SearchPubsCommon",
                            "parameters": {"offset": page, "size": 20, "searchType": "all", "switches": ["lang_zh"],
                                           "aggregation": ["year", "author_year", "keywords", "org"],
                                           "query": str(product_ids), "es_search_condition": {"include_and": {
                                    "conditions": [{"search_type": "range", "field": "year",
                                                    "origin": {"gte": "2011", "lte": "2023"}}]}}, "year_interval": 1,
                                           "search_tab": "latest"}, "schema": {
                        "publication": ["id", "year", "title", "title_zh", "abstract", "abstract_zh", "authors",
                                        "authors._id", "authors.name", "keywords", "authors.name_zh", "num_citation",
                                        "num_viewed", "num_starred", "num_upvoted", "is_starring", "is_upvoted",
                                        "is_downvoted", "venue.info.name", "venue.volume", "venue.info.name_zh",
                                        "venue.info.publisher", "venue.issue", "pages.start", "pages.end", "lang",
                                        "pdf", "ppt", "doi", "urls", "flags", "resources", "issn"]}}]
                urls = "https://apiv2.aminer.cn/n?a=SEARCH__searchapi.SearchPubsCommon___"
                yield scrapy.Request(url=urls,
                                     body=json.dumps(payload),
                                     callback=self.parse_response,
                                     errback=self.err,
                                     dont_filter=True,
                                     method="POST",
                                     meta={'media_entity': media_entity,
                                           "page": page})
