import dateparser
import json
import re
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider


class SemanticScholarSpider(SetuservSpider):
    name = 'semantic-scholar-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Semantic-Scholar process start")
        assert self.source == 'semantic_scholar'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=self.get_review_url(product_url, page),
                                 callback=self.ol_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})

    def ol_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        page = response.meta["page"]
        print('response_response_response_response_response', response.text)
        ol_name = response.css('h1[data-selenium-selector="author-name"] a::text').extract_first()
        stats_response = response.css('div.author-detail-card__stats-row')
        info_dict = {}

        for item in stats_response:
            key = item.css('span.author-detail-card__stats-row__label::text').extract_first()
            val = item.css('span.author-detail-card__stats-row__value::text').extract_first()
            info_dict.update({key: val})
        h_index = info_dict.get('h-index')
        citations = info_dict.get('Citations')
        publications_count = info_dict.get('Publications')
        created_date = self.start_date

        articles_list = response.css('div[class="cl-paper-row author-page-details__paper-row paper-row-normal"]')
        if articles_list:
            for item in articles_list:
                if item:
                    article_id = item.css('::attr(data-paper-id)').extract_first()
                    article_url = 'https://www.semanticscholar.org' + \
                                  item.css('a[data-selenium-selector="title-link"]::attr(href)').extract_first()

                    _created_date = item.css('span.cl-paper-pubdates').extract_first()
                    created_date = ' '.join(BeautifulSoup(_created_date, "html.parser").stripped_strings)
                    created_date = dateparser.parse(str(created_date))

                    extra_info = {'article_url': article_url,
                                  'article_id': article_id,
                                  'h_index': h_index,
                                  'citations': citations,
                                  'publications_count': publications_count,
                                  'ol_name': ol_name,
                                  'created_date': created_date}

                    if self.start_date <= created_date <= self.end_date:
                        yield scrapy.Request(url=article_url,
                                             callback=self.parse_articles,
                                             errback=self.err, dont_filter=True,
                                             meta={'media_entity': media_entity,
                                                   'extra_info': extra_info})
                        self.logger.info(f"Scraping article data for {article_url}")

            if created_date >= self.start_date:
                page += 1
                yield scrapy.Request(url=self.get_review_url(product_url, page),
                                     callback=self.ol_info,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page})
                self.logger.info(f"Request is going for {product_url} and {page}")

    def parse_articles(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta["extra_info"]
        created_date = extra_info["created_date"]

        res = response.css('script[type="application/ld+json"]::text').extract_first()
        res = json.loads(res)
        for item in res['@graph'][1]:
            description = item['description']
            if description is not None:
                description = description.strip()
            article_title = item['name']
            break

        if 'data-selenium-selector="text-truncator-text"' not in response.text:
            description = ''

        authors_list = response.css('meta[name="citation_author"]::attr(content)').extract()
        article_citations = response.css('span[class="scorecard-stat__headline__dark"]::text').extract_first()
        if article_citations is not None:
            article_citations = re.findall(r'\d+', article_citations)[0]
        else:
            article_citations = 0

        self.yield_hcp_profile_articles \
            (product_url=product_url,
             product_id=product_id,
             article_id=extra_info['article_id'],
             article_url=extra_info['article_url'],
             ol_name=extra_info['ol_name'],
             designation='',
             publications_count=extra_info["publications_count"],
             citations=extra_info["citations"],
             citations_since_2016='',
             h_index=extra_info["h_index"],
             h_index_since_2016='',
             i10_index='',
             i10_index_since_2016='',
             citations_2014='',
             citations_2015='',
             citations_2016='',
             citations_2017='',
             citations_2018='',
             citations_2019='',
             citations_2020='',
             citations_2021='',
             authors_list=str(authors_list),
             description=description,
             article_citations=article_citations,
             published_date=created_date,
             publisher='',
             article_title=article_title
             )

    @staticmethod
    def get_review_url(product_url, page):
        url = f'{product_url}?sort=pub-date&page={page}'
        return url
