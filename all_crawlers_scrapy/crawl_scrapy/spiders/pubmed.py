import scrapy
import dateparser
import base64
import json
from bs4 import BeautifulSoup
import pandas as pd
import math
from .setuserv_spider import SetuservSpider
from dateparser import search as datesearch
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.service_account import ServiceAccountCredentials


class PubmedSpider(SetuservSpider):
    name = 'pubmed-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Pubmed process start")
        assert self.source == 'pubmed'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = 1

        if "explanation solr-message query-error-message" not in response.text:
            if "altered-search-explanation query-error-message" not in response.text:
                if "spell-check-warning" not in response.text:
                    self.logger.info(f"Correct results found for {product_id}")
                    if "heading-title" in response.text:
                        self.logger.info(f"Single paper link {product_id}")
                        yield scrapy.Request(url=product_url,
                                             callback=self.get_single_paper_info,
                                             errback=self.err,
                                             dont_filter=True,
                                             meta={'media_entity': media_entity})

                    else:
                        self.logger.info(f"Multiple papers link {product_id}")
                        res = BeautifulSoup(response.text, 'html.parser')
                        total_page_count = int(
                            res.find('div', {'class': 'results-amount'}).find('span').text.replace(',', ''))
                        page_count = math.ceil(total_page_count / 200)

                        yield scrapy.Request(url=self.get_page_url(product_url, page),
                                             callback=self.parse_article,
                                             errback=self.err,
                                             dont_filter=True,
                                             meta={'media_entity': media_entity,
                                                   'page': page,
                                                   'page_count': page_count})
                else:
                    self.logger.info(f"No exact term results found for the product_id {product_id}")
            else:
                self.logger.info(f"No exact term results found for the product_id {product_id}")
        else:
            self.logger.info(f"No exact term results found for the product_id {product_id}")

    def get_single_paper_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']

        created_date = response.css('span.cit::text').extract_first().split(';')[0]
        date_str_length = len(created_date.split())
        if date_str_length == 1:
            created_date = created_date + ' Jan'
        created_date = dateparser.parse(created_date, settings={'PREFER_DAY_OF_MONTH': 'first'})

        if self.start_date <= created_date <= self.end_date:

            article_id = response.css('strong.current-id::text').extract_first()
            article_title = response.css('h1.heading-title::text').extract_first().strip()
            _author_list = response.css('div.inline-authors a.full-name::text').extract()
            if len(_author_list) == 0:
                _author_list = response.css('div.inline-authors span.full-name::text').extract()

            author_list = ''
            for item in _author_list:
                author_list += item + ', '
            extra_info = {'author_list': author_list.replace(", '", "'")}

            _full_text = response.css('div[id="enc-abstract"]').extract_first()
            if _full_text is not None:
                soup = BeautifulSoup(_full_text, 'html.parser')
                for s in soup(['script', 'style']):
                    s.decompose()
                full_text = ' '.join(soup.stripped_strings)
                if '\xa0' in full_text:
                    full_text = full_text.replace('\xa0', ' ')
            else:
                full_text = ''
            article_link = f"https://pubmed.ncbi.nlm.nih.gov/{article_id}/"

            self.yield_research_sources(
                article_id=article_id,
                product_id=product_id,
                created_date=created_date,
                author_name=product_id,
                description="",
                full_text=full_text,
                product_url=product_url,
                title=article_title,
                url=article_link,
                article_link=article_link,
                extra_info=extra_info,
            )

    @staticmethod
    def get_page_url(product_url, page):
        page_url = product_url + f'&page={page}&sort=date'
        return page_url

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        page_count = response.meta["page_count"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        created_date = self.start_date

        res = BeautifulSoup(response.text, 'html.parser')
        res = res.select('div.search-results-chunk article.full-docsum')

        if res:
            for item in res:
                if item:

                    title = item.find("a", {"class": "docsum-title"}).text.strip()
                    author_list = item.find("span", {"class": "docsum-authors full-authors"}).text
                    created_date = item.find("span", {"class": "docsum-journal-citation full-journal-citation"}).text
                    pmid = item.find("span", {"class": "docsum-pmid"}).text
                    # paper_link = item.find("a")['href'].split('//')[2]
                    paper_link = 'https://pubmed.ncbi.nlm.nih.gov' + item.find("a")['href']
                    extra_info = {'author_list': author_list}

                    # if 'Epub' in created_date:
                    #     created_date = created_date.split('Epub ')[1]
                    #     created_date = created_date.replace(".", "")
                    # else:
                    try:
                        if ';' in created_date:
                            created_date = created_date.split('. ')[1].split(';')[0]
                        else:
                            if '.' in created_date:
                                created_date = created_date.split('. ')[1].split(':')[0]
                            else:
                                created_date = 'not present'

                        if created_date is not 'not present':
                            if '/' in created_date:
                                created_date = created_date.split('/')[0]
                            if '-' in created_date:
                                created_date = created_date.split('-')[0]

                            date_str_length = len(created_date.split())

                            if date_str_length == 1:
                                created_date = created_date + ' Jan'

                            created_date = dateparser.parse(created_date, settings={'PREFER_DAY_OF_MONTH': 'first'})

                            print('Yielding Pubmed article')
                            article_details = {
                                'article_id': pmid,
                                'created_date': created_date,
                                'article_title': title,
                                'url': product_url,
                                'product_id': product_id,
                                'product_url': product_url,
                                'author_name': product_id,
                                'article_link': paper_link,
                                'extra_info': extra_info
                            }
                            if self.start_date <= created_date <= self.end_date:
                                yield scrapy.Request(url=paper_link,
                                                     callback=self.parse_abstract,
                                                     errback=self.err,
                                                     dont_filter=True,
                                                     meta={'media_entity': media_entity,
                                                           'article_details': article_details})

                    except:
                        print("problem occurred scraping the article with pmid - ", pmid)

            page += 1
            if created_date >= self.start_date and page <= page_count:
                yield scrapy.Request(url=self.get_page_url(product_url, page),
                                     callback=self.parse_article,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'page_count': page_count})

    def parse_abstract(self, response):
        article_details = response.meta["article_details"]
        _full_text = response.css('div[id="enc-abstract"]').extract_first()
        if _full_text is not None:
            soup = BeautifulSoup(_full_text, 'html.parser')
            for s in soup(['script', 'style']):
                s.decompose()
            full_text = ' '.join(soup.stripped_strings)
            if '\xa0' in full_text:
                full_text = full_text.replace('\xa0', ' ')
        else:
            full_text = ''
        print('full_text', full_text)

        self.yield_research_sources(
            article_id=article_details['article_id'],
            product_id=article_details['product_id'],
            created_date=article_details['created_date'],
            author_name=article_details['author_name'],
            description="",
            full_text=full_text,
            product_url=article_details['product_url'],
            title=article_details['article_title'],
            url=article_details['article_link'],
            article_link=article_details['article_link'],
            extra_info=article_details['extra_info'],
        )
