import scrapy
import json
import dateparser
from urllib.parse import urlparse
import base64
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class EuropepmcSpider(SetuservSpider):
    name = 'europepmc-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Europepmc process start")
        assert self.source == 'europepmc'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            product_id = media_entity['id']
            product_ids = product_id.split('_')[0]
            url = f'https://europepmc.org/api/get/articleApi?query={product_ids}&cursorMark=*&format=json' \
                  f'&pageSize=25&sort=FIRST_PDATE_D+desc&synonym=FALSE'
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
        res = json.loads(response.text)

        next_cursor_mark = res['nextCursorMark']
        print('next_cursor_mark******************', next_cursor_mark)
        val = 0
        print('final val', val)

        if res['resultList']['result']:
            for item in res['resultList']['result']:
                if item:
                    article_id = item['id']
                    _created_date = item['firstPublicationDate']
                    created_date = dateparser.parse(str(_created_date))
                    print('created_dateaaaaaaaaaa', created_date)
                    print('created_dateaaaaaaaaaa', self.start_date)

                    source_id = item["source"]

                    if source_id == 'PMC':
                        article_api = f'https://europepmc.org/api/get/articleApi?query=PMCID:{article_id}' \
                                      f'&format=json&resultType=core'
                    else:
                        article_api = f'https://europepmc.org/api/get/articleApi?query=(EXT_ID:{article_id}' \
                                      f'%20AND%20SRC:{source_id})&format=json&resultType=core'

                    if self.start_date <= created_date <= self.end_date:
                        yield scrapy.Request(url=article_api, callback=self.parse_article,
                                             errback=self.err, dont_filter=True,
                                             meta={'media_entity': media_entity,
                                                   'created_date': created_date,
                                                   'article_id': article_id,
                                                   'source_id': source_id})
                        self.logger.info(f"Generating articles for {article_id}")
                        val += 1
            print('final val', val)

            if created_date >= self.start_date:
                page += 1
                product_id = media_entity['id']
                product_ids = product_id.split('_')[0]
                url = f"https://europepmc.org/api/get/articleApi?query={product_ids}&cursorMark={next_cursor_mark}" \
                      f"&format=json&pageSize=25&sort=FIRST_PDATE_D+desc&synonym=FALSE"
                print('urlsssssss', url)
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
        source_id = response.meta['source_id']

        article_res = json.loads(response.text)

        if article_res['resultList']['result']:
            for item in article_res['resultList']['result']:
                if item:
                    try:
                        full_text = item['abstractText']
                    except:
                        full_text = ''

                    soup = BeautifulSoup(str(full_text), 'html.parser')

                    for s in soup(['script', 'style']):
                        s.decompose()
                    full_text = ' '.join(soup.stripped_strings)
                    if '<b>' in full_text:
                        full_text = full_text.replace('</i>', ' ')
                    elif '</h4>' in full_text:
                        full_text = full_text.replace('</h4>', ' ')

                    article_id = article_id
                    created_date = created_date
                    url = product_url
                    product_id = product_id
                    article_link = f'https://europepmc.org/article/{source_id}/{article_id}'
                    article_title = item['title']
                    soups = BeautifulSoup(str(article_title), 'html.parser')

                    for s in soups(['script', 'style']):
                        s.decompose()
                    article_title = ' '.join(soups.stripped_strings)
                    if '</i>' in article_title:
                        article_title = article_title.replace('</i>', ' ')
                    elif '<i>' in article_title:
                        article_title = article_title.replace('<i>', ' ')
                    author_name = item['authorString']
                    description = ' '
                    abstract = full_text
                    extra_info = ''
                    self.yield_research_sources(
                        article_id=article_id,
                        product_id=product_id,
                        created_date=created_date,
                        author_name=str(author_name),
                        description=description,
                        full_text=abstract,
                        product_url=product_url,
                        title=article_title,
                        url=article_link,
                        article_link=article_link,
                        extra_info=str(extra_info)
                    )
