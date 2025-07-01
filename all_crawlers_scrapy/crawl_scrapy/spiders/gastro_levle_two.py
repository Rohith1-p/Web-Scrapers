from urllib.parse import urlparse
import json
import scrapy
import base64
import dateparser
from urllib.parse import urlparse
import base64
from bs4 import BeautifulSoup
from .setuserv_spider import SetuservSpider


class GastroPharmOneSpider(SetuservSpider):
    name = 'gastro-pharma-level-two'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("gastro pharma level one process start")
        assert self.source == 'gastro_level_two'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            page = 1
            yield scrapy.Request(url=product_url, callback=self.parse_response,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity["url"]
        # res = json.loads(response.text)
        title = response.css('meta[property="og:title"]::attr(content)').extract_first()
        description = response.css('meta[property="og:description"]::attr(content)').extract_first()
        _id = response.css('meta[name="contextly-page"]::attr(content)').extract_first()
        _id = json.loads(_id)
        _id = _id['post_id']
        full_text_class = "elementor elementor-" + str(_id)
        _article_body = response.css('div[class="' + full_text_class + '"]').extract_first()
        if _article_body is None:
            _article_body = ''
        soup = BeautifulSoup(_article_body, 'html.parser')
        for s in soup(['script', 'style']):
            s.decompose()
        article_body = ' '.join(soup.stripped_strings)
        if '\xa0' in article_body:
            article_body = article_body.replace('\xa0', ' ')
        _created_date = response.css(
            'span[class="elementor-icon-list-text elementor-post-info__item elementor-post-info__item--type-date"]::text').extract_first().strip()
        created_date = dateparser.parse(_created_date)

        try:
            username = response.css(
                'cite[class="elementor-blockquote__author"]::text').extract_first().strip()
        except:
            username = ''

        self.yield_article(
            article_id=_id,
            product_id=product_id,
            created_date=created_date,
            username=username,
            description=description,
            title=title,
            full_text=article_body,
            url=product_url,
            disease_area='',
            medicines='',
            trial='',
            views_count=''
        )
