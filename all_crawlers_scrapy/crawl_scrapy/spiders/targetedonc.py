import scrapy
import dateparser
from urllib.parse import urlparse
import base64
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class TargetedoncSpider(SetuservSpider):
    name = 'targetedonc-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Targetedonc process start")
        assert self.source == 'targetedonc'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=f"{product_url}?page={page}", callback=self.parse_response,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, "page": page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        created_date = self.start_date
        if '/' in product_id:
            product_id = product_id.split('/')[0]
        self.dump(response, 'html', 'Articles_resp', self.source, product_id)

        articles = response.css('div.media-body')
        if articles:
            for item in articles:
                if item:
                    url = 'https://' + urlparse(product_url).netloc + \
                          item.css('div.media-body a::attr(href)').extract_first()
                    _created_date = item.css('p::text').extract_first()
                    created_date = dateparser.parse(_created_date)
                    title = item.css('h4::text').extract_first()

                    if self.start_date <= created_date <= self.end_date:
                        yield scrapy.Request(url=url, callback=self.parse_article,
                                             errback=self.err, dont_filter=True,
                                             meta={'media_entity': media_entity, 'url': url,
                                                   'created_date': created_date, 'title': title})
                        self.logger.info(f"Generating articles for {url}")

            if created_date >= self.start_date:
                page += 1
                yield scrapy.Request(url=f"{product_url}?page={page}", callback=self.parse_response,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity, "page": page})

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        created_date = response.meta['created_date']
        title = response.meta['title']
        url = response.meta['url']

        if 'class="page-title"' not in response.text:
            yield scrapy.Request(url=url, callback=self.parse_article,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'url': url})
            return

        _title = title.replace(' ', '')
        timestamp = str(created_date.timestamp())
        message = timestamp + _title
        message_bytes = message.encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)
        _id = base64_bytes.decode('ascii')

        username = response.css('div.left-wrap a::text').extract_first()
        description = response.css('div.video-detail em::text').extract_first()
        if description is None:
            description = ''
        _article_body = response.css('div[class="block-content mt-3"]').extract_first()
        soup = BeautifulSoup(_article_body, 'html.parser')
        for s in soup(['script', 'style']):
            s.decompose()
        article_body = ' '.join(soup.stripped_strings)
        if '\xa0' in article_body:
            article_body = article_body.replace('\xa0', ' ')

        self.yield_article(
            article_id=_id,
            product_id=product_id,
            created_date=created_date,
            username=username,
            description=description,
            title=title,
            full_text=article_body,
            url=url,
            disease_area='',
            medicines='',
            trial='',
            views_count=''
        )