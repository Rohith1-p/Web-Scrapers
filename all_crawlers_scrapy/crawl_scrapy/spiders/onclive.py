import scrapy
import dateparser
from urllib.parse import urlparse
import base64
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class OncliveSpider(SetuservSpider):
    name = 'onclive-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Onclive process start")
        assert self.source == 'onclive'

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

        articles = response.css('div.Deck_article__Plw_t')
        if len(articles) == 0:
            articles = response.css('div[class="jsx-726900813 conference-media"]')
        if 'rapid-readouts' in product_id:
            articles = response.css('a.jsx-597562438')
        if 'latest-conference' in product_id:
            articles = response.css('div[class="jsx-4260365105 conference-media"]')

        if articles:
            for item in articles:
                if item:
                    url = 'https://' + urlparse(product_url).netloc + item.css('a::attr(href)').extract_first()
                    _created_date = item.css('p.Deck_published__7hlkj::text').extract_first()
                    if _created_date is None:
                        _created_date = item.css('p[class="jsx-726900813 conf-media__card-text"]::text').extract_first()
                    if 'rapid-readouts' in product_id:
                        _created_date = item.css('p[class="jsx-597562438 date"]::text').extract_first()
                    if 'latest-conference' in product_id:
                        _created_date = item.css('p[class="jsx-4260365105 conf-media__card-text"]::text').extract_first()
                    created_date = dateparser.parse(_created_date)

                    if self.start_date <= created_date <= self.end_date:
                        yield scrapy.Request(url=url, callback=self.parse_article,
                                             errback=self.err, dont_filter=True,
                                             meta={'media_entity': media_entity, 'url': url,
                                                   'created_date': created_date})
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
        url = response.meta['url']

        title = response.css('h1.page-title::text').extract_first()
        if product_id == 'rapid-readouts':
            title = response.text.split('<title>')[1].split('</title>')[0].replace(' | OncLive', '')

        _title = title.replace(' ', '')
        timestamp = str(created_date.timestamp())
        message = timestamp + _title
        message_bytes = message.encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)
        _id = base64_bytes.decode('ascii')

        username = response.css('div.left-wrap a::text').extract_first()
        description = response.css('div.video-detail em::text').extract_first()
        if product_id != 'rapid-readouts':
            description = description.replace('\n', '')
        _article_body = response.css('div[class="block-content mt-3"]').extract_first()
        if _article_body is None:
            _article_body = ''
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


