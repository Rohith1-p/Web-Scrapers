import scrapy
from urllib.parse import urlparse
import base64
import dateparser
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class LymphomahubSpider(SetuservSpider):
    name = 'lymphomahub-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Lymphomahub process start")
        assert self.source == 'lymphomahub'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=f"{product_url}?page={page}", callback=self.parse_response,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        page = response.meta['page']
        created_date = self.start_date

        articles = response.css('div[class="sc-bdVaJa ffgIZM"] div[class="sc-bdVaJa rRrdd"]')

        print(articles)
        if articles:
            for item in articles:
                if item:
                    url = 'https://' + urlparse(product_url).netloc + item.css(
                        'div[class="sc-bdVaJa gkLaIs"] a::attr(href)').extract_first()
                    print(url)
                    _created_date = item.css('h6[display="inline-block"]::text').extract_first().strip()
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
        created_date = response.meta["created_date"]
        url = response.meta["url"]

        title = response.css('div[id="app"] h1::text').extract_first()

        _title = title.replace(' ', '')
        timestamp = str(created_date.timestamp())
        message = timestamp + _title
        message_bytes = message.encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)
        _id = base64_bytes.decode('ascii')

        try:
            _username = response.css('div[class="sc-bdVaJa fwihsJ"] a::text').extract()
            username = ''
            for _, __body in enumerate(_username):
                username += __body
        except:
            username = ''

        _description = response.css('p.lead span::text').extract()
        print(len(_description))
        print('book')
        if len(_description) == 0 or  len(_description) == 1:
            _description = response.css('p.lead ::text').extract()

        description = ''
        for _, __body in enumerate(_description):
            description += __body


        _res = response.text.split('class="sc-bdVaJa bzXYuE"')[1].split('class="sc-bdVaJa hHEaEq"')[0].split('</p>')[1:]
        _res = ''.join(_res)
        res = BeautifulSoup(_res, 'html.parser')
        res = res.findAll()

        article_body = ''
        for item in res:
            val_text = item.text.strip()
            if '\n' in article_body:
                article_body.replace('\n', ' ').replace('\xa0', '')
            article_body += val_text
        try:
            if 'References' in response.text:
                _references = response.css('div[class="sc-htpNat hGSRsY"] ol li::text').extract()
                references = ''
                for _, __body in enumerate(_references):
                    references += __body
                references = 'References' + references
                article_body += references
        except:
            pass

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
