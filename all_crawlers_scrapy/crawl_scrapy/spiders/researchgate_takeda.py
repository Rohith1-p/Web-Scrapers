import scrapy
from urllib.parse import urlparse
import base64
import dateparser
from bs4 import BeautifulSoup
import json
from .setuserv_spider import SetuservSpider
from scrapy.http import FormRequest


class ResearchgateSpider(SetuservSpider):
    name = 'researchgate-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("researchgate process start")
        assert self.source == 'researchgate'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            urls = product_url + '&page=' + str(page)
            print('urls', urls)
            yield scrapy.Request(url=urls, callback=self.parse_response,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        created_date = self.start_date
        articles = response.css(
            'div[class="nova-legacy-o-stack nova-legacy-o-stack--gutter-xs nova-legacy-o-stack--spacing-none nova-legacy-o-stack--no-gutter-outside"] div[class="nova-legacy-o-stack__item"]')

        print('articles', articles)
        if articles:
            count = 0
            for item in articles:
                if item:
                    print(count)
                    count += 1
                    article_url = item.css(
                        'div[class="nova-legacy-e-text nova-legacy-e-text--size-l nova-legacy-e-text--family-sans-serif nova-legacy-e-text--spacing-none nova-legacy-e-text--color-inherit nova-legacy-v-publication-item__title"] a::attr(href)').extract_first()
                    article_url = 'https://www.researchgate.net/' + article_url
                    _created_date = item.css(
                        'li[class="nova-legacy-e-list__item nova-legacy-v-publication-item__meta-data-item"] span::text').extract_first()
                    _created_date = '01 ' + str(_created_date)
                    created_date = dateparser.parse(_created_date)

                if self.start_date <= created_date <= self.end_date:
                    yield scrapy.Request(url=article_url, callback=self.parse_article,
                                         errback=self.err, dont_filter=True,
                                         meta={'media_entity': media_entity, 'url': article_url,
                                               'created_date': created_date})
                    self.logger.info(f"Generating articles for {article_url}")

        if created_date >= self.start_date:
            page += 1
            urls = product_url + '&page=' + str(page)
            print('urls', urls)
            yield scrapy.Request(url=urls, callback=self.parse_response,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, "page": page})

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        created_date = response.meta["created_date"]
        article_url = response.meta["url"]

        title = response.css(
            'h1[class="nova-legacy-e-text nova-legacy-e-text--size-xl nova-legacy-e-text--family-sans-serif nova-legacy-e-text--spacing-none nova-legacy-e-text--color-grey-900 research-detail-header-section__title"] ::text').extract_first()
        _title = title.replace(' ', '')
        timestamp = str(created_date.timestamp())
        message = timestamp + _title

        message_bytes = message.encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)
        article_id = base64_bytes.decode('ascii')
        username = response.css(
            'div[class="nova-legacy-e-text nova-legacy-e-text--size-m nova-legacy-e-text--family-sans-serif nova-legacy-e-text--spacing-none nova-legacy-e-text--color-inherit nova-legacy-e-text--clamp nova-legacy-v-person-list-item__title"] ::text').extract()
        extra_info = ''
        description = response.css(
            'div[class="nova-legacy-l-flex__item nova-legacy-l-flex__item--grow research-detail-middle-section__item"] ::text').extract()
        description = " ".join(str(x) for x in description)
        description.replace(
            "Discover the world's research 20+ million members 135+ million publications 700k+ research projects Join for free",
            '').strip()
        full_text = response.css('div[class="pf w0 h0"] ::text').extract()
        full_text_image = response.css('div[class="pf w0 hc"] ::text').extract()
        full_text_image = " ".join(str(x) for x in full_text_image)
        full_text = " ".join(str(x) for x in full_text)
        full_text = str(full_text) + str(full_text_image)
        if len(full_text) <= 10:
            full_text = description

        self.yield_research_sources(
            article_id=article_id,
            product_id=product_id,
            created_date=created_date,
            author_name=str(username),
            description=description,
            full_text=full_text,
            product_url=product_url,
            title=title,
            url=article_url,
            article_link=article_url,
            extra_info=str(extra_info)
        )
