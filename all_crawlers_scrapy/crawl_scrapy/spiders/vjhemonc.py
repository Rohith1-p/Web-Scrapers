import scrapy
import dateparser
import base64
from urllib.parse import urlparse

from datetime import datetime
from bs4 import BeautifulSoup
from .setuserv_spider import SetuservSpider


class VjhemoncSpider(SetuservSpider):
    name = 'vjhemonc-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Vjhemonc process start")
        assert self.source == 'vjhemonc'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=f"{product_url}/page/{page}/?showAllVideos",
                                 callback=self.parse_response,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        page = response.meta['page']
        created_date = self.start_date
        product_id = media_entity['id']
        articles = response.css('div[class="col-sm-6 col-md-3 item"]')

        if articles:
            for item in articles:
                if item:
                    url = item.css('a::attr(href)').extract_first()
                    try:
                        _created_date = item.css('span.video-date::text').extract_first()
                        created_date = dateparser.parse(_created_date)
                    except:
                        pass

                    if self.start_date <= created_date <= self.end_date:
                        yield scrapy.Request(url=url,
                                             callback=self.parse_article,
                                             errback=self.err,
                                             dont_filter=True,
                                             meta={'media_entity': media_entity,
                                                   'url': url,
                                                   'created_date': created_date})
                        self.logger.info(f"Generating articles for {url}")

            if created_date >= self.start_date:
                page += 1
                yield scrapy.Request(url=f"{product_url}/page/{page}/?showAllVideos",
                                     callback=self.parse_response,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           "page": page})

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        created_date = response.meta["created_date"]
        url = response.meta["url"]

        title = response.css('div.single-video-title h1::text').extract_first()

        _title = title.replace(' ', '')
        timestamp = str(created_date.timestamp())
        message = timestamp + _title
        message_bytes = message.encode('utf-8')
        base64_bytes = base64.b64encode(message_bytes)
        _id = base64_bytes.decode('ascii')

        username = response.css('span.speaker-name a::text').extract_first()
        description = response.css('div.description p::text').extract_first()
        res = BeautifulSoup(response.text, 'html.parser')
        res = res.findAll('div', {"class": "subject"})

        val_dict = {}
        for item in res:
            try:
                key = item.find('div', {"class": "subject-label"}).text.strip()
                value = item.find('div', {"class": "subject-terms"}).text.strip()
                val_dict.update({key: value})
            except:
                pass

        if 'Subject:' in val_dict.keys():
            disease_area = val_dict['Subject:'].replace('  ', '')
        else:
            disease_area = ''
        if 'Medicines:' in val_dict.keys():
            medicines = val_dict['Medicines:'].replace('  ', '')
        else:
            medicines = ''
        if 'Trial:' in val_dict.keys():
            trial = val_dict['Trial:'].replace('  ', '')
        else:
            trial = ''

        self.yield_article(
            article_id=_id,
            product_id=product_id,
            created_date=created_date,
            username=username,
            description=description,
            title=title,
            full_text=str(title) + '. ' + str(description),
            url=url,
            disease_area=disease_area,
            medicines=medicines,
            trial=trial,
            views_count=''
        )
