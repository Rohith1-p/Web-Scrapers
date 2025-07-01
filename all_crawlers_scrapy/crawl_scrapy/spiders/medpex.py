import math
from urllib.parse import urlsplit
import datetime
from bs4 import BeautifulSoup

import scrapy
from .setuserv_spider import SetuservSpider


class MedpexSpider(SetuservSpider):
    name = 'medpex-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Medpex process start")
        assert self.source == 'medpex'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta['media_entity']
        product_url = media_entity['url']
        try:
            product_name = urlsplit(product_url).path.split('/')[2].split('-p')[0]
        except:
            product_name = ''
        try:
            brand = urlsplit(product_url).path.split('/')[2].split('-p')[0].split('-')[0]
        except:
            brand = ''
        extra_info = {"product_name": product_name, "brand_name": brand}
        page = 0
        _total_pages = response.css('span[itemprop=reviewCount]::text').extract_first()
        total_pages = math.floor(int(_total_pages)/10)
        yield scrapy.Request(url=product_url + f"/erfahrungen/", callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity, 'page': page,
                                   'extra_info': extra_info, 'total_pages': total_pages,
                                   'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        extra_info = response.meta['extra_info']
        total_pages = response.meta['total_pages']
        product_url = media_entity['url']
        product_id = media_entity['id']
        _res = response.text
        _res = _res.split('<div class="clearfix">')
        _res = _res[1].split('<script>')[0]
        _res = BeautifulSoup(_res, 'html.parser')
        res = _res.findAll('div', {"class": 'review-list-entry'})
        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    date_and_name = item.find('div', {'class': 'name'}).text.split('am')
                    _review_date = date_and_name[1].replace('.', '-').strip()
                    # review_date = dateparser.parse(_review_date, '%d-%m-%Y')
                    review_date = datetime.datetime.strptime(_review_date, '%d-%m-%Y')

                    _id = item.find('div', {'class': 'title'})['id']
                    if review_date and self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item.find('div', {'class': 'text'}).text.strip()
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item.find('h3').text,
                                         body=body,
                                         rating=item.find('div', {'class': 'title'})
                                         .find('div')['class'][2].split('-')[2],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))
            if review_date >= self.start_date and page < total_pages-1:
                page += 1
                yield scrapy.Request(url=product_url + f"/erfahrungen/{total_pages-page}",
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity, 'page': page,
                                           'extra_info': extra_info,
                                           'total_pages': total_pages, 'dont_proxy': True})
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))
