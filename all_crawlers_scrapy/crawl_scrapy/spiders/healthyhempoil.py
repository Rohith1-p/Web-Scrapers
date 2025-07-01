import json
from datetime import timedelta

import dateparser
import scrapy
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class HealthyhempoilSpider(SetuservSpider):
    name = 'healthyhempoil-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Healthyhempoil Oil process start")
        assert self.source == 'healthyhempoil'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        try:
            product_name = response.css('h1.entry-title::text').extract_first()
            product_name = product_name.strip()
        except:
            product_name = ''
        try:
            brand = product_name.split(':')[0]
        except:
            brand = ''
        extra_info = {"product_name": product_name, "brand_name": brand}
        page = 1
        yield scrapy.Request(url=self.get_review_url(page, product_id),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity,
                                   'extra_info': extra_info, 'page': page,
                                   'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta["extra_info"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta["page"]
        _res = json.loads(response.text)[0]
        _res = _res["result"]
        _res = BeautifulSoup(_res, 'html.parser')
        res = _res.findAll('div', attrs={'class': 'yotpo-review'})[1:]

        if res:
            for item in res:
                if item:
                    _id = item['data-review-id']
                    review_date = dateparser.parse(item.find
                                                   ('span', {'class': 'y-label yotpo-review-date'}).text)
                    try:
                        if self.type == 'media':
                            body = item.find('div', {'class': 'content-review'}).text.strip()
                            if body:
                                if self.start_date <= review_date <= self.end_date:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item.find
                                         ('div', {'class': 'content-title'}).text,
                                         body=body,
                                         rating=item.find
                                         ('span', {'class': 'sr-only'}).text.split(' ')[0],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)

                                    self.parse_comments(item, _id, review_date,
                                                                   product_url, product_id,
                                                                   extra_info)
                        if self.type == 'comments':
                            self.parse_comments(item, _id, review_date,
                                                           product_url, product_id,
                                                           extra_info)
                    except:
                        self.logger.warning("Body is not their for review {}"
                                            .format(_id))

            page += 1
            yield scrapy.Request(url=self.get_review_url(page, product_id),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 meta={'media_entity': media_entity,
                                       'extra_info': extra_info, 'page': page,
                                       'dont_proxy': True})
        else:
            if '"method":"reviews"' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            comment_date = item.find("div", {"class": "yotpo-comment-box yotpo-comment"})\
                .find("span", {"class": "y-label yotpo-review-date"}).text
            comment_date = dateparser.parse(comment_date)
        except:
            comment_date = ''
        try:
            comment = item.find("div", {"class": "yotpo-comment-box yotpo-comment"}) \
                .find("div", {"class": "content-review"}).text
        except:
            comment = ''
        try:
            comment_id = item.find("div", {"class": "yotpo-comment-box yotpo-comment"}) \
                .get('data-comment-id')
        except:
            comment_id = ''

        try:
            comment_date_until = review_date + \
                                 timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:

                if comment:
                    self.yield_items_comments \
                        (parent_id=_id,
                         _id=comment_id,
                         comment_date=comment_date,
                         title='',
                         body=comment,
                         rating='',
                         url=product_url,
                         review_type='comments',
                         creator_id='',
                         creator_name='',
                         product_id=product_id,
                         extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} on "
                             f"review {_id}")
    
    @staticmethod
    def get_review_url(page, product_id):
        url = "https://staticw2.yotpo.com/batch?methods=%5B%7B%22method%22%3A%22reviews" \
              "%22%2C%22params%22%3A%7B%22page%22%3A{}%2C%22host-widget%22%3A%22main_widget" \
              "%22%2C%22pid%22%3A%22{}%22%2C%22locale%22%3A%22en%22%7D%7D%5D&app_key=8QFvwQI5" \
              "BrmA5LeXI0e32i2JJuFVChFScCeDpSBO&is_mobile=false&widget_version=2019-04-08_13-50-35"\
            .format(page, product_id)
        return url
