import json
import dateparser

import scrapy
from .setuserv_spider import SetuservSpider


class Iceheadshop(SetuservSpider):
    name = 'iceheadshop-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Iceheadshop process start")
        assert self.source == 'iceheadshop'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page_count = 1
            yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'page_count': page_count,
                                       'media_entity': media_entity, 'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        page_count = response.meta['page_count']
        res = json.loads(response.text)
        total_pages = int(res['reviews']['last_page'])
        review_date = self.start_date
        if res['reviews']['data']:
            for item in res['reviews']['data']:
                if item:
                    try:
                        product_name = item['product']['name']
                    except:
                        product_name = ''
                    extra_info = {"product_name": product_name, "brand_name": ''}

                    _id = item['product_review_id']
                    review_date = dateparser.parse(item['date_created'])
                    review_date = review_date.replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item['review']
                                title = item['title']
                                if title is None:
                                    title = ''
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=item['rating'],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}"
                                                .format(_id))
            page_count += 1
            if review_date >= self.start_date and page_count <= total_pages:
                yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page_count': page_count,
                                           'media_entity': media_entity, 'dont_proxy': True})
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))
    
    @staticmethod
    def get_review_url(product_id, page_count):
        url = f'https://api.reviews.co.uk/product/review?store=ice-headshop&sku={product_id}' \
              f'&mpn=&lookup=&product_group=&minRating=4&tag=&sort=undefined&per_page=20&page={page_count}'
        return url
