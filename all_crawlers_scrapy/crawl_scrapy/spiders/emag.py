import json

from datetime import timedelta
import dateparser

import scrapy
from .setuserv_spider import SetuservSpider


class EmagSpider(SetuservSpider):
    name = 'emag-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Emag process start")
        assert self.source == 'emag'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        lifetime = response.meta['lifetime']

        if lifetime:
            yield scrapy.Request(url=self.get_review_url(product_url, 0),
                                 callback=self.get_lifetime_ratings,
                                 meta={'media_entity': media_entity})
        try:
            product_name = response.css('h1.page-title::text').extract_first().strip()
        except:
            product_name = ''
        try:
            brand_name = response.css('div.disclaimer-section a::text').extract_first()
        except:
            brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}
        page = 0
        yield scrapy.Request(url=self.get_review_url(product_url, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page, 'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_id = media_entity['id']
        product_url = media_entity['url']
        page = response.meta['page']
        res = json.loads(response.text)
        total_count = res['reviews']['count']
        review_date = self.start_date

        try:
            if res['reviews']['items']:
                for item in res['reviews']['items']:
                    if item:

                        _id = item["id"]
                        review_date = dateparser.parse(item["created"]).replace(tzinfo=None)
                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    _body = item['content'].strip().replace('<br />', '')
                                    if _body:
                                        self.yield_items \
                                            (_id=_id,
                                             review_date=review_date,
                                             title=item['title'],
                                             body=_body,
                                             rating=item["rating"],
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
                                self.logger.warning("Body is not their for review {}".format(_id))
                page += 10
                if review_date >= self.start_date and page < total_count:
                    yield scrapy.Request(url=self.get_review_url(product_url, page),
                                         callback=self.parse_reviews,
                                         errback=self.err,
                                         meta={'page': page, 'media_entity': media_entity,
                                               'extra_info': extra_info})
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))
        except:
            self.logger.info(f"No more reviews for {product_id}")

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            if item['comments']:
                for comment in item['comments']:
                    comment_date = dateparser.parse(comment['created']).replace(tzinfo=None)
                    comment_date_until = review_date +\
                                         timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:
                        _comment = comment['content'].strip().replace('<br />', '')
                        if _comment:
                            self.yield_items_comments \
                                (parent_id=_id,
                                 _id=comment['id'],
                                 comment_date=comment_date,
                                 title='',
                                 body=_comment,
                                 rating='',
                                 url=product_url,
                                 review_type='comments',
                                 creator_id='',
                                 creator_name='',
                                 product_id=product_id,
                                 extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    @staticmethod
    def get_review_url(product_url, page):
        country_code = product_url.split('/')[2].split('.')[2]
        if country_code in ['ro', 'hu']:
            _product_url = product_url.split(country_code + '/')[1]
            url = f"https://www.emag.{country_code}/product-feedback/{_product_url}reviews/" \
                f"list?source_id=7&token=&page%5Boffset%5D={page}&page%5Blimit%5D=10&sort" \
                f"%5Bcreated%5D=desc"
            return url

    def get_lifetime_ratings(self, response):
        try:
            media_entity = response.meta["media_entity"]
            product_id = media_entity["id"]
            _res = json.loads(response.text)
            res = _res['reviews']
            review_count = res['count']
            ratings = res['rating_distribution']

            _average_ratings = []
            for i in range(1, 6):
                _average_ratings.append(ratings[str(i)]*i)
            average_ratings = sum(_average_ratings) / review_count

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[str(i)]

            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
