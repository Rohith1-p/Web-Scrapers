import json
import hashlib

from datetime import timedelta
import dateparser

import scrapy
from .setuserv_spider import SetuservSpider


class TargetSpider(SetuservSpider):
    name = 'target-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Target process start")
        assert self.source == 'target'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            yield scrapy.Request(url=self.get_ltr_utl(product_id),
                                 callback=self.parse_lifetime_ratings,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_lifetime_ratings(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity["url"]
        lifetime = response.meta["lifetime"]

        if 'accessDenied' in response.text:
            self.logger.info(f"Captcha Found for {product_id}")
            yield scrapy.Request(url=self.get_ltr_utl(product_id),
                                 callback=self.parse_lifetime_ratings,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'lifetime': True})
            return

        if lifetime:
            self.get_lifetime_ratings(product_id, response)

        if self.type == 'media':
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity})
        else:
            self.logger.info(f"Type is not media for {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]

        try:
            product_name = \
                response.css('h1[data-test="product-title"] span::text').extract_first()
        except:
            product_name = ''
        try:
            brand_name = \
                response.css('a[class="Link-sc-1khjl8b-0 ftgTJf"] span::text').extract()[1]
        except:
            brand_name = ''

        extra_info = {"product_name": product_name, "brand_name": brand_name}

        page_count = 0
        yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page_count': page_count,
                                   'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_id = media_entity['id']
        product_url = media_entity['url']
        page_count = response.meta['page_count']

        if 'accessDenied' in response.text:
            self.logger.info(f"Captcha Found for {product_id}")
            yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'page_count': page_count,
                                       'media_entity': media_entity,
                                       'extra_info': extra_info})
            return

        res = json.loads(response.text)
        current_page = res['offset']
        total_pages = res['totalResults']
        review_date = self.start_date

        try:
            if res['result']:
                for item in res['result']:
                    if item:
                        try:
                            content_type = 'syndicated'
                            source_url = item['SyndicationSource']['Name']

                        except:
                            content_type = 'organic'
                            source_url = 'None'

                        content = {'content_type': content_type, 'source_url': source_url}

                        extra_info.update(content)
                        try:
                            title = item['Title']
                        except:
                            title = ''

                        _id = item["Id"]
                        review_date = dateparser.parse(item["SubmissionTime"]).replace(tzinfo=None)

                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    if item["ReviewText"]:
                                        self.yield_items \
                                            (_id=_id,
                                             review_date=review_date,
                                             title=title,
                                             body=item["ReviewText"],
                                             rating=item["Rating"],
                                             url=product_url,
                                             review_type='media',
                                             creator_id='',
                                             creator_name='',
                                             product_id=product_id,
                                             extra_info=extra_info)

                                        self.parse_comments \
                                            (item, _id, review_date, product_url,
                                             product_id, extra_info)

                                if self.type == 'comments':
                                    self.parse_comments \
                                        (item, _id, review_date, product_url,
                                         product_id, extra_info)
                            except:
                                self.logger.warning(f"Body is not their for review {_id}")

                next_page = current_page + 10
                if review_date >= self.start_date and next_page < total_pages:
                    page_count += 10
                    yield scrapy.Request(url=self.get_review_url(product_id, next_page),
                                         callback=self.parse_reviews,
                                         errback=self.err,
                                         meta={'page_count': page_count,
                                               'media_entity': media_entity,
                                               'extra_info': extra_info})
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(current_page))
        except:
            self.logger.info(f"No more reviews for {product_id}")

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            if item['ClientResponses']:
                for comment in item['ClientResponses']:
                    comment_date = \
                        dateparser.parse(comment['Date']).replace(tzinfo=None)
                    comment_date_until = review_date + \
                                         timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:

                        if comment['Response']:
                            comment_id = comment['Department'] + _id \
                                         + hashlib.sha512(comment['Response']
                                                          .encode('utf-8')).hexdigest()
                            self.yield_items_comments \
                                (parent_id=_id,
                                 _id=comment_id,
                                 comment_date=comment_date,
                                 title='',
                                 body=comment['Response'],
                                 rating='',
                                 url=product_url,
                                 review_type='comments',
                                 creator_id='',
                                 creator_name='',
                                 product_id=product_id,
                                 extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id}"
                             f" on review {_id}")

    def get_lifetime_ratings(self, product_id, response):
        print(response.text)
        try:
            _res = json.loads(response.text)
            res = _res['statistics']['rating']
            review_count = res['count']
            average_ratings = res['average']
            ratings = res['distribution']
            print("Values->", review_count, average_ratings, ratings)

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[f'{i}']

            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=float(average_ratings),
                ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")

    @staticmethod
    def get_review_url(product_id, page_count):
        url = f'https://redsky.target.com/groot-domain-api/v1/reviews/{product_id}' \
              f'?sort=time_desc&filter=&limit=10&offset={page_count}'
        return url

    @staticmethod
    def get_ltr_utl(product_id):
        url = f'https://r2d2.target.com/ggc/v2/summary?key=9f36aeafbe60771e321a7cc95a78140772ab3e96&hasOnlyPhotos=false' \
              f'&includes=reviews%2CreviewsWithPhotos%2Centities%2Cmetadata%2Cstatistics&page=0&entity=&ratingFilter=&' \
              f'reviewedId={product_id}'
        return url
