import re
import datetime
from datetime import timedelta
import hashlib
import scrapy
from .setuserv_spider import SetuservSpider


class AponeoSpider(SetuservSpider):
    name = 'aponeo-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Aponeo process start")
        assert self.source == 'aponeo'

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
        product_id = media_entity["id"]

        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        try:
            product_name = response.css('h1[class="apn-product-detail-title"]::text')\
                .extract_first().strip()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ""}
        yield scrapy.Request(url=product_url, callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity, 'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta["extra_info"]
        res = response.css('div#apn-product-ratings div[class="apn-product-detail-rating flex-column '
                           'align-items-start apn-text-14-16 apn-mb-md-40"]')

        if res:
            for item in res:
                if item:
                    _review_date = item.css('div[class="col-12 col-md-auto apn-mb-10 '
                                            'font-weight-bold apn-text-16"]::text').extract_first().strip()
                    _review_date = re.findall(r'\d+', _review_date)
                    _review_date = _review_date[-3] + '.' + _review_date[-2] + '.' + _review_date[-1]
                    _id = item.css('div[class="modal-body"] input[name="rId"]::attr(value)').extract_first()
                    review_date = datetime.datetime.strptime(_review_date, '%d.%m.%Y')
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                _rating = item.css('div[class="col-12 col-md-auto apn-mb-10"]').extract_first()
                                _rating = re.findall(r'class="apn-icon active"', str(_rating))
                                rating = _rating.count('class="apn-icon active"')
                                body = item.css(
                                    'div[class="apn-mb-10 apn-max-width-1200 apn-copy"]::text').extract_first().strip()
                                if body:
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body.strip().replace('\r\n\r\n', ''),
                                         rating=rating,
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
                            self.logger.warning(f"Body is not their for review {_id}")

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        comments = item.css('div[class="comments col-12 apn-max-width-1200"]')
        if comments:
            for comment in comments:
                _comment_date = comment.css('div[class="col-12 col-md-auto apn-mb-10 font-weight-bold apn-text-16"]'
                                           '::text').extract_first().strip().split(' ')[-1]
                comment_date = datetime.datetime.strptime(_comment_date, '%d.%m.%Y')
                comment_body = comment.css('div[class="apn-mb-10 apn-copy"]::text').extract_first().strip()

                comment_date_until = review_date + timedelta(days=SetuservSpider.settings.get('PERIOD'))
                if comment_date <= comment_date_until:
                    if comment_body:
                        comment_id = _comment_date + hashlib.sha512(comment_body.encode('utf-8')).hexdigest()
                        self.yield_items_comments \
                            (parent_id=_id,
                             _id=comment_id,
                             comment_date=comment_date,
                             title='',
                             body=comment_body,
                             rating='',
                             url=product_url,
                             review_type='comments',
                             creator_id='',
                             creator_name='',
                             product_id=product_id,
                             extra_info=extra_info)
        else:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    def get_lifetime_ratings(self, product_id, response):
        try:
            _review_count = response.css('span[class="apn-product-detail-rating-count apn-ml-5"]::text')\
                .extract_first()
            review_count = int(re.findall(r'\d+', _review_count)[0])
            ratings = response.css('div[class="apn-product-detail-rating-bar-progress"]::attr(style)').extract()[::-1]

            rating_map = {}
            _rating_value = []
            for i in range(1, 6):
                value = round(int(re.findall(r'\d+', ratings[i - 1])[0]) * review_count / 100)
                _rating_value.append(value)
                rating_map['rating_' + str(i)] = value

            _average_ratings = []
            for i in range(1, 6):
                avg_item = i * _rating_value[i - 1]
                _average_ratings.append(avg_item)
            average_ratings = sum(_average_ratings) / review_count

            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")