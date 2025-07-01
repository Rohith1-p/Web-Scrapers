import re
import math
import hashlib
from datetime import timedelta

import dateparser
import scrapy
from .setuserv_spider import SetuservSpider


class ReviewscoSpider(SetuservSpider):
    name = 'reviewsco-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Reviewsco process start")
        assert self.source == 'reviewsco'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        try:
            product_name = response.css('h1[class="TextHeading TextHeading--md"]::text').extract_first().strip()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        page = 0
        yield scrapy.Request(url=product_url, callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page, 'extra_info': extra_info,
                                   'media_entity': media_entity})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        extra_info = response.meta["extra_info"]
        page = response.meta["page"]

        count = response.css('span[class="TextBody TextBody--sm TextBody--inline js-reviewsio-review-count"] '
                             'strong::text').extract_first().replace(',', '')
        total_pages = math.ceil(int(count) / 20)
        res = response.css('div.Review')
        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    try:
                        _rating = str(item.css('div.Review__overallStars__stars').extract_first())
                        _rating = str(re.split(r'\s', _rating))
                        _rating = str(re.findall(r'icon-full-star-01', _rating))
                        rating = _rating.count('icon-full-star-01')
                    except:
                        rating = 0

                    _id = item.css('::attr(data-review-id)').extract_first()
                    _review_date = item.css('div.Review__dateSource::text').extract_first()
                    _review_date = dateparser.parse(str(_review_date.split('Posted')[1].strip()))
                    review_date = _review_date.replace(hour=0, minute=0,
                                                       second=0, microsecond=0)

                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item.css('span.Review__body::text')\
                                    .extract_first().strip().replace('\n', '')
                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title='',
                                        body=body,
                                        rating=rating,
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        extra_info=extra_info)

                                    self.parse_comments(item, _id, review_date,
                                                                   product_url, product_id)
                            if self.type == 'comments':
                                self.parse_comments(item, _id, review_date,
                                                               product_url, product_id)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            page += 1
            if review_date >= self.start_date and page < total_pages:
                yield scrapy.Request(url=self.get_review_url(product_url, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page': page, 'extra_info': extra_info,
                                           'media_entity': media_entity})
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    def parse_comments(self, item, _id, review_date, product_url, product_id):
        try:
            _comment = item.css('span.reply__body::text').extract()
            comment = ''
            for _, i in enumerate(_comment):
                comment += i
        except:
            comment = ''
        try:
            _comment_date = item.css('div.reply__date::text').extract_first()
            comment_date = dateparser.parse(str(_comment_date.split('Posted')[1].strip()))
        except:
            comment_date = ''
        try:
            _department = item.css('div.SL__c img::attr(alt)').extract()[0]
            department = _department.replace('Read', '').replace('Reviews', '').strip()
        except:
            department = ''
        try:
            comment_date_until = review_date \
                                 + timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:
                if comment:
                    comment_id = department + _id \
                                 + hashlib.sha512(comment.encode('utf-8')).hexdigest()

                    self.yield_items_comments\
                        (parent_id=_id,
                         _id=comment_id,
                         comment_date=comment_date,
                         title='',
                         body=comment.replace('\n', ''),
                         rating='',
                         url=product_url,
                         review_type='comments',
                         creator_id='',
                         creator_name='',
                         product_id=product_id)
        except:
            self.logger.info(f"There is no comment for product_id "
                             f"{product_id} on review {_id}")

    @staticmethod
    def get_review_url(product_url, page):
        url = f"{product_url}/{page}"
        return url
