import json
import hashlib

import datetime
from datetime import timedelta
import scrapy

from .setuserv_spider import SetuservSpider


class WalgreensSpider(SetuservSpider):
    name = 'walgreens-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Walgreens process start")
        assert self.source == 'walgreens'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=self.get_review_url(product_id, limit=8, offset=0),
                                 callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'dont_proxy': True, 'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        lifetime = response.meta['lifetime']

        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        yield scrapy.Request(url=self.get_review_url(product_id, limit=8, offset=0),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        res = json.loads(response.text)
        limit = res['BatchedResults']['q0']['Limit']
        offset = res['BatchedResults']['q0']['Offset']
        review_date = self.start_date

        if res['BatchedResults']['q0']['Includes']:
            try:
                product_name = res['BatchedResults']['q0']['Includes']['Products'][
                    product_id]['Name']
            except:
                product_name = ''
            try:
                brand = res['BatchedResults']['q0']['Includes']['Products'][
                    product_id]['Brand']['Name']
            except:
                brand = ''

            extra_info = {"product_name": product_name, "brand_name": brand}

        if res['BatchedResults']['q0']['Results']:
            for item in res['BatchedResults']['q0']['Results']:
                if item:
                    review_date = datetime.datetime.strptime(
                        (item.get('SubmissionTime').split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            content_type = 'syndicated'
                            source_url = item['SyndicationSource']['Name']

                        except:
                            content_type = 'organic'
                            source_url = 'None'

                        content = {'content_type': content_type, 'source_url': source_url}
                        extra_info.update(content)

                        _id = item["Id"]
                        try:
                            if self.type == 'media':
                                body = item["ReviewText"]
                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title=item['Title'],
                                        body=body,
                                        rating=item["Rating"], url=product_url,
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

            if review_date >= self.start_date:
                offset += limit
                limit = 30
                yield scrapy.Request(url=self.get_review_url(product_id, limit, offset),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity, 'dont_proxy': True})
        else:
            if '"Results":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))

    def parse_comments(self, review, _id, review_date, product_url, product_id, extra_info):
        try:
            if review['ClientResponses']:
                for comment in review['ClientResponses']:
                    comment_date = datetime.datetime.strptime(
                        (comment.get('Date').split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
                    comment_date_until = review_date \
                                         + timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:

                        if comment['Response']:
                            comment_id = comment['Department'] + _id + \
                                         hashlib.sha512(comment['Response']
                                                        .encode('utf-8')).hexdigest()
                            self.yield_items_comments(
                                parent_id=_id,
                                _id=comment_id,
                                comment_date=comment_date,
                                title='',
                                body=comment['Response'],
                                rating='', url=product_url,
                                review_type='comments',
                                creator_id='',
                                creator_name='',
                                product_id=product_id,
                                extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    @staticmethod
    def get_review_url(product_id, limit, offset):
        url = f"https://api.bazaarvoice.com/data/batch.json?" \
              f"passkey=tpcm2y0z48bicyt0z3et5n2xf&apiversion=5.5&displaycode=2001-en_us" \
              f"&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter." \
              f"q0=productid%3Aeq%3A{product_id}&filter.q0=contentlocale%3Aeq%3Aen%2Cen_AU" \
              f"%2Cen_CA%2Cen_GB%2Cen_US&sort.q0=submissiontime%3Adesc&stats." \
              f"q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2C" \
              f"comments&filter_reviews.q0=contentlocale%3Aeq%3Aen%2Cen_" \
              f"AU%2Cen_CA%2Cen_GB%2Cen_US&filter_reviewcomments.q0=contentlocale" \
              f"%3Aeq%3Aen%2Cen_AU%2Cen_CA%2Cen_GB%2Cen_US&filter_comments.q0=contentlocale" \
              f"%3Aeq%3Aen%2Cen_AU%2Cen_CA%2Cen_GB%2Cen_US&limit.q0={limit}" \
              f"&offset.q0={offset}&limit_comments.q0=3"
        return url

    def get_lifetime_ratings(self, product_id, response):
        try:
            _res = json.loads(response.text)
            res = \
                _res['BatchedResults']['q0']['Includes']['Products'][product_id]['ReviewStatistics']
            review_count = res['TotalReviewCount']
            average_ratings = res['AverageOverallRating']
            ratings = res['RatingDistribution']

            rating_map = {}
            _rating_value = []
            for item in ratings:
                _rating_value.append(item['RatingValue'])
                for i in range(1, 6):
                    if i in _rating_value:
                        rating_map['rating_' + str(item['RatingValue'])] = item['Count']
                    else:
                        rating_map['rating_' + str(i)] = 0

            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=float(average_ratings),
                ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
