import json
import datetime
from datetime import timedelta
import scrapy

from .setuserv_spider import SetuservSpider


class WilkoSpider(SetuservSpider):
    name = 'wilko-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Wilko process start")
        assert self.source == 'wilko'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=self.get_review_url(product_id, limit=5, offset=0),
                                 callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'dont_proxy': True,
                                       'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        lifetime = response.meta['lifetime']

        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        yield scrapy.Request(url=self.get_review_url(product_id, limit=5, offset=0),
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

        review_date = self.start_date

        if res['BatchedResults']['q0']['Results']:
            for item in res['BatchedResults']['q0']['Results']:
                if item:
                    review_date = datetime.datetime.strptime((
                        item.get('SubmissionTime').split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
                    if self.start_date <= review_date <= self.end_date:
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
                                        rating=item["Rating"],
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        extra_info=extra_info)

                                    if len(item['CommentIds']) > 0:
                                        for comment_id in item['CommentIds']:
                                            self.parse_comments(
                                                res['BatchedResults']['q0'], comment_id,
                                                review_date, product_url, product_id,
                                                extra_info)
                            if self.type == 'comments':
                                if len(item['CommentIds']) > 0:
                                    for comment_id in item['CommentIds']:
                                        self.parse_comments(
                                            res['BatchedResults']['q0'], comment_id,
                                            review_date, product_url, product_id,
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

    def parse_comments(self, _review, comment_id, review_date, product_url, product_id, extra_info):
        comment = _review['Includes']['Comments'][comment_id]

        if comment:
            comment_date = datetime.datetime.strptime(
                (comment['SubmissionTime'].split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
            comment_date_until = review_date \
                                 + timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:
                if comment['CommentText']:
                    self.yield_items_comments(
                        parent_id=comment['ReviewId'],
                        _id=comment['Id'],
                        comment_date=comment_date,
                        title='',
                        body=comment['CommentText'],
                        rating='',
                        url=product_url,
                        review_type='comments',
                        creator_id='',
                        creator_name='',
                        product_id=product_id,
                        extra_info=extra_info)

    @staticmethod
    def get_review_url(product_id, limit, offset):
        url = f"https://api.bazaarvoice.com/data/batch.json?" \
              f"passkey=caLFENkeam0oec6f97cdfwiL2aHql2eBMgVZxWMLhWPm4&apiversion=5.5" \
              f"&displaycode=6551-en_gb&resource.q0=reviews&filter.q0=isratingsonly%" \
              f"3Aeq%3Afalse&filter.q0=productid%3Aeq%3A{product_id}&filter." \
              f"q0=contentlocale%3Aeq%3Aen_GB&sort.q0=submissiontime%3Adesc&stats." \
              f"q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2" \
              f"Ccomments&filter_reviews.q0=contentlocale%3Aeq%3Aen_GB" \
              f"&filter_reviewcomments.q0=contentlocale%3Aeq%3Aen_GB&filter_comments." \
              f"q0=contentlocale%3Aeq%3Aen_GB&limit.q0={limit}&offset.q0={offset}" \
              f"&limit_comments.q0=3"\

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
