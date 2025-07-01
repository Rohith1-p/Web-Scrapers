import json
import hashlib

from datetime import timedelta
import datetime

from urllib.parse import urlsplit
import scrapy
from .setuserv_spider import SetuservSpider


class DrogeriemarktSpider(SetuservSpider):
    name = 'drogeriemarkt-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Drogerie markt process start")
        assert self.source == 'drogeriemarkt'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            country_code = urlsplit(product_url).netloc[-2:]
            yield scrapy.Request(url=self.get_review_url(product_id, country_code,
                                                         limit=8, offset=0),
                                 callback=self.parse_info, errback=self.err,
                                 dont_filter=True, meta={'media_entity': media_entity,
                                                         'country_code': country_code,
                                                         'dont_proxy': True,
                                                         'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        country_code = response.meta["country_code"]
        lifetime = response.meta['lifetime']

        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        yield scrapy.Request(url=self.get_review_url(product_id, country_code,
                                                     limit=8, offset=0),
                             callback=self.parse_reviews, errback=self.err,
                             dont_filter=True, meta={'media_entity': media_entity,
                                                     'country_code': country_code,
                                                     'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        country_code = response.meta["country_code"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        res = json.loads(response.text)
        limit = res['BatchedResults']['q0']['Limit']
        offset = res['BatchedResults']['q0']['Offset']

        if res['BatchedResults']['q0']['Includes']:
            try:
                product_name = \
                    res['BatchedResults']['q0']['Includes']['Products'][product_id]['Name']
            except:
                product_name = ''
            try:
                brand = res['BatchedResults']['q0']['Includes']['Products']
                brand = brand[product_id]['Brand']['Name']
            except:
                brand = ''

            extra_info = {"product_name": product_name, "brand_name": brand}

        review_date = self.start_date

        if res['BatchedResults']['q0']['Results']:
            for item in res['BatchedResults']['q0']['Results']:
                if item:
                    try:
                        content_type = 'syndicated'
                        source_url = item['SyndicationSource']['Name']

                    except:
                        content_type = 'organic'
                        source_url = 'None'

                    content = {'content_type': content_type, 'source_url': source_url}
                    extra_info.update(content)

                    _id = item["Id"]
                    review_date = datetime.datetime.strptime\
                        ((item.get('SubmissionTime').split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["ReviewText"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
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
                yield scrapy.Request(url=self.get_review_url(product_id, country_code,
                                                             limit, offset),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity,
                                           'country_code': country_code,
                                           'dont_proxy': True})
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
                    comment_date = datetime.datetime.strptime\
                        ((comment.get('Date').split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
                    comment_date_until = review_date + \
                                         timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:

                        if comment['Response']:
                            comment_id = comment['Department'] + _id +\
                                         hashlib.sha512(comment['Response']
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

    @staticmethod
    def get_review_url(product_id, country_code, limit, offset):
        if country_code and country_code in {'de', 'at'}:
            return {
                'de': "https://api.bazaarvoice.com/data/batch.json?"
                      "passkey=caYXUVe0XKMhOqt6PdkxGKvbfJUwOPDhKaZoAyUqWu2KE&apiversion=5.5"
                      "&displaycode=18357-de_de&resource.q0=reviews&filter.q0=isratingsonly%3"
                      "Aeq%3Afalse&filter.q0=productid%3Aeq%3A{}&filter.q0=contentlocale%3Aeq"
                      "%3Ade*%2Cde_DE&sort.q0=submissiontime%3Adesc&stats.q0=reviews"
                      "&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments"
                      "&filter_reviews.q0=contentlocale%3Aeq%3Ade*%2Cde_DE&filter_review"
                      "comments.q0=contentlocale%3Aeq%3Ade*%2Cde_DE&filter_comments.q0="
                      "contentlocale%3Aeq%3Ade*%2Cde_DE&limit.q0={}&offset.q0={}"
                      "&limit_comments.q0=3".format(product_id, limit, offset),
                'at': "https://api.bazaarvoice.com/data/batch.json?"
                      "passkey=caYecgZ30DOIpvVQcpRszSstiUUjMHBE3i42dVla3aKgY&apiversion=5.5"
                      "&displaycode=17227-de_at&resource.q0=reviews&filter.q0=isratingsonly%3"
                      "Aeq%3Afalse&filter.q0=productid%3Aeq%3A{}&filter.q0=contentlocale%3Aeq"
                      "%3Ade*%2Cde_AT&sort.q0=submissiontime%3Adesc&stats.q0=reviews"
                      "&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments"
                      "&filter_reviews.q0=contentlocale%3Aeq%3Ade*%2Cde_AT&filter_review"
                      "comments.q0=contentlocale%3Aeq%3Ade*%2Cde_AT&filter_comments.q0="
                      "contentlocale%3Aeq%3Ade*%2Cde_AT&limit.q0={}&offset.q0={}"
                      "&limit_comments.q0=3".format(product_id, limit, offset)
            }[country_code]

    def get_lifetime_ratings(self, product_id, response):
        try:
            _res = json.loads(response.text)
            res = _res['BatchedResults']['q0']['Includes']['Products'][product_id]
            res = res['ReviewStatistics']
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
            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
