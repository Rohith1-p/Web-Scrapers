import json
import hashlib

from datetime import timedelta
from urllib.parse import urlsplit
import dateparser

import scrapy
from .setuserv_spider import SetuservSpider


class SamsclubSpider(SetuservSpider):
    name = 'samsclub-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("samsclub process start")
        assert self.source == 'samsclub'

    def start_requests(self):

        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            country_code = urlsplit(product_url).netloc.split('.')[-1]
            page_count = 0
            yield scrapy.Request(url=self.get_review_url(product_id, country_code, page_count),
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'page_count': page_count,
                                       'media_entity': media_entity,
                                       'country_code': country_code,
                                       'dont_proxy': True,
                                       'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        page_count = response.meta['page_count']
        country_code = response.meta["country_code"]
        lifetime = response.meta['lifetime']
        product_id = media_entity["id"]

        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        yield scrapy.Request(url=self.get_review_url(product_id, country_code, page_count),
                             callback=self.parse_reviews,
                             errback=self.err,
                             dont_filter=True,
                             meta={'page_count': page_count,
                                   'media_entity': media_entity,
                                   'country_code': country_code,
                                   'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        country_code = response.meta["country_code"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        res = json.loads(response.text)
        current_page = res['BatchedResults']['q0']['Offset']
        total_pages = res['BatchedResults']['q0']['TotalResults']

        try:
            product_name = res['BatchedResults']['q0']['Includes']['Products'][product_id]['Name']
        except:
            product_name = ''
        try:
            brand_name = \
                res['BatchedResults']['q0']['Includes']['Products'][product_id]['Brand']['Name']
        except:
            brand_name = ''

        extra_info = {"product_name": product_name, "brand_name": brand_name}

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
                    review_date = dateparser.parse(item["SubmissionTime"]).replace(tzinfo=None)

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

            next_page = current_page + 30
            if review_date >= self.start_date and next_page <= total_pages:
                page_count = response.meta['page_count'] + 30
                yield scrapy.Request(url=self.get_review_url(product_id, country_code, next_page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page_count': page_count,
                                           'media_entity': media_entity,
                                           'country_code': country_code,
                                           'dont_proxy': True})
        else:
            if '"Results":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(current_page))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            if item['ClientResponses']:
                for comment in item['ClientResponses']:
                    comment_date = dateparser.parse(comment['Date']).replace(tzinfo=None)
                    comment_date_until = review_date + timedelta(
                        days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:

                        if comment['Response']:
                            comment_id = comment['Department'] + _id \
                                         + hashlib.sha512(comment['Response'].encode('utf-8')
                                                          ).hexdigest()
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
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    @staticmethod
    def get_review_url(product_id, country_code, page_count):
        if country_code and country_code in {'com', 'mx'}:
            return {
                'com': f"https://api.bazaarvoice.com/data/batch.json?passkey"
                       f"=dap59bp2pkhr7ccd1hv23n39x&apiversion=5.5&displaycode=1337-en_us"
                       f"&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter"
                       f".q0=productid%3Aeq%3A{product_id}&filter.q0=contentlocale%3Aeq"
                       f"%3Aen_US&sort.q0=submissiontime%3Adesc&stats.q0=reviews&"
                       f"filteredstats.q0=reviews&include.q0=authors%2Cproducts%2"
                       f"Ccomments&filter_reviews.q0=contentlocale%3Aeq%3Aen_US&"
                       f"filter_reviewcomments.q0=contentlocale%3Aeq%3Aen_"
                       f"US&filter_comments.q0=contentlocale"
                       f"%3Aeq%3Aen_US&limit.q0=30&offset.q0={page_count}",
                'mx': f"https://api.bazaarvoice.com/data/batch.json?passkey"
                      f"=caW7w1BmlrORmRCAMl4aod7OvE1rnbhGHhwjRx4Y1f49w&apiversion=5.5"
                      f"&displaycode=11668-es_mx&resource.q0=reviews&filter.q0"
                      f"=isratingsonly%3Aeq%3Afalse&filter.q0=productid%3Aeq%3A"
                      f"{product_id}&filter.q0=contentlocale%3Aeq%3Aes_MX&sort."
                      f"q0=submissiontime%3Adesc&stats.q0=reviews&filteredstats.q0=reviews"
                      f"&include.q0=authors%2Cproducts%2Ccomments&filter_reviews.q0"
                      f"=contentlocale%3Aeq%3Aes_MX&limit.q0=30&offset.q0={page_count}"
            }[country_code]

    def get_lifetime_ratings(self, product_id, response):
        try:
            _res = json.loads(response.text)
            res = _res['BatchedResults']['q0']['Includes']['Products'][
                product_id]['ReviewStatistics']
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
