import json
from urllib.parse import urlparse
import dateparser

import scrapy
from .setuserv_spider import SetuservSpider


class HollandandbarrettSpider(SetuservSpider):
    name = 'hollandandbarrett-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Hollandandbarrett process start")
        assert self.source == 'hollandandbarrett'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            yield scrapy.Request(url=self.get_review_url(product_id, page),
                                 callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'page': page, 'media_entity': media_entity,
                                       'dont_proxy': True, 'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        page = response.meta['page']
        lifetime = response.meta['lifetime']
        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page, 'media_entity': media_entity,
                                   'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        res = json.loads(response.text)
        total_pages = res['BatchedResults']['q0']['TotalResults']
        try:
            product_name = res['BatchedResults']['q0']['Includes']['Products']
            product_name = product_name[product_id]['Name']
        except:
            product_name = ''
        try:
            brand_name = res['BatchedResults']['q0']['Includes']['Products']
            brand_name = brand_name[product_id]['Brand']['Name']
        except:
            brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}
        review_date = self.start_date
        if res['BatchedResults']['q0']['Results']:
            for item in res['BatchedResults']['q0']['Results']:
                if item:
                    try:
                        source_url = item['SyndicationSource']['Name']
                        source_url_name = urlparse(product_url).netloc.replace('www.', '')
                        if source_url_name != source_url:
                            content_type = 'syndicated'
                        else:
                            content_type = 'organic'
                            source_url = 'None'
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
                        except:
                            self.logger.warning("Body is not their for review {}"
                                                .format(_id))
            if review_date >= self.start_date and page <= total_pages:
                page += 30
                yield scrapy.Request(url=self.get_review_url(product_id, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page': page, 'media_entity': media_entity,
                                           'dont_proxy': True})
        else:
            if '"Errors":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_review_url(product_id, page):
        url = f"https://api.bazaarvoice.com/data/batch.json?passkey=f3visea9f8g42ccsxui8tcale" \
            f"&apiversion=5.5&displaycode=3332-en_gb&resource.q0=reviews&filter.q0=isratingsonly" \
            f"%3Aeq%3Afalse&filter.q0=productid%3Aeq%3A{product_id}&filter.q0=contentlocale%" \
            f"3Aeq%3Aen%2Cen_IE%2Cnl_NL%2Cen_GB&sort.q0=submissiontime%3Adesc&stats.q0=reviews" \
            f"&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments&" \
            f"filter_reviews.q0=contentlocale%3Aeq%3Aen%2Cen_IE%2Cnl_NL%2Cen_GB&" \
            f"filter_reviewcomments.q0=contentlocale%3Aeq%3Aen%2Cen_IE%2Cnl_NL%2Cen_GB" \
            f"&filter_comments.q0=contentlocale%3Aeq%3Aen%2Cen_IE%2Cnl_NL%2Cen_GB&limit." \
            f"q0=30&offset.q0={page}"
        return url

    def get_lifetime_ratings(self, product_id, response):
        try:
            _res = json.loads(response.text)
            res = _res['BatchedResults']['q0']['Includes']['Products']
            res = res[product_id]['ReviewStatistics']
            review_count = res['TotalReviewCount']
            average_ratings = res['AverageOverallRating']
            ratings = res['RatingDistribution']
            rating_map = {}
            _rating_value = []
            for item in ratings:
                _rating_value.append(item['RatingValue'])
                for i in range(1, 6):
                    if i in _rating_value:
                        rating_map['rating_' + str(item['RatingValue'])] = \
                            item['Count']
                    else:
                        rating_map['rating_' + str(i)] = 0
            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
