import datetime
import json
import scrapy
from .setuserv_spider import SetuservSpider


class ChemistwarehouseSpider(SetuservSpider):
    name = 'chemistwarehouse-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Chemistwarehouse process start")
        assert self.source == 'chemistwarehouse'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=self.get_review_url(product_id, limit=8, offset=0),
                                 callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'lifetime': True})
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
                             meta={'media_entity': media_entity})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        res = json.loads(response.text)
        limit = res['BatchedResults']['q0']['Limit']
        offset = res['BatchedResults']['q0']['Offset']

        if res['BatchedResults']['q0']['Includes']:
            try:
                product_name = res['BatchedResults']['q0']['Includes']['Products']
                product_name = product_name[product_id]['Name']
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
                    _id = item["Id"]
                    review_date = datetime.datetime.strptime \
                        ((item.get('SubmissionTime').split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["ReviewText"]
                                title = item['Title']
                                if title is None:
                                    title = ''
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=item["Rating"],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)

                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date:
                offset += limit
                limit = 30
                yield scrapy.Request(url=self.get_review_url(product_id, limit, offset),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity})
        else:
            if '"Results":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))

    @staticmethod
    def get_review_url(product_id, limit, offset):
        url = f"https://api.bazaarvoice.com/data/batch.json?passkey=5tt906fltx756rwlt29pls49v&apiversion=5.5" \
              f"&displaycode=13773-en_au&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter." \
              f"q0=productid%3Aeq%3A{product_id}&filter.q0=contentlocale%3Aeq%3Aen*%2Cen_AU&sort.q0=submissiontime" \
              f"%3Adesc&stats.q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments&" \
              f"filter_reviews.q0=contentlocale%3Aeq%3Aen*%2Cen_AU&filter_reviewcomments.q0=contentlocale%" \
              f"3Aeq%3Aen*%2Cen_AU&filter_comments.q0=contentlocale%3Aeq%3Aen*%2Cen_AU&limit.q0={limit}&offset" \
              f".q0={offset}&limit_comments.q0=3"
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
            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")