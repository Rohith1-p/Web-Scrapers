import scrapy
import dateparser
from .setuserv_spider import SetuservSpider


class CatchSpider(SetuservSpider):
    name = 'catch-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Catch process start")
        assert self.source == 'catch'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url),
                                 meta={'media_entity': media_entity, 'dont_proxy': True,
                                       'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity['url']
        lifetime = response.meta['lifetime']

        if lifetime:
            self.get_lifetime_ratings(product_id, response)

        try:
            product_name = response.css('h1[itemprop="name"]::text').extract()[0]
        except:
            product_name = ''
        try:
            brand_name = response.css('div[class="product--brand "] a::text').extract()[0]
        except:
            brand_name = ''

        extra_info = {"product_name": product_name, "brand_name": brand_name}
        offset = 0
        yield scrapy.Request(url=self.get_review_url(product_id, offset),
                             callback=self.parse_reviews, errback=self.err,
                             dont_filter=True, headers=self.get_headers(product_url),
                             meta={'offset': offset, 'media_entity': media_entity,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity['url']
        extra_info = response.meta['extra_info']
        offset = response.meta["offset"]
        res = response.css('div[itemprop="review"]')
        review_date = self.start_date
        try:
            if res:
                for item in res:
                    if item:
                        _id = item.css('p[class="review-rate-element"]'
                                       '::attr(data-review-id)').extract()[0]
                        review_date = item.css('meta[itemprop="datePublished"]'
                                               '::attr(content)').extract()[0]
                        review_date = dateparser.parse(review_date)
                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    body = item.css('p[class="review-text"]::text').extract_first()
                                    if body:
                                        self.yield_items\
                                            (_id=_id,
                                             review_date=review_date,
                                             title='',
                                             body=body,
                                             rating=item.css('meta[itemprop="ratingValue"]'
                                                             '::attr(content)').extract()[0],
                                             url=product_url,
                                             review_type='media',
                                             creator_id='',
                                             creator_name='',
                                             product_id=product_id,
                                             extra_info=extra_info)
                            except:
                                self.logger.warning("Body is not their for review {}".format(_id))

                if review_date >= self.start_date:
                    offset += 5
                    yield scrapy.Request(url=self.get_review_url(product_id, offset),
                                         callback=self.parse_reviews,
                                         errback=self.err,
                                         headers=self.get_headers(product_url),
                                         meta={'offset': offset, 'media_entity': media_entity,
                                               'extra_info': extra_info, 'dont_proxy': True})
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))
        except:
            self.logger.info(f"No more reviews for {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))

    @staticmethod
    def get_review_url(product_id, offset):
        url = f'https://www.catch.com.au/product/{product_id}/review_list_ajax?' \
              f'offset={offset}&limit=5&reviewsRated=5&sortBy='
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'authorization': '3ff84632-35e9-49b7-8a3a-7638cdd208cf',
            'referer': product_url,
            'origin': 'www.catch.com.au',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/72.0.3626.121 Safari/537.36'
        }
        return headers

    def get_lifetime_ratings(self, product_id, response):
        try:
            review_count = response.css('a[class="js-rating-num review-rating-filter"]'
                                        '::text').extract_first().split()[0]
            average_ratings = response.css('p.review-average-rating::text').extract_first()
            ratings = response.css('div.review-meter--bar span::attr(style)').extract()[::-1]

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = round(float(ratings[int(i) - 1].split(':')[1]
                                                             .replace('%', ''))
                                                       * int(review_count) / 100)
            self.yield_lifetime_ratings\
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
