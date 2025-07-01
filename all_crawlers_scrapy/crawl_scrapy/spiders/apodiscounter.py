import re
import datetime
import hashlib
import scrapy
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class ApodiscounterSpider(SetuservSpider):
    name = 'apodiscounter-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Apodiscounter process start")
        assert self.source == 'apodiscounter'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=f'{product_url}/erfahrungen', callback=self.parse_info,
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
            product_name = response.css('div.main h1::text').extract_first().strip()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ""}
        page = 0
        yield FormRequest(url=self.get_review_url(),
                          callback=self.parse_reviews,
                          errback=self.err,
                          dont_filter=True,
                          method="POST",
                          formdata=self.get_payload(product_id, page),
                          meta={'media_entity': media_entity, 'page': page,
                                'extra_info': extra_info})
        self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta["page"]
        extra_info = response.meta['extra_info']
        res = response.css('div[class="product_feedback product_feedback_hide"]')
        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    _body = item.css('div.product_feedback_right_div p::text').extract()
                    body = ''
                    for _, __body in enumerate(_body):
                        body += __body
                    body = body.strip().replace('War dieser Erfahrungsbericht für Sie hilfreich?', '')
                    if body:
                        _review_date = item.css('div.product_feedback_right_div span::text').extract()[-1].split(' ')[-1]
                        _id = _review_date + hashlib.sha512(body.strip().encode('utf-8')).hexdigest()
                        review_date = datetime.datetime.strptime(_review_date, '%d.%m.%Y')

                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    _rating = item.css(
                                        'div.product_feedback_stars_bar div::attr(style)').extract_first()
                                    rating = int(re.findall(r'\d+', _rating)[0]) / 22
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body.strip(),
                                         rating=rating,
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                            except:
                                self.logger.warning(f"Body is not their for review {_id}")
            if review_date >= self.start_date:
                page += 20
                yield FormRequest(url=self.get_review_url(),
                                  callback=self.parse_reviews,
                                  errback=self.err,
                                  dont_filter=True,
                                  method="POST",
                                  formdata=self.get_payload(product_id, page),
                                  meta={'media_entity': media_entity, 'page': page,
                                        'extra_info': extra_info})

    def get_lifetime_ratings(self, product_id, response):
        try:
            _review_count = response.css('div[id="product_review_balken_information"]::text').extract_first()
            review_count = int(re.findall(r'\d+', _review_count)[0])
            average_ratings = response.css('div.product_review_rating_all_box_count::text').extract_first().replace(',', '.')
            ratings = response.css('div.product_review_bar_chart_count::text').extract()[::-1]

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = re.findall(r'\d+', ratings[i-1])[0]

            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")

    @staticmethod
    def get_payload(product_id, page):
        payload = {
            "products_id": str(product_id),
            "start_value": str(page),
            "review_sort": "latest"
        }
        return payload

    @staticmethod
    def get_review_url():
        url = 'https://www.apodiscounter.de/ajax/get_more_product_reviews.php'
        return url
