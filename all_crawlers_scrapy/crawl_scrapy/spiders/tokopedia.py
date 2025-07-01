import json
import hashlib
import requests
from datetime import datetime
from datetime import timedelta

import scrapy
from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs


class TokopediaSpider(SetuservSpider):
    name = 'tokopedia-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Tokopedia process starts")
        assert self.source == 'tokopedia'

    def start_requests(self):
        self.logger.info("Starting requests")
        monitor_log = 'Successfully Called Tokopedia Consumer Posts Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url,     callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url),
                                 meta={'media_entity': media_entity, 'lifetime': False})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        if 'www.lazada.co.id:443' in response.text or "RGV587_ERROR" in response.text or "lazada_waf_block" in response.text:
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url),
                                 meta={'media_entity': media_entity, 'lifetime': False})
            return

        try:
            _res_text_1 = response.css('script[type="application/ld+json"]::text').extract_first()
            _res_text_1 = "".join(_res_text_1)
            res_text_1 = json.loads(_res_text_1)
            print("res_text_1", res_text_1)
            product_name = res_text_1['name']
        except:
            try:
                product_name = response.css('h1[data-testid="lblPDPDetailProductName"]::text').extract_first()
                print("product_name", product_name)
            except:
                product_name = ''

        try:
            product_id_ = (response.text.split("&productID=")[1]).split("&")[0]
        except:
            product_id_ = ""

        media_entity = response.meta["media_entity"]
        media_entity["product_id_"] = product_id_
        product_id = media_entity["id"]
        product_url = media_entity["url"]
        lifetime = response.meta['lifetime']

        try:
            rating_map = response.css(".css-jtcihq-unf-heading").extract()
        except:
            pass

        try:
            _total_count = response.css('meta[itemprop="ratingCount"]::attr(content)').extract_first()
            _total_count = _total_count.split()[0]
        except:
            _total_count = 0
        total_count = int(_total_count) / 10

        if lifetime:
            self.get_lifetime_ratings(product_id, _total_count, response)

        avg_rating = response.css('meta[itemprop="ratingValue"]::attr(content)').extract_first()
        media_entity["avg_rating"] = avg_rating
        extra_info = {"product_name": product_name, "brand_name": ''}
        page = 1

        yield scrapy.Request(url=self.get_review_url(product_id_, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             headers=self.get_headers(product_url),

                             meta={'page': page, 'total_count': total_count,
                                   'media_entity': media_entity, 'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity['id']
        product_id_ =  media_entity['product_id_']
        extra_info = response.meta['extra_info']
        total_count = response.meta['total_count']
        page = response.meta['page']
        res = json.loads(response.text)
        review_date = self.start_date

        if res['data']['list']:
            for item in res['data']['list']:
                if item:
                    _id = item['review_id']
                    review_date = datetime.utcfromtimestamp(item['update_time'])
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item['message']
                                creator_name = item["reviewer"]["full_name"]
                                if body:
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item['rating'],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name= creator_name,
                                         product_id=product_id,
                                         product_id_ = product_id_,
                                         page_no = page,
                                         extra_info=extra_info)

                                    self.parse_comments\
                                        (item, _id, review_date, product_url,
                                         product_id, extra_info)
                            if self.type == 'comments':
                                self.parse_comments\
                                    (item, _id, review_date, product_url,
                                     product_id, product_id_, extra_info)

                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            page += 1
            if review_date >= self.start_date and page < total_count:
                yield scrapy.Request(url=self.get_review_url(product_id_, page),
                                     callback=self.parse_reviews,

                                     errback=self.err,
                                     headers=self.get_headers(product_url),
                                     meta={'page': page, 'total_count': total_count,
                                           'media_entity': media_entity, 'extra_info': extra_info})
        else:
            if '"list":[]' in response.text:
                self.logger.info(f"Pages exhausted / No Reviews for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    def parse_comments(self, item, _id, review_date, product_url, product_id, product_id_, extra_info):
        try:
            comment = item['response']['message']
        except:
            comment = ''
        try:
            comment_date = datetime.utcfromtimestamp(item['response']['response_time'])
        except:
            comment_date = ''

        try:
            comment_date_until = review_date \
                                 + timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:
                if comment:
                    comment_id = _id + hashlib.sha512(comment.encode('utf-8')).hexdigest()
                    self.yield_items_comments \
                        (parent_id=_id,
                         _id=comment_id,
                         comment_date=comment_date,
                         title='',
                         body=comment,
                         rating='',
                         url=product_url,
                         review_type='comments',
                         creator_id='',
                         creator_name='',
                         product_id=product_id,
                         product_id_ = product_id_,
                         review_date = comment_date,
                         extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    @staticmethod
    def get_review_url(product_id, page):
        url = 'https://www.tokopedia.com/reputationapp/review/api/v2/' \
              'product/{}?page={}&total=10'.format(product_id, page)
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'referer': product_url,
            'origin': 'www.tokopedia.com',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/75.0.3770.100 Safari/537.36'
        }

        return headers

    def get_lifetime_ratings(self, product_id, _total_count, response):
        media_entity = response.meta["media_entity"]
        average_ratings = media_entity["avg_rating"]
        try:
            average_ratings = average_ratings
            ratings = response.css('div[class="ratingtotal fs-14 text-black-38"]::text'
                                   ).extract()[::-1]

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[int(i)-1]

            self.yield_lifetime_ratings\
                (product_id=product_id,
                 review_count=_total_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
