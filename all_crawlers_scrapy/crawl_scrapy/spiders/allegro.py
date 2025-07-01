import json
import time
import dateparser
import datetime
import scrapy
from urllib.parse import urlparse

from .setuserv_spider import SetuservSpider


class AllegroSpider(SetuservSpider):
    name = 'allegro-product-reviews'
    allowed_domains = ['allegro.pl']
    handle_httpstatus_list = [403, 520]

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Allegro process start")
        assert self.source.startswith('allegro')

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            time.sleep(5)
            yield scrapy.Request(url='https://www.google.com/',
                                 callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_product_headers(product_url),
                                 meta={'media_entity': media_entity, 'dont_proxy': True,
                                       'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity["url"]

        # lifetime = response.meta['lifetime']
        # if 'Please enable JS and disable any ad blocker' in response.text:
        #     yield scrapy.Request(url=product_url, callback=self.parse_info,
        #                          errback=self.err, dont_filter=True,
        #                          headers=self.get_product_headers(product_url),
        #                          meta={'media_entity': media_entity, 'dont_proxy': True,
        #                                'lifetime': True})
        #     return

        # Commented LTR Scraping
        # if lifetime:
        #     yield from self.get_lifetime_ratings(product_id, response)

        # try:
        #     product_name = response.css('h1::text').extract_first()
        # except:
        #     product_name = ''
        # try:
        #     brand_name = response.css('a[href="#aboutSeller"]::text').extract_first()
        # except:
        #     brand_name = ''

        extra_info = {"product_name": "Blocked", "brand_name": "Blocked"}
        page = 1
        time.sleep(5)

        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             headers=self.get_headers(product_url),
                             meta={'page': page, 'media_entity': media_entity,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_id = media_entity['id']
        product_url = media_entity['url']
        page = response.meta['page']
        res = json.loads(response.text)

        try:
            total_pages = res['product']['reviews']['pagination']['totalPages']
            review_date = self.start_date

            if res['product']['reviews']['opinions']:
                for item in res['product']['reviews']['opinions']:
                    if item:
                        _id = item['id']
                        review_date = dateparser.parse(item['createdAt']).replace(tzinfo=None)

                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    _body = item['opinion']
                                    if _body:
                                        self.yield_items\
                                            (_id=_id,
                                             review_date=review_date,
                                             title='',
                                             body=_body,
                                             rating=item['rating']['label'],
                                             url=product_url,
                                             review_type='media',
                                             creator_id='',
                                             creator_name='',
                                             product_id=product_id,
                                             extra_info=extra_info)
                                    else:
                                        self.logger.warning("Body is not their for review {}".format(_id))
                            except Exception as exp:
                                print("exception -->", exp)

                if review_date >= self.start_date and page <= total_pages:
                    page += 1
                    time.sleep(5)
                    yield scrapy.Request(url=self.get_review_url(product_id, page),
                                         callback=self.parse_reviews,
                                         errback=self.err,
                                         headers=self.get_headers(product_url),
                                         meta={'page': page, 'media_entity': media_entity,
                                               'extra_info': extra_info,
                                               'dont_proxy': True})
            else:
                if 'pagination' in response.text:
                    self.logger.info(f"Pages exhausted for product_id {product_id}")
                else:
                    self.logger.info(f"Dumping for {self.source} and {product_id} - {datetime.datetime.utcnow()}")
                    self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

        except:
            self.logger.info("No more reviews")

    @staticmethod
    def get_review_url(product_id, page):
        url = f'https://edge.allegro.pl/offers/{product_id}?include=product.reviews&product.reviews' \
              f'.page={page}&destination=SHOW-OFFER_PRODUCT-REVIEWS-PAGINATION_OFDF'
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': 'application/vnd.allegro.offer.view.internal.v1+json',
            'accept-language': 'pl-PL',
            'content-type': 'application/vnd.allegro.public.v1+json',
            'referer': product_url,
            'origin': 'https://allegro.pl',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
        }

        return headers

    @staticmethod
    def get_product_headers(product_url):
        headers = {
            'authority': 'allegro.pl',
            'method': 'GET',
            'path': str(urlparse(product_url).path),
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/'
                      'webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cache-control': 'max-age=0',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML,'
                          ' like Gecko) Chrome/88.0.4324.192 Safari/537.36',
        }

        return headers

    def get_lifetime_ratings(self, product_id, response):
        try:
            review_count = response.css('meta[itemprop="ratingCount"]::attr(content)').extract_first()
            average_ratings = response.css('meta[itemprop="ratingValue"]::attr(content)').extract_first().replace(',', '.')
            ratings = response.css('span[class="mpof_vs msa3_z4 mgn2_12 mgmw_06"]::text').extract()[::-1]
            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[int(i)-1]
            self.yield_lifetime_ratings\
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
