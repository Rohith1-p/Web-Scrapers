import json
import datetime
import requests

import scrapy
from .setuserv_spider import SetuservSpider


class HepsiburadaSpider(SetuservSpider):
    name = 'hepsiburada-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Hepsiburada process start")
        assert self.source == 'hepsiburada'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url='https://www.google.com/',
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        lifetime = response.meta['lifetime']
        page = 0
        review_url = self.get_review_url(product_id, page)
        headers = self.get_headers(product_url, review_url, page)
        response = requests.get(review_url, headers=headers)
        if lifetime:
            self.get_lifetime_ratings(product_id, response)
        self.parse_reviews(media_entity, page, response)

    def parse_reviews(self, media_entity, page, response):
        product_url = media_entity['url']
        product_id = media_entity['id']
        res = json.loads(response.text)
        review_date = self.start_date

        if res['data']['approvedUserContent']['approvedUserContentList']:
            for item in res['data']['approvedUserContent']['approvedUserContentList']:
                if item:
                    try:
                        product_name = item['product']['name']
                    except:
                        product_name = ''
                    try:
                        brand = item['product']['brand']
                    except:
                        brand = ''
                    extra_info = {"product_name": product_name, "brand_name": brand}

                    _id = item["id"]
                    _review_date = item.get('createdAt').replace('Z', '')
                    review_date = datetime.datetime.strptime(
                        (_review_date.split('.')[0]), "%Y-%m-%dT%H:%M:%S")
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["review"]['content']
                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title='',
                                        body=body,
                                        rating=item["star"],
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
                review_url = self.get_review_url(product_id, page)
                headers = self.get_headers(product_url, review_url, page)
                response = requests.get(review_url, headers=headers)
                self.parse_reviews(media_entity, page, response)
                self.logger.info(f"Scraping for {product_url} and page {page}")
        else:
            if 'approvedUserContentList":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_review_url(product_id, page):
        url = f"https://user-content-gw-hermes.hepsiburada.com/queryapi/v2/ApprovedUserContents?" \
            f"skuList={product_id}&from={page}&size=20&sortField=createdAt"
        return url

    def get_lifetime_ratings(self, product_id, response):
        try:
            _res = json.loads(response.text)
            review_count = _res['totalItemCount']
            ratings = _res['data']['filterData']['starCounts']

            rating_map = {}
            _rating_value = []
            for item in ratings.values():
                _rating_value.append(item['starCount'])
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = _rating_value[int(i) - 1]

            _average_ratings = []
            for i in range(1, 6):
                avg_item = i * _rating_value[i - 1]
                _average_ratings.append(avg_item)
            average_ratings = sum(_average_ratings) / review_count

            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=round(float(average_ratings), 1),
                ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")

    @staticmethod
    def get_headers(product_url, review_url, page):
        headers = {
            'authority': 'user-content-gw-hermes.hepsiburada.com',
            'method': 'GET',
            'path': review_url.split('hepsiburada.com')[1],
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBcHBLZXkiOiJBRjdGMkEzNy1DQzRCLTRGMUMtODd'
                             'GRC1GRjM2NDJGNjdFQ0IiLCJJc0F1dGhlbnRpY2F0ZWQiOiJGYWxzZSIsIklzTGF6eVJlZ2lzdHJhdGlvbiI6Ij'
                             'AiLCJNaWdyYXRpb25GbGFnIjoiMCIsIlBhcnRuZXJUeXBlQ29kZSI6IjAiLCJTaGFyZURhdGFQZXJtaXNzaW9uI'
                             'joiRmFsc2UiLCJUb2tlbklkIjoiN0Y5Qzk0RkMtMERDRS00Qzc5LTk4OUEtRTdCMEQzNkFGMDM0IiwiVHlwZUNv'
                             'ZGUiOiIxIiwiVXNlcklkIjoiMTg1MmEzNjEtYTVmOS00YjA4LTk5OWEtMGNmNzVhZDIwMWJjIiwiU2Vzc2lvbkl'
                             'kIjoiNTY4YWJjMTAtNWFiOS00ODMxLWFkM2YtMWU1ZjBhZTI4YzgwIiwibmJmIjoxNjIzNDEzMjA3LCJleHAiOj'
                             'E2MjM0MjA0MDcsImlhdCI6MTYyMzQxMzIwN30.e_XyTqcvzvYuDxIJTZFqkgplMUMHTETynEEHq-aez2g',
            'origin': 'https://www.hepsiburada.com',
            'referer': f'{product_url}-yorumlari?sirala=createdAt&sayfa={page / 20}',
            'cache-control': 'max-age=0',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36'
        }
        return headers