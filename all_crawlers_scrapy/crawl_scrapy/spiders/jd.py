import json
import math
import requests
from datetime import timedelta, datetime
import dateparser
import datetime

import scrapy
from .setuserv_spider import SetuservSpider


class JDSpider(SetuservSpider):
    name = 'jd-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Jd process start")
        assert self.source == 'jd'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity['url']
        country_codes = self.get_country_code(product_url)
        if country_codes == 'ch':
            try:
                product_name = response.css('div.sku-name::text').extract_first().strip()
            except:
                product_name = ''
            try:
                brand_name = response.css('ul[id="parameter-brand"] '
                                          'li::attr(title)').extract_first()
            except:
                brand_name = ''
            extra_info = {"product_name": product_name, "brand_name": brand_name}
            page_count = 0
            yield scrapy.Request(url=self.get_review_url(product_id, product_url, page_count,
                                                         country_codes='ch'),
                                 callback=self.parse_reviews_ch, errback=self.err,
                                 dont_filter=True, meta={'page_count': page_count,
                                                         'media_entity': media_entity,
                                                         'extra_info': extra_info,
                                                         'dont_proxy': True})
        else:
            try:
                product_name = response.css('meta[key="Keywords"]::attr(content)').extract_first().split(' from')[0]
            except:
                product_name = ''
            extra_info = {"product_name": product_name, "brand_name": ''}
            page_count = 1
            url = self.get_review_url(product_id, product_url, page_count, country_codes='id')
            response = requests.get(url=url, headers=self.get_headers_id(url))
            self.parse_reviews_id(media_entity, page_count, extra_info, response)

    def parse_reviews_ch(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        extra_info = response.meta['extra_info']
        page_count = response.meta['page_count']

        _res = response.text
        if _res:
            self.logger.info(f"Getting 200 response for {product_id}")
        else:
            self.logger.info(f"Captcha Found for {product_id}")
            yield scrapy.Request(url=self.get_review_url(product_id, product_url, page_count,
                                                         country_codes='ch'),
                                 callback=self.parse_reviews_ch, errback=self.err,
                                 dont_filter=True, meta={'page_count': page_count,
                                                         'media_entity': media_entity,
                                                         'extra_info': extra_info,
                                                         'dont_proxy': True})
            return

        if 'fetchJSON_comment98(' in response.text:
            _res = _res.split('fetchJSON_comment98(')[1].split(');')[0]
        res = json.loads(_res)
        review_date = self.start_date
        if res['comments']:
            for item in res['comments']:
                if item:
                    _id = item["id"]
                    review_date = dateparser.parse(item["creationTime"]).replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["content"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item["score"],
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
            if page_count < 100:
                page_count += 1
                yield scrapy.Request(url=self.get_review_url(product_id, product_url, page_count,
                                                             country_codes='ch'),
                                     callback=self.parse_reviews_ch,
                                     errback=self.err,
                                     meta={'page_count': page_count, 'media_entity': media_entity,
                                           'extra_info': extra_info, 'dont_proxy': True})
        else:
            if '"comments":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id} - {datetime.datetime.utcnow()}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            if item['replies']:
                for comment in item['replies']:
                    comment_date = dateparser.parse(comment['creationTime']).replace(tzinfo=None)
                    comment_date_until = review_date + \
                                         timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:
                        if comment['content']:
                            self.yield_items_comments \
                                (parent_id=_id,
                                 _id=comment['id'],
                                 comment_date=comment_date,
                                 title='',
                                 body=comment['content'],
                                 rating='',
                                 url=product_url,
                                 review_type='comments',
                                 creator_id='',
                                 creator_name='',
                                 product_id=product_id,
                                 extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} on "
                             f"review {_id}")

    def parse_reviews_id(self, media_entity, page_count, extra_info, response):
        product_id = media_entity['id']
        product_url = media_entity['url']
        res = json.loads(response.text)
        total_count = int(res['data']['total'])
        total_pages = math.ceil(total_count / 10)
        review_date = self.start_date

        if res['data']['items']:
            for item in res['data']['items']:
                if item:
                    _id = item['commentChildrens'][0]['commentId']
                    review_date = item['commentChildrens'][0]['commentDate']
                    review_date = dateparser.parse(str(review_date))
                    if self.start_date <= review_date <= self.end_date:

                        try:
                            if self.type == 'media':
                                body = item['commentChildrens'][0]["content"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item['commentChildrens'][0]["commentType"],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if page_count < total_pages:
                page_count += 1
                url = self.get_review_url(product_id, product_url, page_count, country_codes='id')
                response = requests.get(url=url, headers=self.get_headers_id(url))
                self.logger.info(f"Generating reviews for {product_url} and page {page_count}")
                self.parse_reviews_id(media_entity, page_count, extra_info, response)
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id} - {datetime.datetime.utcnow()}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))

    @staticmethod
    def get_country_code(url):
        if 'https://item.jd.com' in url:
            return 'ch'
        if 'https://www.jd.id' in url:
            return 'id'

    @staticmethod
    def get_headers_id(review_url):
        headers = {
            'authority': 'color.jd.id',
            'method': 'GET',
            'path': review_url.split('https://color.jd.id')[1],
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'origin': 'https://www.jd.id',
            'referer': 'https://www.jd.id/',
            'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/103.0.0.0 Safari/537.36',
            'x-api-client': 'm',
            'x-api-lang': 'id',
            'x-api-platform': 'PC',
            'x-api-timestamp': '1657286613985'
        }
        return headers

    @staticmethod
    def get_review_url(product_id, product_url, page_count, country_codes):
        if country_codes == 'ch':
            url= "https://club.jd.com/comment/skuProductPageComments.action?callback=fetchJSON_comment98&" \
                 "productId={}&score=0&sortType=6&page={}&pageSize=10&isShadowSku=0&rid=0&fold=1"\
                .format(product_id, page_count)
        else:
            url =  'https://color.jd.id/jdid_pc_website/comment_product_page/1.0?spuId={}&skuId={}&store' \
                   'Id=&pageSize=10&pageIndex={}&tagId=&commentType='.\
                format(product_id, product_url.split(product_id)[1].split('/')[1].split('.')[0], page_count)
        return url
