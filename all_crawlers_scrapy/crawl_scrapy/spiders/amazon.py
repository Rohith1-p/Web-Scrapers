import sys
import math
import json
import hashlib
import re
import datetime
from datetime import timedelta
from urllib.parse import urlparse
import dateparser
from dateparser import search as datesearch
import scrapy
from scrapy.conf import settings

import requests
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs

settings.overrides['CONCURRENT_REQUESTS'] = 20
settings.overrides['CONCURRENT_REQUESTS_PER_DOMAIN'] = 20


class AmazonSpider(SetuservSpider):
    name = 'amazon-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        assert self.source.startswith('amazon') or self.source.endswith('amazon')

    def start_requests(self):
        self.logger.info(f"Setu Debug: Starting requests name {self.name} with urls{self.start_urls}")
        monitor_log = 'Successfully Called Amazon Consumer Posts Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'id': product_id, 'url': product_url}
            media_entity = {**media_entity, **self.media_entity_logs}
            print('media_entity_media_entity', media_entity)

            ## add this in super class
            yield scrapy.Request(url=product_url, callback=self.parse_response,
                                 dont_filter=True, errback=self.err,
                                 meta={'media_entity': media_entity, 'lifetime': True, },
                                 headers=self.get_headers())

    def parse_response(self, response):
        media_entity = response.meta['media_entity']
        product_id = media_entity["id"]
        product_url = media_entity["url"]
        lifetime = response.meta['lifetime']

        try:
            if 'id="captchacharacters"' in response.text or 'html' not in response.text:
                self.logger.info(f"Captcha Found for {product_id}")
                yield scrapy.Request(url=product_url, callback=self.parse_response,
                                     errback=self.err, dont_filter=True,
                                     headers=self.get_headers(),
                                     meta={'media_entity': media_entity,
                                           'lifetime': True})
                return

            if 'id="productTitle' in response.text:
                if 'id="acrCustomerReview' in response.text:
                    if lifetime:
                        try:
                            self.get_lifetime_ratings(product_id, response)
                        except:
                            pass
                    extra_info = self.extra_info(response, product_url)

                    # Commented Product details / Rank Info code
                    # if self.source != 'souqamazon':
                    #     if 'https://www.amazon.com' in product_url or 'https://www.amazon.in' in product_url:
                    #         product_info = {}
                    #         print(self.propagation)
                    #         if self.propagation == 'gsheet_scraping':
                    #             try:
                    #                 if 'id="prodDetails"' in response.text or 'id="detailBulletsWrapper_feature_div"' in response.text:
                    #                     if 'id="prodDetails"' in response.text:
                    #                         prod_info_val_list = {}
                    #                         for item in response.css('div[id="prodDetails"] tr'):
                    #                             _prod_key = item.css('th').extract_first()
                    #                             prod_key = ' '.join(
                    #                                 BeautifulSoup(_prod_key, "html.parser").stripped_strings)
                    #                             if '\u200e' in prod_key:
                    #                                 prod_key = prod_key.split('\u200e')[1]
                    #
                    #                             _prod_value = item.css('td').extract_first()
                    #                             prod_value = ' '.join(
                    #                                 BeautifulSoup(_prod_value, "html.parser").stripped_strings)
                    #                             if '\u200e' in prod_value:
                    #                                 prod_value = prod_value.split('\u200e')[1]
                    #                             prod_info_val_list.update({prod_key: prod_value})
                    #                         product_info.update({"product_info": prod_info_val_list})
                    #                     else:
                    #                         prod_info_val_list = []
                    #                         for item in response.css(
                    #                                 'div[id="detailBulletsWrapper_feature_div"] span[class="a-list-item"]'):
                    #                             _prod_val = item.css('span[class="a-list-item"]').extract_first()
                    #                             _prod_val = str(_prod_val)
                    #                             _prod_val = ' '.join(
                    #                                 BeautifulSoup(_prod_val, "html.parser").stripped_strings)
                    #                             _prod_val = _prod_val.replace(
                    #                                 '\n                                    \u200e', '') \
                    #                                 .replace('\n                                    \u200f', '') \
                    #                                 .replace('\n                                    \u200f', '') \
                    #                                 .replace('\n                                        ', '')
                    #                             soup = BeautifulSoup(_prod_val, 'html.parser')
                    #                             for s in soup(['script', 'style']):
                    #                                 s.decompose()
                    #                             prod_val = ' '.join(soup.stripped_strings)
                    #                             if 'text-decoration: none;' in prod_val:
                    #                                 prod_val = 'Customer Reviews:' + prod_val.split(
                    #                                     'text-decoration: none; \n    }')[1].split('P.when')[0]
                    #                             prod_info_val_list.append(prod_val)
                    #                         product_info.update({"product_info": prod_info_val_list})
                    #                 # else:
                    #                 #     yield scrapy.Request(url=product_url, callback=self.parse_response,
                    #                 #                          errback=self.err, dont_filter=True,
                    #                 #                          headers=self.get_headers(),
                    #                 #                          meta={'media_entity': media_entity,
                    #                 #                                'lifetime': False})
                    #                 #     return
                    #             except Exception as exp:
                    #                 self.logger.info(f"getting error while scraping product information "
                    #                                  f"for {product_id}", exp)
                    #         else:
                    #             self.logger.info(f"propagation is not given to scrape product info {product_id}")
                    #         try:
                    #             self.product_details(media_entity, product_info, response)
                    #         except:
                    #             pass

                    parsed_uri = urlparse(response.url)
                    host = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
                    try:
                        page = 1
                        url_paths = response.css('a[data-hook="see-all-reviews-link-foot"]::attr(href)') \
                            .extract_first()
                        if url_paths is None:
                            url_paths = response.css('a[data-hook="cmps-expand-link"]::attr(href)') \
                                .extract_first()
                        if url_paths is None:
                            url_paths = f'/product-reviews/{product_id.split("-")[0]}/ref='
                        if self.source == 'souqamazon':
                            url_paths = 'https://www.amazon.ae/' + url_paths

                        if 'https' not in url_paths:
                            review_url = host + url_paths
                        else:
                            review_url = url_paths

                        yield scrapy.Request(url=self.get_review_url(review_url, page),
                                             callback=self.parse_reviews,
                                             errback=self.err,
                                             dont_filter=True,
                                             headers=self.get_headers(),
                                             meta={'media_entity': media_entity,
                                                   'page': page, 'host': host,
                                                   'review_url': review_url,
                                                   'extra_info': extra_info})
                        self.logger.info(f"Sending requests for Reviews for "
                                         f"{review_url}, {product_id}, and page no. {page} - {datetime.datetime.utcnow()}")
                    except:
                        self.logger.info(
                            f"Unknown response Found for product_id {product_id} - {datetime.datetime.utcnow()}")
                        self.dump(response, 'html', 'unknown_prod', self.source, product_id)
                else:
                    self.logger.info(f"No reviews for {product_id} and Product available")
                    self.product_err("Review", product_url, product_id)
            else:
                self.logger.info(f"Product {product_id} is not available")
                self.product_err("Product", product_url, product_id)
                return

        except:
            self.logger.info(f"Unknown response Found for product_id {product_id} - {datetime.datetime.utcnow()}")
            self.dump(response, 'html', 'unknown_prod', self.source, product_id)

    def parse_reviews(self, response):
        media_entity = response.meta['media_entity']
        product_id = media_entity["id"]
        product_url = media_entity['url']
        page = response.meta['page']
        host = response.meta['host']
        review_url = response.meta['review_url']
        extra_info = response.meta['extra_info']

        try:
            if 'id="captchacharacters"' in response.text or 'html' not in response.text:
                self.logger.info(f"Captcha Found for {product_id}")
                yield scrapy.Request(url=self.get_review_url(review_url, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     dont_filter=True,
                                     headers=self.get_headers(),
                                     meta={'media_entity': media_entity,
                                           'page': page, 'host': host,
                                           'review_url': review_url,
                                           'extra_info': extra_info})

            elif 'id="cm_cr-review_list"' in response.text:
                self.logger.info(f"Reviews are fetching for {product_id}")
                reviews = response.css('div[id="cm_cr-review_list"] div[class="a-section celwidget"]')

                try:
                    total_review_count = response.css('div[data-hook = "cr-filter-info-review-rating-count"]::text').extract()
                    num_list=re.split(r'(?<!\d),(?!\d)',total_review_count[0])
                    no_of_reviews_list = []
                    for i in num_list:
                        no_of_reviews_list=re.findall(r'\d+', i)
                    total_review_count=''.join(no_of_reviews_list)
                except:
                    total_review_count = response.css('div[data-hook="total-review-count"] span::text').extract_first().replace(',', '').replace('.', '')
                    self.dump(response, 'html', 'amazon_prod', self.source, product_id)
                total_review_count = int(re.findall(r'\d+', total_review_count)[0])
                page_count = math.ceil(total_review_count / 10)
                review_date = self.start_date

                if reviews:
                    for item in reviews:
                        if item:
                            _id = item.css('::attr(id)').extract_first().split('-')[-1]
                            _review_date = item.css('span[data-hook="review-date"]::text').extract_first()
                            # if 'https://www.amazon.de/' in product_url:
                            #     _review_date = _review_date.split('vom')[1]
                            #     review_date = dateparser.parse(_review_date)
                            # else:
                            if 'https://www.amazon.com.tr' in product_url:
                                print("Turkey country reviews")
                                review_date = datesearch.search_dates(text = _review_date,languages = ['en','tr',])[0][1]
                            else:
                                review_date = datesearch.search_dates(_review_date)[0][1]
                            creator_name = item.css('span[class="a-profile-name"]::text').extract_first()
                            _rating = item.css('i[data-hook="review-star-rating"]::attr(class)').extract_first()
                            if _rating is not None:
                                rating = re.findall(r'\d+', _rating)[0]
                            else:
                                break
                            try:
                                url = host + item.css('a.review-title::attr(href)').extract_first()
                            except:
                                if '-' in product_id:
                                    _product_id = product_id.split('-')[0]
                                else:
                                    _product_id = product_id
                                url = f'{host}/gp/customer-reviews/{_id}/ref=cm_cr_arp_d_rvw_ttl?ie=UTF8&ASIN={_product_id}'
                            try:
                                _body = item.css('span[data-hook="review-body"] span').extract()[0]
                            except:
                                _body = item.css('span[data-hook="review-body"]').extract()[0]
                            body = ''
                            for _, __body in enumerate(_body):
                                body += __body
                            body = body.replace('<br>', '').replace('<span>', '').replace('</span>', '').replace('.  ',
                                                                                                                 '. ').strip()
                            if '">' in body:
                                body = body.split('">')[1]
                            try:
                                title = item.css('span[data-hook=review-title] span::text').extract()[0]
                            except:
                                title = item.css('a[data-hook=review-title] span::text').extract_first()

                            if review_date and self.start_date <= review_date <= self.end_date:
                                try:
                                    if self.type == 'media':
                                        if body:
                                            self.yield_items \
                                                (_id=_id,
                                                 review_date=review_date,
                                                 title=title,
                                                 body=body.strip(),
                                                 rating=rating,
                                                 url=url,
                                                 review_type=self.type,
                                                 creator_id=creator_name,
                                                 creator_name=creator_name,
                                                 product_id=product_id,
                                                 extra_info=extra_info,
                                                 page_no=page,
                                                 )

                                    # Commenting the Amazon Comments Code
                                    #         self.parse_comments \
                                    #             (_id, review_date,
                                    #              product_url, product_id,
                                    #              extra_info)
                                    # if self.type == 'comments':
                                    #     self.parse_comments \
                                    #         (_id, review_date,
                                    #          product_url, product_id,
                                    #          extra_info)

                                except:
                                    self.logger.warning(f"Body is not their for review {_id}")

                    if page <= page_count and review_date >= self.start_date:
                        page += 1
                        try:
                            yield scrapy.Request(url=self.get_review_url(review_url, page),
                                                 callback=self.parse_reviews,
                                                 errback=self.err,
                                                 dont_filter=True,
                                                 headers=self.get_headers(),
                                                 meta={'media_entity': media_entity,
                                                       'page': page, 'host': host,
                                                       'review_url': review_url,
                                                       'extra_info': extra_info})
                        except:
                            self.logger.info(f"No more reviews for {product_id} - {datetime.datetime.utcnow()}")

            else:
                self.logger.info(f"Dumping 200 no_review response for {review_url}")
                self.dump(response, 'html', '200_noreview', self.source, product_id, str(page))

        except Exception as exp:
            self.logger.info('error is', exp)
            self.logger.info(f"Unknown response Found for review_url {review_url} - {datetime.datetime.utcnow()}")
            self.dump(response, 'html', 'unknown_rev', self.source, product_id, str(page))

    def parse_comments(self, _id, review_date, product_url, product_id, extra_info):
        try:
            _response = self.get_additional_info(_id, product_id, product_url)
            _res = "".join(_response.text.split("\\n")).replace("\\", "").split('&&&')[1]
            res = BeautifulSoup(_res, 'html.parser')
            _comment_date = str(res.find('span', {'class': 'comment-time-stamp'}).text)
            comment_date = dateparser.parse(_comment_date)
            comment_date_until = review_date + timedelta(days=30)

            creator_name = res.find('a', {'data-hook': 'review-author'}).text
            comment_body = res.find('span', {'class': 'review-comment-text'}).text
            comment_id = creator_name + _id + \
                         hashlib.sha512(comment_body.encode('utf-8')).hexdigest()
            url = product_url.rsplit('/', 2)
            comment_url = url[0] + '/gp/customer-reviews/' + \
                          str(_id) + '/ref=cm_cr_dp_d_rvw_ttl?ie=UTF8&ASIN=' + url[2]
            if comment_date <= comment_date_until:
                self.yield_items_comments \
                    (parent_id=_id,
                     _id=comment_id,
                     comment_date=comment_date,
                     title='',
                     body=comment_body.strip(),
                     rating='',
                     url=comment_url,
                     review_type='comments',
                     creator_id='',
                     creator_name=creator_name,
                     product_id=product_id,
                     extra_info=extra_info,
                     review_date = comment_date)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    @staticmethod
    def get_additional_info(_id, product_id, product_url):
        _product_id = product_id.split('-')[0]
        _country_code = urlparse(product_url).netloc.split('.')

        if len(_country_code) > 3:
            country_code = _country_code[-2] + '.' + _country_code[-1]
        else:
            country_code = _country_code[-1]

        headers = {'accept': 'text/html,*/*', 'accept-encoding': 'gzip, deflate, br',
                   'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                   'content-length': '128', 'Content-Type': 'application/x-www-form-urlencoded',
                   'origin': f"https://www.amazon.{country_code}"}
        payload = {"sortCommentsBy": "newest", "offset": 0, "count": 5, "pageIteration": 0,
                   "asin": _product_id, "reviewId": _id, "nextPageToken": "",
                   "scope": "reviewsAjax1"}
        url = f"https://www.amazon.{country_code}/hz/reviews-render/ajax/comment/get/" \
              f"ref=cm_cr_arp_d_cmt_opn"
        _response = requests.post(url, data=payload, headers=headers)
        return _response

    @staticmethod
    def get_headers():
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,'
                      'image/apng,*/*;q=0.8, application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,pt;q=0.7',
            'Content-Type': 'text/plain;charset=UTF-8'
        }
        return headers

    # Product info method
    # def product_details(self, media_entity, product_info, response):
    #     product_id = media_entity['id']
    #
    #     if 'class="cardRoot bucket"' in response.text:
    #         true = "true"
    #         false = "false"
    #         price = response.css('div[class="cardRoot bucket"]::attr(data-components)').extract_first().strip()
    #         price = json.loads(price)
    #         price = price['1']['price']['displayString']
    #     else:
    #         price = ''
    #
    #     rank_info = {}
    #     try:
    #         rank_res = response.text
    #         if 'Rank' in rank_res:
    #             rank_res = rank_res.replace('Rank', 'rank')
    #         if 'Additional Information' in rank_res:
    #             res = rank_res.split('Additional Information')[1].split('rank')[1].split('Date First Available')[0]
    #         else:
    #             res = rank_res.split('rank:')[1].split('Customer Reviews:')[0]
    #
    #         rank_first = res.split('(<a')[0]
    #         if 'href=' in rank_first:
    #             rank_first = rank_first.split('<a')[0] + rank_first.split("'>")[1].split('<')[0]
    #         rank_first = rank_first.split(' in ')
    #         if 'e{color:#666' not in rank_first:
    #             rank_info.update({rank_first[1].strip(): rank_first[0].strip().split('#')[1]})
    #
    #         from bs4 import BeautifulSoup
    #         res = BeautifulSoup(res, 'html.parser')
    #         res = res.findAll("span")
    #         for item in res:
    #             rank = item.text
    #             if '(See' not in rank:
    #                 rank = rank.split(' in')
    #                 rank_info.update({rank[1].strip(): rank[0].strip().split('#')[1]})
    #     except:
    #         pass
    #     self.yield_rank_info(
    #         product_id=product_id,
    #         product_info=product_info.get('product_info'),
    #         price=price,
    #         rank_info=rank_info)

    @staticmethod
    def payload(product_id, page):
        payload = f'sortBy=recent&reviewerType=all_reviews&formatType=&mediaType=&filterByStar=&pageNumber={page}' \
                  f'&filterByLanguage=&filterByKeyword=&shouldAppend=undefined&deviceType=desktop&' \
                  f'reftag=cm_cr_arp_d_paging_btm_next_{page}&pageSize=10&asin={product_id.split("-")[0]}&scope=reviewsAjax0'
        return payload

    @staticmethod
    def review_url(page):
        url = f'https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_getr_d_paging_btm_next_{page}'
        return url

    @staticmethod
    def headers():
        headers = {
            'accept': 'text/html,*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,pt;q=0.7',
            'connection': 'keep-alive',
            'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'host': 'www.amazon.com',
            'origin': 'https://www.amazon.com',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/83.0.4103.116 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest'
        }
        return headers

    @staticmethod
    def get_review_url(review_url, page):
        review_url = review_url.split("/ref=")[0] + f'/ref=cm_cr_getr_d_paging_btm_next_{page}' \
                                                    f'?ie=UTF8&reviewerType=all_reviews&sortBy=recent&pageNumber={page}'
        return review_url

    @staticmethod
    def extra_info(response, product_url):
        try:
            product_name = response.css('span[id="productTitle"]::text').extract_first()
            if product_name is None:
                new_host = urlparse(product_url).netloc.split('www.')[1].capitalize()
                _product_name = response.css('meta[name="title"]::attr(content)').extract_first()
                product_name = _product_name.replace(f'{new_host}:', '').replace(f'{new_host}：', '').replace(
                    f'- {new_host}', '').replace(f'{new_host} :', '').replace('Buy', '').replace(
                    'Amazon |', '').split(':')[0].split('|')[0]
        except:
            product_name = ''
        try:
            if 'class="a-spacing-none a-spacing-top-small po-brand"' in response.text:
                brand=response.css('tr[class="a-spacing-none a-spacing-top-small po-brand"] span::text').extract()
                brand=brand[1]
            elif 'class="a-spacing-small po-brand"' in response.text:
                brand = response.css('tr[class="a-spacing-small po-brand"] span::text').extract()
                brand = brand[1]
            elif 'id="titleBlockLeftSection"' in response.text:
                brand = response.css('a[id="bylineInfo"] ::text').extract_first()
                if "visit" in brand or "Visiter" in brand or "Store" in brand:
                    brand="-"
                else:
                    brand=' '.join(brand.split()[1:])
            elif 'id="centerCol"' in response.text:
                brand=response.css('a[id="bylineInfo"] ::text').extract_first()
                if "visit" in brand or "Store" in brand or "Visiter" in brand:
                    brand="-"
                else:
                    brand=' '.join(brand.split()[1:])
            else:
                brand = '-'
            if brand=='None' or brand==None:
                    brand='-'
        except:
            self.dump(response, 'html', 'amazon_prod', self.source, product_id)
            brand = '-'
        extra_info = {
            "product_name": product_name.strip(), "brand_name": brand}
        return extra_info

    def get_lifetime_ratings(self, product_id, response):
        try:
            ratings = response.css('td.a-span10 div::attr(aria-valuenow)').extract()[::-1]
            _review_count = response.css(
                'span[id="acrCustomerReviewText"]::text').extract_first().replace(',', '').replace('.', '')
            review_count = re.findall(r'\d+', _review_count)
            review_count="".join(review_count)
            _average_ratings = response.css(
                'span.a-declarative span.a-icon-alt::text').extract_first().replace(',', '.')
            average_ratings = re.findall(r'\d+\.\d+', _average_ratings)[0]
            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = round(int(ratings[int(i) - 1].replace('%', '')) *
                                                       int(review_count) / 100)
            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=float(average_ratings),
                ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
