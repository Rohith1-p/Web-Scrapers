import json
import hashlib
from datetime import timedelta
import dateparser
from dateparser import search as datesearch

import scrapy
from bs4 import BeautifulSoup
from .setuserv_spider import SetuservSpider


class YhdSpider(SetuservSpider):
    name = 'yhd-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Yhd process start")
        assert self.source == 'yhd'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity["id"]

        try:
            product_name = response.css('h1[id="productMainName"]::text').extract_first()
        except:
            product_name = ''
        try:
            brand_name = response.css('div.desitem dd::attr(title)').extract_first()
            brand_name = brand_name.split('品牌：')[1]
        except:
            brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}

        review_page = 1
        yield scrapy.Request(url=self.get_review_url(product_id, review_page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             headers=self.get_headers(product_url),
                             meta={'review_page': review_page, 'media_entity': media_entity,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        extra_info = response.meta['extra_info']
        review_page = response.meta['review_page']

        _res = response.text.split('comment_handler_success(')[1][:-1]
        _res = json.loads(_res)
        _res = BeautifulSoup(_res["value"], 'html.parser')
        res = _res.findAll("div", {"class": "item"})

        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    _id = item.find("span", {"class": "name"}).get('id')
                    _id = _id.split('userName')[1]
                    _review_date = str(item.find("span", {"class": "date"}).text)
                    review_date = datesearch.search_dates(_review_date)[0][1]
                    try:
                        if self.type == 'media':
                            body = item.find("span", {"class": "text"}).text.strip()
                            if body:
                                if self.start_date <= review_date <= self.end_date:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=str(item.find("span", {"class": "star"})
                                                    .get('class')[1])[1],
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

            review_page += 1
            yield scrapy.Request(url=self.get_review_url(product_id, review_page),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 headers=self.get_headers(product_url),
                                 meta={'review_page': review_page,
                                       'media_entity': media_entity,
                                       'extra_info': extra_info, 'dont_proxy': True})
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(review_page))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            comment_date = item.find("p", {"class": "time"}).text
            comment_date = dateparser.parse(comment_date).replace(tzinfo=None)
        except:
            comment_date = ''
        try:
            comment = item.find("p", {"class": "user"}).text.split()[2]
        except:
            comment = ''
        try:
            comment_date_until = review_date + \
                                 timedelta(days=SetuservSpider.settings.get('PERIOD'))
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
                         extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    @staticmethod
    def get_review_url(product_id, review_page):
        url = f"https://item.yhd.com/squ/comment/getCommentDetail.do?productId={product_id}" \
              f"&pagenationVO.currentPage={review_page}&pagenationVO.preCurrentPage=" \
              f"{review_page - 1}&pagenationVO.rownumperpage=10&filter." \
              f"commentFlag=0&filter.sortType=5&callback=comment_handler_success"
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {'Accept': 'text/javascript, application/javascript, application/ecmascript, '
                             'application/x-ecmascript, */*; q=0.01',
                   'X-Requested-With': 'XMLHttpRequest',
                   'Accept-Encoding': 'gzip, deflate, br',
                   'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                   'origin': 'https://www.yhd.com',
                   'referer': product_url,
                   'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu '
                                 'Chromium/67.0.3396.99 Chrome/67.0.3396.99 Safari/537.36'}
        return headers
