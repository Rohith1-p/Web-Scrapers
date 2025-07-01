import json
from urllib.parse import urlsplit
from datetime import timedelta
import dateparser
import scrapy
from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs


class LazadaSpider(SetuservSpider):
    name = 'lazada-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Lazada process start")
        assert self.source == 'lazada' or 'redmart'

    def start_requests(self):
        self.logger.info("Starting requests")
        monitor_log = 'Successfully Called Lazada Consumer Posts Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'lifetime': True})

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        lifetime = response.meta['lifetime']
        country_code = urlsplit(product_url).netloc[-2:]
        if 'www.lazada.co.id:443' in response.text or "RGV587_ERROR" in response.text or "lazada_waf_block"  in response.text or "#nocaptcha" in response.text:
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'lifetime': True})
            return
        if lifetime:
            yield scrapy.Request(url=self.get_review_url(product_id, country_code, 1),
                                 callback=self.get_lifetime_ratings,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
        try:
            product_name = response.css('div.pdp-product-title h1::text').extract_first()
            print(product_name)
        except:
            product_name = ''
        try:
            brand = response.css('div#module_product_brand_1 a.pdp-link::text')\
                .extract_first()
            if brand is None:
                brand = response.css\
                    ('div.pdp-product-brand a.pdp-link pdp-link_size_s '
                     'pdp-link_theme_blue pdp-product-brand__brand-link::text')\
                    .extract_first()
                if brand is None:
                    brand = response.text.split('<script type="text/javascript">')[1]
                    brand = brand.split('var pdpTrackingData = ')[1].replace('\\', '')
                    brand = brand.splitlines()[0].split('brand_name')[1].split(',')[0]
                    brand = brand.replace('"', '').replace(':', '')
        except:
            brand = ''
        extra_info = {"product_name": product_name, "brand_name": brand}
        page_count = 1
        yield scrapy.Request(url=self.get_review_url(product_id, country_code, page_count),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'extra_info': extra_info, 'country_code': country_code,
                                   'page_count': page_count, 'media_entity': media_entity})
        self.logger.info(
            f"Processing for product_url {product_url} and {product_id} which belong to "
            f"country code {country_code}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        country_code = response.meta["country_code"]
        extra_info = response.meta["extra_info"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page_count = response.meta['page_count']
        review_date = self.start_date

        if '443//pdp/review/getReviewList/_____tmd_____/punish?' in response.text or 'RGV587_ERROR' in response.text:
            yield scrapy.Request(url=self.get_review_url(product_id, country_code, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'extra_info': extra_info, 'country_code': country_code,
                                       'page_count': page_count, 'media_entity': media_entity})
            return

        if '"HOST": "my.lazada' in response.text or '"action": "captcha"' in response.text \
                or 'CONTENT="NO-CACHE"' in response.text:
            yield scrapy.Request(url=self.get_review_url(product_id, country_code, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'extra_info': extra_info, 'country_code': country_code,
                                       'page_count': page_count, 'media_entity': media_entity})
            return
        res = json.loads(response.text)
        total_results = res['model']['items']
        try:
            total_pages = int(res['model']['paging']['totalPages'])
        except:
            total_pages = 0
        try:
            current_page = int(res['model']['paging']['currentPage'])
        except:
            current_page = 0

        if extra_info['product_name'] is None:
            extra_info['product_name'] = res['model']['item']['itemTitle']

        if total_results:
            for item in total_results:
                if item:
                    if country_code == 'th':
                        __review_date = item["reviewTime"].split(' ')
                        try:
                            period = __review_date[0]
                            if __review_date[1] == '':
                                duration = __review_date[2]
                            else:
                                duration = __review_date[1]
                            _review_date = self.get_review_date(period, duration)
                            review_date = dateparser.parse(str(_review_date))
                        except:
                            review_date = dateparser.parse(item["reviewTime"])
                    else:
                        review_date = dateparser.parse(item["reviewTime"])

                    _id = item["reviewRateId"]
                    body = item["reviewContent"]
                    if body == None:
                       body = "body is null"
                       continue
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                print("in if&&&")
                                print(item["reviewContent"])
                                print("after body***")
                                self.yield_items \
                                    (_id=_id,
                                     review_date=review_date,
                                     title='',
                                     body=body,
                                     rating=item["rating"],
                                     url=product_url,
                                     review_type='media',
                                     creator_id='',
                                     creator_name='',
                                     product_id=product_id,
                                     page_no = page_count,
                                     extra_info=extra_info)
                                self.parse_comments(item, _id, review_date,
                                                    product_url, product_id,
                                                    extra_info, country_code)
                            if self.type == 'comments':
                                self.parse_comments(item, _id, review_date,
                                                    product_url, product_id,
                                                    extra_info, country_code)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))
            next_page = current_page + 1
            if review_date >= self.start_date and next_page <= total_pages:
                page_count += 1
                yield scrapy.Request(url=self.get_review_url(product_id, country_code, next_page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'extra_info': extra_info, 'country_code': country_code,
                                           'page_count': page_count, 'media_entity': media_entity})
        else:
            yield scrapy.Request(url=self.get_review_url(product_id, country_code, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'extra_info': extra_info, 'country_code': country_code,
                                       'page_count': page_count, 'media_entity': media_entity})
            if '"success":true' not in response.text:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))

    def parse_comments(self, review, _id, review_date, product_url, product_id, extra_info, country_code):
        if review['replies']:
            for comment in review['replies']:
                if country_code == 'th':
                    __comment_date = comment["reviewTime"].split(' ')
                    try:
                        period = __comment_date[0]
                        if __comment_date[1] == '':
                            duration = __comment_date[2]
                        else:
                            duration = __comment_date[1]
                        _comment_date = self.get_review_date(period, duration)
                        comment_date = dateparser.parse(str(_comment_date))
                    except:
                        comment_date = dateparser.parse(comment["reviewTime"])
                else:
                    comment_date = dateparser.parse(comment["reviewTime"])

                comment_date_until = review_date + \
                                     timedelta(days=SetuservSpider.settings.get('PERIOD'))

                if comment_date <= comment_date_until:
                    comment_body = comment['reviewContent']
                    if comment_body:
                        self.yield_items_comments \
                            (parent_id=_id,
                             _id=comment['reviewRateId'],
                             comment_date=comment_date,
                             title='',
                             body=comment_body,
                             rating=item['rating'],
                             url=product_url,
                             review_type='comments',
                             creator_id='',
                             creator_name='',
                             product_id=product_id,
                             extra_info=extra_info)
        else:
            self.logger.info(f"There is no comment for product_id {product_id} on review {_id}")

    def get_review_url(self, product_id, country_code, page):
        if country_code:
            if country_code in {'my', 'vn', 'ph', 'sg', 'id', 'th'}:
                return {
                    'my': "https://my.lazada.com.my/pdp/review/getReviewList?itemId={}"
                          "&pageSize=5&filter=0&sort=1&pageNo={}".format(product_id, page),
                    'vn': "https://my.lazada.vn/pdp/review/getReviewList?itemId={}"
                          "&pageSize=5&filter=0&sort=1&pageNo={}".format(product_id, page),
                    'ph': "https://my.lazada.com.ph/pdp/review/getReviewList?itemId={}"
                          "&pageSize=5&filter=0&sort=1&pageNo={}".format(product_id, page),
                    'sg': "https://my.lazada.sg/pdp/review/getReviewList?itemId={}"
                          "&pageSize=5&filter=0&sort=1&pageNo={}".format(product_id, page),
                    'id': "https://my.lazada.co.id/pdp/review/getReviewList?itemId={}"
                          "&pageSize=5&filter=0&sort=1&pageNo={}".format(product_id, page),
                    'th': "https://my.lazada.co.th/pdp/review/getReviewList?itemId={}"
                          "&pageSize=5&filter=0&sort=1&pageNo={}".format(product_id, page)
                }[country_code]
        else:
            self.logger.error(f"Country code {country_code} for product - {product_id}"
                              f" is wrong, Please check again")

    @staticmethod
    def get_review_date(period, duration):
        if duration:
            return {
                'นาทีก่อน': period + ' minutes ago',
                'ชั่วโมงก่อน': period + ' hours ago',
                'วันก่อน': period + ' days ago',
                'สัปดาห์ก่อน': period + ' weeks ago',
                'เดือนก่อน': period + ' months ago',
                'ปีก่อน': period + ' years ago'
                }[duration]

    def get_lifetime_ratings(self, response):
        print("in lifetime")
        try:
            media_entity = response.meta["media_entity"]
            product_id = media_entity["id"]
            _res = json.loads(response.text)
            print("_res", _res)
            res = _res['model']['ratings']
            review_count = res['rateCount']
            average_ratings = res['average']
            ratings = res['scores'][::-1]
            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[int(i)-1]

            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)
        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
