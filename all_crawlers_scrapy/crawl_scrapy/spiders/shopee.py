import hashlib
import json

import datetime
from datetime import timedelta
from urllib.parse import urlsplit

import scrapy
from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs


class ShopeeSpider(SetuservSpider):
    name = 'shopee-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee process starts")
        assert self.source == 'shopee'

    def start_requests(self):
        self.logger.info("Starting requests")
        monitor_log = 'Successfully Called Shopee Consumer Posts Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)
        print("total no_of_ulrs to shopee_consumer_posts spider are: ", len(self.start_urls))

        for product_url, product_id in zip(self.start_urls, self.product_ids):
            if '?' in product_url:
                product_url = product_url.split('?')[0]
            _product_id = product_url.split('-i.')[1]
            shop_id = _product_id.split('.')[0]
            item_id = _product_id.split('.')[1]
            _product_id = _product_id.replace(".", "_")
            media_entity = {'url': product_url, 'id': product_id,
                            'item_id': item_id, 'shop_id': shop_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            offset = 0
            page = 1
            country_code = urlsplit(product_url).netloc[-2:]
            yield scrapy.Request(url=self.get_review_url(item_id, offset, shop_id, country_code),
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'country_code': country_code,
                                       'offset': offset,
                                       'page': page,
                                       'media_entity': media_entity,
                                       'lifetime': True},
                                 headers=self.get_headers(product_url))
            self.logger.info(f"Processing for product_url {product_url} and "
                             f"{item_id} which belong to "
                             f"country code {country_code}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity["url"]
        item_id = media_entity["item_id"]
        shop_id = media_entity["shop_id"]
        country_code = response.meta['country_code']
        offset = response.meta['offset']
        page = response.meta['page']
        lifetime = response.meta['lifetime']
        if lifetime:
            self.get_lifetime_ratings(product_id, response)

        yield scrapy.Request(url=self.get_review_url(item_id, offset, shop_id, country_code),
                             callback=self.parse_reviews,
                             errback=self.err,
                             dont_filter=True,
                             meta={'country_code': country_code,
                                   'offset': offset,
                                   'page': page,
                                   'media_entity': media_entity},
                             headers=self.get_headers(product_url))

    def parse_reviews(self, response):
        print("response.meta: ",response.meta)
        country_code = response.meta["country_code"]
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        item_id = media_entity['item_id']
        shop_id = media_entity['shop_id']
        offset = response.meta['offset']
        try:
            page = response.meta['page']
        except:
            page = "-"

        try:
            res = json.loads(response.text)
            count = res['data']['item_rating_summary']['rcount_with_context']
            # print("befor rising custom exceptions")
            # x = 5/0
            # print("after rising custom exception")
        except:
            self.logger.info(f"Captcha Found for {product_id}")
            print(f"Captcha Found for {product_id}")
            yield scrapy.Request(url=self.get_review_url(item_id, offset, shop_id,
                                                         country_code),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'country_code': country_code,
                                       'offset': offset,
                                       'page' : page,
                                       'media_entity': media_entity},
                                 headers=self.get_headers(product_url))
            return

        total_results = res['data']['ratings']

        if total_results:
            print("*******total results*******")
            for item in total_results:
                if item:
                    try:
                        product_name = item['product_items'][0]['name'].strip()
                    except:
                        product_name = ''
                    try:
                        brand_name = item['product_items'][0]['brand'].strip()
                        if brand_name is None or brand_name == '0' or brand_name == 'No Brand':
                            brand_name = ''
                    except:
                        brand_name = ''
                    extra_info = {"product_name": product_name, "brand_name": brand_name}

                    _id = item["cmtid"]
                    review_date = datetime.datetime.utcfromtimestamp(item["ctime"])
                    if review_date:
                        try:
                            if self.type == 'media':
                                body = item["comment"]
                                if body:
                                    if self.start_date <= review_date <= self.end_date:
                                        print(self.start_date, review_date, self.end_date)
                                        print("in if  ************",body)
                                        print(_id,review_date,item["rating_star"],product_url,product_id,extra_info)
                                        self.yield_items(
                                            _id=_id,
                                            review_date=review_date,
                                            title='',
                                            body=body,
                                            rating=item["rating_star"],
                                            url=product_url,
                                            review_type='media',
                                            creator_id='',
                                            creator_name='',
                                            product_id=product_id,
                                            page_no = (offset // 6)+1,
                                            extra_info=extra_info,
                                            )
                                        self.parse_comments(item, _id, review_date,
                                                            product_url, product_id,
                                                            extra_info)
                            if self.type == 'comments':
                                self.parse_comments(item, _id, review_date,
                                                    product_url, product_id,
                                                    extra_info)
                        except:
                            print("in exception ******")
                            self.logger.warning("Body is not their for review {}".format(_id))

            if offset < count:
                offset += 6
                yield scrapy.Request(url=self.get_review_url(item_id, offset, shop_id,
                                                             country_code),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'country_code': country_code,
                                           'offset': offset,
                                           'page': page+1,
                                           'media_entity': media_entity},
                                     headers=self.get_headers(product_url))
        else:
            if '"ratings":[]' not in response.text:
                self.logger.info(f"Dumping for {self.source} and {product_id} - {datetime.datetime.utcnow()}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))
            else:
                self.logger.info(f"Pages exhausted / No Reviews for product_id {product_id}")

    def parse_comments(self, review, _id, review_date, product_url, product_id, extra_info):
        try:
            comment = review['ItemRatingReply']
            _comment_date = comment["ctime"]
            comment_date = datetime.datetime.utcfromtimestamp(_comment_date)
            comment_date_until = review_date \
                                 + timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:
                if comment['comment']:
                    comment_id = _id + hashlib.sha512(comment['comment']
                                                      .encode('utf-8')).hexdigest()
                    self.yield_items_comments(
                        parent_id=_id,
                        _id=comment_id,
                        comment_date=comment_date,
                        title='',
                        body=comment['comment'],
                        rating='',
                        url=product_url,
                        review_type='comments',
                        creator_id='',
                        creator_name='',
                        product_id=product_id,
                        page_no = (offset // 6)+1,
                        extra_info=extra_info,
                        review_date = review_date)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    def get_review_url(self, item_id, offset, shop_id, country_code):
        if country_code:
            if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th', 'tw'}:
                return {
                    'my': f"https://shopee.com.my/api/v2/item/get_ratings?filter=1&flag=1"
                          f"&itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0",
                    'vn': f"https://shopee.vn/api/v2/item/get_ratings?filter=1&flag=1"
                          f"&itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0",
                    'ph': f"https://shopee.ph/api/v2/item/get_ratings?filter=1&flag=1"
                          f"&itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0",
                    'id': f"https://shopee.co.id/api/v2/item/get_ratings?filter=1&flag=1"
                          f"&itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0",
                    'sg': f"https://shopee.sg/api/v2/item/get_ratings?filter=1&flag=1"
                          f"&itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0",
                    'th': f"https://shopee.co.th/api/v2/item/get_ratings?filter=1&flag=1"
                          f"&itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0",
                    'tw': f"https://shopee.tw/api/v2/item/get_ratings?filter=1&flag=1"
                          f"&itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0"
                }[country_code]

        else:
            self.logger.error(f"Country code {country_code} for product - "
                              f"{shop_id}.{item_id} is wrong, "
                              f"Please check again")

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'referer': product_url,
            'origin': 'www.' + product_url.split('/')[2],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/72.0.3626.121 Safari/537.36'
        }

        return headers

    def get_lifetime_ratings(self, product_id, response):
        try:
            _res = json.loads(response.text)
            res = _res['data']['item_rating_summary']
            review_count = res['rating_total']
            ratings = res['rating_count']
            _average_ratings = []
            for i in range(1, 6):
                _average_ratings.append(ratings[i - 1] * i)
            average_ratings = sum(_average_ratings) / review_count

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[int(i) - 1]

            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=float(average_ratings),
                ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
