import re
import json
import dateparser
import scrapy
import requests

from .setuserv_spider import SetuservSpider


class Qoo10Spider(SetuservSpider):
    name = 'qoo10-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Qoo10 process start")
        assert self.source == 'qoo10'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        lifetime = response.meta["lifetime"]

        if 'Qoo10 - Sorry. There is too much traffic and the server is delaying processing.' in response.text:
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'lifetime': True})
            return

        if 'tab_name ="CustomerReview' in response.text:
            if lifetime:
                self.get_lifetime_ratings(product_id, response)

            try:
                product_name = response.css('h2#goods_name::text').extract()[-1]
            except:
                product_name = ''
            try:
                try:
                    brand = response.css('h2#goods_name a#btn_brand::text').extract()[-1]
                except:
                    brand = response.css('h2#goods_name span#btn_brand::text').extract()[-1]
            except:
                brand = ''
            extra_info = {"product_name": product_name, "brand_name": brand}
            url_text = 'https://www.qoo10.sg/gmkt.inc/swe_GoodsAjaxService.asmx/GetReviewList'
            url_photo = 'https://www.qoo10.sg/gmkt.inc/swe_GoodsAjaxService.asmx/GetPhotoReviewList'
            page_no_text, page_no_photo = 1, 1

            self.get_text_reviews(media_entity, url_text, page_no_text, extra_info)
            self.get_photo_reviews(media_entity, url_photo, page_no_photo, extra_info)
        else:
            self.logger.info(f"No Reviews for product_id {product_id}")

    def get_text_reviews(self, media_entity, url_text, page_no_text, extra_info):
        product_url = media_entity['url']
        product_id = media_entity["id"]
        response = requests.post(url=url_text,
                                 headers=self.get_headers(product_url, url_text),
                                 data=json.dumps(self.get_payload_text(product_id, page_no_text)))
        res_text = json.loads(response.text)
        review_date = self.start_date

        if res_text['d']['Rows']:
            for item in res_text['d']['Rows']:
                if item:
                    _id = item["no"]
                    _review_date = item['date']
                    review_date = dateparser.parse(_review_date)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["comment"]
                                if item["total_point"] == 1:
                                    rating = item["total_point"]
                                else:
                                    rating = item["total_point"] - 1
                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title=item['title'],
                                        body=body,
                                        rating=rating,
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}"
                                                .format(_id))
            if review_date >= self.start_date:
                page_no_text += 1
                self.get_text_reviews(media_entity, url_text, page_no_text, extra_info)
        else:
            if '"Rows":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_no_text))

    def get_photo_reviews(self, media_entity, url_photo, page_no_photo, extra_info):
        product_url = media_entity['url']
        product_id = media_entity["id"]
        response = requests.post(url=url_photo,
                                 headers=self.get_headers(product_url, url_photo),
                                 data=json.dumps(self.get_payload_photo(product_id, page_no_photo)))

        res_photo = json.loads(response.text)
        review_date = self.start_date
        if res_photo['d']['Rows']:
            for item in res_photo['d']['Rows']:
                if item:
                    _id = item["pr_no"]
                    _review_date = item['date']
                    review_date = dateparser.parse(_review_date)
                    _rating = item['rate_item']
                    rating = self.get_rating(_rating)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["comment"]
                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title=item['title'],
                                        body=body,
                                        rating=rating,
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}"
                                                .format(_id))
            if review_date >= self.start_date:
                page_no_photo += 1
                self.get_photo_reviews(media_entity, url_photo, page_no_photo, extra_info)

        else:
            if '"Rows":[]' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_no_photo))

    @staticmethod
    def get_rating(_rating):
        if _rating in {10, 7, 4, 1}:
            return {10: 4, 7: 3, 4: 2, 1: 1}[_rating]

    @staticmethod
    def get_headers(product_url, url):
        headers = {
            'authority': 'www.qoo10.sg',
            'method': 'POST',
            'path': '/gmkt.inc/swe_GoodsAjaxService.asmx/' + str(url.split('/')[-1]),
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'content-type': 'application/json',
            'giosis_srv_name': 'SGWWW-B-09',
            'origin': 'https://www.qoo10.sg',
            'referer': product_url,
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/78.0.3904.97 Safari/537.36',
        }
        return headers

    @staticmethod
    def get_payload_text(product_id, page):
        payload_text = {"goodscode": product_id, "page_no": page, "page_size": 10,
                        "delivery_nation_cd": "", "global_order_type": "L",
                        "lang_cd": "en", "disp_type": "A", "jumsu": "0", "option_info": "",
                        "___cache_expire___": "1639388097450", "gd_svc_nation_cd": "SG"}
        return payload_text

    @staticmethod
    def get_payload_photo(product_id, page):
        payload_photo = {"goodscode": product_id, "page_no": page, "page_size": 5,
                         "sort_by": "", "delivery_nation_cd": "", "global_order_type": "L",
                         "lang_cd": "en", "disp_type": "A", "jumsu": "0","option_info": "",
                         "___cache_expire___": "1639388097450", "gd_svc_nation_cd": "SG"}
        return payload_photo

    def get_lifetime_ratings(self, product_id, response):
        try:
            review_count = response.css('em[id="opinion_count"]::text').extract_first().replace(',', '')
            rating_map = {}
            _rating_value = []
            _average_ratings = []

            for i in range(1, 5):
                rating_map['rating_' + str(i)] = re.findall(r'\d+', (response.css(f'span[id="review_rating_filter_{i}'
                                                                                  f'_jumsu"]::text').extract_first()).replace(
                    ',', ''))[0]
            if rating_map:
                for value in rating_map.values():
                    _rating_value.append(value)

            for i in range(1, 5):
                avg_item = i * int(_rating_value[i-1])
                _average_ratings.append(avg_item)
            average_ratings = sum(_average_ratings) / int(review_count)

            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=round(float(average_ratings), 1),
                ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
