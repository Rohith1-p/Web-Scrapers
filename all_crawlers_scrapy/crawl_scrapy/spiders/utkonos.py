import dateparser
import scrapy
import json
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class UtkonosSpider(SetuservSpider):
    name = 'utkonos-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Utkonos process start")
        assert self.source == 'utkonos'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        try:
            product_name = response.css('span[itemprop="name"]::text').extract_first().strip()
        except:
            product_name = ''
        try:
            brand_name = response.css('meta[itemprop="brand"]::attr(content)').extract_first()
        except:
            brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}
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
        res = json.loads(response.text)
        review_date = self.start_date

        if res['Body']['CommentList']:
            for item in res['Body']['CommentList']:
                if item:
                    _id = item["Id"]
                    review_date = dateparser.parse(item["Created"])
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["Text"]
                                if body and _id != 0:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=item['Stars'],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

            if review_date >= self.start_date:
                page += 10
                yield FormRequest(url=self.get_review_url(),
                                  callback=self.parse_reviews,
                                  errback=self.err,
                                  dont_filter=True,
                                  method="POST",
                                  formdata=self.get_payload(product_id, page),
                                  meta={'media_entity': media_entity, 'page': page,
                                        'extra_info': extra_info})

    @staticmethod
    def get_payload(product_id, page):
        false = "false"
        _data = {"Head": {"DeviceId": "E373635D9E53E32FF5E0133E9E81C730", "Domain": "www.utkonos.ru",
                          "RequestId": "fd947996c73548e3f5fe1cb65ec88da8",
                          "MarketingPartnerKey": "mp-cc3c743ffd17487a9021d11129548218",
                          "Version": "angular_web_0.0.2", "Client": "angular_web_0.0.2",
                          "Method":"feedback/getGoodsItemFeedback", "Store": "utk",
                          "SessionToken":"ED6D4E2158BFF27568E4C9B8C999965C"},
                 "Body": {"goodsItemId": product_id, "limit": 10, "offset": page, "order": "created",
                          "withText": false, "orderDirection": "desc", "type": "[Product Page] Get Product Comments"}}
        data = json.dumps(_data)
        data = ''.join(data.replace("\\", ""))
        data = data.replace('"false"', 'false')
        formdata = {"request": data}
        return formdata

    @staticmethod
    def get_review_url():
        url = 'https://www.utkonos.ru/api/v1/feedback/getGoodsItemFeedback'
        return url
