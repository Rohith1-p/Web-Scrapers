import dateparser
import scrapy
import json
from .setuserv_spider import SetuservSpider
from scrapy.http import FormRequest


class TmallSpider(SetuservSpider):
    name = 'tmall-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Tmall process start")
        assert self.source == 'tmall'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1

            params = {"num_iid": str(product_id), "page_num": str(page), "sort": "2", "type": "append"}

            yield FormRequest(url=self.get_api_url(),
                              method='GET',
                              formdata=params,
                              headers=self.get_headers(),
                              dont_filter=True,
                              callback=self.parse_info,
                              errback=self.err,
                              meta={'media_entity': media_entity,
                                    'page': page})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        page = response.meta["page"]

        if 'Invalid API key' in response.text or 'Too many requests' in response.text:
            import time
            time.sleep(2)
            params = {"num_iid": str(product_id), "page_num": str(page), "sort": "2", "type": "append"}

            yield FormRequest(url=self.get_api_url(),
                              method='GET',
                              formdata=params,
                              headers=self.get_headers(),
                              dont_filter=True,
                              callback=self.parse_info,
                              errback=self.err,
                              meta={'media_entity': media_entity,
                                    'page': page})
            return

        res = response.text.replace('\\', '')
        res = res.split('rateList":')[1].split(',"showInvite":"false"')[0]
        res = json.loads(res)
        review_date = self.start_date

        if res:
            for item in res:
                _id = item['id']
                body = item['feedback']
                _review_date = item['feedbackDate']
                review_date = dateparser.parse(_review_date)

                if self.start_date <= review_date <= self.end_date:
                    self.yield_items(
                        _id=_id,
                        review_date=review_date,
                        title='',
                        body=body,
                        rating=0,
                        url=product_url,
                        review_type='media',
                        creator_id='',
                        creator_name='',
                        product_id=product_id,
                        extra_info={})

            page += 1
            if review_date >= self.start_date:
                params = {"num_iid": str(product_id), "page_num": str(page), "sort": "2", "type": "append"}
                yield FormRequest(url=self.get_api_url(),
                                  method='GET',
                                  formdata=params,
                                  headers=self.get_headers(),
                                  dont_filter=True,
                                  callback=self.parse_info,
                                  errback=self.err,
                                  meta={'media_entity': media_entity,
                                        'page': page})

    @staticmethod
    def get_headers():
        headers = {
            'x-rapidapi-key': "9e61d3cf13mshc29ee009473ccf8p15fc91jsn9d6fc533199d",
            'x-rapidapi-host': "taobao-tmall-data-service.p.rapidapi.com"
        }
        return headers

    @staticmethod
    def get_api_url():
        url = "https://taobao-tmall-data-service.p.rapidapi.com/Rate/MobileWDetailGetItemRates.ashx"
        return url


