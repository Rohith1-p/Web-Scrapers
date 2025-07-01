import json
import hashlib
import dateparser

import scrapy
from .setuserv_spider import SetuservSpider


class SuningSpider(SetuservSpider):
    name = 'suning-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Suning process start")
        assert self.source == 'suning'

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
        code = product_url.split('/')[2]

        if code == 'product.suning.com':
            try:
                product_name = response.css('h1[id="itemDisplayName"]::text').extract()[-1]
            except:
                product_name = ''
            try:
                brand_name = response.css('h2[id="promotionDesc"]::text').extract_first()
            except:
                brand_name = ''
        else:
            try:
                product_name = response.css('h1[id="productName"]::text').extract_first()
            except:
                product_name = ''
            try:
                brand_name = response.css('div[class="desc-spec-param-i"] a::text').extract_first()
            except:
                brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}

        page_count = 1
        if len(product_id.split('-')) == 3:
            review_code = 'cluster'
        else:
            review_code = 'general'
        yield scrapy.Request(url=self.get_review_url(product_url, product_id, page_count, review_code=review_code),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             headers=self.get_headers(product_url),
                             meta={'page_count': page_count, 'media_entity': media_entity,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page_count = response.meta['page_count']
        extra_info = response.meta['extra_info']
        _res = response.text.split('reviewList(')[1]
        _res = _res[:-1]
        res = json.loads(_res)

        if res['commodityReviews']:
            for item in res['commodityReviews']:
                if item:
                    _id = str(item['commodityReviewId']) + item['publishTime'] \
                          + hashlib.sha512(item["content"].encode('utf-8')).hexdigest()
                    review_date = dateparser.parse(item["publishTime"]).replace(tzinfo=None)

                    try:
                        body = item["content"]
                        if body:
                            if self.start_date <= review_date <= self.end_date \
                                    and self.type == 'media':
                                self.yield_items(
                                    _id=_id,
                                    review_date=review_date,
                                    title='',
                                    body=body,
                                    rating=item["qualityStar"],
                                    url=product_url,
                                    review_type='media',
                                    creator_id='',
                                    creator_name='',
                                    product_id=product_id,
                                    extra_info=extra_info)
                    except:
                        self.logger.warning("Body is not their for review {}".format(_id))

            page_count += 1
            if len(product_id.split('-')) == 3:
                review_code = 'cluster'
            else:
                review_code = 'general'
            yield scrapy.Request(url=self.get_review_url(product_url, product_id, page_count, review_code=review_code),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url),
                                 meta={'page_count': page_count, 'media_entity': media_entity,
                                       'extra_info': extra_info, 'dont_proxy': True})
        else:
            yield scrapy.Request(
                url=self.get_review_url(product_url, product_id, page_count, review_code='package'),
                callback=self.parse_reviews,
                headers=self.get_headers(product_url),
                meta={'page_count': page_count, 'media_entity': media_entity,
                      'extra_info': extra_info, 'dont_proxy': True})

    @staticmethod
    def get_review_url(product_url, product_id, page_count, review_code):

        if review_code:
            if review_code in {'package', 'general', 'cluster'}:
                return {
                    'package': f"https://review.suning.com/ajax/cluster_review_lists/"
                               f"package--{product_id}-total-{page_count}-default-10-----"
                               f"reviewList.htm?callback=reviewList",
                    'general': f"https://review.suning.com/ajax/cluster_review_lists/"
                               f"general--{product_id}-total-{page_count}-default-10-----"
                               f"reviewList.htm?callback=reviewList",
                    'cluster': f"https://review.suning.com/ajax/cluster_review_lists/"
                               f"cluster-{product_id}-total-{page_count}-default-10-----"
                               f"reviewList.htm?callback=reviewList"
                }[review_code]

    @staticmethod
    def get_headers(product_url):
        headers = {
            'referer': product_url,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/75.0.3770.100 Safari/537.36'
        }
        return headers
