import hashlib
import json
from datetime import datetime, timedelta

import scrapy
from .setuserv_spider import SetuservSpider


class JetSpider(SetuservSpider):
    handle_httpstatus_list = [401]
    HOST = 'https://jet.com'
    name = 'jet-product-reviews'
    page_size = 5

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Jet process start")
        assert self.source == 'jet'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity},
                                 headers=self.get_headers(product_url))
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity['url']
        page_count = 0

        try:
            product_name = response.css('span[itemprop="name"]::text').extract()[0]
        except:
            product_name = ''
        try:
            brand_name = response.css('a[class="core__Box-sc-1qfvr3i-0 '
                                      'core__BaseInput-sc-1qfvr3i-7 core__'
                                      'Hyperlink-sc-1qfvr3i-9 icbgOY"]::text').extract()[0]
        except:
            brand_name = ''

        extra_info = {"product_name": product_name, "brand_name": brand_name}
        yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             headers=self.get_headers(product_url),
                             meta={'page_count': page_count, 'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_url = media_entity['url']
        product_id = media_entity['id']
        page_count = response.meta['page_count']
        res = json.loads(response.text)
        review_date = self.start_date

        if 'results' in res and 'reviews' in res['results'][0]:
            for item in res['results'][0]['reviews']:
                if item:

                    try:
                        content_type = 'syndicated'
                        source_url = item['details']['brand_base_url']

                    except:
                        content_type = 'organic'
                        source_url = 'None'

                    content = {'content_type': content_type, 'source_url': source_url}

                    extra_info.update(content)
                    _id = str(item["review_id"])
                    review_date = item['details']['created_date']/1000
                    review_date = datetime.utcfromtimestamp(review_date)
                    if review_date and self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                if item["details"]["comments"]:
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item["details"]["headline"],
                                         body=item["details"]["comments"],
                                         rating=item["metrics"]["rating"],
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
                            self.logger.warning("Body is not their for review {}"
                                                .format(_id))

            if review_date >= self.start_date:
                page_count += len(res['results'][0]['reviews'])
                yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page_count': page_count, 'media_entity': media_entity,
                                           'extra_info': extra_info},
                                     headers=self.get_headers(product_url))
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            comment = item['details']['merchant_response']
        except:
            comment = ''
        try:
            comment_date = item['details']['merchant_response_date']/1000
            comment_date = datetime.utcfromtimestamp(comment_date)
        except:
            comment_date = ''
        try:
            comment_date_until = review_date + \
                                 timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:
                if comment:
                    comment_id = _id + \
                                 hashlib.sha512(comment.encode('utf-8')).hexdigest()

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
            self.logger.info(f"There is no comment for product_id {product_id} on "
                             f"review {_id}")

    @staticmethod
    def get_review_url(product_id, page_count):
        url = "https://readservices-b2c.powerreviews.com/m/786803/l/en_US/product/{}/" \
              "reviews?paging.from={}&paging.size=5&filters=&search=&sort=Newest&image_only=false"\
            .format(product_id, page_count)
        return url
    
    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
            'authorization': '3ff84632-35e9-49b7-8a3a-7638cdd208cf',
            'referer': product_url,
            'origin': 'https://jet.com',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/72.0.3626.121 Safari/537.36'
        }
        return headers
