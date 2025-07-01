import json
import scrapy
from dateparser import search as datesearch

from .setuserv_spider import SetuservSpider


class LohacoSpide(SetuservSpider):
    name = 'lohaco-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Lohaco process start")
        assert self.source == 'lohaco'

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
        product_id = media_entity["id"]
        try:
            product_name = response.css('h1[class="text-h3 text-sm-h1"]::text').extract_first()
        except:
            product_name = ''
        try:
            brand_name = response.css('span[class="v-btn__content"]::text').extract_first().strip()
        except:
            brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}
        page = 0
        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page, 'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_id = media_entity["id"]
        product_url = media_entity['url']
        page = response.meta["page"]
        res = json.loads(response.text)
        if '"message":"これ以上のレビューは表示できません"' in response.text:
            self.logger.info(f"No More Reviews for product_id {product_id}")
            return

        total_pages = res['result']['itemReview']['filteredCount']
        review_date = self.start_date

        if res['result']['itemReview']['reviews']:
            for item in res['result']['itemReview']['reviews']:
                if item:
                    _id = item['reviewId']
                    _review_date = item['postedTime']
                    review_date = datesearch.search_dates(_review_date)[0][0]
                    review_date = datesearch.search_dates(review_date)[0][1]

                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item['body']
                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title=item['title'],
                                        body=body,
                                        rating=item['rating'],
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date and page < total_pages:
                page += 10
                yield scrapy.Request(url=self.get_review_url(product_id, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page': page, 'media_entity': media_entity,
                                           'extra_info': extra_info})
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_review_url(product_id, page):
        url = f"https://lohaco.yahoo.co.jp/lapi/review/product/?sellerId=h-lohaco&srid={product_id}" \
              f"&filterQuery=%7B%22offset%22:{page}%7D"
        return url
