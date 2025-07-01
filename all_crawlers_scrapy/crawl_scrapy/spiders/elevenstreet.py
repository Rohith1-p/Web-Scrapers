import json
import dateparser
import scrapy

from .setuserv_spider import SetuservSpider


class ElevenStreetSpider(SetuservSpider):
    name = 'elevenstreet-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("11street process starts")
        assert self.source == '11street'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        try:
            product_name = response.css('h1[itemprop="name"]::text').extract_first()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        page_count = 1
        yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity, 'page_count': page_count,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity['id']
        extra_info = response.meta["extra_info"]
        res = json.loads(response.text)
        page_count = response.meta['page_count']
        total_pages = int(res['pageCount'])
        review_date = self.start_date

        if res['reviewList']:
            for item in res['reviewList']:
                if item:
                    review_date = dateparser.parse(item["createDtString"])
                    if self.start_date <= review_date <= self.end_date \
                            and self.type == 'media':
                        _id = item["contentNo"]
                        try:
                            body = item["content"]
                            if body:
                                self.yield_items\
                                    (_id=_id,
                                     review_date=review_date,
                                     title=item["subject"],
                                     body=body,
                                     rating=self.get_rating_star(item["rating"]),
                                     url=product_url,
                                     review_type='media',
                                     creator_id='',
                                     creator_name='',
                                     product_id=product_id,
                                     extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date and page_count < total_pages:
                page_count += 1
                yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity,
                                           'page_count': page_count,
                                           'extra_info': extra_info, 'dont_proxy': True})
        else:
            if '"reviewList":[]' in response.text:
                self.logger.info(f"Pages exhausted / No Reviews for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))

    @staticmethod
    def get_review_url(product_id, page_count):
        url = f"https://www.prestomall.com/productdetail/ajax/reviews/{product_id}" \
              f"?pageNo={page_count}&order=CREATE_DATE_DESC&filter=ALL"
        return url

    @staticmethod
    def get_rating_star(rating):
        return {0: 1, 1: 3, 2: 4, 3: 5}[rating]
