import json
import dateparser
import scrapy
from .setuserv_spider import SetuservSpider


class PetLazadaSpider(SetuservSpider):
    name = 'pet-lazada-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Pet Lazada process start")
        assert self.source == 'pet_lazada'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page_count = 1
            yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'page_count': page_count,
                                       'media_entity': media_entity})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page_count = response.meta['page_count']

        if '443//pdp/review/getReviewList/_____tmd_____/punish?' in response.text:
            yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'page_count': page_count,
                                       'media_entity': media_entity})
            return
        if '"HOST": "my.lazada.com.ph:443"' in response.text:
            yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'page_count': page_count,
                                       'media_entity': media_entity})
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
        review_date = self.start_date

        if total_results:
            for item in total_results:
                if item:
                    extra_info = {"additional_fields(review)": item['skuInfo']}
                    review_date = dateparser.parse(item["reviewTime"])
                    _id = item["reviewRateId"]
                    if self.start_date <= review_date <= self.end_date:
                        body = item["reviewContent"]
                        if body == "" or body is None:
                            body = "No Review Text"
                        if self.type == 'media':
                            self.yield_items \
                                (_id=_id,
                                 review_date=review_date,
                                 title='No Review Title',
                                 body=body,
                                 rating=item["rating"],
                                 url=product_url,
                                 review_type='media',
                                 creator_id='',
                                 creator_name=item['buyerName'],
                                 product_id=product_id,
                                 extra_info=extra_info)

            next_page = current_page + 1
            if review_date >= self.start_date and next_page <= total_pages:
                page_count += 1
                yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                     callback=self.parse_reviews,
                                     errback=self.err, dont_filter=True,
                                     meta={'page_count': page_count,
                                           'media_entity': media_entity})
        else:
            if '"success":true' not in response.text:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page_count))
            else:
                self.logger.info(f"Pages exhausted for {product_id}")

    def get_review_url(self, product_id, page):
        url = f"https://my.lazada.com.ph/pdp/review/getReviewList?itemId={product_id}" \
              f"&pageSize=5&filter=0&sort=1&pageNo={page}"
        return url
