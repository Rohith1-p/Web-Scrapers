import dateparser
import scrapy
import json

from .setuserv_spider import SetuservSpider


class SainsburysSpider(SetuservSpider):
    name = 'sainsburys-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Sainsburys process start")
        assert self.source == 'sainsburys'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            if 'productId=' in product_url:
                _product_id = product_url.split('productId=')[1].split('&')[0]
                product_url_api = f"https://www.sainsburys.co.uk/groceries-api/gol-services/product/v1/product?" \
                                  f"cat_entry_id={_product_id}&filter[available]=true&include=ASSOCIATIONS&" \
                                  f"include=DIETARY_PROFILE&minimised=false"
            else:
                if '?' in product_url:
                    product_url = product_url.split('?')[0]
                _product_url = product_url.split('/')[-1]
                product_url_api = f"https://www.sainsburys.co.uk/groceries-api/gol-services/product/v1/product?filter" \
                                  f"[product_seo_url]={_product_url}&include[ASSOCIATIONS]=true&include[DIETARY_PROFILE]" \
                                  f"=true&include[PRODUCT_AD]=citrus"
            yield scrapy.Request(url=product_url_api, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        prod_res = json.loads(response.text)
        if prod_res['products']:
            for item in prod_res['products']:
                product_name = item['name']
                extra_info = {"product_name": product_name, "brand_name": ""}
                page_count = 0
                yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                     callback=self.parse_reviews,
                                     errback=self.err, dont_filter=True,
                                     meta={'page_count': page_count,
                                           'media_entity': media_entity,
                                           'extra_info': extra_info,
                                           'dont_proxy': True})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        extra_info = response.meta['extra_info']
        product_id = media_entity['id']
        product_url = media_entity['url']
        res = json.loads(response.text)
        current_page = res['Offset']
        total_pages = res['TotalResults']
        review_date = self.start_date

        if res['Results']:
            for item in res['Results']:
                if item:
                    _id = item["Id"]
                    review_date = dateparser.parse(item["SubmissionTime"]).replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item["ReviewText"]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=item['Title'],
                                         body=body,
                                         rating=item["Rating"],
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)

                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

            next_page = current_page + 10
            if review_date >= self.start_date and next_page < total_pages:
                page_count = response.meta['page_count'] + 10
                yield scrapy.Request(url=self.get_review_url(product_id, next_page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page_count': page_count,
                                           'media_entity': media_entity,
                                           'extra_info': extra_info,
                                           'dont_proxy': True})
        else:
            if '"Results":[]' in response.text:
                self.logger.info(f"Pages exhausted / No Reviews for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(current_page))

    @staticmethod
    def get_review_url(product_id, page_count):
        url = f"https://reviews.sainsburys-groceries.co.uk/data/reviews.json?ApiVersion=5.4&" \
              f"Filter=ProductId%3A{product_id}-P&Offset={page_count}&Limit=10"
        return url
