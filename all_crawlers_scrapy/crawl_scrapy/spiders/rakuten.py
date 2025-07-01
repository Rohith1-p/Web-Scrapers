import hashlib
import dateparser
import scrapy
from .setuserv_spider import SetuservSpider


class RakutenSpider(SetuservSpider):
    name = 'rakuten-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Rakuten process start")
        assert self.source.startswith('rakuten')

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=self.get_review_url(product_id, page),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'page': page, 'media_entity': media_entity,
                                       'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity['url']
        page = response.meta["page"]

        try:
            product_name = response.css('a[sid_linkname="item_01"]::text').extract_first().strip()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}

        res = response.css('div.revRvwUserSec')
        review_date = self.start_date

        if res:
            for item in res:
                if item:

                    try:
                        title = \
                            item.css('dt[class="revRvwUserEntryTtl summary"]::text').extract_first()
                        if title is None:
                            title = ''
                    except:
                        title = ''
                    _review_date = \
                        item.css('span[class="revUserEntryDate dtreviewed"]::text').extract_first()
                    review_date = dateparser.parse(_review_date).replace(tzinfo=None)

                    _id = str(_review_date) + hashlib.sha512(
                        item.css('dd[class="revRvwUserEntryCmt description"]::text').extract_first()
                        .encode('utf-8')).hexdigest()

                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                _body = item.css\
                                    ('dd[class="revRvwUserEntryCmt description"]::text').extract()
                                body = ''
                                for _, i in enumerate(_body):
                                    body += i
                                if body:
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body.replace('\n', ''),
                                         rating=item.css('span[class="revUserRvwerNum value"]'
                                                         '::text').extract_first(),
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)

                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date:
                page += 1
                yield scrapy.Request(url=self.get_review_url(product_id, page),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'page': page, 'media_entity': media_entity,
                                           'dont_proxy': True})
        else:
            if '対象のレビューが見つかりませんでした' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_review_url(product_id, page):
        url = 'https://review.rakuten.co.jp/item/1/{}/{}.1/sort6/'.format(product_id, page)
        return url
