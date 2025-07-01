import dateparser
import scrapy
from .setuserv_spider import SetuservSpider


class ShoppingcbdSpider(SetuservSpider):
    name = 'shoppingcbd-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shoppingcbd process start")
        assert self.source == 'shoppingcbd'

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
        try:
            product_name = response.css('h1.entry-title::text').extract()[0]
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        yield scrapy.Request(url=product_url + '?show_all_comments=true',
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity, 'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta['extra_info']
        res = response.css('div[class="comments_block_list"] div[class="comments_block_item cf"]')

        if res:
            for item in res:
                if item:
                    try:
                        title = item.css('h3.pixrating_title::text').extract_first()
                    except:
                        title = ''
                    _id = item.css('div[class="comments_block_item cf"]::attr(id)').extract_first()
                    _id = _id.split('comment-')[1]
                    _review_date = item.css('span.comment_date::text').extract_first()
                    review_date = dateparser.parse(str(_review_date)).replace(tzinfo=None)

                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                _body = item.css('div.comment-content').extract_first()
                                body = _body.split('<p>')[1].strip().replace('</p>', '')\
                                    .replace('<br>', '').replace('\n', '')\
                                    .replace('</div>', '').replace('\t', '')

                                if body:
                                    self.yield_items\
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=item.css('div.review_rate::attr(data-pixrating)')
                                         .extract_first(),
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(1))
