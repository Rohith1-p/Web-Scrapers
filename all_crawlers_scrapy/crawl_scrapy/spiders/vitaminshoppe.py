import dateparser
import scrapy

from .setuserv_spider import SetuservSpider


class Vitaminshoppe(SetuservSpider):
    name = 'vitaminshoppe-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Vitaminshoppe process start")
        assert self.source == 'vitaminshoppe'

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
        res = response.css('div.TTreview')
        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    try:
                        content_type = 'syndicated'
                        source_url = item.css('span.TTrevAttributionSiteName::text')\
                            .extract_first().strip()
                    except:
                        content_type = 'organic'
                        source_url = 'None'
                    content = {'content_type': content_type, 'source_url': source_url}

                    try:
                        title = item.css('div.TTreviewTitle::text').extract()[0]
                    except:
                        title = ''
                    _id = item.css('div.TTmediaForUgc::attr(data-ugc-id)').extract_first()
                    review_date = \
                        item.css('div[itemprop="dateCreated"]::attr(datetime)').extract_first()
                    review_date = dateparser.parse(review_date)

                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item.css('div.TTreviewBody::text').extract_first()
                                rating = item.css('meta[itemprop="ratingValue"]::attr(content)')\
                                    .extract_first()

                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title=title,
                                        body=body,
                                        rating=rating,
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        extra_info=content)
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
            if 'class' not in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_review_url(product_id, page):
        url = f'https://static.www.turnto.com/sitedata/I1h5bZdr3OSP4fMsite/v4_3/' \
              f'{product_id}/d/en_US/catitemreviewshtml/{page}/mostRecent'
        return url
