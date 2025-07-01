import hashlib
from datetime import timedelta
import dateparser
from bs4 import BeautifulSoup
from dateparser import search as datesearch

import scrapy
from .setuserv_spider import SetuservSpider


class TrustpilotSpider(SetuservSpider):
    name = 'trustpilot-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Trustpilot process start")
        assert self.source == 'trustpilot'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        res = response.css('article[data-service-review-card-paper="true"]')
        try:
            product_name = response.css('span.multi-size-header__big::text').extract_first().strip()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        review_date = self.start_date

        if res:
            for item in res:
                if item:
                    _id = item.css('h2 a::attr(href)').extract_first().split('/')[-1]
                    _review_date = item.css('time[data-service-review-date-time-ago="true"]::text').extract_first()
                    print(_review_date)
                    # review_date = dateparser.parse(str(_review_date)).replace(tzinfo=None)
                    review_date = datesearch.search_dates(_review_date)[0][1]
                    try:
                        title = item.css('h2 a::text').extract_first()
                    except:
                        title = ''
                    rating = item.css('div[class="styles_reviewHeader__iU9Px"]::'
                                      'attr(data-service-review-rating)').extract_first()
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                _body = item.css('p[data-service-review-text-typography="true"]').extract_first()
                                soup = BeautifulSoup(_body, 'html.parser')
                                for s in soup(['script', 'style']):
                                    s.decompose()
                                body = ' '.join(soup.stripped_strings)
                                if '\xa0' in body:
                                    body = body.replace('\xa0', ' ')
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title.strip(),
                                         body=body.replace('\n', '').strip(),
                                         rating=rating,
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
                            self.logger.warning("Body is not their for review {}".format(_id))

            if review_date >= self.start_date and 'aria-label="Next page"' in response.text:
                next_page_url = 'https://uk.trustpilot.com/' + \
                                response.css('a[aria-label="Next page"]::attr(href)').extract_first()
                yield scrapy.Request(url=next_page_url,
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'media_entity': media_entity})
                self.logger.info(f"sending request for next page {next_page_url}")
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(1))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        if item.css('div.styles_content__Hl2Mi'):
            _comment = item.css('div.styles_content__Hl2Mi '
                               'p[data-service-review-business-reply-text-typography="true"]').extract_first()
            soup = BeautifulSoup(_comment, 'html.parser')
            for s in soup(['script', 'style']):
                s.decompose()
            comment = ' '.join(soup.stripped_strings)
            if '\xa0' in comment:
                comment = comment.replace('\xa0', ' ')
            _comment_date = item.css('div.styles_content__Hl2Mi time::text').extract_first()
            comment_date = dateparser.parse(str(_comment_date)).replace(tzinfo=None)
            try:
                department = item.css('div.styles_content__Hl2Mi p::text').extract_first().split('Reply from ')[1]
            except:
                department = ''
            comment_date_until = review_date + \
                                 timedelta(days=SetuservSpider.settings.get('PERIOD'))
            if comment_date <= comment_date_until:
                if comment:
                    comment_id = department + _id + \
                                 hashlib.sha512(comment.encode('utf-8')).hexdigest()
                    self.yield_items_comments \
                        (parent_id=_id,
                         _id=comment_id,
                         comment_date=comment_date,
                         title='',
                         body=comment.strip().replace('\n ', ''),
                         rating='',
                         url=product_url,
                         review_type='comments',
                         creator_id='',
                         creator_name='',
                         product_id=product_id,
                         extra_info=extra_info)
        else:
            self.logger.info(f"There is no comment for product_id {product_id} on review {_id}")
