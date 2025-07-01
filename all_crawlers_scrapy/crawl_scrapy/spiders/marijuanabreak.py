import requests
import dateparser
import scrapy
from datetime import timedelta
from bs4 import BeautifulSoup

from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class MarijuanabreakSpider(SetuservSpider):
    name = 'marijuanabreak-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Marijuanabreak process start")
        assert self.source == 'marijuanabreak'

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
        try:
            product_name = response.css('main.site-main h1::text').extract_first()
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ''}
        page = 0
        yield FormRequest(url='https://wayofleaf.com/wp-admin/admin-ajax.php',
                          dont_filter=True,
                          callback=self.parse_reviews,
                          errback=self.err,
                          formdata=self.payload(page, 0, product_id),
                          meta={'media_entity': media_entity,
                                'page': str(page), 'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity["id"]
        extra_info = response.meta['extra_info']
        page = response.meta["page"]

        _res = "".join(response.text.split("\\n")).replace("\\", "")
        try:
            parent_id = str(_res.split('"last_parent_id":"')[1].split('"')[0])
            res = BeautifulSoup(_res, 'html.parser')
            res = res.findAll("div", {"class": "wc-comment wc-blog-guest wc_comment_level-1"})
            review_date = self.start_date

            if res:
                for item in res:
                    if item:
                        _id = item['id'].split('-')[-1].split('_')[0]
                        review_date = item.find("div", {"class": "wc-comment-date"}).text
                        review_date = dateparser.parse(str(review_date)).replace(tzinfo=None)
                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    body = item.find("div", {"class": "wc-comment-text"}).text
                                    if body:
                                        self.yield_items \
                                            (_id=_id,
                                             review_date=review_date,
                                             title='',
                                             body=body,
                                             rating=0,
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

                if review_date >= self.start_date:
                    page += str(1)
                    yield FormRequest(url='https://wayofleaf.com/wp-admin/admin-ajax.php',
                                      dont_filter=True,
                                      callback=self.parse_reviews,
                                      errback=self.err,
                                      formdata=self.payload(page, parent_id, product_id),
                                      meta={'media_entity': media_entity,
                                            'page': str(page), 'extra_info': extra_info})
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))
        except:
            self.logger.info(f"No more reviews for {product_id}")

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        res = item.findAll("div", {"class": "wc-comment wc-reply wc-blog-user wc-blog-administrator wc_comment_level-2"}) \
          + item.findAll("div", {"class": "wc-comment wc-reply wc-blog-guest wc_comment_level-3"}) \
              + item.findAll("div", {"class": "wc-comment wc-reply wc-blog-guest wc_comment_level-2"})
        if res:
            for comment in res:
                if comment:
                    comment_id = comment['id'].split('-')[-1]
                    comment_date = comment.find("div", {"class": "wc-comment-date"}).text
                    comment_date = dateparser.parse(str(comment_date)).replace(tzinfo=None)
                    comment_text = comment.find("div", {"class": "wc-comment-text"}).text

                    comment_date_until = review_date + \
                                         timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:
                        if comment_text:
                            self.yield_items_comments \
                                (parent_id=_id,
                                 _id=comment_id,
                                 comment_date=comment_date,
                                 title='',
                                 body=comment_text,
                                 rating='',
                                 url=product_url,
                                 review_type='comments',
                                 creator_id='',
                                 creator_name='',
                                 product_id=product_id,
                                 extra_info=extra_info)
        else:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")

    @staticmethod
    def payload(page, parent_id, product_id):
        payload = {
            'action': 'wpdLoadMoreComments',
            'offset': str(page),
            'orderBy': 'comment_date_gmt',
            'order': 'desc',
            'lastParentId': str(parent_id),
            'postId': str(product_id)
        }
        return payload
