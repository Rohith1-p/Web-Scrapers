import scrapy
import requests
import dateparser

from .setuserv_spider import SetuservSpider


class FacebookSpider(SetuservSpider):
    name = 'facebook-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Facebook process start")
        assert self.source == 'facebook'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta['media_entity']
        product_id = media_entity['id']
        fields = 'id,created_time,message,from,comments{id,created_time,message,from,' \
                 'comments{id,created_time,message,from,comments}}'
        page_url = self.get_review_url(product_id, fields)
        self.get_post(page_url, media_entity)

    def get_post(self, page_url, media_entity):
        product_url = media_entity['url']
        product_id = media_entity['id']

        try:
            posts_on_page = requests.get(url=page_url)
            posts_data = posts_on_page.json()
            res = posts_data['data']
            review_date = self.start_date
            if res:
                for item in res:
                    if item:
                        try:
                            brand = item['from']['name']
                        except:
                            brand = ''

                        try:
                            body = item['message']
                        except:
                            body = ''

                        extra_info = {"product_name": '', "brand_name": brand}

                        review_date = dateparser.parse(item['created_time']).\
                            replace(tzinfo=None)
                        _id = item["id"]
                        if self.start_date <= review_date <= self.end_date:
                            try:
                                if self.type == 'media':
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=0,
                                         url=product_url+product_id,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                                    _comment = item['comments']
                                    if _comment:
                                        self.parse_comments(_comment, _id, product_url,
                                                                       product_id, extra_info)
                                if self.type == 'comments':
                                    _comment = item['comments']
                                    if _comment:
                                        self.parse_comments(_comment, _id, product_url,
                                                                       product_id, extra_info)
                            except:
                                self.logger.info(f"Body is not their for review {_id}")
                try:
                    next_page = posts_data['paging']['next']
                    self.get_post(next_page, media_entity)
                except:
                    self.logger.info("No more pages")
        except:
            self.logger.info("No more posts")

    def parse_comments(self, _comment, _id, product_url, product_id, extra_info):
        for i in range(0, 3):
            if _comment['data']:
                data = _comment['data']
                for comment in data:
                    try:
                        comment_date = dateparser.parse(comment['created_time'])\
                            .replace(tzinfo=None)
                        _comment_id = comment["id"]
                        self.yield_items_comments \
                            (parent_id=_id,
                             _id=_comment_id,
                             comment_date=comment_date,
                             title='',
                             body=comment['message'],
                             rating='',
                             url=product_url + product_id,
                             review_type='comments',
                             creator_id='',
                             creator_name='',
                             product_id=product_id,
                             extra_info=extra_info)
                        _reply = comment['comments']
                        if _reply:
                            self.parse_replies(_reply, _comment_id, product_url,
                                                          product_id, extra_info)
                    except:
                        self.logger.info(f"Comment is not their for review {_id[0]}")
                try:
                    next_page = _comment['paging']['next']
                    next_page_data = requests.get(url=next_page)
                    next_page_data = next_page_data.json()
                    self.parse_comments(next_page_data, _id, product_url,
                                                   product_id, extra_info)
                except:
                    self.logger.info("No more comments")

    def parse_replies(self, _reply, _id, product_url, product_id, extra_info):
        if _reply['data']:
            data = _reply['data']
            for reply in data:
                try:
                    reply_date = dateparser.parse(reply['created_time']).replace(tzinfo=None)
                    _reply_id = reply["id"]
                    self.yield_items_comments \
                        (parent_id=_id,
                         _id=_reply_id,
                         comment_date=reply_date,
                         title='',
                         body=reply['message'],
                         rating='',
                         url=product_url + product_id,
                         review_type='comments',
                         creator_id='',
                         creator_name='',
                         product_id=product_id,
                         extra_info=extra_info)
                except:
                    self.logger.info(f"Reply is not their for comment {_id[0]}")
            try:
                next_page = _reply['paging']['next']
                next_page_data = requests.get(url=next_page)
                next_page_data = next_page_data.json()
                self.parse_replies(next_page_data, _id, product_url,
                                              product_id, extra_info)
            except:
                self.logger.info("No more replies")
    
    @staticmethod
    def get_review_url(product_id, fields):
        access_token = '183523538951600|ae8dd22fbc6ed781cd12aa57d2d44828'
        page_url = f"https://graph.facebook.com/{product_id}/feed?" \
            f"access_token={access_token}&fields={fields}"
        return page_url
