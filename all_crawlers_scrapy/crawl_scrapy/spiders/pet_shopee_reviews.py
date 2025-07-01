import json
import datetime

import scrapy
from .setuserv_spider import SetuservSpider


class ShopeeSpider(SetuservSpider):
    name = 'pet-shopee-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee process start")
        assert self.source == 'pet_shopee'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            _product_id = product_url.split('-i.')[1]
            shop_id = _product_id.split('.')[0]
            item_id = _product_id.split('.')[1]
            media_entity = {'url': product_url, 'id': product_id,
                            'item_id': item_id, 'shop_id': shop_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            offset = 0
            yield scrapy.Request(url=self.get_review_url(item_id, offset, shop_id),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'offset': offset,
                                       'media_entity': media_entity, },
                                 headers=self.get_headers(product_url))
            self.logger.info(f"Processing for product_url {product_url} and {item_id}")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        item_id = media_entity['item_id']
        shop_id = media_entity['shop_id']
        offset = response.meta['offset']
        res = json.loads(response.text)
        count = res['data']['item_rating_summary']['rcount_with_context']
        total_results = res['data']['ratings']

        if total_results:
            for item in total_results:
                if item:
                    try:
                        options = item['product_items'][0]['options']
                    except:
                        options = ''

                    try:
                        creator_name = item["author_username"]
                    except:
                        creator_name = ''

                    tags = []
                    tag = item['tags']
                    if tag == '' or tag is None:
                        tags = []
                    else:
                        for items in tag:
                            x = items['tag_description']
                            tags.append(x)
                    tags = str(tags)
                    options = {"options": options, 'tags':tags}

                    _id = item["cmtid"]
                    review_date = datetime.datetime.utcfromtimestamp(item["mtime"])
                    if self.start_date <= review_date <= self.end_date:
                        body = item["comment"]
                        if body == "" or body is None:
                            body = "No Review Text"
                        if self.type == 'media':
                            self.yield_items(
                                _id=_id,
                                review_date=review_date,
                                title='',
                                body=body,
                                rating=item["rating_star"],
                                url=product_url,
                                review_type='media',
                                creator_id='',
                                creator_name=creator_name,
                                product_id=product_id,
                                extra_info=options)

            if offset < count:
                offset += 6
                yield scrapy.Request(url=self.get_review_url(item_id, offset, shop_id),
                                     callback=self.parse_reviews,
                                     errback=self.err,
                                     meta={'offset': offset,
                                           'media_entity': media_entity},
                                     headers=self.get_headers(product_url))
        else:
            yield scrapy.Request(url=self.get_review_url(item_id, offset, shop_id),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 meta={'offset': offset,
                                       'media_entity': media_entity},
                                 headers=self.get_headers(product_url))

            if '"error_msg":null' not in response.text:
                self.logger.info(f"Dumping for {self.source} and {product_id} - {datetime.datetime.utcnow()}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(offset))

    @staticmethod
    def get_review_url(item_id, offset, shop_id):
        url = f"https://shopee.ph/api/v2/item/get_ratings?filter=1&flag=1&" \
              f"itemid={item_id}&limit=6&offset={offset}&shopid={shop_id}&type=0"

        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'referer': product_url,
            'origin': 'www.' + product_url.split('/')[2],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/72.0.3626.121 Safari/537.36'
        }

        return headers
