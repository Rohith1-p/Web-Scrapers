import json
import datetime
from datetime import datetime
import scrapy
from scrapy.conf import settings

from .setuserv_spider import SetuservSpider


class ShopeeVnSpider(SetuservSpider):
    name = 'shopee-vn-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee Vn Products Scraping starts")
        assert self.source == 'shopee_vn_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            _product_id = product_url.split('-i.')[1]
            shop_id = _product_id.split('.')[0]
            item_id = _product_id.split('.')[1]
            yield scrapy.Request(url=self.get_review_url(item_id, shop_id),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(self.get_review_url(item_id, shop_id)),
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating products for {product_url} category ")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]

        res = json.loads(response.text)
        print("*********RES", res)
        res = res['data']
        shop_id = res["shopid"]
        item_id = res["itemid"]

        product_description = res['description']
        if product_description == '':
            yield scrapy.Request(url=self.get_review_url(item_id, shop_id),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            return

        brand_name = res['brand']
        if brand_name == '' or brand_name is None:
            brand_name = ''
        else:
            brand_name = res['brand']

        # origin = ''
        # for item in res['attributes']:
        #     if 'xứ' in item['name']:
        #         origin += item['value']
        price = '₫' + str(int(res['price']) / 100000000)
        if res['price'] != res['price_max']:
            price = '₫' + str(int(res['price']) / 100000000) + '-' \
                    + '₫' + str(int(res['price_max']) / 100000000)

        try:
            volume = ''
            for item in res['attributes']:
                if 'Thể tích' in item['name']:
                    volume += item['value']
        except:
            volume = ''

        media = {
            "category_url": "https://shopee.vn/%C4%90%E1%BB%93-u%E1%BB%91ng-c%C3%B3-c%E1%BB%93n-cat.9824.11977",
            "product_url": f"https://shopee.vn/--i.{shop_id}.{item_id}",
            "media_entity_id": f"{shop_id}.{item_id}",
            "product_price": price,
            "no_of_unites_sold": res['historical_sold'],
            "avg_rating": round(float(res['item_rating']['rating_star']), 1),
            "total_reviews": res['cmt_count'],
            "product_description": product_description,
            "additional_fields": '',
            "volume/weight": volume,
            "product_name": res['name'],
            "brand_name": brand_name,
        }
        yield scrapy.Request(url=self.get_seller_url(shop_id),
                             callback=self.parse_seller,
                             errback=self.err,
                             dont_filter=True,
                             headers=self.get_headers(product_url),
                             meta={'media_entity': media_entity,
                                   'media': media})
        self.logger.info(f"Requests is going for shop id {shop_id} ")

    def parse_seller(self, response):
        res = json.loads(response.text)
        media = response.meta['media']
        try:
            seller_avg_rating = round(float(res['data']['rating_star']), 1)
        except:
            seller_avg_rating = 0
        self.yield_product_details \
            (category_url=media['category_url'],
             product_url=media['product_url'],
             product_id=media['media_entity_id'],
             product_name=media['product_name'],
             brand_name=media['brand_name'],
             product_price=media['product_price'],
             no_of_unites_sold=media['no_of_unites_sold'],
             avg_rating=media['avg_rating'],
             total_reviews=media['total_reviews'],
             product_description=media['product_description'],
             volume_or_weight=media['volume/weight'],
             additional_fields=media['additional_fields'],
             seller_name=res['data']['name'],
             seller_url=f"https://shopee.vn/{res['data']['account']['username']}",
             seller_avg_rating=seller_avg_rating,
             seller_no_of_ratings=res['data']['rating_good'] + res['data']['rating_bad']
                                  + res['data']['rating_normal'],
             seller_followers=res['data']['follower_count'],
             seller_no_of_unites_sold="")

    @staticmethod
    def get_review_url(item_id, shop_id):
        url = f"https://shopee.vn/api/v4/item/get?itemid={item_id}&shopid={shop_id}"
        return url

    @staticmethod
    def get_seller_url(shop_id):
        url = f"https://shopee.vn/api/v4/product/get_shop_info?shopid={shop_id}"
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'authority': 'shopee.vn',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'path': product_url.split('https://shopee.vn')[1],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
            'x-api-source': 'pc',
            'x-requested-with': 'XMLHttpRequest',
            'x-shopee-language': 'vi'
        }

        return headers
