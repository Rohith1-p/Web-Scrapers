import json
import datetime
from datetime import datetime
import scrapy
from .setuserv_spider import SetuservSpider
from urllib.parse import urlsplit

class ShopeeIDSpider(SetuservSpider):
    name = 'shopee-id-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee Products Scraping starts")
        assert self.source == 'shopee_products' or 'shopee_vn_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            if '?' in product_url:
                product_url = product_url.split('?')[0]
            media_entity = {**media_entity, **self.media_entity_logs}
            _product_id = product_url.split('-i.')[1]
            shop_id = _product_id.split('.')[0]
            item_id = _product_id.split('.')[1]
            _product_id = _product_id.replace(".", "_")
            country_code = urlsplit(product_url).netloc[-2:]
            media_entity["country_code"] = country_code
            yield scrapy.Request(url=self.get_review_url(item_id, shop_id, country_code),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(self.get_review_url(item_id, shop_id, country_code)),
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating products for {product_url} category ")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        res = json.loads(response.text)
        res = res['data']
        shop_id = res["shopid"]
        item_id = res["itemid"]
        media_entity["shop_id"] = shop_id
        country_code = media_entity["country_code"]
        currency = self.get_currency(country_code)
        price = currency + str(int(res['price']) / 100000)
        if res['price'] != res['price_max']:
            price = currency+str(int(res['price']) / 100000) + '-' \
                     + currency+str(int(res['price_max']) / 100000)
        # if len(price) > 18:
        #     yield scrapy.Request(url=self.get_review_url(item_id, shop_id),
        #                          callback=self.parse_products,
        #                          errback=self.err, dont_filter=True,
        #                          meta={'media_entity': media_entity})
        #     return

        product_description = res['description']
        if product_description == '':
            yield scrapy.Request(url=self.get_review_url(item_id, shop_id, country_code),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            return

        brand_name = res['brand']
        if brand_name == '' or brand_name is None:
            brand_name = ''
        else:
            brand_name = res['brand']
        try:
            volume = ''
            for item in res['attributes']:
                if 'Volume' in item['name']:
                    volume += item['value']
        except:
            volume = ''

        viscosity = []
        for item in res['tier_variations']:
            if 'VISC' in item['name']:
                viscosity += item['options']

        media = {
            "product_url": media_entity["url"],
            "media_entity_id": str(shop_id)+"_"+str(item_id),
            "product_price": price,
            "no_of_unites_sold": res['historical_sold'],
            "avg_rating": round(float(res['item_rating']['rating_star']), 1),
            "total_reviews": res['cmt_count'],
            "product_description": product_description,
            "additional_fields": viscosity,
            "volume/weight": volume,
            "product_name": res['name'],
            "brand_name": brand_name,
        }
        yield scrapy.Request(url=self.get_seller_url(shop_id, country_code),
                             callback=self.parse_seller,
                             errback=self.err,
                             dont_filter=True,
                             headers=self.get_headers(self.get_review_url(item_id, shop_id, country_code)),
                             meta={'media_entity': media_entity,
                                   'media': media})
        self.logger.info(f"Requests is going for shop id {shop_id} ")

    def parse_seller(self, response):
        res = json.loads(response.text)
        media = response.meta['media']
        media_entity = response.meta['media_entity']
        shop_id = media_entity["shop_id"]
        country_code = media_entity["country_code"]
        try:
            seller_avg_rating = round(float(res['data']['rating_star']), 1)
        except:
            seller_avg_rating = 0

        seller_url = self.get_seller_url(shop_id, country_code)
        seller_url = ((seller_url).split("/api"))[0]

        self.yield_product_details \
            (
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
             additional_fields="-",
             seller_name=res['data']['name'],
             seller_url=seller_url + f"/{res['data']['account']['username']}",
             seller_avg_rating=seller_avg_rating,
             seller_no_of_ratings=res['data']['rating_good'] + res['data']['rating_bad']
                                  + res['data']['rating_normal'],
             seller_followers=res['data']['follower_count'],
             seller_no_of_unites_sold="")

    @staticmethod
    def get_currency(country_code):
        if country_code:
            if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th', 'tw'}:
                return {
                    'my': "RM" ,
                    'vn': "₫",
                    'ph': "₱",
                    'id': "Rp",
                    'sg': "$",
                    'th': "฿",
                    'tw': "$"
                }[country_code]

    @staticmethod
    def get_review_url(item_id, shop_id, country_code):
        if country_code:
            if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th', 'tw'}:
                return {
                    'my': f"https://shopee.com.my/api/v4/item/get?itemid={item_id}&shopid={shop_id}" ,
                    'vn': f"https://shopee.vn/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
                    'ph': f"https://shopee.ph/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
                    'id': f"https://shopee.co.id/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
                    'sg': f"https://shopee.sg/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
                    'th': f"https://shopee.co.th/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
                    'tw': f"https://shopee.tw/api/v4/item/get?itemid={item_id}&shopid={shop_id}"
                }[country_code]

        else:
            self.logger.error(f"Country code {country_code} for product - "
                              f"{shop_id}.{item_id} is wrong, "
                              f"Please check again")

        return url

    @staticmethod
    def get_seller_url(shop_id, country_code):
        if country_code:
            if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th', 'tw'}:
                return {
                    'my': f"https://shopee.com.my/api/v4/product/get_shop_info?shopid={shop_id}" ,
                    'vn': f"https://shopee.vn/api/v4/product/get_shop_info?shopid={shop_id}",
                    'ph': f"https://shopee.ph/api/v4/product/get_shop_info?shopid={shop_id}",
                    'id': f"https://shopee.co.id/api/v4/product/get_shop_info?shopid={shop_id}",
                    'sg': f"https://shopee.sg/api/v4/product/get_shop_info?shopid={shop_id}",
                    'th': f"https://shopee.co.th/api/v4/product/get_shop_info?shopid={shop_id}",
                    'tw': f"https://shopee.tw/api/v4/product/get_shop_info?shopid={shop_id}"
                }[country_code]

        else:
            self.logger.error(f"Country code {country_code} for product - "
                              f"{shop_id}.{item_id} is wrong, "
                              f"Please check again")

        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
        'authority': 'shopee.ph',
        'scheme': 'https',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        #'path': product_url.split('https://shopee.com.my')[1],
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
        'x-api-source': 'pc',
        'x-requested-with': 'XMLHttpRequest'
        }
        return headers
