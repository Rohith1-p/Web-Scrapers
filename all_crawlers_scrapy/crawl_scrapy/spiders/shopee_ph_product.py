import json
import datetime
from datetime import datetime
from datetime import datetime
import scrapy
from .setuserv_spider import SetuservSpider


class ShopeePHSpider(SetuservSpider):
    name = 'shopee-ph-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Shopee Products Scraping starts")
        assert self.source == 'shopee_ph_products'

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
        res = json.loads(response.text)

        res = res['data']
        shop_id = res["shopid"]
        item_id = res["itemid"]

        price = 'p' + str(int(res['price']) / 100000)
        if res['price'] != res['price_max']:
            price = 'p' + str(int(res['price']) / 100000) + '-' \
                    + 'p' + str(int(res['price_max']) / 100000)
        if len(price) > 18:
            yield scrapy.Request(url=self.get_review_url(item_id, shop_id),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            return
        if res['price_max_before_discount'] == -1:
            discounted_price = ''
        else:
            discounted_price = 'p' + str(int(res['price_max_before_discount']) / 100000)
            if res['price_max_before_discount'] != res['price_min_before_discount']:
                discounted_price = 'p' + str(int(res['price_min_before_discount']) / 100000) + '-' \
                                   + 'p' + str(int(res['price_max_before_discount']) / 100000)

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

        try:
            volume = ''
            for item in res['attributes']:
                if 'volume' in item['name']:
                    volume += item['value']
        except:
            volume = ''

        categories = []
        for item in res['fe_categories']:
            categorie = item['display_name']
            categories.append(categorie)

        additional_filed = []
        additional = res['tier_variations']
        if additional is None or additional == []:
            x = ''
            additional_filed.append(x)
        else:
            for item in additional:
                x = str(item['name']) + ":" + str(item['options'])
                additional_filed.append(x)
        [i.replace("'", "") for i in additional_filed]

        full_product_type = res['attributes']
        product_type = []
        if full_product_type is None or full_product_type == []:
            product_type = ''
        else:
            for item in full_product_type:
                if item['name'] =='Expiry Date':
                    x = item['name'] + ":" + str(datetime.fromtimestamp(int(item['value'])))
                else:
                    x = item['name'] + ":" + item['value']
                product_type.append(x)

        if res['bundle_deal_info'] is None or res['bundle_deal_info'] == 0 :
            promotions = ''
        else:
            promotions = str(res['bundle_deal_info']['bundle_deal_label'])

        wholesale_value = res["wholesale_tier_list"]
        wholesale = []
        for item in wholesale_value:
            y = ['min_count: ' + str(item['min_count']) + ', max_count: ' + str(item['max_count']) + ', price: p' + str(
                item['price'] / 100000)]
            wholesale.append(y)

        deep_discount = res['deep_discount']

        if deep_discount is None or deep_discount == 0:
            offers = ''
        else:
            offers = deep_discount['skin']['pc']['pre_hype_text']

        media = {
            "product_url": f"https://shopee.ph/--i.{shop_id}.{item_id}",
            "media_entity_id": f"{shop_id}.{item_id}",
            "product_price": price,
            "price_before_discount": discounted_price,
            "Discount percentage": res['discount'],
            "no_of_unites_sold": res['historical_sold'],
            "avg_rating": round(float(res['item_rating']['rating_star']), 1),
            "total_reviews": res['cmt_count'],
            "stock": res['stock'],
            "shop_location": res['shop_location'],
            "product_description": product_description,
            "additional_fields": additional_filed,
            "promotions": str(promotions),
            "product_name": res['name'],
            "brand_name": res['brand'],
            "wholesale": wholesale,
            "offers": offers,
            "breadcrumb_product": categories,
            "product_specification": product_type,
        }
        yield scrapy.Request(url=self.get_seller_url(item_id, shop_id),
                             callback=self.parse_seller,
                             errback=self.err,
                             dont_filter=True,
                             headers=self.get_headers(self.get_review_url(item_id, shop_id)),
                             meta={'media_entity': media_entity,
                                   'media': media})
        self.logger.info(f"Requests is going for shop id {shop_id} ")

    def parse_seller(self, response):
        res = json.loads(response.text)
        media = response.meta['media']
        res = res['data']

        vouchers_money = []
        if res['voucher_list'] == [] or res['voucher_list'] is None:
            voucher = ''
        else:
            for item in res['voucher_list']:
                voucher = item['discount_value']
                if voucher is None or voucher == '' or voucher == 0:
                    voucher = ''
                else:
                    voucher = round(int(voucher) / 100000)
                vouchers_money.append(voucher)

        vouchers_percentage = []
        if res['voucher_list'] == [] or res['voucher_list'] is None:
            voucher = ''
        else:

            for item in res['voucher_list']:
                voucher = item['discount_percentage']
                if voucher is None or voucher == '' or voucher == 0:
                    voucher = ''
                else:
                    voucher = str(voucher) + "%"
                vouchers_percentage.append(voucher)
        shop_vouchers = str(vouchers_money) + str(vouchers_percentage)

        self.yield_product_variation_details(
            category_url="Add Category URL Here",
            category_breadcrumb="Copy it from Website",
            product_url=media['product_url'],
            product_id=media['media_entity_id'],
            product_name=media['product_name'],
            brand_name=media['brand_name'],
            total_reviews=media['total_reviews'],
            avg_rating=media['avg_rating'],
            no_of_unites_sold=media['no_of_unites_sold'],
            product_breadcrumb=media['breadcrumb_product'],
            product_price=media['price_before_discount'],
            discount_price=media['product_price'],
            discount_percentage=media['Discount percentage'],
            sale_price="",
            wholesale=media['wholesale'],
            product_description=media['product_description'],
            product_info_selected="",
            product_info_options="",
            product_specifications=media['product_specification'],
            shop_vouchers=shop_vouchers,
            promotions=media['promotions'],
            sku="",
            offers=media['offers'],
            stock=media['stock'],
            shop_location=media['shop_location'],
            additional_fields=media['additional_fields'])

    @staticmethod
    def get_review_url(itemid, shopid):
        url = f"https://shopee.ph/api/v4/item/get?itemid={itemid}&shopid={shopid}"
        return url

    @staticmethod
    def get_seller_url(itemid, shopid):
        url = f"https://shopee.ph/api/v2/voucher_wallet/get_shop_vouchers_by_shopid?itemid={itemid}&shopid={shopid}&with_claiming_status=true"
        return url

    @staticmethod
    def get_headers(product_url):
        headers = {
            'authority': 'shopee.ph',
            'scheme': 'https',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'path': product_url.split('https://shopee.ph')[1],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
            'x-api-source': 'pc',
            'x-requested-with': 'XMLHttpRequest',
            'x-shopee-language': 'en'
        }

        return headers
