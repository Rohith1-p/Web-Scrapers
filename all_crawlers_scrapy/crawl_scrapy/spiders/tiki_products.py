import json
import scrapy
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class TikiProductsSpider(SetuservSpider):
    name = 'tiki-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Tiki Products Scraping starts")
        assert self.source == 'tiki_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            url = media_entity['url']
            spid = media_entity['id']
            yield scrapy.Request(url=self.get_products_api(url, spid),
                                 callback=self.parse_reviews,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url),
                                 meta={'media_entity': media_entity})

            self.logger.info(f"Generating products for {product_url} category ")

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        res = json.loads(response.text)
        url = f"https://tiki.vn/{res['url_path']}"
        if res["current_seller"] is not None:
            product_id = res["current_seller"]['product_id']
        else:
            product_id = product_url.split('spid=')[1]
        product_description = res['description']
        product_description = ' '.join(BeautifulSoup(product_description, "html.parser").findAll(text=True))
        try:
            brand = res['brand']['name']
        except:
            brand = ''
        try:
            sold = res['quantity_sold']['value']
        except:
            sold = 0

        origin = ''
        if '"specifications":[]' not in response.text:
            for item in res['specifications'][0]['attributes']:
                if 'Xuất xứ' == item['name']:
                    origin += item['value']

        volume = ''
        if '"specifications":[]' not in response.text:
            for item in res['specifications'][0]['attributes']:
                if 'Dung tích' == item['name']:
                    volume += item['value']
        try:
            price = list(str(res['price']))

            price.reverse()
            i=3
            while i<len(price):
                price.insert(i,'.')
                i=i+4
            price.reverse()
            price=''.join(price)+' ₫'
        except:
            price = ''
        try:
            list_price = list(str(res['list_price']))
            list_price.reverse()
            i=3
            while i<len(list_price):
                list_price.insert(i,'.')
                i=i+4
            list_price.reverse()
            list_price=''.join(list_price)+' ₫'
        except:
            list_price = ''

        try:
            discount_percentage = str(res['discount_rate'])
            discount_percentage+='%'
        except:
            discount_percentage = ''



        media = {
            "category_url": product_url,
            "product_url": product_url,
            "media_entity_id": product_id,
            "product_description": product_description,
            "no_of_unites_sold": sold,
            "type": "product_details",
            "price": price,
            "list_price" : list_price,
            "discount_percentage" : discount_percentage,
            "avg_rating": round(float(res['rating_average']), 1),
            "total_reviews": res['review_count'],
            "product_name": res["name"],
            "brand_name": brand,
            "additional_fields": origin,
            "volume_or_weight": volume
        }
        if '"current_seller":null' not in response.text:
            shop_id = res['current_seller']['id']
            yield scrapy.Request(url=self.get_seller_url(shop_id),
                                 callback=self.parse_seller,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(url),
                                 meta={'media_entity': media_entity,
                                       'media': media, 'seller': 'yes'})
            self.logger.info(f"Requests is going for shop id {shop_id} ")
        else:
            yield scrapy.Request(url="https://tiki.vn/",
                                 callback=self.parse_seller,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(url),
                                 meta={'media_entity': media_entity,
                                       'media': media, 'seller': 'no'})

    def parse_seller(self, response):
        media = response.meta['media']
        seller = response.meta['seller']

        if seller == 'yes':
            res = json.loads(response.text)
            res = res['data']['seller']
            seller_url = res['url']
            seller_name = res['name']
            seller_avg_rating = round(float(res['avg_rating_point']), 1)
            seller_followers = res['total_follower']
            seller_no_of_ratings = res['review_count']
        else:
            seller_url = ""
            seller_name = ""
            seller_avg_rating = ""
            seller_followers = ""
            seller_no_of_ratings = ""

        yield self.yield_product_details \
            (category_url=media['category_url'],
             product_url=media['product_url'],
             product_id=media['media_entity_id'],
             product_name=media['product_name'],
             brand_name=media['brand_name'],
             discount_price=media['price'],
             product_price = media['list_price'],
             discount = media['discount_percentage'],
             no_of_unites_sold=media['no_of_unites_sold'],
             avg_rating=media['avg_rating'],
             total_reviews=media['total_reviews'],
             product_description=media['product_description'],
             volume_or_weight=media['volume_or_weight'],
             additional_fields=media['additional_fields'],
             seller_name=seller_name,
             seller_url=seller_url,
             seller_avg_rating=seller_avg_rating,
             seller_no_of_ratings=seller_no_of_ratings,
             seller_followers=seller_followers,
             seller_no_of_unites_sold="")

    @staticmethod
    def get_products_api(url, seller_product_id):
        if '?' in url:
            seller_product_id = url.split('?')[1].split('=')[1]
            url = url.split('?')[0]
        _id = url.split('-')[-1].split('.')[0].replace('p', '')
        url = f"https://tiki.vn/api/v2/products/{_id}?platform=web&spid={seller_product_id}"
        print("url is",url)
        return url

    @staticmethod
    def get_seller_url(shopid):
        url = f"https://tiki.vn/api/shopping/v2/widgets/seller?seller_id={shopid}&platform=desktop"
        return url

    @staticmethod
    def get_headers(url):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Referer': url,
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
            'x-guest-token': 'M5sF3Zk8hveUOonGT7HDBt6CNSwKLly2'
        }

        return headers
