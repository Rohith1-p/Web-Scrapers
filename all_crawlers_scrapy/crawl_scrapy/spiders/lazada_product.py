import json
import re
import scrapy
from .setuserv_spider import SetuservSpider
from bs4 import BeautifulSoup
from urllib.parse import urlsplit



class LazadaProductsSpider(SetuservSpider):
    name = 'lazada-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Lazada Products Scraping starts")
        assert self.source == 'lazada_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            country_code = urlsplit(product_url).netloc[-2:]
            media_entity["country_code"] = country_code
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_product,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity},
                                 headers=self.get_headers())

    def parse_product(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        country_code = media_entity["country_code"]
        product_id = media_entity["id"]
        print("responsee***************",response.text)

        if 'www.lazada.co.id:443' in response.text or "RGV587_ERROR" in response.text or "lazada_waf_block"  in response.text or "#nocaptcha" in response.text:
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_product,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity},
                                 headers=self.get_headers())
            return

        self.logger.info(f"200 response came for product url {product_url}")
        try:
            full_desc = response.text.split('var __moduleData__ = ')[1].split('var __googleBot__ = "";')[0].strip()
            full_desc = full_desc[:-1].strip()
            full_desc = json.loads(full_desc)
            print("full_desc*****", full_desc)
        except:
            full_desc = {}

        try:
            _res_text_1 = response.css('script[type="application/ld+json"]::text').extract_first()
            _res_text_1 = "".join(_res_text_1)
            res_text_1 = json.loads(_res_text_1)
            print("res_text_1*****", res_text_1)
        except:
            res_text_1 = {}

        if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th'}:
            if country_code == "my":
                try:
                    product_description = full_desc['data']['root']['fields']['product']['highlights']
                    product_description = ' '.join(BeautifulSoup(product_description, "html.parser").stripped_strings)
                except:
                    try:
                        product_description = res_text_1['description']
                    except:
                        product_description = '-'
            elif country_code == "vn" or country_code == "ph" or country_code == "sg" or country_code == "th" or country_code == "id":
                try:
                    try:
                        product_description = full_desc['data']['root']['fields']['product']['highlights']
                        product_description = ' '.join(BeautifulSoup(product_description, "html.parser").stripped_strings)
                        product_description_2 = full_desc['data']['root']['fields']['product']['desc']
                        product_description_2 = ' '.join(BeautifulSoup(product_description_2, "html.parser").stripped_strings)
                        product_description = product_description + product_description_2
                    except:
                        try:
                            product_description = full_desc['data']['root']['fields']['product']['desc']
                            product_description = ' '.join(BeautifulSoup(product_description, "html.parser").stripped_strings)
                            if product_description == '':
                                product_description = full_desc['data']['root']['fields']['product']['highlights']
                                product_description = ' '.join(BeautifulSoup(product_description, "html.parser").stripped_strings)
                        except:
                            product_description = full_desc['data']['root']['fields']['product']['highlights']
                            product_description = ' '.join(BeautifulSoup(product_description, "html.parser").stripped_strings)
                            if product_description == '':
                                product_description = res_text_1['description']
                except:
                    try:
                        product_description = res_text_1['description']
                    except:
                        product_description = '-'
        print("product_description", product_description)

        try:
            avg_rating = res_text_1['aggregateRating']['ratingValue']
            total_reviews = res_text_1['aggregateRating']['ratingCount']
        except:
            avg_rating = 0
            total_reviews = 0

        try:
            print("sellller", 'https:' + response.css('div.seller-name__detail a::attr(href)').extract_first())
            _seller_url = 'https:' + response.css('div.seller-name__detail a::attr(href)').extract_first()
            print("_seller_url", _seller_url)
            seller_url = _seller_url.split('?')[0]
            print("seller_url", seller_url)
        except:
            seller_url = '-'

        try:
            product_name = res_text_1['name']
        except:
            product_name = full_desc['data']['root']['fields']['product']['title']

        try:
            brand_name = res_text_1['brand']['name']
        except:
            brand_name = full_desc['data']['root']['fields']['productOption']['product']['brand']['name']


        product_price= response.css('span[class="pdp-price pdp-price_type_normal '
                                          'pdp-price_color_orange pdp-price_size_xl"]::text').extract_first()
        original_price = response.css('span[class="pdp-price pdp-price_type_deleted '
     'pdp-price_color_lightgray pdp-price_size_xs"]::text').extract_first()

        discount_percentage = response.css('span[class="pdp-product-price__discount"]::text').extract_first()


        media = {
            "product_price": response.css('span[class="pdp-price pdp-price_type_normal '
                                          'pdp-price_color_orange pdp-price_size_xl"]::text').extract_first(),
            "total_reviews": total_reviews,
            "avg_rating": avg_rating,
            "product_name": product_name,
            "brand_name": brand_name,
            "product_description": product_description,
            "seller_name": response.css('div.seller-name__detail a::text').extract_first(),
            "seller_url": seller_url,
            "original_price": original_price,
            "discount_percentage": discount_percentage
        }

        if seller_url == '-':
            review_type = "product"
            date_remove = self.start_date
            page_no = 1
            status = "Fail"
            dump_name = self.source + product_id + self.gsheet_id
            self.log_error(review_type, product_url, date_remove, page_no, self.gsheet_id, status, dump_name, response)
        else:
            yield scrapy.Request(url=seller_url,
                                 callback=self.get_seller_api,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'media': media})

    def get_seller_api(self, response):
        media_entity = response.meta["media_entity"]
        media = response.meta['media']

        if 'www.lazada.co.id:443' in response.text or "RGV587_ERROR" in response.text or "lazada_waf_block" in response.text or "#nocaptcha" in response.text:
            yield scrapy.Request(url=media['seller_url'],
                                 callback=self.get_seller_api,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'media': media})
            return

        media = response.meta['media']
        print("seller api", response.text)
        seller_details_api = 'https:' + response.text.split('window.shopPageDataApi')[1].split("'")[1]
        print("seller_details_api", seller_details_api)
        yield scrapy.Request(url=seller_details_api,
                             callback=self.parse_seller,
                             errback=self.err,
                             dont_filter=True,
                             meta={'media_entity': media_entity,
                                   'media': media,
                                   'seller_details_api': seller_details_api})

    def parse_seller(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity["url"]
        media = response.meta['media']
        seller_details_api = response.meta['seller_details_api']

        if 'www.lazada.co.id:443' in response.text or "RGV587_ERROR" in response.text or "lazada_waf_block" in response.text or "#nocaptcha" in response.text:
            yield scrapy.Request(url=seller_details_api,
                                 callback=self.parse_seller,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'media': media,
                                       'seller_details_api': seller_details_api})
            return

        res = json.loads(response.text)
        seller = res['result']['components']

        seller = str(seller)
        followers = seller.split('followUserNumber')[-1].split("'")[1].split(':')[1].split(',')[0]
        _avg_rating = seller.split('shopRating')[1].split(":")[1].split(",")[0]
        try:
            avg_rating = re.findall(r'\d+', _avg_rating)[0]
            avg_rating = int(avg_rating) / 20
        except:
            avg_rating = '-'
        self.logger.info(f"Item got Scraped")
        self.yield_product_details \
            (category_url='-',
             product_url=product_url,
             product_id=product_id,
             product_name=media['product_name'],
             brand_name=media['brand_name'],
             discount_price=media['product_price'],
             discount = media["discount_percentage"],
             product_price = media["original_price"],
             no_of_unites_sold='-',
             avg_rating=media['avg_rating'],
             total_reviews=media['total_reviews'],
             product_description=media['product_description'],
             volume_or_weight='-',
             additional_fields='-',
             seller_name=media['seller_name'],
             seller_url=media['seller_url'],
             seller_avg_rating=avg_rating,
             seller_no_of_ratings='-',
             seller_followers=followers,
             seller_no_of_unites_sold='-')

    @staticmethod
    def get_category_url(page):
        url = f'https://www.lazada.co.id/shop-motorcycle-oils-fluids/?ajax=true&page={page}' \
              f'&spm=a2o4j.searchlistcategory.cate_12.11.2bd150cfrnyP5H'

        return url

    @staticmethod
    def get_headers():
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
        return headers
