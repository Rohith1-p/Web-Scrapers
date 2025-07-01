import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
import scrapy
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider


class TokopediaProductsSpider(SetuservSpider):
    name = 'tokopedia-products-scrapjjjer'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name)
        self.logger.info("Tokopedia Products Scraping starts")
        assert self.source == 'tokopedia_producthhs'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            page = 0
            yield scrapy.Request(url=self.get_category_url(),callback=self.parse_category,
                                 body=json.dumps(self.category_payload(product_id)),errback=self.err, dont_filter=True,method="POST",
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating products for {product_url} category ")
    def parse_category(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity["url"]
        page = response.meta["page"]
        res = json.loads(response.text)
        print(product_url)

        if res['data']['CategoryProducts']['data']:
            for item in res['data']['CategoryProducts']['data']:
                    media = {
                        "client_id": str(self.client_id),
                        "media_source": str(self.source),
                        "category_url": product_url,
                        "product_url": item["url"],
                        "media_entity_id": item['id'],
                        "type": "product_details",
                        "product_price": item['price'],
                        "total_reviews": item['countReview'],
                        "additional_fields": '',
                        "product_name": item["name"],
                        "brand_name": item["name"].split(' ')[0],
                        "seller_url":item['shop']['url'],
                        "propagation": self.propagation,
                        "created_date": datetime.utcnow()

                     }
                    url = item['url']
                    yield scrapy.Request(url= url,callback=self.parse_seller_details,errback=self.err, dont_filter=True,meta={'media_entity': media_entity, 'page': page,'media':media})


            page += 1
            yield scrapy.Request(url=self.get_category_url(),callback=self.parse_category,body=json.dumps(self.category_payload(product_id)),errback=self.err, dont_filter=True,method="POST",
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"{page} is going")

    def parse_seller_details(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        page = response.meta["page"]
        media_a = response.meta['media']
        product_url = media_a["seller_url"]
        print(product_url)
        #res = json.loads(response.text)
        print(response.text)

        product_description = response.css('div[data-testid="lblPDPDescriptionProduk"]::text').extract()
        #product_description = BeautifulSoup(_product_description, "lxml").text
        Units_sold = response.css('div[data-testid="lblPDPDetailProductSoldCounter"]::text').extract()
        volume = response.css('span.main::text').extract()[1]
        avg_rating = response.css('span.main[data-testid="lblPDPDetailProductRatingNumber"]::text').extract()
        print(avg_rating)
        media_b = {'product_description': product_description,
                    "Units_sold": '',
                    "volume/weight":volume,
                    "avg_rating": avg_rating,}
        media_c = {}
        media_c.update(media_a)
        media_c.update(media_b)

        yield scrapy.Request(url=product_url,callback=self.parse_seller_rating,errback=self.err, dont_filter=True,meta={'media_entity': media_entity, 'page': page,'media': media_c})


    def parse_seller_rating(self,response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        page = response.meta["page"]
        #res = json.loads(response.text)
        media_c = response.meta['media']

        print(response.text)
        seller_followers = response.css("h6.css-1xrfbuw-unf-heading-unf-heading::text").extract()
        seller_avg_rating =  response.css("h2.css-3dui13-unf-heading-unf-heading::text").extract()
        seller_no_of_ratings = response.css("h6.css-tnwjjq-unf-heading-unf-heading::text").extract()
        seller_no_of_ratings = re.findall(r'\d+', seller_no_of_ratings)[0]
        seller_no_of_unites_sold = response.css("h2.css-dn9ti9-unf-heading-unf-heading::text").extract()
        seller_no_of_unites_sold = re.findall(r'\d+', seller_no_of_unites_sold)[0]


        meida_seller = {"seller_followers":seller_followers,
                        "seller_avg_rating":seller_avg_rating,
                        "seller_no_of_ratings":seller_no_of_ratings,
                        "seller_no_of_unites_sold":seller_no_of_unites_sold}
        media = {}
        media.update(media_c)
        media.update(meida_seller)
        self.logger.info(f"Item got Scraped")
        yield media



    @staticmethod
    def category_payload(product_id):
        payload = {"operationName":"SearchProductQuery","variables":{"params":f"page={product_id}&ob=&identifier=otomotif_perawatan-kendaraan_pelumas&sc=1366&user_id=162422359&rows=60&start=61&source=directory&device=desktop&page={product_id}&related=true&st=product&safe_search=false","adParams":f"page={product_id}&page={product_id}&dep_id=1366&ob=&ep=product&item=15&src=directory&device=desktop&user_id=162422359&minimum_item=15&start=61&no_autofill_range=5-14"},"query":"query SearchProductQuery($params: String, $adParams: String) {\n  CategoryProducts: searchProduct(params: $params) {\n    count\n    data: products {\n      id\n      url\n      imageUrl: image_url\n      imageUrlLarge: image_url_700\n      catId: category_id\n      gaKey: ga_key\n      countReview: count_review\n      discountPercentage: discount_percentage\n      preorder: is_preorder\n      name\n      price\n      original_price\n      rating\n      wishlist\n      labels {\n        title\n        color\n        __typename\n      }\n      badges {\n        imageUrl: image_url\n        show\n        __typename\n      }\n      shop {\n        id\n        url\n        name\n        goldmerchant: is_power_badge\n        official: is_official\n        reputation\n        clover\n        location\n        __typename\n      }\n      labelGroups: label_groups {\n        position\n        title\n        type\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  displayAdsV3(displayParams: $adParams) {\n    data {\n      id\n      ad_ref_key\n      redirect\n      sticker_id\n      sticker_image\n      productWishListUrl: product_wishlist_url\n      clickTrackUrl: product_click_url\n      shop_click_url\n      product {\n        id\n        name\n        wishlist\n        image {\n          imageUrl: s_ecs\n          trackerImageUrl: s_url\n          __typename\n        }\n        url: uri\n        relative_uri\n        price: price_format\n        campaign {\n          original_price\n          discountPercentage: discount_percentage\n          __typename\n        }\n        wholeSalePrice: wholesale_price {\n          quantityMin: quantity_min_format\n          quantityMax: quantity_max_format\n          price: price_format\n          __typename\n        }\n        count_talk_format\n        countReview: count_review_format\n        category {\n          id\n          __typename\n        }\n        preorder: product_preorder\n        product_wholesale\n        free_return\n        isNewProduct: product_new_label\n        cashback: product_cashback_rate\n        rating: product_rating\n        top_label\n        bottomLabel: bottom_label\n        __typename\n      }\n      shop {\n        image_product {\n          image_url\n          __typename\n        }\n        id\n        name\n        domain\n        location\n        city\n        tagline\n        goldmerchant: gold_shop\n        gold_shop_badge\n        official: shop_is_official\n        lucky_shop\n        uri\n        owner_id\n        is_owner\n        badges {\n          title\n          image_url\n          show\n          __typename\n        }\n        __typename\n      }\n      applinks\n      __typename\n    }\n    template {\n      isAd: is_ad\n      __typename\n    }\n    __typename\n  }\n}\n"}
        # payload = {"operationName":"SearchProductQuery","variables":{"params":f"page={page}&ob=&identifier=otomotif_perawatan-kendaraan_pelumas&sc={product_id}&user_id=0&rows=60&start={start}&source=directory&device=desktop&page={page}&related=true&st=product&safe_search=false","adParams":f"page={page}&page={page}&dep_id={product_id}&ob=&ep=product&item=15&src=directory&device=desktop&user_id=0&minimum_item=15&start={start}&no_autofill_range=5-14"},"query":"query SearchProductQuery($params: String, $adParams: String) {\n  CategoryProducts: searchProduct(params: $params) {\n    count\n    data: products {\n      id\n      url\n      imageUrl: image_url\n      imageUrlLarge: image_url_700\n      catId: category_id\n      gaKey: ga_key\n      countReview: count_review\n      discountPercentage: discount_percentage\n      preorder: is_preorder\n      name\n      price\n      original_price\n      rating\n      wishlist\n      labels {\n        title\n        color\n        __typename\n      }\n      badges {\n        imageUrl: image_url\n        show\n        __typename\n      }\n      shop {\n        id\n        url\n        name\n        goldmerchant: is_power_badge\n        official: is_official\n        reputation\n        clover\n        location\n        __typename\n      }\n      labelGroups: label_groups {\n        position\n        title\n        type\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  displayAdsV3(displayParams: $adParams) {\n    data {\n      id\n      ad_ref_key\n      redirect\n      sticker_id\n      sticker_image\n      productWishListUrl: product_wishlist_url\n      clickTrackUrl: product_click_url\n      shop_click_url\n      product {\n        id\n        name\n        wishlist\n        image {\n          imageUrl: s_ecs\n          trackerImageUrl: s_url\n          __typename\n        }\n        url: uri\n        relative_uri\n        price: price_format\n        campaign {\n          original_price\n          discountPercentage: discount_percentage\n          __typename\n        }\n        wholeSalePrice: wholesale_price {\n          quantityMin: quantity_min_format\n          quantityMax: quantity_max_format\n          price: price_format\n          __typename\n        }\n        count_talk_format\n        countReview: count_review_format\n        category {\n          id\n          __typename\n        }\n        preorder: product_preorder\n        product_wholesale\n        free_return\n        isNewProduct: product_new_label\n        cashback: product_cashback_rate\n        rating: product_rating\n        top_label\n        bottomLabel: bottom_label\n        __typename\n      }\n      shop {\n        image_product {\n          image_url\n          __typename\n        }\n        id\n        name\n        domain\n        location\n        city\n        tagline\n        goldmerchant: gold_shop\n        gold_shop_badge\n        official: shop_is_official\n        lucky_shop\n        uri\n        owner_id\n        is_owner\n        badges {\n          title\n          image_url\n          show\n          __typename\n        }\n        __typename\n      }\n      applinks\n      __typename\n    }\n    template {\n      isAd: is_ad\n      __typename\n    }\n    __typename\n  }\n}\n"}
        return payload

    @staticmethod
    def get_category_url():
        url = 'https://gql.tokopedia.com/'
        return url
