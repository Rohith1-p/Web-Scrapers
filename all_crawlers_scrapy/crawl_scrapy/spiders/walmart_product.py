import json
import ast
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider
import re
from urllib.parse import urlparse


class WalmartProductsSpider(SetuservSpider):
    name = 'walmart-product-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("walmart Products Scraping starts")
        assert self.source == 'walmart_product'

    def start_requests(self):
        self.logger.info("Starting requests")
        print("inside start_requests")
        # for product_url, product_id in zip(self.start_urls, self.product_ids):
        #     media_entity = {'url': product_url, 'id': product_id}
        #     #media_entity = {**media_entity, **self.media_entity_logs}
        #     media_entity = {**media_entity, **self.media_entity_logs}
        #     print("media eniity logs are ", self.media_entity_logs)
        #     print("media_entity is: ", media_entity)

        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            print("media eniity logs are ", self.media_entity_logs)
            country_code = urlparse(product_url).netloc.split('.')[2]
            print("country code is ", country_code)
            if country_code == "ca":
                print("inside walmart ca product scraper")
                yield scrapy.Request(url=product_url,
                                 callback=self.parse_products_ca,
                                 errback=self.err,
                                 meta={'media_entity': media_entity})

                pass
            else:
                yield scrapy.Request(url=product_url,
                                 callback=self.parse_products_us,
                                 errback=self.err,
                                 meta={'media_entity': media_entity})
            self.logger.info(" scraping is going")

    def parse_products_ca(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        print("inside parse_products_ca method")
        if 'Are you human?' in response.text or 'Robot or human' in response.text or 'class="re-captcha"' in response.text or 'Please download one of the supported browsers to keep shopping' in response.text:
            print("Robot or human occured, retrying")
            yield scrapy.Request(url= product_url,
                                 callback=self.parse_products_ca,
                                 errback=self.err,
                                 meta={'media_entity': media_entity})
            return

        print("dumping response of ca_products_scraper")
        self.dump(response, "html")
        print("response text of ca prodcuct scraper is ", response.text)

        title = response.css('h1[data-automation="product-title"]::text').extract()
        brand = response.css('a[class="css-1syn49 elkyjhv0"]::text').extract()[1]
        rating = response.css('div[data-automation="rating-stars"][class="css-1e5eh5o e1sr67l90"]::attr("aria-label")').extract_first()
        breadcrumb = response.css('a[class="css-ynt9mz elkyjhv0"]::text').extract()
        category_url = response.css('a[class="css-ynt9mz elkyjhv0"]::text').extract()[3]


        review_count_1 = response.css('div[class="css-k008qs e187mulv0"]::attr("aria-label")').extract_first()
        #review_count_2 = response.css('div[class="css-k008qs e187mulv0"]::text').extract_first()
        price = response.css('span[class="css-2vqe5n esdkp3p0"][data-automation="buybox-price"]::text').extract()

        seller_name = response.css('button[data-automation="vendor-link"]::text').extract()

        #long_description = response.css('div[data-automation="long-description"]::text').extract()
        #specification_description = response.css('div[data-automation="feature-specifications"]::text').extract()

        #Outputs With json response
        #sub_response = response.css('script[id="__NEXT_DATA__"]').extract_first()

        title = response.css('h1[data-automation="product-title"]::text').extract_first()
        rating = response.css('div[data-automation="rating-stars"][class="css-14giasw e1sr67l90"]::attr("aria-label")').extract_first()
        breadcrumb = "/".join(breadcrumb)
        category_url = response.css('a[class="css-ynt9mz elkyjhv0"]::attr("href")').extract()[3]
        category_url_new = "https://www.walmart.ca"+category_url
        #print("category_urls list", response.css('a[class="css-ynt9mz elkyjhv0"]::attr("href")').extract())
        print("new changes are, title:{}, rating:{}, breadcrumb:{}, category_url:{}".format(title, rating, breadcrumb, category_url))




        response_new = str(response.text).split("window.__PRELOADED_STATE__")[1][1:]
        print("response_new is: ", response_new)
        response_final_json = response_new.split('storeSelector"')[0][:-2]+"}"
        response_dic = json.loads(response_final_json)
        skus_id = response_dic["product"]["activeSkuId"]
        price_range = response_dic["entities"]["skus"][skus_id]["endecaDimensions"][1]["value"]
        seller_id = response_dic["entities"]["skus"][skus_id]["endecaDimensions"][0]["value"]

        longDescription = response_dic["entities"]["skus"][skus_id]["longDescription"]
        featuresSpecifications = response_dic["entities"]["skus"][skus_id]["featuresSpecifications"]

        isSoldByWeight = response_dic["entities"]["skus"][skus_id]["grocery"]["isSoldByWeight"]
        maxWeight = response_dic["entities"]["skus"][skus_id]["grocery"]["maxWeight"]
        minWeight = response_dic["entities"]["skus"][skus_id]["grocery"]["minWeight"]
        sellQuantity = response_dic["entities"]["skus"][skus_id]["grocery"]["sellQuantity"]
        print("Required fields using json response are, skus_id:{}, price_range:{}, seller_id:{}, longDescription:{}, featuresSpecifications:{}, isSoldByWeight:{}, maxWeight:{}, minWeight:{}, sellQuantity:{}".format(skus_id, price_range, seller_id, longDescription, featuresSpecifications, isSoldByWeight, maxWeight, minWeight, sellQuantity))








        print(" Required fields are, title:{}, brand:{}, rating:{}, review_count_1:{},price:{}, seller_name:{}, breadcrumb:{}, category_url :{}".format(title, brand, rating, review_count_1, price, seller_name, breadcrumb, category_url))
        # product_url,product_id,product_name, brand_name,product_price,avg_rating,
        #                       seller_name,category_url,
        #                       volume_or_weight='',additional_fields='',seller_url='', seller_avg_rating='',about='',
        #                       seller_no_of_ratings='',no_of_unites_sold='',total_reviews='',
        #                       seller_followers='', seller_no_of_unites_sold='',discount_price='', discount='',rating_count='',
        #                       best_seller_rating='',rating_map='',asin='', stock='',product_description='', product_information='', product_specifications= " ", breadcrumb = " "
        self.yield_product_details \
            (product_url = product_url,
             product_id = product_id,
             product_name= title,
             brand_name= brand,
             product_price= price_range,
             avg_rating= rating,
             seller_name= {"seller_id":seller_id},
             category_url= category_url_new,
             volume_or_weight= {"isSoldByWeight": isSoldByWeight, "maxWeight":maxWeight, "minWeight":minWeight},
             additional_fields='',
             seller_avg_rating='',
             about='',
             seller_no_of_ratings='',
             total_reviews= review_count_1,
             no_of_unites_sold = sellQuantity,
             discount_price='',
             discount='',
             rating_count = '',
             best_seller_rating='',
             rating_map='',
             asin='',
             stock='',
             product_description= str(longDescription)+ "\n" +str(featuresSpecifications),
             product_information='',
             product_specifications= "",
             breadcrumb = breadcrumb
             )

    def parse_products_us(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity["id"]
        print("inside parse_products of walmart_product")
        if 'Robot or human' in response.text or 'class="re-captcha"' in response.text or 'Please download one of the supported browsers to keep shopping' in response.text:
            print("Robot or human occured, retrying")
            yield scrapy.Request(url= product_url,
                                 callback=self.parse_products_ca,
                                 errback=self.err,
                                 meta={'media_entity': media_entity})
            return
        print("dumping")
        self.dump(response, "html")
        #media_entity = response.meta["media_entity"]

        print("response text is: ", response.text)


        product_name = response.css("h1.f3::text").extract_first()
        print("product_name of walmart product scraper ", product_name)

        brand_name = response.css('a[class="f6 mid-gray lh-title"]::text').extract_first()
        print("new brand_name", "-".join(brand_name.split(" ")))

        breadcrumb = response.css('span[itemprop="name"]::text').extract()
        print("breadcrumb is ", breadcrumb)
        #print("brand is with html new", response.css('div[itemprop="brand"]').extract())
        # print("brand_name of walmart product scraper ", brand_name)
        # print("brand name3: ", response.css('div.f6.mid-gray.lh-title::text').extract())

        #brand = response.css('p[class="mv0 lh-copy f6 mid-gray"]').extract()[0]
        #print("brand 2 is senconsd: ",brand.css('span::text').extract())

        price = response.css('span[itemprop="price"]::text').extract_first()
        print("price of walmart product scraper ", price)

        avg_rating = response.css('span[class="f7 rating-number"]::text').extract_first()[1:-1]
        print("avg_rating of walmart product scraper ", avg_rating)

        # category_url_lst = response.css('a[class="w_E gray "]::attr(href)').extract()
        # print("category_url_lst is: ", category_url_lst)
        # category_url = 'https://www.walmart.com/'+str(category_url_lst[1])
        # print("category url is ", category_url)
        category_url = 'https://www.walmart.com/c/brand/'+"-".join(brand_name.split(" "))


        units_sold = response.css('span[class="w_En f6 lh-copy fw7 w_Eo w_Et"]::text').extract_first()
        print("units sold are ", units_sold)

        #Extraction using json
        sub_response = response.css('script[id="__NEXT_DATA__"]').extract_first()
        split_data = re.split(r'''<script id="__NEXT_DATA__" type="application/json" .*">''',str(sub_response),1)
        response_data = split_data[1][:-9]
        response_dic = json.loads(response_data)

        #seller_name = response.css('a[link-identifier="Generic Name"][data-testid="seller-name-link"]::text').extract_first()
        short_description = response_dic["props"]["pageProps"]["initialData"]["data"]["product"]["shortDescription"]
        long_description = response_dic["props"]["pageProps"]["initialData"]["data"]["idml"]["longDescription"]
        product_details = "Product_details"+ "\n" +str(short_description)+"\n"+str(long_description)
        specifications = response_dic["props"]["pageProps"]["initialData"]["data"]["idml"]["specifications"]
        seller_name = response_dic["props"]["pageProps"]["initialData"]["data"]["product"]["sellerName"]

        print("short_description,long_description,product_details,specifications,seller_name of walmart product scraper ",
        short_description, long_description, product_details, specifications, seller_name)

        additional_fields = {}

        # print("details are", response.css('div[class="ph3 pb4 pt1"]').extract())
        # product_details_html = response.css('div[class="ph3 pb4 pt1"]').extract()
        # print("product_details_html ", product_details_html)
        # soup = BeautifulSoup(product_details_html, 'html.parser')
        # print("soup is:", soup)
        # product_details = soup.text
        #
        # product_specifications_html = response.css('div[class="ph3 pb4 pt1"]').extract()[1]
        # soup = BeautifulSoup(product_specifications_html, 'html.parser')
        # product_specifications = soup.text

        review_count = response.css('a[link-identifier="reviewsLink"]::text').extract_first()
        review_count = review_count.split(" ")[0]
        print("review_count is ", review_count)

        #product_id =
        scraper_type = "Product Details"
        units_sold = ""
        volume_or_weight = ""



        self.yield_product_details \
            (category_url= category_url,
             product_url= product_url,
             product_id= product_id,
             product_name= product_name,
             brand_name= brand_name,
             product_price= price,
             no_of_unites_sold= units_sold,
             avg_rating= avg_rating,
             total_reviews= review_count,
             product_description= product_details,
             volume_or_weight= "",
             additional_fields= "",
             seller_name= seller_name,
             seller_url= "", #f"https://shopee.co.id/{res['data']['account']['username']}",
             seller_avg_rating= "",
             seller_no_of_ratings= "",
             seller_followers= "",
             seller_no_of_unites_sold = "",
             product_specifications = specifications,
             breadcrumb = "/".join(breadcrumb))








        # price = 'Rp' + str(int(res['price']) / 100000000)
        # if res['price'] != res['price_max']:
        #     price = 'Rp' + str(int(res['price']) / 100000000) + '-' \
        #             + 'Rp' + str(int(res['price_max']) / 100000000)
        #
        #
        # product_description = res['description']
        # if product_description == '':
        #     yield scrapy.Request(url=self.get_review_url(item_id, shop_id),
        #                          callback=self.parse_products,
        #                          errback=self.err, dont_filter=True,
        #                          meta={'media_entity': media_entity})
        #     return
        #
        # brand_name = res['brand']
        # if brand_name == '' or brand_name is None:
        #     brand_name = ''
        # else:
        #     brand_name = res['brand']
        # try:
        #     volume = ''
        #     for item in res['attributes']:
        #         if 'Volume' in item['name']:
        #             volume += item['value']
        # except:
        #     volume = ''
        #
        # viscosity = []
        # for item in res['tier_variations']:
        #     if 'VISC' in item['name']:
        #         viscosity += item['options']
        #
        # media = {
        #     "category_url": "https://shopee.co.id/Oli-Pelumas-cat.155.12364",
        #     "product_url": f"https://shopee.co.id/--i.{shop_id}.{item_id}",
        #     "media_entity_id": f"{shop_id}.{item_id}",
        #     "product_price": price,
        #     "no_of_unites_sold": res['historical_sold'],
        #     "avg_rating": round(float(res['item_rating']['rating_star']), 1),
        #     "total_reviews": res['cmt_count'],
        #     "product_description": product_description,
        #     "additional_fields": viscosity,
        #     "volume/weight": volume,
        #     "product_name": res['name'],
        #     "brand_name": brand_name,
        # }
        # yield scrapy.Request(url=self.get_seller_url(shop_id),
        #                      callback=self.parse_seller,
        #                      errback=self.err,
        #                      dont_filter=True,
        #                      headers=self.get_headers(self.get_review_url(item_id, shop_id)),
        #                      meta={'media_entity': media_entity,
        #                            'media': media})
        # self.logger.info(f"Requests is going for shop id {shop_id} ")
