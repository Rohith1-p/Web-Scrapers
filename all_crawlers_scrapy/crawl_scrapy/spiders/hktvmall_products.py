import json
from bs4 import BeautifulSoup
import scrapy

from .setuserv_spider import SetuservSpider

import json
from bs4 import BeautifulSoup
import scrapy

from .setuserv_spider import SetuservSpider


class HktvmallProductsSpider(SetuservSpider):
    name = 'hktvmall-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Hktvmall Products Scraping starts")
        assert self.source == 'hktvmall_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            yield scrapy.Request(url=media_entity['url'],
                                 callback=self.product_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

            self.logger.info(f"Generating products for {product_url} category ")

    def product_info(self, response):

        media_entity = response.meta['media_entity']
        product_id = media_entity['id']
        product_url = media_entity['url']

        ##
        brand_product = response.css('meta[name="og:title"]::attr(content)').extract_first()
        try:
            brand_name = brand_product.split('|')[0]
        except:
            brand_name = ''

        try:
            product_name = brand_product.split('|')[1]
        except:
            product_name = brand_product

        price = response.css('span[class="discount"]::text').extract_first()
        '''removing "sold" chinese character from volume column '''
        no_of_units_sold = response.css('div[class="salesNumber-container"] span::text').extract_first()
        try:
            no_of_units_sold = no_of_units_sold.split("已售出 ")[1].strip()
        except:
            no_of_units_sold = ''

        try:
            avg_rating = response.text.split('"averageRating":')[1].split(',"name"')[0].strip()
        except:
            avg_rating = ''

        try:
            total_reviews = response.text.split("reviewStat.reviewCount = ")[1].split(";")[0]
        except:
            total_reviews = ''

        descriptions = response.css('div[class="tabarea"]')
        descriptions = descriptions.css('div[id="descriptionsTab"]')
        descriptions_title = descriptions.css('div[class="title"] span::text').extract_first()
        descriptions_body_list = descriptions.css('div[class="tabBody"] div ::text').extract()
        # descriptions_body = str(descriptions_body).replace('\\n', ' ', regex=True)
        descriptions_body = []
        for element in descriptions_body_list:
            descriptions_body.append(element.replace('\\n', '').strip())

        descriptions_product = str(descriptions_title) + ":" + str(descriptions_body)

        short_descriptions = response.css('span[class="short-desc"] ::text').extract()
        short_descriptions = ''.join(str(x) for x in short_descriptions)

        descriptions_product = short_descriptions + "||" + descriptions_product

        '''removing "capacity" chinese character from volume column '''
        volume = []
        for item in descriptions_body_list:
            if "容量" in item:
                item = item.replace('容量：', '').strip()
                volume.append(item.replace('- ', '').strip())

        '''removing "Package" chinese character from additional_fields column '''
        packing = []
        for item in descriptions_body_list:
            if "包裝" in item:
                item = item.replace('包裝：', '').replace('包裝 :', '').strip()
                packing.append(item.replace('-', '').replace('一盒', '').strip())

        storeName = response.text.split(',"storeName":"')[1].split('"')[0]
        store_code = response.text.split('"storeCode":"')[1].split('"')[0]
        seller_avg_rating = response.css('div[class="storeRatingValue"]::text').extract_first()
        seller_url = f'https://www.hktvmall.com/hktv/zh/main/{storeName}/s/{store_code}'
        yield from self.yield_product_details(category_url='',
                                              product_url=product_url,
                                              product_id=product_id,
                                              product_name=product_name,
                                              brand_name=brand_name,
                                              product_price=price,
                                              no_of_unites_sold=no_of_units_sold,
                                              avg_rating=avg_rating,
                                              total_reviews=total_reviews,
                                              product_description=descriptions_product,
                                              volume_or_weight=volume,
                                              additional_fields=packing,
                                              seller_name=storeName,
                                              seller_url=seller_url,
                                              seller_avg_rating=seller_avg_rating,
                                              seller_no_of_ratings="",
                                              seller_followers="",
                                              seller_no_of_unites_sold="")
