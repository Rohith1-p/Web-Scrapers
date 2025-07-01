import json
import datetime
from datetime import datetime
import scrapy
from .setuserv_spider import SetuservSpider


class LazadaPHSpider(SetuservSpider):
    name = 'lazada-ph-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Lazada PH Country Products Scraping starts")
        assert self.source == 'lazada_ph_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating products for {product_url} category ")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity["url"]
        # print('response.text', response.text)

        if 'www.lazada.com.ph:443' in response.text:
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            return

        _res_text_1 = response.css('script[type="application/ld+json"]::text').extract_first()
        _res_text_1 = "".join(_res_text_1)
        res_text_1 = json.loads(_res_text_1)

        breadcrumb = response.css('script[type="application/ld+json"]::text').extract()[-1]
        breadcrumb = json.loads(breadcrumb)
        product_breadcrumb = ''
        for i in breadcrumb['itemListElement']:
            val = i['name'] + ' > '
            product_breadcrumb += val

        product_info = {response.css('div.sku-prop h6::text').extract_first():
                        response.css('div.sku-prop span.sku-name ::text').extract_first()}
        try:
            product_info_2 = {response.css('div.sku-prop h6::text').extract()[1]:
                                response.css('div.sku-prop span.sku-name ::text').extract()[1]}

            product_info.update(product_info_2)
        except:
            pass
        try:
            product_info_3 = {response.css('div.sku-prop h6::text').extract()[-1]:
                                response.css('div.sku-prop span.sku-name ::text').extract()[-1]}

            product_info.update(product_info_3)
        except:
            pass

        product_info_multi = response.text.split('"skuBase":')[1].split(',"skus":')[0]
        product_info_multi = json.loads(product_info_multi+'}')

        final_dict = {}
        for i in product_info_multi['properties']:
            key = i['name']
            val = []
            for j in i['values']:
                val.append(j['name'])
            spec_dict = {key: str(val)}
            final_dict.update(spec_dict)

        if product_info == {None: None}:
            product_info = ''

        _specifications = response.text.split('"specifications":')[1].split(',"tracking"')[0]
        _specifications = json.loads(_specifications)
        specifications = {}
        for i in _specifications.values():
            specifications.update(i)

        try:
            avg_rating = res_text_1['aggregateRating']['ratingValue']
            total_reviews = res_text_1['aggregateRating']['ratingCount']
        except:
            avg_rating = ''
            total_reviews = 0
        try:
            sale_price = response.text.split('"priceText":"')[1].split('"')[0]
        except:
            sale_price = 'No Sale price'

        self.yield_product_variation_details(
            category_url="Add Category URL Here",
            category_breadcrumb="Copy it from Website",
            product_url=product_url,
            product_id=product_id,
            product_name=res_text_1['name'],
            brand_name=res_text_1['brand']['name'],
            total_reviews=total_reviews,
            avg_rating=avg_rating,
            no_of_unites_sold="Not Present",
            product_breadcrumb=product_breadcrumb,
            product_price=response.css('span[class=" pdp-price pdp-price_type_deleted pdp-price_color_lightgray pdp-price_size_xs"]::text').extract_first(),
            discount_price=response.css('span[class=" pdp-price pdp-price_type_normal pdp-price_color_orange pdp-price_size_xl"]::text').extract_first(),
            discount_percentage=response.css('span[class="pdp-product-price__discount"]::text').extract_first(),
            sale_price=sale_price,
            wholesale="",
            product_description=res_text_1['description'],
            product_info_selected=product_info,
            product_info_options=final_dict,
            product_specifications=specifications,
            shop_vouchers="",
            promotions=response.css('div[class="section-content"] div[class="tag-name"]::text').extract(),
            sku=res_text_1['sku'],
            offers="",
            stock="",
            shop_location="",
            additional_fields="")
