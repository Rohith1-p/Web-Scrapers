import json
import datetime
from datetime import datetime
import scrapy
from scrapy.conf import settings

from .setuserv_spider import SetuservSpider


class TikiVnSpider(SetuservSpider):
    name = 'tiki-vn-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("tiki Vn category Scraping starts")
        assert self.source == 'tiki_vn_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            if "q=" in product_url:
                keyword = product_url.split('q=')[1].split('&page')[0]
                product_id=''
            else:
                keyword=product_url.split('tiki.vn/')[1].split('/c')[0]
                product_id=product_url.split('/c')[1].split('?page')[0]
            print(product_id)
            media_entity = {'url': product_url, 'id': product_id,'keyword':keyword}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=self.get_category_url(page,product_url,keyword,product_id),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})
            self.logger.info(f"Generating products for {product_url} category ")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        category_url = media_entity["url"]
        page = response.meta["page"]
        category_id=media_entity['id']
        keyword=media_entity['keyword']
        res = json.loads(response.text)
        print('res_text', res)
        if res['data']:
            for item in res['data']:
                item_id = item['id']
                url = item['url_path']
                spid = url.split('spid=')[1]
                category_url = category_url
                product_id = spid
                product_name=item['name']
                if 'brand_name' in item.keys():
                    brand_name=item['brand_name']
                else:
                    brand_name=''
                avg_rating = item['rating_average']
                no_of_reviews=item['review_count']
                units_sold=''
                if 'quantity_sold' in item.keys():
                    if item['quantity_sold'] is not None:
                        units_sold=item['quantity_sold']['value']
                price = item['price']
                price = list(str(price))
                price.reverse()
                i=3
                while i<len(price):
                    price.insert(i,'.')
                    i=i+4
                price.reverse()
                price=''.join(price)+' ₫'
                product_url = f"https://tiki.vn/{url}"

                if item:
                    media = {
                        "client_id": str(self.client_id),
                        "media_source": str(self.source),
                        "category_url": category_url,
                        "product_url": product_url,
                        "media_entity_id": product_id,
                        "product_name": product_name,
                        "brand_name": brand_name,
                        "rating":avg_rating,
                        "rating_count":no_of_reviews,
                        "actual_price":price,
                        "units_sold":units_sold,
                        "type": "product_details",
                        "propagation": self.propagation,
                        "created_date": datetime.utcnow()
                    }
                    yield self.yield_category_details(category_url=media['category_url'],
                                                      product_url=media['product_url'],
                                                      product_id=media['media_entity_id'],
                                                      product_name=media['product_name'],
                                                      brand_name=media['brand_name'],
                                                      rating=media['rating'],
                                                      rating_count=media['rating_count'],
                                                      actual_price=media['actual_price'],
                                                      units_sold=media['units_sold'],
                                                      extra_info='',page_no=page)
            page += 1
            yield scrapy.Request(url=self.get_category_url(page,category_url,keyword,category_id),
                                 callback=self.parse_products,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    @staticmethod
    def get_category_url(page,product_url,keyword,product_num):
        if 'q=' in product_url:
            url=f'https://tiki.vn/api/v2/products?limit=40&include=advertisement&aggregations=2&trackity_id=a6bf45a4-1564-a4ab-8c64-e0c5584befdf&q={keyword}&page={page}'
        else:
            url=f'https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&include=advertisement&aggregations=2&trackity_id=a6bf45a4-1564-a4ab-8c64-e0c5584befdf&category={product_num}&page={page}&urlKey={keyword}'
        return url
