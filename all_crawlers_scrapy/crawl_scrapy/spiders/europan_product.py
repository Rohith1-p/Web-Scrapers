import json
import scrapy
from .setuserv_spider import SetuservSpider

class EuraponSpider(SetuservSpider):
    name = 'eurapon-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id,env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name,env)
        self.logger.info("Eurapon Products Scraping starts")
        assert self.source == 'eurapon_reviews'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_products,
                                  dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating products for {product_url}  ")

    def parse_products(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        product_name = response.css('h1.product--title::text').extract()
        no_of_reviews = response.css('span.rating--count::text').extract()
        if len(no_of_reviews)>0:
            no_of_reviews = no_of_reviews[0]
        else:
            no_of_reviews='0'
        price = response.css('div.product-detail-pzn__item .channel_idl::text').extract()

        product_description = response.css('div.product-description__inner p::text, strong::text, div.product--bulletpoint li::text').extract()
        media = {
            "category_url": "",
            "product_url": product_url,
            "media_entity_id":"",
            "product_price": ''.join(price[0].split()).replace(',','.'),
            "no_of_unites_sold": '',
            "avg_rating": '',
            "total_reviews": no_of_reviews,
            "product_description": ' '.join(product_description),
            "additional_fields": '',
            "volume/weight": '',
            "product_name": product_name[0].replace('\n',''),
            "brand_name": '',
            "media_entity_id":product_id
        }
        yield scrapy.Request(url=product_url,
                             errback=self.err,
                             callback=self.parse_seller,
                             dont_filter=True,
                             meta={'media_entity': media_entity,
                                   'media': media})

    def parse_seller(self, response):
        media = response.meta['media']
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
             seller_name='',
             seller_url="",
             seller_avg_rating='',
             seller_no_of_ratings='',
             seller_followers='',
             seller_no_of_unites_sold="")


