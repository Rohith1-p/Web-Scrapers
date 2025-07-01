from bs4 import BeautifulSoup
import scrapy
import ast
from .setuserv_spider import SetuservSpider


class TokopediaProductsSpider(SetuservSpider):
    name = 'tokopedia-products-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Tokopedia Products Scraping startss")
        assert self.source == 'tokopedia_products'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url),
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity["url"]
        content = response.text
        try:
            soup = BeautifulSoup(content, 'html.parser')
            breadcrumb = []
            for item in soup.find_all('li',{'class':'css-3a7rwp'}):
               breadcrumb.append(item.get("text"))
            breadcrumb= " > ".join(breadcrumb)
        except:
            breadcrumb = ''

        try:
            product_id_ = (response.text.split("&productID=")[1]).split("&")[0]
        except:
            product_id_ = ""

        _product_description = response.css('div[data-testid="lblPDPDescriptionProduk"]::text').extract()
        product_description = ''
        for _, __product_description in enumerate(_product_description):
            product_description += " " + __product_description
        product_description = BeautifulSoup(product_description, "lxml").text
        if '\t' in product_description:
            product_description = product_description.replace('\t', ' ')
        units_sold = response.text.split('"countSold":"')[1].split('"')[0]
        volume = response.css('span.main::text').extract()[1]
        # if 'ram' not in volume:
        #     yield scrapy.Request(url=product_url, callback=self.parse_info,
        #                          errback=self.err, dont_filter=True,
        #                          headers=self.get_headers(product_url),
        #                          meta={'media_entity': media_entity})
        #     return
        total_reviews = response.css('meta[itemprop="ratingCount"]::attr(content)').extract_first()
        avg_rating = response.css('meta[itemprop="ratingValue"]::attr(content)').extract_first()
        if avg_rating is None:
            avg_rating = '-'
        product_name = response.css('h1[data-testid="lblPDPDetailProductName"]::text').extract_first()
        product_price = response.css('div.price::text').extract_first()
        seller_url = "https://www.tokopedia.com/" + product_url.split('/')[3]

        media = {
            "category_url": "https://www.tokopedia.com/p/otomotif/oli-penghemat-bbm/oli-mobil",
            "product_url": product_url,
            "media_entity_id": product_id_,
            "product_price": product_price,
            "total_reviews": total_reviews,
            "product_name": product_name,
            "brand_name": product_name.split(' ')[0],
            "seller_url": seller_url,
            "product_description": product_description,
            "volume/weight": volume,
            "avg_rating": avg_rating,
            "no_of_unites_sold": units_sold,
            "breadcrumb": breadcrumb
        }
        yield scrapy.Request(url=seller_url,
                             callback=self.parse_seller,
                             errback=self.err, dont_filter=True,
                             meta={'media_entity': media_entity,
                                   'media': media})

    def parse_seller(self, response):
        media = response.meta['media']
        seller_dict = (response.text.split('type="application/ld+json">')[1]).split("</")[0]
        seller_dict = ast.literal_eval(seller_dict)
        seller_name = seller_dict["name"]
        seller_avg_rating = seller_dict["aggregateRating"]["ratingValue"]
        seller_no_of_ratings = seller_dict["aggregateRating"]["ratingCount"]
        seller_followers = ""
        seller_no_of_unites_sold = ""

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
             additional_fields="",
             seller_name=seller_name,
             seller_url=media['seller_url'],
             seller_avg_rating=seller_avg_rating,
             seller_no_of_ratings=seller_no_of_ratings,
             seller_followers=seller_followers,
             seller_no_of_unites_sold=seller_no_of_unites_sold,
             breadcrumb = media["breadcrumb"],
             )

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'referer': product_url,
            'origin': 'www.tokopedia.com',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/75.0.3770.100 Safari/537.36'
        }

        return headers
