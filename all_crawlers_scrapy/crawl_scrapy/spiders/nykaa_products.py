import json
import ast
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider


class NykaaProductsSpider(SetuservSpider):
    name = 'nykaa-product-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Nykaa Products Scraping starts")
        assert self.source == 'nykaa_product'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_category,
                                 errback=self.err,
                                 meta={'media_entity': media_entity})
            self.logger.info(" scraping is going")

    def parse_category(self, response):
        #print(response.text)
        media_entity = response.meta['media_entity']
        product_url = media_entity['url']
        product_id = media_entity['id']
        if 'Access denied' in response.text:
            self.logger.info("Access denied")
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_category,
                                 errback=self.err,
                                 meta={'media_entity': media_entity})
            return
        split_text = response.text.split("window.__PRELOADED_STATE__ = ")[1]
        #print(split_text)
        split_text = split_text.split("</script")[0]
        split_text = split_text.replace("false", "False")
        split_text = split_text.replace("true", "True")
        split_text = split_text.replace("null", "None")

        res = ast.literal_eval(split_text)
        text = res["productPage"]["product"]["description"]

        description = ''
        dict_res = res["productPage"]["product"]
        add_text = {"Expiry Date: ": dict_res["expiry"], "Country of Origin: ": dict_res["originOfCountryName"],
                    "Manufacturer: ": dict_res["manufacturerName"], "Address: ": dict_res["manufacturerAddress"]}
        add_str = ""
        for key, val in add_text.items():
            if val != None:
                add_str = add_str + key + val + " "
        soup = BeautifulSoup(text, 'html.parser')
        for s in soup(['script', 'style']):
            s.decompose()
            description = ' '.join(soup.stripped_strings)
        if description == '':
            description = ' '.join(BeautifulSoup(text, "html.parser").findAll(text=True))

        try:
            rating_count = response.css(".css-1hvvm95::text").extract()[0]
            if rating_count:
                rating = response.css(".css-m6n3ou::text").extract()[0]
                star_list = response.css(".css-11lfsnj::text").extract()[::-1]
                star_dict = {}
                for i in range(1, 6):
                    star_dict[str(i)] = star_list[i - 1]
        except:
            rating_count = ''
            rating = ''
            star_dict = ''

        try:
            reviews_count = response.css(".css-1hvvm95::text").extract()[3]
        except:
            reviews_count = ''

        try:
            discount = response.css(".css-f5j3vf .css-2w3ruv::text").extract()[0].split()[0]
            discounted_price = response.css(".css-1jczs19::text").extract()[0][1:]
            listed_price = response.css(".css-f5j3vf .css-u05rr span::text").extract()[0][1:]
        except:
            listed_price = response.css('.css-1jczs19::text').extract()[0][1:]
            discount = ''
            discounted_price = ''

        self.yield_nykaa_product_details\
        (
            product_url=product_url,
            product_name=response.css(".css-1gc4x7i::text").extract()[0],
            description = description + " " + add_str,
            discount= discount,
            listed_price = listed_price,
            discounted_price = discounted_price,
            rating = rating,
            rating_count = rating_count,
            reviews_count = reviews_count,
            star_rating = star_dict,
            product_id = product_id
        )
