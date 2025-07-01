import math
import hashlib
import re
import json
import datetime
from datetime import timedelta
from urllib.parse import urlparse
import dateparser
from dateparser import search as datesearch
import scrapy
import requests
from bs4 import BeautifulSoup
from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs
import traceback
import re

class FlipkartProductSpider(SetuservSpider):
    name = 'flipkart-product-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id,env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name,env)
        assert self.source == 'flipkart_product'

    def start_requests(self):
        self.logger.info("Starting requests")
        print("start_requests of flipkart product scraper...")
        monitor_log = 'Successfully Called Amazon Product Details Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)
        print("length of urls list: ", len(self.start_urls))
        print("product_ids lst: ", len(self.product_ids))
        count_ = 1
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            print("scraping product_url number: ", count_)
            print("and id:{} url:{}".format(product_id, product_url))
            count_+=1
            media_entity = {'id': product_id, 'url': product_url}
            media_entity = {**media_entity, **self.media_entity_logs}
            try:
                yield scrapy.Request(url=product_url, callback=self.parse_response,
                                     dont_filter=True, errback=self.err,
                                     meta={'media_entity': media_entity},
                                     )
            except Exception as e:
                print("Error occured some where for product_url:", product_url, " error:", e)
                traceback.print_exc()
                pass
    def parse_response(self, response):
        media_entity = response.meta['media_entity']
        product_id = media_entity["id"]
        product_url = media_entity["url"]
        #print("response is: ", response.text)
        self.dump(response, 'html', 'flipkart_product', self.source, product_id)
        try:
            product_name_lst = response.css('span[class="B_NuCI"]::text').extract()
            product_name = " ".join(product_name_lst).replace("\xa0","")
            if product_name==None or len(product_name)==0:
                product_name = "-"
        except Exception as e:
            print("Excepition, e at 49:",e)
            traceback.print_exc()
            product_name = "-"
        try:
            list_price = response.css('div[class="_3I9_wc _2p6lqe"]::text').extract()
            list_price = "".join(list_price)
            if list_price==None or len(list_price)==0:
                list_price = "-"
        except Exception as e:
            print("Excepition, e at 54:",e)
            traceback.print_exc()
            list_price = "-"
        try:
            discount_percent = response.css('div._3Ay6Sb._31Dcoz.pZkvcx span::text').extract_first()
            print("discount_percent at 58:", discount_percent)
            if discount_percent==None or len(discount_percent) == 0:
                discount_percent = response.css('div._3Ay6Sb._31Dcoz span::text').get()
            if discount_percent==None or len(discount_percent)==0:
                discount_percent = "-"
        except Exception as e:
            print("Excepition, e at 62:",e)
            traceback.print_exc()
            discount_percent = "-"
        try:
            discounted_price = response.css('div[class="_30jeq3 _16Jk6d"]::text').extract_first()
            if discounted_price==None or len(discounted_price)==0:
                discounted_price = "-"
        except Exception as e:
            print("Excepition, e at 67:",e)
            traceback.print_exc()
            discounted_price = "-"
        try:
            brand = response.css('span[class="G6XhRU"]::text').extract_first()
            if brand == None or len(brand)==0:
                brand = "-"
        except Exception as e:
            print("Excepition, e at 72:",e)
            traceback.print_exc()
            brand = "-"
        try:
            avg_rating = response.css('span._2dMYsv::text').extract_first()
            if str(avg_rating) == 'Be the first to Review this product':# or len(avg_rating)>10:
                avg_rating = '-'
            else:
                avg_rating = response.css('div[class="_3LWZlK _3uSWvT"]::text').extract_first()
                if avg_rating==None:
                    avg_rating = response.css('div[class="_3LWZlK"]::text').extract_first()
                if avg_rating==None or len(avg_rating)==0:
                    avg_rating = "-"
        except Exception as e:
            print("Excepition, e at 77:",e)
            traceback.print_exc()
            avg_rating = "-"

        try:
            seller_name = response.css('div#sellerName span span::text').get()
            if seller_name == None or len(seller_name)==0:
                seller_name = "-"
        except Exception as e:
            print("Excepition, e at 83:",e)
            traceback.print_exc()
            seller_name = "-"

        try:
            rating_count_text = response.css('span._2_R_DZ._2IRzS8 span::text').get()
            if rating_count_text != None:
                print("rating_count_text at 90: ", rating_count_text)
                pattern = r'\d+'
                regex = re.compile(pattern)
                matches = regex.findall(rating_count_text)
                rating_count = "".join(matches[:-1])
                review_count = matches[-1]
            else:
                print("inside else at 106 line")
                rating_count_text_lst = response.css('span._2_R_DZ span span::text').extract()
                print("rating_count_text_lst at 108: ", rating_count_text_lst)
                pattern = r'\d+'
                regex = re.compile(pattern)
                rating_count_matches = regex.findall(rating_count_text_lst[0])
                rating_count = "".join(rating_count_matches[:])

                review_count_matches = regex.findall(rating_count_text_lst[-1].replace('\xa0', ""))
                review_count = "".join(review_count_matches[:])

            if review_count==None:
                review_count = "-"
            if rating_count==None:
                rating_count = "-"
        except Exception as e:
            print("Excepition, e at 95:",e)
            traceback.print_exc()
            rating_count = "-"
            review_count = "-"

        try:
            seller_rating = response.css('div[class="_3LWZlK _1D-8OL"]::text').extract_first()
            if seller_rating==None:
                seller_rating = response.css('div[class="_3LWZlK _32lA32 _1D-8OL"]::text').extract_first()#_3LWZlK _32lA32 _1D-8OL
            if seller_rating==None:
                seller_rating = "-"
        except Exception as e:
            print("Excepition, e at 102:",e)
            traceback.print_exc()
            seller_rating = "-"
        try:
            txt = response.css('div[class="row _i6Wtg"]')
            product_details_dic = {}
            for feature_html in txt.css('div[class="row"]').extract():
                soup = BeautifulSoup(feature_html, "html.parser")
                key = soup.select_one('div[class*="col col-3-12 _2H87wv"]')
                value = soup.select_one('div[class*="col col-9-12 _2vZqPX"]')
                product_details_dic[key.text] = value.text
            product_details = product_details_dic
            print("product_details: ", product_details)
            if len(product_details)==0 or product_details==None:
                product_details = "-"
        except Exception as e:
            print("Excepition, e at 114:",e)
            traceback.print_exc()
            product_details = "-"

        try:
            highlights = response.css('li[class="_21Ahn-"]::text').extract()
            highlights = "\n".join(highlights)
            if len(highlights)==0 or highlights==None:
                highlights = "-"
        except Exception as e:
            print("Excepition, e at 114:",e)
            traceback.print_exc()
            highlights = "-"

        try:
            product_description = response.css('div._1mXcCf.RmoJUa p::text').get()
            print("product_description at 163 line: ", product_description)
            if product_description == None:
                product_description = response.css('div._1mXcCf::text').get()
            print("product_description at 166 line: ", product_description)
            if product_description==None:
                print("inside 164 line")
                if product_details == None:
                    product_description = "-"
                else:
                    product_description = product_details

        except Exception as e:
            print("Exception e at 153: ", e)
            traceback.print_exc()
            product_description = "-"
        try:
            ratings_lst = response.css('ul._36LmXx li._28Xb_u div._1uJVNT::text').extract()
            if len(ratings_lst) == 0:
                print("inside if at 174 line")
                ratings_lst = response.css('ul._2jr1F_ li._28Xb_u div._1uJVNT::text').extract()
            print("ratings_lst: ", ratings_lst)
            rating_breakdown = {"5": ratings_lst[0], "4": ratings_lst[1], "3": ratings_lst[2], "2": ratings_lst[3], "1": ratings_lst[4]}
        except Exception as e:
            print("Exception e at 163: ", e)
            traceback.print_exc()
            rating_breakdown = {}

        try:
            specification_keys_lst = response.css('div._1UhVsV._3AsE0T div._3k-BhJ tr._1s_Smc.row td._1hKmbr.col.col-3-12::text').extract()
            specification_values_lst = response.css('div._1UhVsV._3AsE0T div._3k-BhJ tr._1s_Smc.row td.URwL2w.col.col-9-12 ul li._21lJbe::text').extract()
            specification = {specification_keys_lst[i]: specification_values_lst[i] for i in range(len(specification_keys_lst))}
            if len(specification)==0:
                specification_keys_lst = response.css('div._3k-BhJ table._14cfVK tr._1s_Smc.row td._1hKmbr.col.col-3-12::text').extract()
                specification_values_lst = response.css('div._3k-BhJ table._14cfVK tr._1s_Smc.row td.URwL2w.col.col-9-12 li._21lJbe::text').extract()
                specification = {specification_keys_lst[i]: specification_values_lst[i] for i in range(len(specification_keys_lst))}

        except Exception as e:
            print("Exception e at 172: ", e)
            traceback.print_exc()
            specification = "-"

        try:
            category_url_html = response.css('a._2whKao').extract()[-1]
            category_url_soup = BeautifulSoup(category_url_html, "html.parser")
            category_url = "https://www.flipkart.com/"+category_url_soup.select_one('a').get('href')
        except Exception as e:
            print("Exception e at 180: ", e)
            traceback.print_exc()
            category_url = "-"

        if list_price=="-" and discounted_price !="-":
            list_price = discounted_price
        if discounted_price=="-" and list_price !="-":
            discounted_price = list_price

        self.yield_product_details(product_url=product_url, product_id=product_id, product_name=product_name, brand_name=brand, product_price=list_price ,avg_rating=avg_rating,
                              seller_name=seller_name, category_url=category_url,
                              volume_or_weight='',additional_fields='',seller_url='', seller_avg_rating=seller_rating, about='',
                              seller_no_of_ratings='', no_of_unites_sold='', total_reviews=review_count,
                              seller_followers='', seller_no_of_unites_sold='',discount_price=discounted_price, discount=discount_percent,rating_count=rating_count,
                              best_seller_rating='',rating_map=rating_breakdown,asin='', stock='',product_description=product_description, product_information='',
                              product_specifications= specification, breadcrumb = " ",image_url = " ", highlights=highlights)


        print(seller_name)
        print("Printing scraped details \n product_name: {} \n, list_price: {} \n, discount_percent:{} \n, discounted_price: {} \n, brand: {} \n, avg_rating: {} \n, rating_count: {} \n, seller_name: {}\n review_count:{} ".format(product_name, list_price, discount_percent, discounted_price, brand, avg_rating, rating_count, seller_name, review_count))


        print("seller_rating: {}, product_details: {}, highlights:{}".format(seller_rating, product_details, highlights))
        print("product_description:{}, rating_breakdown:{}, specification:{}, category_url:{}".format(product_description, rating_breakdown, specification, category_url))
