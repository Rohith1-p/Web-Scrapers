import scrapy
from urllib.parse import urlparse
from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs
from bs4 import BeautifulSoup

class FlipkartCategorySpider(SetuservSpider):

    name = 'flipkart-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Flipkart Category Scraping starts")
        assert self.source == 'flipkart_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        monitor_log = 'Successfully Called Flipkart Category Products Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

        for category_url, category_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': category_url, 'id': category_id}
            media_entity = {**media_entity,**self.media_entity_logs}
            country_code = 'https://' + urlparse(category_url).netloc
            page = 1

            yield scrapy.Request(url=category_url,
                                     callback=self.parse_category_products,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                            'page' : page,
                                           'page_url': category_url,
                                           'total_page':-1})
                                         #  'country_code': country_code})

    def parse_category_products(self,response):
        media_entity = response.meta["media_entity"]
        category_url = media_entity['url']
        page = response.meta["page"]
        total_page=response.meta["total_page"]
        if page ==1:
            total_page = str(response.css("div._2MImiq span::text").extract_first()).split(" ")[-1]
            total_page= eval(total_page.replace(",",""))
            if type(total_page) is tuple:
                total_page = int(''.join(map(str, total_page)))
            print("total_page!!!",total_page)
        # flipkart = open("flipkart.html",'w')
        # flipkart.write(response.text)
        # flipkart.close()
        print(response.text)
        self.dump(response, 'html', 'flipkart_category_dump1_page', self.source)
        Product_Name = []
        Brand = []
        Discounted_Price = []
        Original_Price = []
        Discount_Percentage = []
        Color = []
        Sizes_Available = []
        Stock_Availability = []
        product_url=" "
        products = response.css('div[class="_1YokD2 _3Mn1Gg"] div[class = "_13oc-S"]')
        print(len(products))
        category_breadcrumb_list = response.css("div[class='_2q_g77'] a::text").extract()
        category_breadcrumb_dict = {}
        for i in range(len(category_breadcrumb_list)):
            category_breadcrumb_dict.update({f'category_level_{i + 1}': category_breadcrumb_list[i]})
        category_level_1 = category_breadcrumb_dict.get('category_level_1', '')
        category_level_2 = category_breadcrumb_dict.get('category_level_2', '')
        category_level_3 = category_breadcrumb_dict.get('category_level_3', '')
        category_level_4 = category_breadcrumb_dict.get('category_level_4', '')
        category_level_5 = category_breadcrumb_dict.get('category_level_5', '')
        category_level_6 = category_breadcrumb_dict.get('category_level_6', '')
        if category_level_1 == '':
            category_level_1 = "-"
        if category_level_2 == '':
            category_level_2 = "-"
        if category_level_3 == '':
            category_level_3 = "-"
        if category_level_4 == '':
            category_level_4 = "-"
        if category_level_5 == '':
            category_level_5 = "-"
        if category_level_6 == '':
            category_level_6 = "-"

        if len(products) != 24:
            for product in products:
                try:
                    item_1 = product.css('div[class = "_4ddWXP"]') #_1xHGtK _373qXS
                    if item_1 ==[]:
                        item_1 = product.css('div[class = "_1xHGtK _373qXS"]')
                except Exception as e:
                    print("Excepition,:",e)
                for item in item_1:
                    try:
                        Product_Name =(item.css('a[class = "s1Q9rs"]::attr(title)').extract_first())
                        if Product_Name is None:
                            Product_Name =item.css('.IRpwTa::attr(title)').extract()[0]
                    except:
                        Product_Name = "-"
                    try:
                        Brand=item.css('div[class = "_2WkVRV"]::text').extract_first()
                    except:
                        Brand ="-"
                    try:
                        rating = item.css("._3LWZlK::text").extract()[0]
                    except:
                        rating= "-"
                    try:
                        rating_count = eval(item.css("._2_R_DZ::text").extract()[0])
                        if type(rating_count) is tuple:
                            rating_count = int(''.join(map(str, rating_count)))
                    except:
                        rating_count= "-"

                    try:
                        Discounted_Price =item.css('div[class = "_30jeq3"]::text').extract()[0]

                    except:
                        Discounted_Price= "-"
                    try:
                        Original_Price = ''.join(item.css('div[class = "_3I9_wc"]::text').extract())

                    except:
                        Original_Price =Discounted_Price
                    print("Discounted_Price", Discounted_Price)
                    print("Original_Price", Original_Price)

                    Discount=item.css('div[class = "_3Ay6Sb"]').extract_first()
                    if Discount is not None:
                        soup = BeautifulSoup(Discount,'html.parser')
                        Discount_Percentage =soup.find('span').text
                    else:
                        Discount_Percentage = '-'

                    try:
                        product_url=item.css('a[class = "_3bPFwb"]::attr(href)').extract_first()
                        if product_url is None:
                            print("product_url is None")
                            product_url=item.css('a[class = "_8VNy32"]::attr(href)').extract_first()
                        product_url ="https://www.flipkart.com"+product_url
                    except Exception as e:
                        print("product_url in except",e)
                        product_url="-"
                    try:
                        product_description = (item.css('div[class  = "_3Djpdu"]::text').extract())
                    except:
                        product_description="-"
                    try:
                        Sizes_Available = item.css('div[class = "._376u-U::text"]').extract_first()
                        if Sizes_Available is  None:
                            Sizes_Available= "-"
                    except:
                        Sizes_Available = "-"
                    try:
                        Stock_Availability = item.css('div[class = "_2Tpdn3 _18hQoS"]::text').extract()
                    except:
                        Stock_Availability ="-"
                    try:
                        product_id = product_url.split('?pid=')[1].split('&')[0]
                    except:
                        product_id="-"


        #print(len(Product_Name),len(Brand),len(Discounted_Price),len(Discount_Percentage),len(Color),len(Sizes_Available),len(Stock_Availability),len(Product_URLs),len(product_id))
        #print(Product_Name,Brand,Discounted_Price,Discount_Percentage,Color,Sizes_Available,Stock_Availability,Product_URLs,product_id)

        #print(len(Product_Name),len(Brand),len(Discounted_Price),len(Discount_Percentage),len(Color),len(Sizes_Available),len(Stock_Availability),len(Product_URLs))
        #print(Product_Name,Brand,Discounted_Price,Discount_Percentage,Color,Sizes_Available,Stock_Availability,Product_URLs)
                    self.yield_category_details \
                        (category_url=category_url,
                         product_url=product_url,
                         product_id = product_id,
                         product_name=Product_Name,
                         rating=rating,
                         rating_count=rating_count,
                         discounted_price=str(Discounted_Price),
                         actual_price=str(Original_Price),
                         discount_percentage=Discount_Percentage,
                         product_description= product_description,
                         product_availability=Stock_Availability,
                         sponsored_listing='',
                         brand_name=Brand,
                         size=Sizes_Available,
                         category_level_1=category_level_1,
                         category_level_2=category_level_2,
                         category_level_3=category_level_3,
                         category_level_4=category_level_4,
                         category_level_5=category_level_5,
                         category_level_6=category_level_6,
                         page_no=page,
                         )
                    print("Sent to yield_category_details")

        else:
            for product in products:
                try:
                    Product_Name = (product.css('._4rR01T::text').extract_first())
                except:
                    Product_Name = ("-")
                try:
                    Brand=product.css('div[class = "_2WkVRV"]::text').extract_first()
                except:
                    Brand ="-"
                try:
                    Discounted_Price=(product.css('div[class = "_30jeq3 _1_WHN1"]::text').extract()[0])
                except:
                    Discounted_Price=("-")
                try:
                    print("Original_Price!!!!!!!!!!!",product.css('div[class = "_3I9_wc _27UcVY"]::text').extract())
                    Original_Price="₹"+(''.join(product.css('div[class = "_3I9_wc _27UcVY"]::text').extract())).split("₹")[1]
                except:
                    Original_Price= Discounted_Price

                print("Discounted_Price!", Discounted_Price)
                print("Original_Price!", Original_Price)

                Discount=product.css('div[class = "_3Ay6Sb"]').extract_first()
                if Discount is not None:
                    soup = BeautifulSoup(Discount,'html.parser')
                    Discount_Percentage=(soup.find('span').text)
                else:
                    Discount_Percentage='-'

                Sizes_Availabl = product.css("._376u-U::text").extract_first()
                if Sizes_Availabl is not None:
                    soup = BeautifulSoup(Sizes_Availabl,'html.parser')
                    #print(soup)
                    Sizes_Available=(soup.find('span').text)
                else:
                    Sizes_Available="-"

                try:
                    rating = product.css("._3LWZlK::text").extract()[0]
                except:
                    rating= "-"

                try:
                    rating_count = (product.css("._2_R_DZ").extract_first())
                    soup = BeautifulSoup(rating_count,'html.parser')
                    rating_count = soup.find('span').text.split(" ")[0].replace(",","")
                except:
                    rating_count= "-"
                try:
                    product_url=product.css('a[class = "_1fQZEK"]::attr(href)').extract()[0]
                    product_url ="https://www.flipkart.com"+product_url
                except Exception as e:
                    print("product_url in except",e)
                    product_url="-"
                try:
                    product_description=""
                    product_desc = (product.css('div[class  = "fMghEO"]').extract_first())
                    soup = BeautifulSoup(product_desc,'html.parser')
                    for description in soup.find_all("li"):
                        product_description=product_description + description.text+"\n"
                except Exception as e:
                    product_description="-"
                    print(e)
                try:
                    Sizes_Available = product.css('div[class = "._376u-U::text"]').extract_first()
                    if Sizes_Available is  None:
                        Sizes_Available= "-"
                except:
                    Sizes_Available = "-"
                try:
                    Stock_Availability = product.css('div[class = "_2Tpdn3 _18hQoS"]::text').extract_first()
                except:
                    Stock_Availability ="-"
                try:
                    product_id = product_url.split('?pid=')[1].split('&')[0]
                except:
                    product_id="-"
                self.yield_category_details \
                    (category_url=category_url,
                     product_url=product_url,
                     product_id = product_id,
                     product_name=Product_Name,
                     rating=rating,
                     rating_count=rating_count,
                     discounted_price=str(Discounted_Price),
                     actual_price=str(Original_Price),
                     discount_percentage=Discount_Percentage,
                     product_description= product_description,
                     product_availability=Stock_Availability,
                     sponsored_listing='-',
                     brand_name=Brand,
                     size=Sizes_Available,
                     category_level_1=category_level_1,
                     category_level_2=category_level_2,
                     category_level_3=category_level_3,
                     category_level_4=category_level_4,
                     category_level_5=category_level_5,
                     category_level_6=category_level_6,
                     page_no=page,
                     )
                print("Sent to yield_category_details")

        page = page+1
        if (page <= total_page):
            category_url_page =category_url+"&page={}".format(page)
            print("category_url_page!!!!!!!!", category_url_page)
            yield scrapy.Request(url=category_url_page,
                                     callback=self.parse_category_products,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                            'page' : page,
                                            'page_url': category_url,
                                            'total_page':total_page})
                                         #  'country_code': country_code})
