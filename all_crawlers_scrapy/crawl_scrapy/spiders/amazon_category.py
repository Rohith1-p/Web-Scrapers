import scrapy
from urllib.parse import urlparse
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs


class AmazonCategorySpider(SetuservSpider):
    name = 'amazon-category-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Amazon Category Scraping starts")
        assert self.source == 'amazon_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        monitor_log = 'Successfully Called Amazon Category Product Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

        for category_url, category_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': category_url, 'id': category_id}
            media_entity = {**media_entity,**self.media_entity_logs}
            country_code = 'https://' + urlparse(category_url).netloc
            page = 1
            link = category_url.lower().replace('-', '')
            if 'bestsellers' in link:
                yield scrapy.Request(url=self.get_category_url(category_url),
                                     callback=self.parse_best_seller_products,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'page_url': category_url,
                                           'country_code': country_code})
            else:
                yield scrapy.Request(url=self.get_category_url(category_url),
                                     callback=self.parse_category_products,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'page_url': category_url,
                                           'country_code': country_code})
            self.logger.info(f"Generating reviews for {category_url} and {category_id}")

    def parse_category_products(self, response):
        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        category_url = media_entity["url"]
        category_id = media_entity["id"]
        page_url = response.meta["page_url"]
        country_code = response.meta["country_code"]
        res = response.css('div[data-component-type="s-search-result"]')
        if 'id="captchacharacters"' in response.text or 'html' not in response.text  or res==[]:
            self.logger.info(f"Captcha Found for {category_url}")
            yield scrapy.Request(url=self.get_category_url(page_url),
                                 callback=self.parse_category_products,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'page_url': page_url,
                                       'country_code': country_code})
            return
        category_breadcrumb_list = list()
        bold_res =  response.css('ul[aria-labelledby="n-title"]').extract_first()
        if 'class="a-list-item"' in response.text and 'class="a-size-base a-color-base a-text-bold"' in str(bold_res) and response.text.count('class="a-spacing-micro s-navigation-indent"') ==1:
            category_breadcrumb_list.append('Department')
            category_indices_1 = response.css("#departments .a-spacing-medium > .a-spacing-micro > .a-list-item > .s-navigation-item .a-color-base::text").extract()
            for sub_list in category_indices_1:
                category_breadcrumb_list.append(sub_list)
            #category_indices_last = response.css('ul[aria-labelledby="n-title"] span[class="a-size-base a-color-base a-text-bold"]::text').extract_first()
            ##    print("inside 2")
            #    category_indices_last=response.css("#departments .a-spacing-medium > .a-spacing-micro > .a-list-item > .a-text-bold::text").extract()
            #category_breadcrumb_list.append(category_indices_last)
            #print("inside indices")

        elif response.text.count('class="a-spacing-micro s-navigation-indent-1"') ==1 and response.text.count('class="a-spacing-micro s-navigation-indent-2"') ==0:
            category_breadcrumb_list.append('Department')
            category_indices_1 = response.css(
                "#departments .a-spacing-medium > .a-spacing-micro > .a-list-item > .s-navigation-item .a-color-base::text").extract()
            for sub_list in category_indices_1:
                category_breadcrumb_list.append(sub_list)
            category_indices_last=response.css("#departments .a-spacing-medium > .a-spacing-micro > .a-list-item > .a-text-bold::text").extract()
            print(category_indices_last)
            if len(category_indices_last):
                category_breadcrumb_list.append(category_indices_last[0])


        elif response.text.count('class="a-spacing-micro s-navigation-indent-1"') > 1 :
            category_breadcrumb_list.append(' '.join(category_url.split('=')[1].split('&')[0].split('+')).title())

        elif response.text.count('class="a-spacing-micro s-navigation-indent-1"') ==1 and response.text.count('class="a-spacing-micro s-navigation-indent-2"') >0:
            category_breadcrumb_list.append('Department')
            category_indices_last=response.css("#departments .a-spacing-medium > .a-spacing-micro > .a-list-item > .a-text-bold::text").extract()
            category_breadcrumb_list.append(category_indices_last)


        elif 'aria-labelledby="n-title"' in response.text and 'class="s-back-arrow aok-inline-block"' in response.text:
            check_indented_categories=response.css('ul[aria-labelledby="n-title"] li[class="a-spacing-micro"] span[class="s-back-arrow aok-inline-block"]').extract()
            if check_indented_categories != []:
                category_breadcrumb_list.append('Department')
                category_indices_1 = response.css('ul[aria-labelledby="n-title"] li[class="a-spacing-micro"] span[class="a-size-base a-color-base"]::text').extract()
                for sub_list in category_indices_1:
                    category_breadcrumb_list.append(sub_list)
                category_indices_last = response.css(
                    'ul[aria-labelledby="n-title"] span[class="a-size-base a-color-base a-text-bold"]::text').extract_first()
                category_breadcrumb_list.append(category_indices_last)
            else:
                category_breadcrumb_list.append(' '.join(category_url.split('=')[1].split('&')[0].split('+')).title())

        elif response.text.count('class="a-spacing-micro s-navigation-indent-1"') ==1 and response.text.count('class="a-spacing-micro s-navigation-indent-2"') >0:
            category_breadcrumb_list.append('Department')
            category_breadcrumb_list.append(response.css('li[class="a-spacing-micro s-navigation-indent-1"] span[class="a-size-base a-color-base a-text-bold"] ::text').extract())
        elif response.text.count('class="a-spacing-micro s-navigation-indent-1"') ==0:
            category_breadcrumb_list.append(' '.join(category_url.split('=')[1].split('&')[0].split('+')).title())


        category_breadcrumb_dict = {}
        for i in range(len(category_breadcrumb_list)):
            category_breadcrumb_dict.update({f'category_level_{i + 1}': category_breadcrumb_list[i]})
        category_level_1 = category_breadcrumb_dict.get('category_level_1', '')
        category_level_2 = category_breadcrumb_dict.get('category_level_2', '')
        category_level_3 = category_breadcrumb_dict.get('category_level_3', '')
        category_level_4 = category_breadcrumb_dict.get('category_level_4', '')
        category_level_5 = category_breadcrumb_dict.get('category_level_5', '')
        category_level_6 = category_breadcrumb_dict.get('category_level_6', '')


        if res:
            for item in res:
                if item:
                    product_url = country_code + item.css('a[class="a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal"]::attr(href)').extract_first()
                    product_id = item.css('::attr(data-asin)').extract_first()
                    if product_id is None:
                        product_id='-'
                    product_name = item.css('span[class="a-size-base-plus a-color-base a-text-normal"]::text').extract_first()
                    if product_name is None:
                        product_name = item.css('span[class="a-size-medium a-color-base a-text-normal"]::text').extract_first()
                    rating = item.css('div[class="a-row a-size-small"] span[class="a-icon-alt"]::text').extract_first()
                    if item.css('div[class="a-row a-size-small"] span[class="aria-label"]::text').extract_first() == "No reviews":
                        rating = '0.0 out of 5 stars'
                    if rating is not None and ',' in rating:
                        rating=rating.replace('\xa0',' ').replace(',','.')
                    if rating is None:
                        rating = '-'
                    rating_count = item.css('div[class="a-row a-size-small"] span[class="a-size-base s-underline-text"]::text').extract_first()
                    if rating_count is None:
                        rating_count = item.css('div[class="a-row a-size-small"] span[class="a-size-base puis-light-weight-text s-link-centralized-style"]::text').extract_first()
                    if rating_count is not None:
                        rating_count=rating_count.replace(' ',',').replace('.',',')
                    else:
                        rating_count = '-'
                    discount_price = item.css('span[class="a-price"] span[class="a-offscreen"]::text').extract_first()
                    if discount_price is None:
                        discount_price = '-'
                    discount_percentage = item.css('span[class="a-size-extra-large s-color-discount puis-light-weight-text"]::text').extract_first()
                    if discount_percentage is None and 'https://www.amazon.in/' in category_url:
                        try:
                            discount_percentage = item.css('div[class="a-row a-size-base a-color-base"] span::text').extract()[-1]
                        except:
                            pass
                    print(discount_percentage)
                    if '%' not in str(discount_percentage):
                        discount_percentage = '-'
                    actual_price = item.css('span[class="a-price a-text-price"] span[class="a-offscreen"]::text').extract_first()

                    # product_availability = item.css('div[class="a-row a-size-base a-color-secondary"] span[class="a-size-base a-color-price"]::text').extract_first()
                    # print("product_availability", product_availability)
                    # if product_availability is not None:
                    #     product_availability = "In Stock"
                    # else:
                    #     product_availability = "No"
                    sponsored_listing = item.css('span[class="a-declarative"] span[class="a-color-base"]::text').extract_first()
                    print("sponsored_listing", sponsored_listing)
                    if sponsored_listing is not None:
                        sponsored_listing = "Yes"
                    else:
                        sponsored_listing = "No"
                    if actual_price is None or actual_price == '':
                        actual_price = discount_price
                    if discount_percentage is None or discount_percentage == '':
                        discount_percentage = '-'

                    product_availability= "Yes"
                    product_availability= item.css('div[class="a-row a-size-base a-color-secondary"] span[class="a-size-small puis-light-weight-text"]::text').extract_first()
                    if product_availability is None:
                        product_availability = item.css('div[class="a-row a-size-base a-color-secondary" ] span[class="a-size-mini puis-light-weight-text"]::text').extract_first()
                    if product_availability is None:
                        product_availability = item.css('div[class="a-row a-size-base a-color-secondary" ] span[class="a-size-base a-color-price puis-light-weight-text"]::text').extract_first()
                    if product_availability is None:
                        product_availability = item.css('div[class = "a-row a-size-base a-color-secondary"] span[class="a-size-base a-color-price"]::text').extract_first()
                    if product_availability is None:
                        product_availability="Yes"
                    print('product_availability_',product_availability)

                    self.yield_category_details \
                        (category_url=category_url,
                         product_url=product_url,
                         asin = product_id,
                         product_id = product_id,
                         product_name=product_name,
                         rating=rating,
                         rating_count=rating_count,
                         discounted_price=str(discount_price),
                         actual_price=str(actual_price),
                         discount_percentage=discount_percentage,
                         product_availability=product_availability,
                         sponsored_listing=sponsored_listing,
                         category_level_1=category_level_1,
                         category_level_2=category_level_2,
                         category_level_3=category_level_3,
                         category_level_4=category_level_4,
                         category_level_5=category_level_5,
                         category_level_6=category_level_6,
                         page_no=page,
                         )

            if 's-pagination-item s-pagination-next s-pagination-button s-pagination-separator' in response.text:
                next_page_url = response.css('a[class="s-pagination-item s-pagination-next s-pagination-button '
                                             's-pagination-separator"]::attr(href)').extract_first()
                if country_code not in next_page_url:
                    next_page_url = country_code+ next_page_url
                page += 1
                yield scrapy.Request(url=self.get_category_url(next_page_url),
                                     callback=self.parse_category_products,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'page_url': next_page_url,
                                           'country_code': country_code})
            else:
                self.logger.info(f"No more pages for the category {category_url}")

    def parse_best_seller_products(self, response):
        self.logger.info("**** best seller ")
        media_entity = response.meta["media_entity"]
        page = response.meta["page"]
        category_url = media_entity["url"]
        category_id = media_entity["id"]
        page_url = response.meta["page_url"]
        country_code = response.meta["country_code"]
        if 'id="captchacharacters"' in response.text or 'html' not in response.text:
            self.logger.info(f"Captcha Found for {category_url}")
            yield scrapy.Request(url=self.get_category_url(page_url),
                                 callback=self.parse_best_seller_products,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'page_url': page_url,
                                       'country_code': country_code})
            return
        if 'id="gridItemRoot"' not in response.text:
            yield scrapy.Request(url=self.get_category_url(page_url),
                                 callback=self.parse_category_products,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page,
                                       'page_url': page_url,
                                       'country_code': country_code})
            return

        res = response.css('div[id="gridItemRoot"]')
        print(len(res), category_url)
        category_breadcrumb_list = list()
        category_breadcrumb_list.append('Any Department')
        category_indices_1 = response.css('div[class="_p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf _p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8 _p13n-zg-nav-tree-all_style_zg-browse-up__XTlqh"] a::text').extract()
        for sub_list in category_indices_1:
            category_breadcrumb_list.append(sub_list)
        if '_p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf _p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8' in response.text :
            category_but_one=x=response.css('._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-up__XTlqh+ ._p13n-zg-nav-tree-all_style_zg-browse-group__88fbz > ._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8 a::text').extract()
            print(category_but_one)
            if(len(category_but_one)>0):
                category_breadcrumb_list.append(category_but_one[0])
        category_indices_last = response.css('span[class="_p13n-zg-nav-tree-all_style_zg-selected__1SfhQ"]::text').extract_first()
        category_breadcrumb_list.append(category_indices_last)
        category_breadcrumb_dict = {}
        for i in range(len(category_breadcrumb_list)):
            category_breadcrumb_dict.update({f'category_level_{i + 1}': category_breadcrumb_list[i]})
        category_level_1 = category_breadcrumb_dict.get('category_level_1', '')
        category_level_2 = category_breadcrumb_dict.get('category_level_2', '')
        category_level_3 = category_breadcrumb_dict.get('category_level_3', '')
        category_level_4 = category_breadcrumb_dict.get('category_level_4', '')
        category_level_5 = category_breadcrumb_dict.get('category_level_5', '')
        category_level_6 = category_breadcrumb_dict.get('category_level_6', '')

        if res:
            for item in res:
                if item:
                    try:
                        category_url = category_url
                        product_url = country_code + item.css('a[class="a-link-normal"]::attr(href)'
                                               ).extract()[1]
                        product_id = item.css('div[class="p13n-sc-uncoverable-faceout"]::attr(id)').extract_first()
                        if product_id is None:
                            product_id = '-'
                        product_name = item.css('a[class="a-link-normal"] span div::text').extract_first()
                        rating = item.css('a[class="a-link-normal"]::attr(title)').extract_first()
                        if rating is not None:
                            rating=rating.replace('\xa0',' ')
                        rank = item.css('div[class="a-section zg-bdg-body zg-bdg-clr-body aok-float-left"] span[class="zg-bdg-text"]::text').extract_first()
                        rank=rank.replace('#','')
                        if rating is None:
                            rating = '-'
                        elif ',' in rating:
                            rating=rating.replace(',','.')
                        rating_count = item.css('span[class="a-size-small"]::text').extract_first()
                        if rating_count is not None:
                            rating_count=rating_count.replace(' ',',').replace('.',',')
                        else:
                            rating_count = '-'
                        price = item.css('span[class="a-size-base a-color-price"]').extract_first()
                        if price is None:
                            price = item.css('span[class="p13n-sc-price"]::text').extract_first()
                        if price is not None:
                            soup = BeautifulSoup(price, 'html.parser')
                            for s in soup(['script', 'style']):
                                s.decompose()
                            price = ' '.join(soup.stripped_strings)
                        if price is None:
                            price = '-'

                        self.yield_amazon_category_details \
                            (category_url=category_url,
                             product_url=product_url,
                             product_id=product_id,
                             product_name=product_name,
                             rating=rating,
                             rating_count=rating_count,
                             discounted_price=str(price),
                             actual_price=str(price),
                             discount_percentage='-',
                             product_availability='Not mentioned in best seller page',
                             sponsored_listing='Not mentioned in best seller page',
                             category_level_1=category_level_1,
                             category_level_2=category_level_2,
                             category_level_3=category_level_3,
                             category_level_4=category_level_4,
                             category_level_5=category_level_5,
                             category_level_6=category_level_6,
                             page_no=page,
                             rank=rank)
                    except:
                        pass
            if 'class="a-last"' in response.text:
                next_page_url = response.css('li[class="a-last"] a::attr(href)').extract_first()
                if country_code not in next_page_url:
                    next_page_url = country_code + next_page_url
                page += 1
                yield scrapy.Request(url=self.get_category_url(next_page_url),
                                     callback=self.parse_best_seller_products,
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page,
                                           'page_url': next_page_url,
                                           'country_code': country_code})
            else:
                self.logger.info(f"No more pages for the best seller category {category_url}")

    @staticmethod
    def get_category_url(url):
        return url
