import json
import hashlib
from urllib.parse import urlparse

import datetime
from datetime import timedelta
import dateparser
import scrapy

from .setuserv_spider import SetuservSpider
from ..produce_monitor_logs import KafkaMonitorLogs
import time


class WalmartSpider(SetuservSpider):
    name = 'walmart-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Walmart process start")
        assert self.source == 'walmart'

    def start_requests(self):
        print("inside start_requests method, waiting for 5 sec to observe results **")
        self.logger.info("Starting requests")
        monitor_log = 'Successfully Called Walmart Consumer Posts Scraper'
        monitor_log = {"G_Sheet_ID": self.media_entity_logs['gsheet_id'], "Client_ID": self.client_id,
                       "Message": monitor_log}
        KafkaMonitorLogs.push_monitor_logs(monitor_log)

        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            #media_entity = {**media_entity, **self.media_entity_logs}
            media_entity = {**media_entity, **self.media_entity_logs}
            print("media eniity logs are ", self.media_entity_logs)
            print("media_entity is: ", media_entity)
            country_code = urlparse(product_url).netloc.split('.')[2]
            if country_code == 'ca':
                print("inside ca country code")
                #media_entity = {'url': product_url, 'id': product_id}
                page_count = 0
                #print("url is ", self.get_review_url(product_url, page_count))
                yield scrapy.Request(url=self.get_review_url(product_url, page_count),
                                     callback=self.parse_ca,
                                     errback=self.err, dont_filter=True,
                                     meta={'page_count': page_count,
                                           'media_entity': media_entity,
                                           'lifetime': True})
            else:
                yield scrapy.Request(url=product_url, callback=self.parse_us,
                                     errback=self.err, dont_filter=True,
                                     headers=self.get_headers(),
                                     meta={'media_entity': media_entity, 'lifetime': True})

            if self.type == 'comments':
                self.logger.info(f"Generating comments for {product_url} and {product_id}")
            else:
                self.logger.info(f"Generating reviews for {product_url} and {product_id}")


    @staticmethod
    def get_review_url(product_url, page_count):
        print("product_url, page_count are: ", product_url, page_count)
        product_id = product_url.split('?')[0].split("/")[-1]
        print("product_id is ", product_id)
        # if page_count == 0:
        #     scaler = 0
        # else:
        #     scaler = 1

        if page_count == 0:
            offset = 0
            limit = 6
        else:
            offset = 6+(30*page_count) - 30
            limit = 30
        print("inside get_review_url ")

        url = f"https://api.bazaarvoice.com/data/batch.json?" \
              f"resource.q0=reviews&filter.q0=productid%3Aeq%3A{product_id}&filter.q0=contentlocale%3Aeq%3Aen_CA%2Cen_GB%2Cen_US%2Cen_CA&filter.q0=isratingsonly%3Aeq%3Afalse&filter_reviews.q0=contentlocale%3Aeq%3Aen_CA%2Cen_GB%2Cen_US%2Cen_CA&include.q0=authors%2Cproducts&filteredstats.q0=reviews&limit.q0={limit}&offset.q0={offset}&sort.q0=submissiontime%3Adesc&passkey=e6wzzmz844l2kk3v6v7igfl6i&apiversion=5.5&displaycode=2036-en_ca"
        print("final url is ", url)
        #url = f'https://api.bazaarvoice.com/data/batch.json?resource.q0=reviews&filter.q0=productid%3Aeq%3A6000190007308&filter.q0=contentlocale%3Aeq%3Aen_CA%2Cen_GB%2Cen_US%2Cen_CA&filter.q0=isratingsonly%3Aeq%3Afalse&filter_reviews.q0=contentlocale%3Aeq%3Aen_CA%2Cen_GB%2Cen_US%2Cen_CA&include.q0=authors%2Cproducts&filteredstats.q0=reviews&limit.q0=30&offset.q0=6&sort.q0=submissiontime%3Adesc&passkey=e6wzzmz844l2kk3v6v7igfl6i&apiversion=5.5&displaycode=2036-en_ca'
        return url
        #6000190007309

    def parse_ca_product_id(self, response):
        product_url = response.meta["product_url"]
        page_count = response.meta["page_count"]
        if 'Robot or human' in response.text or 'class="re-captcha"' in response.text:
            yield scrapy.Request(url=self.get_review_url(product_url, page_count),
                                     callback=self.parse_ca_product_id,
                                     errback=self.err, dont_filter=True,
                                     meta={'product_url': product_url,
                                           'page_count': page_count})
            return


        print("html response for getting produ id is ", response.text)

        prod_id_link = response.css('meta[property="og:url"]::attr("content")').extract()
        print("prod_id_link is: ", prod_id_link)





    def parse_ca(self, response):
        print("inside parse_ca #############################################################################################")
        #
        # print("dumping response of ca_products_scraper")
        # self.dump(response, "html")
        # print("response text of ca prodcuct scraper is ", response.text)
        #
        # media_entity = response.meta["media_entity"]
        # print("dumping response in ca")
        # self.dump(response, "html")
        # print("response text is ", response.text)
        # product_id = media_entity["id"]
        # product_url = media_entity['url']
        # page_count = response.meta['page_count']
        # lifetime = response.meta['lifetime']
        #
        # if 'Are you human?' in response.text or 'Robot or human' in response.text or 'class="re-captcha"' in response.text or 'Please download one of the supported browsers to keep shopping' in response.text:
        #     print("Robot or human occured, retrying")
        #     yield scrapy.Request(url=self.get_review_url(product_url, page_count),
        #                          callback=self.parse_ca,
        #                          errback=self.err, dont_filter=True,
        #                          meta={'page_count': page_count,
        #                                'media_entity': media_entity,
        #                                'lifetime': True})
        #     return
        #


        print("dumping response of ca_products_scraper")
        self.dump(response, "html")
        print("response text of ca prodcuct scraper is ", response.text)

        media_entity = response.meta["media_entity"]
        print("dumping response in ca")
        self.dump(response, "html")
        print("response text is ", response.text)
        product_id = media_entity["id"]
        product_url = media_entity['url']
        page_count = response.meta['page_count']
        lifetime = response.meta['lifetime']
        print("media_entity and product_id and otheres", media_entity, product_id, page_count, lifetime)

        if lifetime:
            self.get_lifetime_ratings_ca(product_id, response)
        yield scrapy.Request(url=self.get_review_url(product_url, page_count),
                             callback=self.parse_reviews_ca,
                             errback=self.err, dont_filter=True,
                             meta={'page_count': page_count, 'media_entity': media_entity})

    def parse_us(self, response):
        print("dumping response, just to observe blockage")
        self.dump(response, "html")
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        lifetime = response.meta['lifetime']


        if lifetime:
            self.get_lifetime_ratings_us(product_id, response)
        page = 1
        yield scrapy.Request(url=self.get_review_url_us(product_id, page),
                             dont_filter=True,
                             callback=self.parse_reviews_us,
                             errback=self.err,
                             meta={'media_entity': media_entity, 'page': page})

    def parse_reviews_us(self, response):
        print("inside parse_reviews_us")
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity["id"]
        page = response.meta["page"]

        if 'Robot or human' in response.text or 'class="re-captcha"' in response.text:
            yield scrapy.Request(url=self.get_review_url_us(product_id, page),
                                 dont_filter=True,
                                 callback=self.parse_reviews_us,
                                 errback=self.err,
                                 meta={'media_entity': media_entity,
                                       'page': page})
            return

        res = response.text.split(':{"initialData":')[1].split(',"headers"')[0] + '}'
        res = json.loads(res)

        try:
            product_name = res['data']['product']['name']
        except:
            product_name = ''
        extra_info = {"product_name": product_name, "brand_name": ""}

        if res['data']['reviews']['customerReviews']:
            print("inside if")
            for review in res['data']['reviews']['customerReviews']:
                if review:
                    try:
                        title = review['reviewTitle']
                    except:
                        title = ''
                    _id = review['reviewId']
                    review_date = dateparser.parse(review["reviewSubmissionTime"])
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = review['reviewText']
                                if body:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title=title,
                                        body=body,
                                        rating=review['rating'],
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        page_no= page,
                                        extra_info=extra_info)


                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            if 'class="ld ld-ChevronRight pv1 blue"' in response.text:
                page += 1
                yield scrapy.Request(url=self.get_review_url_us(product_id, page),
                                     dont_filter=True,
                                     callback=self.parse_reviews_us,
                                     errback=self.err,
                                     meta={'media_entity': media_entity,
                                           'page': page})
                self.logger.info(f"Generating reviews for {product_url} and page {page}")

    def parse_reviews_ca(self, response):
        print("inside parse_reviews_ca")
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']

        if 'Are you human?' in response.text or 'Robot or human' in response.text or 'class="re-captcha"' in response.text or 'Please download one of the supported browsers to keep shopping' in response.text:
            print("Robot or human occured, retrying")
            yield scrapy.Request(url=self.get_review_url(product_url, page_count),
                                 callback=self.parse_reviews_ca,
                                 errback=self.err, dont_filter=True,
                                 meta={'page_count': page_count,
                                       'media_entity': media_entity,
                                       'lifetime': True})


        res = json.loads(response.text)
        print("res after json loading is :", res)
        current_page = res['BatchedResults']['q0']['Offset']
        total_pages = res['BatchedResults']['q0']['TotalResults']
        print("current_page and total_pages are ", current_page, total_pages)

        try:
            product_name = res['BatchedResults']['q0']['Includes']['Products'][
                product_id]['Name']
        except:
            product_name = ''
        try:
            brand_name = res['BatchedResults']['q0']['Includes']['Products'][
                product_id]['Brand']['Name']
        except:
            brand_name = ''

        extra_info = {"product_name": product_name, "brand_name": brand_name}
        print("extra_info is", extra_info)

        review_date = self.start_date

        if res['BatchedResults']['q0']['Results']:
            print("inside batchedresults if ")
            for item in res['BatchedResults']['q0']['Results']:
                print("for loop")
                if item:
                    print("insed if item")
                    try:
                        content_type = 'syndicated'
                        source_url = item['SyndicationSource']['Name']
                    except:
                        content_type = 'organic'
                        source_url = 'None'
                    content = {'content_type': content_type, 'source_url': source_url}
                    extra_info.update(content)

                    _id = item["Id"]
                    review_date = dateparser.parse(item["SubmissionTime"]).replace(tzinfo=None)
                    if self.start_date <= review_date <= self.end_date:
                        print("inside if of self.start")
                        try:
                            print("inside try")
                            if self.type == 'media':
                                print("if media")
                                body = item["ReviewText"]
                                if body:
                                    print("inside if body")
                                    #elf, _id, review_date, title, body, rating, url, review_type, creator_id, creator_name, product_id,
                                                    #page_no, extra_info={}
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title=item['Title'],
                                        body=body,
                                        rating=item["Rating"],
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        page_no = current_page,
                                        extra_info=extra_info)
                                    # self.parse_comments(item, _id, review_date,
                                    #                                product_url, product_id,
                                    #                                extra_info)
                            if self.type == 'comments':
                                self.parse_comments(item, _id, review_date,
                                                               product_url, product_id,
                                                               extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}".format(_id))

            next_page = current_page + 1
            print("review_data, start_Date  next_page, total_pages are: ", review_date, self.start_date, next_page, total_pages)

            if review_date >= self.start_date and next_page <= total_pages:
                page_count = response.meta['page_count'] + 1
                print("inside re-requesting and page is ", page_count)
                yield scrapy.Request(url=self.get_review_url(product_url, page_count),
                                     callback=self.parse_reviews_ca,
                                     errback=self.err,
                                     meta={'page_count': page_count, 'media_entity': media_entity})

        else:
            if res['FailedRequests'] == 0:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(current_page))

    def parse_comments(self, item, _id, review_date, product_url, product_id, extra_info):
        try:
            if item['ClientResponses']:
                for comment in item['ClientResponses']:
                    comment_date = datetime.datetime.strptime(
                        (comment.get('Date').split('+')[0]), "%Y-%m-%dT%H:%M:%S.%f")
                    comment_date_until = review_date \
                                         + timedelta(days=SetuservSpider.settings.get('PERIOD'))
                    if comment_date <= comment_date_until:

                        if comment['Response']:
                            comment_id = comment['Department'] + _id + \
                                         hashlib.sha512(comment['Response']
                                                        .encode('utf-8')).hexdigest()
                            self.yield_items_comments(
                                parent_id=_id,
                                _id=comment_id,
                                comment_date=comment_date,
                                title='',
                                body=comment['Response'],
                                rating='',
                                url=product_url,
                                review_type='comments',
                                creator_id='',
                                creator_name='',
                                product_id=product_id,
                                extra_info=extra_info)
        except:
            self.logger.info(f"There is no comment for product_id {product_id} "
                             f"on review {_id}")






    @staticmethod
    def get_review_url_us(product_id, page):
        url = f'https://www.walmart.com/reviews/product/{product_id}?sort=submission-desc&page={page}'
        return url

    @staticmethod
    def get_headers():
        headers = {'Content-Type': 'application/json',
                   'X-Requested-With': 'XMLHttpRequest',
                   'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/63.0.3239.132 Safari/537.36'}
        return headers

    def get_lifetime_ratings_ca(self, product_id, response):
        try:
            _res = json.loads(response.text)
            res = \
                _res['BatchedResults']['q0']['Includes']['Products'][product_id]['ReviewStatistics']
            review_count = res['TotalReviewCount']
            average_ratings = res['AverageOverallRating']
            ratings = res['RatingDistribution']

            rating_map = {}
            _rating_value = []
            for item in ratings:
                _rating_value.append(item['RatingValue'])
                for i in range(1, 6):
                    if i in _rating_value:
                        rating_map['rating_' + str(item['RatingValue'])] = item['Count']
                    else:
                        rating_map['rating_' + str(i)] = 0

            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=float(average_ratings),
                ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")

    def get_lifetime_ratings_us(self, product_id, response):
        try:
            review_count = int(response.css('.dark-gray.pl1::text').extract_first()[0:3])#response.css('span.seo-review-count::text').extract_first()
            average_ratings = float(response.css('.rating-number::text').extract_first()[1:4])#response.css('span.seo-avg-rating::text') .extract_first()
            temp_lst = response.css('#item-review-section div.w-50 ol.w-100 .w3')
            print("temp_list is just to observe the result:", temp_list)
            ratings = []
            for temp in temp_lst:
                ratings.append(temp.css("span.w3::text"))

            #ratings = response.css('.rating-number::text').extract()[0][1:4]#response.css('span.font-normal::text').extract()[::-1]
            print("review_count, average_ratings, ratings", review_count, average_ratings, ratings)
            print("length of ratings is", len(ratings))

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[int(i)-1]

            self.yield_lifetime_ratings(
                product_id=product_id,
                review_count=review_count,
                average_ratings=float(average_ratings),
                ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
