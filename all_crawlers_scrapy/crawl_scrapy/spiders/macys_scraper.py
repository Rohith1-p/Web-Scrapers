# -*- coding: utf-8 -*-
import datetime

import scrapy
from bs4 import BeautifulSoup

from .setuserv_spider_elc import SetuservSpiderELC
from .utils import re_classify_review_source

class Macys(SetuservSpiderELC):
    name = 'macys_spider'
    review_count = 0

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Macys process start")
        assert (self.source == 'macys')

        self.cookies = {'currency': 'USD', 'shippingCountry': 'US',
                        'ak_bmsc': '5E6A66A9C34CB4A655D46DA11801FD907BB0201F916E000098D67F5B618E4C63~plb1T8ADGdkYSTCgNka2w1c2KxlgQIwmXXhO4cHIWRb056Qbm++APfhY8aln9acik4xUtHf5en+iFwY6yxuRSUbNUD4NNLsJuahSohxt45cwQ5i/Rr7x1OnfuQdLH5eC5SjDwo848XFRMUayOMQhj7wke0HoFKbLh8Ju2Gi08Q5T5RKfI1yAit985FsagL5gQLieczuOTt62kbNG4RpHOFNFjI0B9WIkyQVO7IdGaw+julfvSfditE1imJ23o2P9q5'}

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page_count = 1
            yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                 callback=self.parse_reviews_macys, cookies=self.cookies,
                                 meta={'page_count': page_count, 'product_id': product_id,
                                       'media_entity': media_entity, 'lifetime': True},
                                 errback=self.err, dont_filter=True)

            self.logger.info(f"Processing for product_id {product_id}")
            self.logger.info(f"Generating reviews for {product_url}")

    def parse_reviews_macys(self, response):
        parse_html = "".join(response.text.split("\\n"))
        parse_html = parse_html.replace("\\", "")
        if response.meta['lifetime']:
            self.create_lifetime_ratings(parse_html, response.meta['product_id'])

        self.create_reviews(parse_html, response)

    def create_reviews(self, parse_html, response):
        cookies = {'currency': 'USD', 'shippingCountry': 'US'}
        review_date = self.start_date
        page_count = response.meta["page_count"]
        product_id = response.meta['product_id']
        media_entity = response.meta["media_entity"]

        if '"numPages":' in parse_html:
            if int(page_count) == 1:
                num_pages = parse_html.split('"numPages":')[1]
                num_pages = num_pages.split('}')[0]
                num_pages = int(num_pages.split(',')[0])
                self.log(num_pages)
            else:
                num_pages = response.meta['num_pages']
            get_req_html = parse_html.split('<div id="BVRRDisplayContentBodyID" class="BVRRDisplayContentBody">')[1]
            get_req_html = \
                get_req_html.split(
                    '<div id="BVRRDisplayContentFooterID" class="BVRRFooter BVRRDisplayContentFooter">')[
                    0]
            get_req_html = '<div id="BVRRDisplayContentBodyID" class="BVRRDisplayContentBody">' + get_req_html
            soup = BeautifulSoup(get_req_html, 'html.parser')
            for main_div in soup.findAll('div', {
                "class": lambda x: x and x.startswith('BVRRContentReview BVRRDisplayContentReview')}):
                review_date = datetime.datetime.strptime(
                    main_div.find('span', attrs={'class': 'BVRRValue BVRRReviewDate'}).text, '%B %d, %Y')
                sub_source = self.get_media_sub_source(main_div)
                allow_review, media_sub_source = re_classify_review_source(self.source, sub_source)
                if allow_review:
                    if self.start_date <= review_date <= self.end_date:
                        rev_id = main_div.get('id').split('BVRRDisplayContentReviewID_')[1]
                        review_id = str(sub_source + rev_id if sub_source else rev_id)
                        review_text = main_div.find('span', attrs={'class': 'BVRRReviewText'}).text
                        body = '' if review_text is None else str(review_text)

                        # creator_id = review_id
                        # creator_name = str(main_div.find('span',attrs={'class': 'BVRRNickname'}).text.strip())

                        creator_id = ""
                        creator_name = ""

                        rating = float(main_div.find('span', attrs={'class': 'BVRRNumber BVRRRatingNumber'}).text)
                        review_title = main_div.find('span', attrs={'class': 'BVRRValue BVRRReviewTitle'}).text

                        title = '' if review_title is None else str(review_title)
                        self.yield_items_elc(review_id, media_sub_source, review_date, title, body,
                                                    rating, response.url, 'media', creator_id,
                                                    creator_name, media_entity['id'])

                        Macys.review_count += 1
                else:
                    self.logger.warning('Dropping review with id ' + main_div.get('id') + ', with content '
                                        + main_div.find('span',
                                                        attrs={'class': 'BVRRReviewText'}).text + ', for source '
                                        + self.source + ' and created date ' + str(review_date))

            page_count += 1

            if Macys.review_count > 0: self.logger.info(f"Successfully scraped {Macys.review_count} reviews")

            if review_date >= self.start_date and num_pages >= page_count:
                yield scrapy.Request(url=self.get_review_url(product_id, page_count),
                                     callback=self.parse_reviews_macys, cookies=cookies,
                                     meta={'page_count': page_count, 'num_pages': num_pages,
                                           'product_id': product_id,
                                           "media_entity": media_entity, 'lifetime': False})
            if review_date <= self.start_date:
                    self.logger.info(f"Finished scraping reviews for url with product_id: {product_id}")

    def create_lifetime_ratings(self, parse_html, product_id):
        soup = BeautifulSoup(parse_html, 'html.parser')
        rating_map = {}
        for i in range(1, 6):
            try:
                rating = soup.findAll('div', {"class": lambda x: x and x.startswith('BVRRHistogramBarRow' + str(
                    i))})[0]
                text = rating.find('span', attrs={'class': 'BVRRHistAbsLabel'}).text
                rating_map['rating_' + str(i)] = int(text.replace(',', ''))
            except IndexError as e:
                self.logger.warning('Index error occured while fetching '
                                    'rating for product ' + str(product_id) + ' ' +
                                    str(e))
                rating_map['rating_' + str(i)] = 0

        try:
            avg_rating = soup.findAll('div', {"class": lambda x: x and x.startswith('BVRRRatingNormalOutOf')})[0]
            average_ratings = avg_rating.find('span', attrs={'class': 'BVRRRatingNumber'}).text
            review_counts = soup.findAll('div', {"class": lambda x: x and x.startswith('BVRRHistogramTitle')})[0]
            text = review_counts.find('span', attrs={'class': 'BVRRNumber'}).text
            total_review_count = int(text.replace(',', ''))
        except IndexError as e:
            self.logger.warning('Index error occured while fetching '
                                'average rating and review count for product '
                                '' + str(product_id) + ' ' + str(e))
            average_ratings = 0.0
            total_review_count = 0

        self.yield_lifetime_ratings_elc(product_id, total_review_count, round(float(average_ratings), 2), rating_map)

    @staticmethod
    def get_review_url(product_id, page_count):
        key = "7129aa/" + str(product_id)
        return "https://macys.ugc.bazaarvoice.com/{}/reviews.djs?format=embeddedhtml&page={}&&sort=submissionTime".format(
            key, str(page_count))

    @staticmethod
    def get_media_sub_source(main_div):
        media_sub_source = None
        element = main_div.find('div', attrs={'class': 'BVRRSyndicatedContentSource'})
        if element:
            media_sub_source = element.text.split()[-1].lower()
        return media_sub_source
