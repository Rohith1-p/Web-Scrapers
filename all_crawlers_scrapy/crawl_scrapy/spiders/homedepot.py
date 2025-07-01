import dateparser

from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider


class HomedepotSpider(SetuservSpider):
    name = 'homedepot-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Homedepot process start")
        assert self.source == 'homedepot'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        try:
            product_name = response.css('h1.product-title__title::text').extract_first()
            if product_name is None:
                product_name = response.css('h1.product-details__title::text').extract_first()
        except:
            product_name = ''
        try:
            brand = response.css('h2.product-title__brand span::text').extract_first()
            if brand is None:
                brand = response.css('span.product-details__brand-name::text').extract_first()
        except:
            brand = ''
        extra_info = {"product_name": product_name, "brand_name": brand}
        num_pages, page = 0, 1
        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page, 'num_pages': num_pages,
                                   'media_entity': media_entity,
                                   'extra_info': extra_info, 'dont_proxy': True})

    def parse_reviews(self, response):
        page = response.meta["page"]
        extra_info = response.meta["extra_info"]
        num_pages = response.meta["num_pages"]
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        parse_html = "".join(response.text.split("\\n"))
        parse_html = parse_html.replace("\\", "")

        if '"numPages":' in parse_html:
            if int(page) == 1:
                num_pages = parse_html.split('"numPages":')[1]
                num_pages = int(num_pages.split('}')[0])
            else:
                num_pages = response.meta['num_pages']

        get_req_html = parse_html.split('<div id="BVRRDisplayContentBodyID"'
                                        ' class="BVRRDisplayContentBody">')[1]
        get_req_html = get_req_html.split('<div id="BVRRDisplayContentFooterID" '
                                          'class="BVRRFooter BVRRDisplayContentFooter">')[0]
        get_req_html = '<div id="BVRRDisplayContentBodyID" class="BVRRDisplayContentBody">' + \
                       get_req_html
        _res = BeautifulSoup(get_req_html, 'html.parser')
        res = _res.findAll('div', {"class": 'BVRRContentReview'})
        review_date = self.start_date

        if res and num_pages >= page:
            for item in res:
                if item:
                    _id = item.find('div', {'class': 'BVRRReviewText'})['id']
                    _id = _id.split('BVRRReview')[1].split('_')[0]
                    try:
                        body = item.find('span', {'class': 'BVRRReviewText'}).text
                    except:
                        body = item.find('span', {'class': 'BVRRReviewText'}).get_text()

                    _review_date = item.find('meta', {'itemprop': 'datePublished'})['content']
                    review_date = dateparser.parse(_review_date)
                    if review_date and self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                if body:
                                    title = item.find\
                                        ('span', {'class': 'BVRRReviewTitle'}).string
                                    rating = item.find\
                                        ('span', {'class': 'BVRRRatingNumber'}).string
                                    creator_name = item.find\
                                        ('span', {'class': 'BVRRNickname'}).string
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=rating,
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name=creator_name,
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning("Body is not their for review {}"
                                                .format(_id))
            if review_date >= self.start_date:
                page = page + 1
                yield scrapy.Request(url=self.get_review_url(product_id, page),
                                     callback=self.parse_reviews,
                                     meta={'page': page, 'num_pages': num_pages,
                                           'media_entity': media_entity,
                                           'extra_info': extra_info, 'dont_proxy': True})

    @staticmethod
    def get_review_url(product_id, page):
        url = "https://homedepot.ugc.bazaarvoice.com/1999aa/{}/reviews.djs?" \
              "format=embeddedhtml&page={}&scrollToTop=true&sort=submissionTime"\
            .format(product_id, page)
        return url

