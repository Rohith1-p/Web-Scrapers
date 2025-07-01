import re
import dateparser
from dateparser import search as datesearch
import scrapy
from bs4 import BeautifulSoup
from .setuserv_spider import SetuservSpider


class EbaySpider(SetuservSpider):
    name = 'ebay-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Ebay process start")
        assert self.source == 'ebay'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'id': product_id, 'url': product_url}
            media_entity = {**media_entity, **self.media_entity_logs}
            if 'https://www.ebay.com/' in product_url:
                country_code = 'US'
            else:
                country_code = 'IT'
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'country_code': country_code,
                                       'dont_proxy': True, 'lifetime': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta['media_entity']
        lifetime = response.meta['lifetime']
        product_id = media_entity['id']
        product_url = media_entity['url']
        country_code = response.meta['country_code']

        if lifetime:
            self.get_lifetime_ratings(product_id, response, country_code)
        if country_code == 'US':
            try:
                product_name = response.css('h1.product-title::text').extract_first()
            except:
                product_name = ''
            try:
                brand_name = response.css('div.s-value::text').extract_first()
            except:
                brand_name = ''
        else:
            try:
                product_name = response.css('h1[itemprop="name"]::text').extract_first()
            except:
                product_name = ''
            try:
                brand_name = response.css('div[class="section"]').extract_first()
                soup = BeautifulSoup(brand_name, 'html.parser')
                for s in soup(['script', 'style']):
                    s.decompose()
                brand_name = ' '.join(soup.stripped_strings)
                brand_name = brand_name.split('Marca: ')[1].split(' ')[0]
            except:
                brand_name = ''

        extra_info = {"product_name": product_name, "brand_name": brand_name}

        if country_code == 'US':
            if 'see--all--reviews' in response.text:
                review_url = response.css('div.see--all--reviews a::attr(href)').extract_first()
                yield scrapy.Request(url=review_url,
                                     callback=self.parse_reviews_above_10,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'country_code': country_code,
                                           'review_url': review_url,
                                           'extra_info': extra_info})
                self.logger.info(f"Scraping for US country and Above 10 reviews")
            else:
                yield scrapy.Request(url=product_url,
                                     callback=self.parse_reviews_us_below_10,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'extra_info': extra_info})
                self.logger.info(f"Scraping for US country and Below 10 reviews")
        else:
            if '_sp="p2047675.m3946.l7020"' in response.text:
                review_url = response.css('a[_sp="p2047675.m3946.l7020"]::attr(href)').extract_first()
                yield scrapy.Request(url=review_url,
                                     callback=self.parse_reviews_above_10,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'country_code': country_code,
                                           'review_url': review_url,
                                           'extra_info': extra_info})
                self.logger.info(f"Scraping for IT country and Above 10 reviews")
            else:
                yield scrapy.Request(url=product_url,
                                     callback=self.parse_reviews_it_below_10,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'extra_info': extra_info})
                self.logger.info(f"Scraping for IT country and Below 10 reviews")

    def parse_reviews_us_below_10(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta['extra_info']
        res = response.css('li.review--section')

        if res:
            for item in res:
                if item:
                    _id = item.css('::attr(id)').extract_first().split('_')[1]
                    _review_date = item.css('span.review--date::text').extract_first()
                    review_date = dateparser.parse(_review_date)
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item.css('p.review--content::text').extract_first()
                                title = item.css('h4.review--title::text').extract_first()
                                rating = item.css('span.clipped::text').extract_first().split(' ')[0]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=rating,
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

    def parse_reviews_it_below_10(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta['extra_info']
        res = response.css('div[itemprop="review"]')

        if res:
            for item in res:
                if item:
                    _id = item.css('::attr(id)').extract_first().split('_')[1]
                    _review_date = item.css('span[itemprop="datePublished"]::attr(content)').extract_first()
                    review_date = datesearch.search_dates(_review_date)[0][1]
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item.css('p[itemprop="reviewBody"]::text').extract_first()
                                title = item.css('p[itemprop="name"]::text').extract_first()
                                rating = item.css('div.ebay-star-rating::attr(aria-label)').extract_first().split(' ')[0]
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=rating,
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

    def parse_reviews_above_10(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        review_url = response.meta["review_url"]
        extra_info = response.meta['extra_info']
        country_code = response.meta['country_code']
        res = response.css('div[itemprop="review"]')

        review_date = self.start_date
        if res:
            for item in res:
                if item:
                    _id = item.css('::attr(id)').extract_first().split('_')[1]
                    _review_date = item.css('span[itemprop="datePublished"]::attr(content)').extract_first()
                    if country_code == 'US':
                        review_date = dateparser.parse(_review_date)
                    else:
                        review_date = datesearch.search_dates(_review_date)[0][1]
                    if self.start_date <= review_date <= self.end_date:
                        try:
                            if self.type == 'media':
                                body = item.css('p[itemprop="reviewBody"]::text').extract_first()
                                title = item.css('h3[itemprop="name"]::text').extract_first()
                                rating = item.css('meta[itemprop="ratingValue"]::attr(content)').extract_first()
                                if body:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title=title,
                                         body=body,
                                         rating=rating,
                                         url=review_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                        except:
                            self.logger.warning(f"Body is not their for review {_id}")

            if review_date >= self.start_date:
                review_url = response.css('a[rel="next"]::attr(href)').extract_first()
                yield scrapy.Request(url=review_url, callback=self.parse_reviews_above_10,
                                     errback=self.err,
                                     meta={'media_entity': media_entity,
                                           'country_code': country_code,
                                           'review_url': review_url,
                                           'extra_info': extra_info})
        else:
            if 'reviewsSection' in response.text:
                self.logger.info(f"Pages exhausted for product_id {product_id}")
            else:
                self.logger.info(f"Dumping for {self.source} and {product_id}")
                self.dump(response, 'html', 'rev_response', self.source, product_id, str(1))

    def get_lifetime_ratings(self, product_id, response, country_code):
        try:
            if country_code == 'US':
                _review_count = response.css('span.reviews--count::text').extract_first()
                average_ratings = response.css('span.review--start--rating::text').extract_first().split()[0]
                ratings = response.css('li.review--item span::text').extract()[::-1]
            else:
                _review_count = response.css('span.ebay-reviews-count::text').extract_first()
                average_ratings = response.css('span.ebay-review-start-rating::text').extract_first()
                ratings = response.css('li.ebay-review-item span::text').extract()[::-1]
            review_count = re.findall(r'\d+', _review_count)[0]

            rating_map = {}
            for i in range(1, 6):
                rating_map['rating_' + str(i)] = ratings[int(i)-1]
            self.yield_lifetime_ratings \
                (product_id=product_id,
                 review_count=review_count,
                 average_ratings=float(average_ratings),
                 ratings=rating_map)

        except Exception as exp:
            self.logger.error(f"Unable to fetch the lifetime ratings, {exp}")
