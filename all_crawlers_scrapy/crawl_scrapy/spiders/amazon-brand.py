import scrapy
from urllib.parse import urlparse
from .setuserv_spider import SetuservSpider


class AmazonbrandSpider(SetuservSpider):

    name = 'amazon-brand-product'
    review_count = 0

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Amazon Brand process start")
        assert (self.source == 'amazon-brand')

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_reviews, errback=self.err,
                                 dont_filter=True, meta={'media_entity': media_entity})

    def parse_reviews(self, response):
        media_entity = response.meta['media_entity']
        product_url = media_entity['url']
        self.logger.info(f"Url opened successfully - {response.url}")
        parsed_uri = urlparse(response.url)
        host = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
        url_paths = response.css('a#dp-summary-see-all-reviews::attr(href)').extract()
        reviews_url_path = None
        if url_paths:
            reviews_url_path = url_paths[0]
        else:
            # TODO : Find the exact css style path to make it work without this complex way
            html_text = response.text.split()

            for text in html_text:
                if 'reviewerType=all_reviews' in text:
                    url_paths = text.split('href="')
                    if len(url_paths) > 1:
                        reviews_url_path = url_paths[1].split('">')[0]
                    break

        if reviews_url_path:
            current_review_url = '{host}{reviews_url_path}&sortBy=recent'.format(host=host,
                                                                                 reviews_url_path=reviews_url_path)

            sorted_review_url = '{current_review_url}&pageNumber={page}'.format(current_review_url=current_review_url,
                                                                                page=1)
            self.logger.info(f"Review url generated successfully - {sorted_review_url}")
            yield scrapy.Request(sorted_review_url, callback=self.parse_info_with_reviews, errback=self.err,
                                 meta={"media_entity": media_entity, "total_pages": 0,
                                       "current_review_url": current_review_url, "host": host})
            self.logger.info(f"Generating reviews for {sorted_review_url}")
        else:
            yield scrapy.Request(url=product_url, callback=self.parse_info_without_reviews, errback=self.err,
                                 meta={"media_entity": media_entity})
            self.logger.info(f"Generating reviews for {product_url}")

    def parse_info_with_reviews(self, response):

        media_entity = response.meta['media_entity']
        product_url = media_entity['url']
        product_id = media_entity['id']

        __title = ['div.product-title a.a-link-normal', 'span#productTitle', 'div#mas-title span.a-size-large',
                   'h1.a-size-large a-spacing-micro', 'span#ebooksProductTitle']
        __brand = ['div.product-by-line a.a-link-normal', 'a#bylineInfo', 'a#brand', 'a#ProductInfoArtistLink',
                   'a.a-link-normal contributorNameID', 'a.a-link-normal', 'span.a-size-mini a-text-bold a-text-italic']

        title, brand = '', ''

        try:
            for i in range(0, len(__title)):
                _title = response.css('{}::text'.format(__title[i])).extract_first()
                if _title:
                    title = _title.strip()
        except:
            title = ''

        try:
            for i in range(0, len(__brand)):
                _brand = response.css('{}::text'.format(__brand[i])).extract_first()
                if _brand:
                    brand = _brand.strip()
        except:
            brand = ''

        if title is '' and brand is '':
            pass
        else:
            extra_info = {"product_name": title, "brand_name": brand}

            yield {
                    'id': '',
                    'created_date': '',
                    'body': '',
                    'rating': '',
                    'parent_type': 'media_entity',
                    'url': product_url,
                    'media_source': self.source,
                    'type': 'media',
                    'creator_id': '',
                    'creator_name': '',
                    'media_entity_id': product_id,
                    'title': '',
                    'client_id': self.client_id,
                    'propagation': self.propagation,
                    'extra_info': extra_info
                    }
            self._add_product_count(product_id)

    def parse_info_without_reviews(self, response):

        media_entity = response.meta['media_entity']
        product_url = media_entity['url']
        product_id = media_entity['id']

        __title = ['div.product-title a.a-link-normal', 'span#productTitle', 'div#mas-title span.a-size-large',
                   'h1.a-size-large a-spacing-micro', 'span#ebooksProductTitle',
                   'div#dmusicProductTitle_feature_div h1.a-size-large a-spacing-micro']
        __brand = ['div.product-by-line a.a-link-normal', 'a#bylineInfo', 'a#brand', 'a#ProductInfoArtistLink',
                   'a.a-link-normal contributorNameID', 'a.a-link-normal', 'span.a-size-mini a-text-bold a-text-italic']

        title, brand = '', ''

        try:
            for i in range(0, len(__title)):
                _title = response.css('{}::text'.format(__title[i])).extract_first()
                if _title:
                    title = _title.strip()
        except:
            title = ''

        try:
            for i in range(0, len(__brand)):
                _brand = response.css('{}::text'.format(__brand[i])).extract_first()
                if _brand:
                    brand = _brand.strip()
        except:
            brand = ''

        if title is '' and brand is '':
            pass
        else:
            extra_info = {"product_name": title, "brand_name": brand}

            yield {
                    'id': '',
                    'created_date': '',
                    'body': '',
                    'rating': '',
                    'parent_type': 'media_entity',
                    'url': product_url,
                    'media_source': self.source,
                    'type': 'media',
                    'creator_id': '',
                    'creator_name': '',
                    'media_entity_id': product_id,
                    'title': '',
                    'client_id': self.client_id,
                    'propagation': self.propagation,
                    'extra_info': extra_info
                    }
            self._add_product_count(product_id)

