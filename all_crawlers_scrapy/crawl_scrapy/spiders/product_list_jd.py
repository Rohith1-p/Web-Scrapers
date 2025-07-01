import scrapy
import json
import time

# from scrapy.conf import settings
# settings.overrides['DOWNLOADER_MIDDLEWARES'] = {}

from .setuserv_spider import SetuservSpider


class ProductListJD(SetuservSpider):
    name = 'jd-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("jd_product_list scraping process start")
        assert self.source == 'jd_product_list'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            query_url = self.get_product_url(product_id, page)
            print("Query URL", self.source, query_url)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 headers=self.get_headers(),
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        page = response.meta["page"]

        res = json.loads(response.text)
        total_page_count = res['data']['head']['Summary']['Page']['PageCount']

        if res['data']['paragraphs']:
            for item in res['data']['paragraphs']:
                if item:
                    product_id = item['spuid']
                    product_url = 'https://www.jd.id/product/_' + product_id + '/' + item['skuid'] + '.html'
                    product_name = item['Content']['title']
                    try:
                         review_count = item['commentcount']
                    except:
                         review_count = 0

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=review_count)

            if page < total_page_count:
                page += 1
                time.sleep(0.5)
                query_url = self.get_product_url(sub_brand, page)
                print("Query URL", self.source, query_url)
                yield scrapy.Request(url=query_url,
                                     callback=self.parse_product_list,
                                     headers=self.get_headers(),
                                     errback=self.err,
                                     dont_filter=True,
                                     meta={'media_entity': media_entity,
                                           'page': page})

    @staticmethod
    def get_product_url(sub_brand, page_count):
        url = f"https://color.jd.id/jdid_pc_website/search_pc/1.0?keywords={sub_brand}&pvid=2ff737a649d44e069867ea04d2b00e77" \
              f"&page={page_count}&pagesize=60"
        return url

    @staticmethod
    def get_headers():
        headers = {
            'authority': 'color.jd.id',
            'method': 'GET',
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'lang': 'id',
            'origin': 'https://www.jd.id',
            'platform': 'PC',
            'referer': 'https://www.jd.id/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/99.0.4844.74 Safari/537.36',
            'x-api-lang': 'id',
            'x-api-platform': 'PC',
            'x-api-timestamp': '1648013540031',
        }
        return headers
