import dateparser
from datetime import datetime
import re
import time
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider


class ScholarGoogleHandleSpider(SetuservSpider):
    name = 'scholar-google-articles-handle'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Scholar-google process start")
        assert self.source == 'scholar_google_handle'

    '''Commenting Handle names scraping Script'''

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            time.sleep(5)
            page = 0
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'authority': 'scholar.google.co.in',
                'x-client-data': 'CIa2yQEIpLbJAQjEtskBCKmdygEIwIDLAQiUocsBCOvyywEInvnLAQjmhMwBCKWOzAEImY/MAQibnMwBCISfzAEIt5/MAQiaocwBCM+izAEYrKnKAQ==',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
            }
            url = f"https://scholar.google.com/citations?view_op=search_authors&mauthors={product_id}&hl=en&oi=ao"
            yield scrapy.Request(url=url,
                                 callback=self.get_handle_names,
                                 errback=self.err,
                                 dont_filter=True,
                                 headers=headers,
                                 meta={'media_entity': media_entity})

    def get_handle_names(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        if 'id="gs_captcha_ccl"' in response.text:
            import time
            time.sleep(3)
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'authority': 'scholar.google.co.in',
                'x-client-data': 'CIa2yQEIpLbJAQjEtskBCKmdygEIwIDLAQiUocsBCOvyywEInvnLAQjmhMwBCKWOzAEImY/MAQibnMwBCISfzAEIt5/MAQiaocwBCM+izAEYrKnKAQ==',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
            }
            url = f"https://scholar.google.com/citations?view_op=search_authors&mauthors={product_id}&hl=en&oi=ao"
            yield scrapy.Request(url=url,
                                 callback=self.get_handle_names,
                                 errback=self.err,
                                 dont_filter=True,
                                 headers=headers,
                                 meta={'media_entity': media_entity})
            return

        res = response.css('div[class="gs_ai_t"]')
        for item in res:
            _handle_name = item.css("a").extract_first()
            _handle_name = BeautifulSoup(str(_handle_name), 'lxml')
            handle_name = _handle_name.get_text()
            handle_url = "https://scholar.google.co.in" + str(item.css("a::attr(href)").extract_first())
            citation_count = item.css('div[class="gs_ai_cby"]::text').extract_first()
            bio = item.css('div[class="gs_ai_aff"]::text').extract_first()

            media = {
                "client_id": self.client_id,
                "media_source": self.source,
                "product_url": product_url,
                "type": "handle_names",
                "propagation": self.propagation,
                "handle_url": handle_url,
                "citation_count": citation_count,
                "paper_count": "",
                "handle_name": handle_name,
                "bio": bio
            }
            yield self.yield_get_article_handle_names(product_url=media['product_url'],
                                                      product_id=product_id,
                                                      handle_url=media['handle_url'],
                                                      citation_count=media['citation_count'],
                                                      paper_count='',
                                                      handle_name=media['handle_name'],
                                                      bio=bio)

            # yield scrapy.Request(url=media['handle_url'],
            #                      callback=self.parse_bio,
            #                      errback=self.err, dont_filter=True,
            #                      meta={'media_entity': media_entity, 'media': media})

    # def parse_bio(self, response):
    #     media_entity = response.meta["media_entity"]
    #     media = response.meta["media"]
    #     product_id = media_entity['id']
    #
    #     bio = response.css('meta[property="og:description"]::attr(content)').extract_first()
    #     yield self.yield_get_article_handle_names(product_url=media['product_url'],
    #                                               product_id=product_id,
    #                                               handle_url=media['handle_url'],
    #                                               citation_count=media['citation_count'],
    #                                               paper_count='',
    #                                               handle_name=media['handle_name'],
    #                                               bio=bio)
