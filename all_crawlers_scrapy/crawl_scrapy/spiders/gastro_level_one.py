import dateparser
import json
from datetime import datetime
import re
import time
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider


class GastroPharmOneSpider(SetuservSpider):
    name = 'gastro-pharma-level-one'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("gastro pahrm level one process start")
        assert self.source == 'gastro_level_1'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            page = 1
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]
        product_url = media_entity["url"]
        page = response.meta["page"]
        res = response.css(
            'section[class="has_ma_el_bg_slider elementor-section elementor-top-section elementor-element elementor-element-2ec5836c elementor-section-boxed elementor-section-height-default elementor-section-height-default jltma-glass-effect-no"]')

        for item in res:
            _handle_url = item.css(
                'div[class="has_ma_el_bg_slider elementor-column elementor-col-100 elementor-top-column elementor-element elementor-element-5a991056 jltma-glass-effect-no"]::attr(data-jltma-wrapper-link)').extract_first()
            _handle_url = BeautifulSoup(str(_handle_url), 'lxml')
            handle_url = _handle_url.get_text()
            handle_url = json.loads(handle_url)
            handle_url = handle_url['url']
            date = item.css('div[class="elementor-text-editor elementor-clearfix"]::text').extract_first().strip()
            bio = item.css(
                'div[class="elementor-element elementor-element-ab4c287 news-card-title jltma-glass-effect-no elementor-widget elementor-widget-text-editor"]')
            bio = bio.css('div[class="elementor-text-editor elementor-clearfix"]::text').extract_first().strip()

            media = {
                "client_id": self.client_id,
                "media_source": self.source,
                "product_url": product_url,
                "type": "handle_names",
                "propagation": self.propagation,
                "handle_url": handle_url,
                "citation_count": "",
                "paper_count": "",
                "handle_name": "",
                "bio": bio
            }
            yield self.yield_get_article_handle_names(product_url=media['product_url'],
                                                      product_id=product_id,
                                                      handle_url=media['handle_url'],
                                                      citation_count='',
                                                      paper_count=media['paper_count'],
                                                      handle_name=media['handle_name'],
                                                      bio=media['bio'])
        if page <= 100:
            page += 1
            product_url = product_url + str(page) + '/'
            yield scrapy.Request(url=product_url,
                                 callback=self.parse_info,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})
