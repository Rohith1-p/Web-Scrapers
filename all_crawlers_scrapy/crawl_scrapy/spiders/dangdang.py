from bs4 import BeautifulSoup
import scrapy
from dateparser import search as datesearch
import dateparser

from .setuserv_spider import SetuservSpider


class DangdangSpider(SetuservSpider):
    name = 'dangdang-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Dangdang process start")
        assert self.source == 'dangdang'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_id, product_url in zip(self.product_ids, self.start_urls):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity["id"]

        try:
            product_name = response.css('div.name_info h1::text').extract_first().split()[0]
        except:
            product_name = ''
        try:
            if response.css('div.pro_content li::text').extract_first() == '品牌：':
                brand_name = response.css('div.pro_content a::text').extract_first()
            else:
                brand_name = ''
        except:
            brand_name = ''
        extra_info = {"product_name": product_name, "brand_name": brand_name}
        page = 1
        yield scrapy.Request(url=self.get_review_url(product_id, page),
                             callback=self.parse_reviews,
                             errback=self.err, dont_filter=True,
                             meta={'page': page, 'media_entity': media_entity,
                                   'extra_info': extra_info})

    def parse_reviews(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity['url']
        extra_info = response.meta['extra_info']
        page = response.meta['page']

        _res = "".join(response.text.split("\n")).replace("\\", "")
        _res = BeautifulSoup(_res, 'html.parser')
        res = _res.findAll("div", {"class": "comment_items clearfix"})

        if res:
            for item in res:
                if item:
                    _id = item.find("div", {"class": "support"}).get('data-comment-id')
                    _review_date = str(item.find("div", {"class": "starline clearfix"}).text.strip(
                            ).replace('n', ''))
                    _review_date = datesearch.search_dates(_review_date)[0][1]
                    review_date = dateparser.parse(str(_review_date)).replace(hour=0, minute=0, second=0)
                    try:
                        if self.type == 'media':
                            body = item.find("div", {"class": "describe_detail"}).text.strip(
                            ).replace('n', '').split()[0]
                            if body:
                                if self.start_date <= review_date <= self.end_date:
                                    self.yield_items \
                                        (_id=_id,
                                         review_date=review_date,
                                         title='',
                                         body=body,
                                         rating=int(item.find("span", {"class": "star"}).get('style')
                                                    .split('width:')[1].split('%')[0])/20,
                                         url=product_url,
                                         review_type='media',
                                         creator_id='',
                                         creator_name='',
                                         product_id=product_id,
                                         extra_info=extra_info)
                    except:
                        self.logger.warning("Body is not their for review {}".format(_id))

            page += 1
            yield scrapy.Request(url=self.get_review_url(product_id, page),
                                 callback=self.parse_reviews,
                                 errback=self.err,
                                 meta={'page': page,
                                       'media_entity': media_entity,
                                       'extra_info': extra_info})
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_review_url(product_id, page):
        url = f'http://product.dangdang.com/index.php?r=comment%2Flist&productId={product_id}' \
              f'&mainProductId={product_id}&pageIndex={page}&sortType=1&filterType=1'
        return url
