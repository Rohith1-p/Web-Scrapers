import dateparser
import scrapy
import requests

from .setuserv_spider import SetuservSpider


class InfluensterSpider(SetuservSpider):
    name = 'influenster-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Influenster process start")
        assert self.source == 'influenster'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url='https://scrapinghub.com/crawlera',
                                 callback=self.parse_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': 1,
                                       'dont_proxy': True})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        page = response.meta["page"]

        self.parse_reviews(product_url, product_id, page)

    def parse_reviews(self, product_url, product_id, page):
        _response = requests.post(
            url='https://www.influenster.com/adjax/marketplace/get-infinite-serpy-reviews',
            json={"product_id": product_id, "page": page, },
            headers=InfluensterSpider.get_headers(product_url))
        response = _response.json()

        if response:
            for item in response:
                if item:
                    _id = item['id']
                    review_date = dateparser.parse(item['timestamp']).replace(tzinfo=None)
                    extra_info = {'product_name': item['product']['name'], 'brand_name': ''}

                    try:
                        if self.type == 'media':
                            body = item['text']
                            if body:
                                if self.start_date <= review_date <= self.end_date:
                                    self.yield_items(
                                        _id=_id,
                                        review_date=review_date,
                                        title='',
                                        body=body,
                                        rating=item['stars'],
                                        url=product_url,
                                        review_type='media',
                                        creator_id='',
                                        creator_name='',
                                        product_id=product_id,
                                        extra_info=extra_info)
                    except:
                        self.logger.warning("Body is not their for review {}".format(_id))

            self.parse_reviews(product_url, product_id, page+1)
        else:
            self.logger.info(f"Dumping for {self.source} and {product_id}")
            self.dump(response, 'html', 'rev_response', self.source, product_id, str(page))

    @staticmethod
    def get_headers(product_url):
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'origin': 'www.influenster.com',
            'path': '/adjax/marketplace/get-paginated-serpy-reviews',
            'referer': product_url,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/72.0.3626.121 Safari/537.36',
            'Content-Type': 'application/json',
            'authority': 'www.influenster.com',
            'method': 'POST',
            'scheme': 'https',
            'content-length': '30',
            'content-type': 'application/json',
            'cookie': '__cfduid=d96c84f9563af24c9a08c55cbb9ecc8101565955434; '
                      'csrftoken=8hcVh9bK6t8nZnt6L9cdAtq9XO9u3qjdcWYElPnh41qcry7TUI9Hq0VRVgSSopzt;'
                      ' sessionid_v3=t7om1yrrxavt189vc30qkuaoldxhsaox; ab.storage.deviceId.44aa633'
                      '8-c4bc-4fd7-a19b-5a28989e128e=%7B%22g%22%3A%224ac15793-70fe-ca9e-fe72-3b88a'
                      '8dda44c%22%2C%22c%22%3A1565955440932%2C%22l%22%3A1565955440932%7D; _ga=GA1.'
                      '2.1570402762.1565955441; _fbp=fb.1.1565955445921.636852661; _hjid=bdee7a2a-'
                      '6ab4-47cb-8050-51fe784b72d5; __gads=ID=0f58c9126d4dac6a:T=1567510254:S=ALNI'
                      '_MbCiHUtMvhM_jK9OI1E8b5epTytsw; ab.storage.sessionId.44aa6338-c4bc-4fd7-a19'
                      'b-5a28989e128e=%7B%22g%22%3A%22dcacdb0d-ff20-4f85-4465-2dc2cd9adf0a%22%2C%2'
                      '2e%22%3A1568293103779%2C%22c%22%3A1568291303783%2C%22l%22%3A1568291303783%7'
                      'D; _conv_v=vi:1*sc:10*cs:1568291306*fs:1565955445*pv:21*exp:{}*ps:156757458'
                      '5; _conv_r=s:www.google.com*m:organic*t:*c:; _gid=GA1.2.1744875207.15716619'
                      '87; visited=False; mp_9cc9b840c981980a421375cf78f07176_mixpanel=%7B%22disti'
                      'nct_id%22%3A%20%2216c9a37d3da3ce-0a9b9228c930f1-37677e02-13c680-16c9a37d3dd'
                      '50c%22%2C%22%24device_id%22%3A%20%2216c9a37d3da3ce-0a9b9228c930f1-37677e02-'
                      '13c680-16c9a37d3dd50c%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2'
                      'C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22__mps%22%3A%20'
                      '%7B%22%24os%22%3A%20%22Mac%20OS%20X%22%2C%22%24browser%22%3A%20%22Chrome%22'
                      '%2C%22%24browser_version%22%3A%2077%2C%22%24initial_referrer%22%3A%20%22%24'
                      'direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D%2C%22'
                      '__mpso%22%3A%20%7B%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%'
                      '2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B'
                      '%5D%2C%22Current_page_name%22%3A%20%22%22%2C%22Previous_page%22%3A%20%22%22'
                      '%2C%22platform%22%3A%20%22web%22%2C%22profile_user_id%22%3A%20null%2C%22%24'
                      'search_engine%22%3A%20%22google%22%7D',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-csrftoken': '8hcVh9bK6t8nZnt6L9cdAtq9XO9u3qjdcWYElPnh41qcry7TUI9Hq0VRVgSSopzt'
        }

        return headers
