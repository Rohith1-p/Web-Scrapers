import base64
import random
from urllib.parse import urlparse
import time
from scrapy.exceptions import IgnoreRequest
from . import useragent
from . import proxy

class ProxyMiddleware(proxy.baseproxymiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        super().__init__(settings)
        self.endpoint = settings.get('STORMPROXY_ENDPOINT')
        self.port = str(settings.get('STORMPROXY_PORT'))
        self.retry_counter = dict()

    def process_request(self, request, spider):
        key, prod_id, url, page = self.gen_key_prodid_url_page(request)
        self.update_key(request, key)
        if self.retry_counter[key] > 20:
            print('More than 20 calls made. Maybe captcha or other blocking efforts from the domain')
            raise IgnoreRequest
        headers = request.headers
        headers['User-Agent'] = useragent.get_user_agent()
        request.headers = headers
        # user_credentials = '{user}:{passw}'.format(user=self.user, passw=self.password)
        # basic_authentication = 'Basic ' + base64.b64encode(user_credentials.encode()).decode()
        host = 'http://{endpoint}:{port}'.format(endpoint=self.endpoint, port=self.port)
        print(host,self.endpoint,self.port)
        request.meta['proxy'] = host


    def gen_key_prodid_url_page(self, request):
        prod_id = request.meta['media_entity']['id']
        print("PRODID: ",prod_id)
        url = str(request.url)
        if 'page' in request.meta:
            page = request.meta['page']
        elif 'page_count' in request.meta:
            page = request.meta['page_count']
        elif 'offset' in request.meta:
            page = request.meta['offset']
        else:
            page = 'no_page_in_meta'
        key = (prod_id,url,page)
        return key, prod_id, url, page

    def update_key(self, request , key):
        if key not in self.retry_counter:
            self.retry_counter[key] = 1
            print(self.retry_counter[key])
        else:
            self.retry_counter[key] += 1
            print(self.retry_counter[key])
