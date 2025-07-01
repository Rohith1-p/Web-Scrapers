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
        self.user = settings.get('SMARTPROXY_USER')
        self.password = settings.get('SMARTPROXY_PASSWORD')
        self.endpoint = settings.get('SMARTPROXY_ENDPOINT')
        self.ports = dict()
        self.resperr_counter = dict()

    def process_request(self, request, spider):
        key, prod_id, url, page = self.gen_key_prodid_url_page(request)
        self.update_key(request, key)
        if self.retry_counter[key] > 20:
            print('More than 20 calls made. Maybe captcha or other blocking efforts from the domain')
            raise IgnoreRequest
        self.change_ip(prod_id)
        headers = request.headers
        headers['User-Agent'] = useragent.get_user_agent()
        request.headers = headers
        print("PORT: ",self.get_ip(prod_id))
        user_credentials = '{user}:{passw}'.format(user=self.user, passw=self.password)
        basic_authentication = 'Basic ' + base64.b64encode(user_credentials.encode()).decode()
        host = 'http://{endpoint}:{port}'.format(endpoint=self.endpoint, port=self.get_ip(prod_id))
        request.meta['proxy'] = host
        request.headers['Proxy-Authorization'] = basic_authentication

    # def process_response(self, request, response, spider):
    #     url = str(request.url)
    #     if 'amazon' in urlparse(request.url).netloc and response.status == 200:
    #         if 'html' not in response.text or 'id="captchacharacters"' in response.text:
    #             if 'html' not in response.text:
    #                 print("Html not found in response", url)
    #             if 'id="captchacharacters"' in response.text:
    #                 print('Captcha found in response', url)
    #             if url not in self.resperr_counter.keys():
    #                 self.resperr_counter[url] = 1
    #             else:
    #                 self.resperr_counter[url] += 1
    #             print("Captcha/html dog counter", url, self.resperr_counter[url])
    #             sleep_time = baseproxy.fib(self.resperr_counter[url])
    #             print("Sleeping for captcha/html dog seconds ", str(sleep_time))
    #             if sleep_time > 100:
    #                 print('raising exception',url)
    #                 request.meta['captcha_sleep'] = sleep_time
    #                 raise IgnoreRequest
    #             else:
    #                 time.sleep(sleep_time)
    #             print('returning request',url)
    #             return request
    #     return response

    def change_ip(self,prod_id):
        self.ports[prod_id] = random.randint(20001, 37960)
        return self.ports[prod_id]

    def get_ip(self,prod_id):
        if prod_id not in self.ports:
            self.ports[prod_id] = random.randint(20001,37960)
        return self.ports[prod_id]


