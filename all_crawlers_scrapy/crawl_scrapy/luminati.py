import base64
import random
from urllib.parse import urlparse
import time
from scrapy.exceptions import IgnoreRequest
from . import useragent
from . import proxy 

class ProxyMiddleware(proxy.baseproxymiddleware):

    def __init__(self, settings):
        super().__init__(settings)
        self.user = settings.get('LUMINATI_USER')
        self.password = settings.get('LUMINATI_PASSWORD')
        self.endpoint = settings.get('LUMINATI_ENDPOINT')
        self.port = str(settings.get('LUMINATI_PORT'))
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
        user_credentials = '{user}:{passw}'.format(user=self.user, passw=self.password)
        basic_authentication = 'Basic ' + base64.b64encode(user_credentials.encode()).decode()
        host = 'http://{endpoint}:{port}'.format(endpoint=self.endpoint, port=self.port)
        request.meta['proxy'] = host
        request.headers['Proxy-Authorization'] = basic_authentication

luminati_countrtwise_userlist = [
        'lum-customer-setuservin-zone-zone_dc_2-country-us',
        'lum-customer-setuservin-zone-zone_dc_2-country-gb',
        'lum-customer-setuservin-zone-zone_dc_2-country-al',
        'lum-customer-setuservin-zone-zone_dc_2-country-ar',
        'lum-customer-setuservin-zone-zone_dc_2-country-am',
        'lum-customer-setuservin-zone-zone_dc_2-country-au',
        'lum-customer-setuservin-zone-zone_dc_2-country-at',
        'lum-customer-setuservin-zone-zone_dc_2-country-az',
        'lum-customer-setuservin-zone-zone_dc_2-country-bd',
        'lum-customer-setuservin-zone-zone_dc_2-country-by',
        'lum-customer-setuservin-zone-zone_dc_2-country-be',
        'lum-customer-setuservin-zone-zone_dc_2-country-bo',
        'lum-customer-setuservin-zone-zone_dc_2-country-br',
        'lum-customer-setuservin-zone-zone_dc_2-country-bg',
        'lum-customer-setuservin-zone-zone_dc_2-country-kh',
        'lum-customer-setuservin-zone-zone_dc_2-country-ca',
        'lum-customer-setuservin-zone-zone_dc_2-country-cl',
        'lum-customer-setuservin-zone-zone_dc_2-country-cn',
        'lum-customer-setuservin-zone-zone_dc_2-country-co',
        'lum-customer-setuservin-zone-zone_dc_2-country-cr',
        'lum-customer-setuservin-zone-zone_dc_2-country-hr',
        'lum-customer-setuservin-zone-zone_dc_2-country-cy',
        'lum-customer-setuservin-zone-zone_dc_2-country-cz',
        'lum-customer-setuservin-zone-zone_dc_2-country-dk',
        'lum-customer-setuservin-zone-zone_dc_2-country-do',
        'lum-customer-setuservin-zone-zone_dc_2-country-ec',
        'lum-customer-setuservin-zone-zone_dc_2-country-eg',
        'lum-customer-setuservin-zone-zone_dc_2-country-ee',
        'lum-customer-setuservin-zone-zone_dc_2-country-fi',
        'lum-customer-setuservin-zone-zone_dc_2-country-fr',
        'lum-customer-setuservin-zone-zone_dc_2-country-ge',
        'lum-customer-setuservin-zone-zone_dc_2-country-de',
        'lum-customer-setuservin-zone-zone_dc_2-country-gr',
        'lum-customer-setuservin-zone-zone_dc_2-country-gt',
        'lum-customer-setuservin-zone-zone_dc_2-country-hk',
        'lum-customer-setuservin-zone-zone_dc_2-country-hu',
        'lum-customer-setuservin-zone-zone_dc_2-country-is',
        'lum-customer-setuservin-zone-zone_dc_2-country-in',
        'lum-customer-setuservin-zone-zone_dc_2-country-id',
        'lum-customer-setuservin-zone-zone_dc_2-country-ie',
        'lum-customer-setuservin-zone-zone_dc_2-country-im',
        'lum-customer-setuservin-zone-zone_dc_2-country-il',
        'lum-customer-setuservin-zone-zone_dc_2-country-it',
        'lum-customer-setuservin-zone-zone_dc_2-country-jm',
        'lum-customer-setuservin-zone-zone_dc_2-country-jp',
        'lum-customer-setuservin-zone-zone_dc_2-country-jo',
        'lum-customer-setuservin-zone-zone_dc_2-country-kz',
        'lum-customer-setuservin-zone-zone_dc_2-country-kg',
        'lum-customer-setuservin-zone-zone_dc_2-country-la',
        'lum-customer-setuservin-zone-zone_dc_2-country-lv',
        'lum-customer-setuservin-zone-zone_dc_2-country-lt',
        'lum-customer-setuservin-zone-zone_dc_2-country-lu',
        'lum-customer-setuservin-zone-zone_dc_2-country-my',
        'lum-customer-setuservin-zone-zone_dc_2-country-mx',
        'lum-customer-setuservin-zone-zone_dc_2-country-md',
        'lum-customer-setuservin-zone-zone_dc_2-country-nl',
        'lum-customer-setuservin-zone-zone_dc_2-country-nz',
        'lum-customer-setuservin-zone-zone_dc_2-country-no',
        'lum-customer-setuservin-zone-zone_dc_2-country-pe',
        'lum-customer-setuservin-zone-zone_dc_2-country-ph',
        'lum-customer-setuservin-zone-zone_dc_2-country-ru',
        'lum-customer-setuservin-zone-zone_dc_2-country-sa',
        'lum-customer-setuservin-zone-zone_dc_2-country-sg',
        'lum-customer-setuservin-zone-zone_dc_2-country-kr',
        'lum-customer-setuservin-zone-zone_dc_2-country-es',
        'lum-customer-setuservin-zone-zone_dc_2-country-lk',
        'lum-customer-setuservin-zone-zone_dc_2-country-se',
        'lum-customer-setuservin-zone-zone_dc_2-country-ch',
        'lum-customer-setuservin-zone-zone_dc_2-country-tw',
        'lum-customer-setuservin-zone-zone_dc_2-country-tj',
        'lum-customer-setuservin-zone-zone_dc_2-country-th',
        'lum-customer-setuservin-zone-zone_dc_2-country-tr',
        'lum-customer-setuservin-zone-zone_dc_2-country-tm',
        'lum-customer-setuservin-zone-zone_dc_2-country-ua',
        'lum-customer-setuservin-zone-zone_dc_2-country-ae',
        'lum-customer-setuservin-zone-zone_dc_2-country-uz',
        'lum-customer-setuservin-zone-zone_dc_2-country-vn',
]

def get_luminati_countrtwise_userlist():
        ua = random.choice(luminati_countrtwise_userlist)
        return ua

