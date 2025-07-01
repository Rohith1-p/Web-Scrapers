import base64
import random
from urllib.parse import urlparse
import time
from scrapy.exceptions import IgnoreRequest
from . import useragent
from . import proxy
import time
from scrapy.conf import settings
import traceback

from . import luminati
from proxycrawl.proxycrawl_api import ProxyCrawlAPI

from .spiders.lymphomahub import LymphomahubSpider
from .spiders.oncnet import OncnetSpider
from .spiders.newpharma import NewpharmaSpider
from .spiders.shopee_vn_products import ShopeeVnSpider
from .spiders.tiki_products import TikiProductsSpider
from .spiders.tiki_vn_category import TikiVnSpider
from .spiders.tokopedia_products import TokopediaProductsSpider
from .spiders.lazada_product import LazadaProductsSpider
from .spiders.shopee_id_product import ShopeeIDSpider
from .spiders.walmart import WalmartSpider
from .spiders.amazon_category import AmazonCategorySpider
from .spiders.shopee_ph_product import ShopeePHSpider
from urllib.request import urlopen, HTTPError, Request
from urllib.parse import urlencode, quote_plus
from confluent_kafka import Producer
import os
import sys
import configparser
from .produce_monitor_logs import KafkaMonitorLogs

sys.path.append("/mfi_scrapers/setuserv_scrapy/setuserv_scrapy")
from utils import kafkaProducer
from utils import MFILogs
from utils import payment_gateway_api

class SetuProxyManager(proxy.baseproxymiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        super().__init__(settings)
        self.proxyMiddleware = ProxyMiddleware(settings)
        self.proxyCrawler = ProxyCrawlerMiddleware(settings)
        self.jsonCrawler = ProxyCrawlerJSONMiddleware(settings)
        self.noproxy = noproxy_middleware(settings)
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.setuserv/kafka.ini'))
        kafka_config = config['Kafka']
        kafka_server = kafka_config['KAFKA_SERVER']
        self.producer = Producer({'bootstrap.servers': kafka_server})
        self.monitor_log_flag = "False"


    def process_request(self, request, spider):
         #get_email_id
        print("inside process_request method at line 60")
        response_dict = payment_gateway_api().get_email_id(spider.client_id)
        print("response_dict:",response_dict)
        email_id = response_dict
        print("request meta is ", request.meta, request.url)
        if int(payment_gateway_api().get_quota(email_id)) > 0:
            print("Quota Greater than 0")
            pass

        else:
            try:
                status = "Quota Exhausted"
                _meta = request.meta
                print("requst.meta is: ", request.meta)
                type = _meta['media_entity']['type_media']
                url = _meta['media_entity']['url']
                client_id = _meta['media_entity']['client_id']
                date_remove = str(_meta['media_entity']['start_date'])
                date_remove = date_remove[:11] + date_remove[12:]
                page_no = _meta['page']
            except Exception as msg:
                page_no = "-"
                print(msg)

            mfi_logs = MFILogs()
            print("~~~~~~~~~MFI LOGS~~~~~~~~",status,type,url,date_remove,page_no,spider.gsheet_id)
            mfi_logs.scraper_logs(type, url, date_remove, page_no, spider.gsheet_id, status, client_id)

            raise IgnoreRequest

        if self.monitor_log_flag == 'False':
            monitor_log = 'Since User has Quota Successfully Sending Request'
            monitor_log = {"G_Sheet_ID": spider.gsheet_id, "Client_ID": spider.client_id,
                            "Message": monitor_log}
            KafkaMonitorLogs.push_monitor_logs(monitor_log)
            self.monitor_log_flag = 'True'

        key, prod_id, url, page = self.gen_key_prodid_url_page(request)
        self.update_key(request, key)
        if "api.proxycrawl.com" in request.url:
            no_of_attempts = self.retry_counter[key]
            print("is no.of attempts greater than 5 i.e 6: ", no_of_attempts)
            if no_of_attempts > spider.settings.get('RETRY_TIMES') + 1:
                mfi_logs = MFILogs()
                status = "Fail"
                _meta = request.meta
                print("requst.meta is: ", request.meta)
                type = _meta['media_entity']['type_media']
                url = _meta['media_entity']['url']
                date_remove = str(_meta['media_entity']['start_date'])
                date_remove = date_remove[:11] + date_remove[12:]
                try:
                    page_no = _meta['page']
                except:
                    page_no = "-"
                client_id = _meta['media_entity']['client_id']
                # mfi_logs = MFILogs()
                # mfi_logs.scraper_logs(review_type, product_url, date_remove, str(page_no), gsheet_id, status,self.client_id)
                print("~~~~~~~~~MFI LOGS~~~~~~~~ at 113",status,type,url,date_remove,page_no,spider.gsheet_id, client_id)
                mfi_logs.scraper_logs(type, url, date_remove, page_no, spider.gsheet_id, status, client_id)
                spider.logger.info('More than 5 calls made. Maybe captcha or other blocking efforts from the domain')
                raise IgnoreRequest
            return

        retry_count = spider.settings.get('RETRY_TIMES')
        spider_name = spider.name
        json_list = [LymphomahubSpider.name, OncnetSpider.name, NewpharmaSpider.name]
        proxy_crawl_spiders_list = [TokopediaProductsSpider.name, TikiProductsSpider.name,TikiVnSpider.name,
                                    ShopeeVnSpider.name, ShopeeIDSpider.name,
                                    WalmartSpider.name, ShopeePHSpider.name]

        if spider_name in proxy_crawl_spiders_list:
            print("inside 130 line")
            request.meta['middleware'] = 'proxyCrawler'
            return self.proxyCrawler.process_request(request, spider)

        if spider_name in json_list:
            print("inside 135 line")
            request.meta['middleware'] = 'proxyJSONCrawler'
            return self.jsonCrawler.process_request(request, spider)

        elif (spider_name in ["amazon-category-scraper"] and self.retry_counter[key]<=retry_count-2) or (spider_name in ["apotal-product-reviews","medikamente-product-reviews"]):
            print("sending process request for amazon-category-scraper to no_proxy", spider_name)
            request.meta['middleware'] = 'noproxy'
            return self.noproxy.process_request(request, spider)

        elif self.retry_counter[key] == retry_count or self.retry_counter[key] == retry_count - 1:
            print("inside 145 line")
            request.meta['middleware'] = 'proxyCrawler'
            return self.proxyCrawler.process_request(request, spider)
        else:
            print("inside else at 146 line in setuproxy file")
            request.meta['middleware'] = 'proxyMiddleware'
            return self.proxyMiddleware.process_request(request, spider)

    def process_response(self, request, response, spider):
        print("process_response ### at line 150")
        spider_name = spider.name
        _url = str(request.url)

        _meta = request.meta

        _prodid = _meta['media_entity']['id']
        original_url = _meta['media_entity']['url']
        start_date = _meta['media_entity']['start_date']
        scraper_type = _meta['media_entity']['type_media']
        media_entity = _meta['media_entity']
        client_id = _meta['media_entity']['client_id']
        try:
            page_no = _meta['page']
        except:
            page_no = "-"#"page number are not present"

        if 'gsheet_id' in media_entity:
            gsheet_id = _meta['media_entity']['gsheet_id']
        else:
            gsheet_id = ''
        status_code = response.status
        print("status_code at 189 line***: ", status_code)
        status = "Fail"
        date_remove = str(start_date)
        date_remove = date_remove[:11] + date_remove[12:]
        if str(status_code) != '200':
            mfi_logs = MFILogs()
            mfi_logs.scraper_logs(scraper_type, original_url, date_remove, str(page_no), gsheet_id, status, client_id)
        print("request.meta['middleware']: ", request.meta['middleware'])
        if request.meta['middleware'] == 'proxyCrawler':
            return self.proxyCrawler.process_response(request, response, spider)
        elif request.meta['middleware']=='noproxy':
            return self.noproxy.process_response(request, response, spider)
        elif request.meta['middleware'] == 'proxyMiddleware':
            return self.proxyMiddleware.process_response(request, response, spider)
        elif request.meta['middleware'] == 'proxyJSONCrawler':
            return self.jsonCrawler.process_response(request, response, spider)
        else:
            print("inside else at 192 line")
            spider.logger.error("No middleware in meta to process response")


class ProxyCrawlerMiddleware(proxy.baseproxymiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        super().__init__(settings)
        self.api = ProxyCrawlAPI({'token': 'W0l3970KmjkV4iSMEhPMcA'})

    def process_request(self, request, spider):
        spider.stats.info(f"SPIDER_SOURCE:{spider.source}")
        if "api.proxycrawl.com" in request.url: return
        key, prod_id, url, page = self.gen_key_prodid_url_page(request)
        proxycrawl_url = self.api.buildURL(url, {})

        if request.method == 'POST':
            print("Getting into Proxy Crawl post method")
            url = quote_plus(url)
            proxycrawl_url = 'https://api.proxycrawl.com/?token=W0l3970KmjkV4iSMEhPMcA&' \
                             'post_content_type=application%2Fjson%3Bcharset%3DUTF-8&url=' + url

        print("request.method == 'POST': ", request.method)
        spider.stats.info(f"In process_request MIDDLEWARE USED: Proxycrawl FOR:{spider.source}")
        request = request.replace(url=proxycrawl_url)
        return request

    def process_response(self, request, response, spider):
        try:
            print("inside process_response of proxycrawl at line 231")
            self.dump(response, 'html', 'proxy_crawl_in_setuproxy')
            spider.stats.info(f"SPIDER_SOURCE:{spider.source}")
            new_response = response
            # if 'amazon' in spider.source:
            status = response.status
            _url = str(request.url)
            _meta = request.meta
            _prodid = _meta['media_entity']['id']
            original_url = _meta['media_entity']['url']
            new_response = response.replace(url=original_url)
            user_agent = request.headers["User-Agent"].decode("utf-8")
            spider.stats.info(f"In process_response MIDDLEWARE USED: Proxycrawl FOR:{spider.source}")
            spider.stats.info(f"RESPONSE-TRACKER:,{_prodid},{_url},{spider.client_id},{spider.source},"
                              f"{spider.start_date},{spider.end_date},{status},{spider.task_id},{_meta},{user_agent}")
        except:
            print("Exception: ", traceback.print_exc())
            self.dump(response, 'html', 'proxy_crawl_in_setuproxy')

        return new_response


class ProxyCrawlerJSONMiddleware(ProxyCrawlerMiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        super().__init__(settings)
        self.api = ProxyCrawlAPI({'token': 'ochC5-7loAP3cmOsciDLyQ'})


class ProxyMiddleware(proxy.baseproxymiddleware):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        super().__init__(settings)
        self.proxy_configs = [LuminatiproxyConfig(settings), LuminatiproxyConfig2(settings),
                              LuminatiproxyConfig_Countrywise(settings)]
        self.resperr_counter = dict()
        self.list_indices = dict()
        self.sel_proxy_config = dict()

    def process_request(self, request, spider):
        print("inside process_request of proxymiddleware class at line 266 $$$")
        spider.stats.info(f"SPIDER_SOURCE:{spider.source}")
        # if 'amazon' not in spider.source:
        key, prod_id, url, page = self.gen_key_prodid_url_page(request)
        self.update_key(request, key)
        headers = request.headers
        if spider.source != 'hepsiburada':
            headers['User-Agent'] = useragent.get_user_agent()
        request.headers = headers
        success = self.get_proxy_config(request, key, spider)
        print("success: ", success)
        if not success:
            print("inside if not success true at 272 line")
            try:
                status = "Fail"
                _meta = request.meta
                print("requst.meta is: ", request.meta)
                type = _meta['media_entity']['type_media']
                url = _meta['media_entity']['url']
                date_remove = str(_meta['media_entity']['start_date'])
                date_remove = date_remove[:11] + date_remove[12:]
                page_no = _meta['page']
                client_id = _meta['media_entity']['client_id']
            except Exception as msg:
                page_no = "-"
                print(msg)

            mfi_logs = MFILogs()
            print("~~~~~~~~~MFI LOGS~~~~~~~~",status,type,url,date_remove,page_no,spider.gsheet_id)
            mfi_logs.scraper_logs(type, url, date_remove, page_no, spider.gsheet_id, status, client_id)
            raise IgnoreRequest
        proxy_config = self.proxy_configs[self.sel_proxy_config[key]]
        timestamp = time.time()

        if spider.source != 'hepsiburada':
            port = proxy_config.get_port(prod_id, url)
            user = proxy_config.get_user()
            password = proxy_config.get_password()
            endpoint = proxy_config.get_endpoint()
            if proxy_config.get_name() in ["Luminatiproxy_zone_dc2", "Luminatiproxy_static",
                                           "Luminatiproxy_countrywise"]:
                header_agent = useragent.get_extra_user_agents()
            else:
                header_agent = useragent.get_user_agent()
            request.headers['User-Agent'] = header_agent
            header_agent = "-" + header_agent
            spider.stats.info(
                f'PROXY-TRACKER:\t{endpoint}\t{str(port) + "-" + proxy_config.get_name()}\t{prod_id}\t{url}\t{page}\t{timestamp}\t{header_agent}')
            spider.logger.info("ENDPOINT-PORT: " + endpoint + "-" + str(port))
            user_credentials = '{user}:{passw}'.format(user=user, passw=password)
            basic_authentication = 'Basic ' + base64.b64encode(user_credentials.encode()).decode()
            host = 'http://{endpoint}:{port}'.format(endpoint=endpoint, port=port)
            request.meta['proxy'] = host
            request.meta['proxy_name'] = proxy_config.get_name()
            request.headers['Proxy-Authorization'] = basic_authentication
        else:
            print('Dont Proxies comes here')
            endpoint = "0.0.0.0"
            port = ":0000"
            spider.stats.info(f'PROXY-TRACKER:\t{endpoint}\t{port}\t{prod_id}\t{url}\t{page}\t{timestamp}')
            spider.logger.info("ENDPOINT-PORT: " + endpoint + "-" + str(port))
        spider.stats.info(f"In process_request MIDDLEWARE USED: ProxyMiddleware FOR:{spider.source}")

    def get_proxy_config(self, request, key, spider):
        no_of_attempts = self.retry_counter[key]
        if no_of_attempts > spider.settings.get('RETRY_TIMES') + 1:
            print("inside if at line 308")
            mfi_logs = MFILogs()
            status = "Fail"
            _meta = request.meta
            print("requst.meta at 326 line is: ", request.meta)
            type = _meta['media_entity']['type_media']
            url = _meta['media_entity']['url']
            date_remove = str(_meta['media_entity']['start_date'])
            date_remove = date_remove[:11] + date_remove[12:]
            try:
                page_no = _meta['page']
            except:
                page_no = "-"
            client_id = _meta['media_entity']['client_id']
            # mfi_logs = MFILogs()
            # mfi_logs.scraper_logs(review_type, product_url, date_remove, str(page_no), gsheet_id, status,self.client_id)
            print("~~~~~~~~~MFI LOGS~~~~~~~~ at 316 line:",status,type,url,date_remove,page_no,spider.gsheet_id, client_id)
            mfi_logs.scraper_logs(type, url, date_remove, page_no, spider.gsheet_id, status, client_id)
            spider.logger.info('More than 5 calls made. Maybe captcha or other blocking efforts from the domain')
            return False
        if key not in self.list_indices:
            self.list_indices[key] = list(range(len(self.proxy_configs)))
        if key not in self.sel_proxy_config:
            sel_proxy_config = random.choice(self.list_indices[key])
        elif (no_of_attempts - 1) % 2 == 0:
            self.list_indices[key].remove(self.sel_proxy_config[key])
            if not self.list_indices[key]:
                self.list_indices[key] = list(range(len(self.proxy_configs)))
            sel_proxy_config = random.choice(self.list_indices[key])
        else:
            sel_proxy_config = self.sel_proxy_config[key]
        self.sel_proxy_config[key] = sel_proxy_config
        return True

    def process_response(self, request, response, spider):
        spider.stats.info(f"SPIDER_SOURCE:{spider.source}")
        # if 'amazon' not in spider.source:
        status = response.status
        _url = str(request.url)
        _meta = request.meta
        _prodid = _meta['media_entity']['id']
        user_agent = request.headers["User-Agent"].decode("utf-8")
        spider.stats.info(f"RESPONSE-TRACKER:,{_prodid},{_url},{spider.client_id},{spider.source},"
                          f"{spider.start_date},{spider.end_date},{status},{spider.task_id},{_meta},{user_agent}")
        spider.stats.info(f"In process_response MIDDLEWARE USED: ProxyMiddleware FOR:{spider.source}")
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
        return response


class noproxy_middleware(proxy.baseproxymiddleware):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    #def __init__(self, settings):
        #super().__init__(settings)

    # @classmethod
    # def from_crawler(cls, crawler):
    #     # This method is used by Scrapy to create your spiders.
    #     s = cls()
    #     crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
    #     return s
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        super().__init__(settings)

    def process_request(self, request, spider):
        print("****************************************************************** scraping without proxy for",str(spider.name))
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        print("****************************************************************** getting response without proxy for",str(spider.name))
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

class BaseProxyConfig():

    def __init__(self, settings):
        self.name = "Not Implemented"
        self.user = "Not Implemented"
        self.password = "Not Implemented"
        self.endpoint = "Not Implemented"
        self.port = "Not Implemented"
        self.last_call = time.time()
        self.wait_thresh = 2

    def get_port(self, prod_id, url):
        self.check_wait_time()
        return self.port

    def check_wait_time(self, ):
        cur_time = time.time()
        waited_time = cur_time - self.last_call
        wait_time = self.wait_thresh - waited_time
        if wait_time <= 0:
            return 0
        else:
            time.sleep(wait_time)
            print("waited for proxy", self, str(wait_time))
            return wait_time

    def get_endpoint(self, ):
        return self.endpoint

    def get_user(self, ):
        return self.user

    def get_password(self, ):
        return self.password

    def get_name(self, ):
        return self.name


class SmartproxyConfig(BaseProxyConfig):

    def __init__(self, settings):
        self.name = "Smartproxy"
        self.user = settings.get('SMARTPROXY_USER')
        self.password = settings.get('SMARTPROXY_PASSWORD')
        self.endpoint = settings.get('SMARTPROXY_ENDPOINT')
        self.port = settings.get('SMARTPROXY_PORT')
        # self.prodidurl = dict()
        self.last_call = time.time()
        self.wait_thresh = 0.001

    # def change_port(self,prod_id):
    #     self.ports[prod_id] = random.randint(20001, 37960)

    # def get_port(self,prod_id,url):
    #     key_prodid_url = (prod_id,url)
    #     if prod_id not in self.ports:
    #         self.change_port(prod_id)
    #         self.prodidurl[key_prodid_url] = 1
    #     else:
    #         if key_prodid_url not in self.prodidurl:
    #             self.prodidurl[key_prodid_url] = 1
    #         else:
    #             self.change_port
    #             self.prodidurl[key_prodid_url] += 1
    #     return self.ports[prod_id]


class LuminatiproxyConfig(BaseProxyConfig):

    def __init__(self, settings):
        self.name = "Luminatiproxy_static"
        self.user = settings.get('LUMINATI_USER')
        self.password = settings.get('LUMINATI_PASSWORD')
        self.endpoint = settings.get('LUMINATI_ENDPOINT')
        self.port = str(settings.get('LUMINATI_PORT'))
        self.last_call = time.time()
        self.wait_thresh = 0.04


class LuminatiproxyConfig2(BaseProxyConfig):

    def __init__(self, settings):
        self.name = "Luminatiproxy_zone_dc2"
        self.user = settings.get('LUMINATI_USER_2')
        self.password = settings.get('LUMINATI_PASSWORD_2')
        self.endpoint = settings.get('LUMINATI_ENDPOINT')
        self.port = str(settings.get('LUMINATI_PORT'))
        self.last_call = time.time()
        self.wait_thresh = 0.04


class LuminatiproxyConfig_Countrywise(BaseProxyConfig):

    def __init__(self, settings):
        self.name = "Luminatiproxy_countrywise"
        self.user = luminati.get_luminati_countrtwise_userlist()
        self.password = "99ctgshg7tz1"
        self.endpoint = settings.get('LUMINATI_ENDPOINT')
        self.port = str(settings.get('LUMINATI_PORT'))
        self.last_call = time.time()
        self.wait_thresh = 0.04
