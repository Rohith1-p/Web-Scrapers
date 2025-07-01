# -*- coding: utf-8 -*-
import base64
import random
from urllib.parse import urlparse
import time
from scrapy.exceptions import IgnoreRequest
from scrapy.utils.gz import gunzip
from . import useragent
from . import proxy
import time
from . import settings
from . import useragent


class MonitorMiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.settings = settings
    #     self.proxy_configs = [SmartproxyConfig(settings), LuminatiproxyConfig(settings)]
    #     self.resperr_counter = dict()
    #     self.list_indices = dict()
    #     self.sel_proxy_config = dict()

    def process_request(self, request, spider):
        _url = str(request.url)
        _meta = request.meta
        _prodid = _meta['media_entity']['id']
        user_agent = request.headers["User-Agent"].decode("utf-8")
        spider.stats.info(f"FINAL-REQUEST-TRACKER:,{_prodid},{_url},{spider.client_id},{spider.source},"
                         f"{spider.start_date},{spider.end_date},{spider.task_id},{_meta},{user_agent}")
        
    def process_response(self, request, response, spider):
        status = response.status
        captcha = False
        if status == 200 and spider.source == "amazon":
            try:
                body = gunzip(response.body)
                if 'id="captchacharacters"' in str(body) or 'html' not in str(body):
                    captcha = True
                if 'Try clearing or changing some filters' in str(body):
                    captcha = 'Try clearing or changing some filters'
                if "please try again later" in response.text:
                    captcha = 'please try again later'
            except:
                error_ = "error_while_getting_response_body" + str(request.url) + str(request.meta['media_entity']['id']) + str(spider.client_id) + str(spider.task_id)
                spider.logger.info(error_)
        _url = str(request.url)
        _meta = request.meta
        _prodid = _meta['media_entity']['id']
        user_agent = request.headers["User-Agent"].decode("utf-8")
        spider.stats.info(f"FINAL-RESPONSE-TRACKER:,{_prodid},{_url},{spider.client_id},{spider.source},"
                          f"{spider.start_date},{spider.end_date},{status},{spider.task_id},{_meta},{user_agent},{'--' + str(captcha)}")
        return response
