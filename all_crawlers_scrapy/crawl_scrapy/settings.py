# -*- coding: utf-8 -*-
import configparser
import os
import random

# Scrapy settings for setuserv_scrapy project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'setuserv_scrapy'
SPIDER_MODULES = ['setuserv_scrapy.spiders']
NEWSPIDER_MODULE = 'setuserv_scrapy.spiders'
SPLASH_URL = 'http://localhost:8050'
DUPEFILTER_CLASS = 'scrapy_splash.SplashAwareDupeFilter'

CONCURRENT_REQUESTS = 100
CONCURRENT_REQUESTS_PER_DOMAIN = 50
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_DEBUG = True
DOWNLOAD_DELAY = 0
AUTOTHROTTLE_START_DELAY = 0
AUTOTHROTTLE_MAX_DELAY = 5
AUTOTHROTTLE_TARGET_CONCURRENCY = 50

DOWNLOAD_TIMEOUT = 120
PERIOD = 14
FEED_EXPORT_ENCODING = 'utf-8'

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
}

ITEM_PIPELINES = {
    'setuserv_scrapy.pipelines.MongoPipeline': 300,
    'setuserv_scrapy.elc_pipelines.MongoELCPipeline': 600,
    'setuserv_scrapy.databricks_pipelines.MongoDatabricksPipeline': 900,
    # 'setuserv_scrapy.pubmed_ct_pipelines.PubmedCTPipeline': 1000,
}

RETRY_TIMES = 4
RETRY_ENABLED = True
# Retry on most error codes since proxies fail for different reasons
RETRY_HTTP_CODES = [500, 502, 503, 504, 509, 511, 520, 400, 403, 404, 408, 444, 429, 302, 307, 456]
ROBOTSTXT_OBEY = False

#proxy_middleware = 'setuserv_scrapy.stormproxy.ProxyMiddleware'
#proxy_middleware = 'setuserv_scrapy.smartproxy_auth.ProxyMiddleware'
#proxy_middleware = 'setuserv_scrapy.luminati.ProxyMiddleware'
proxy_middleware_2 = 'setuserv_scrapy.setuproxy.ProxyMiddleware'
proxy_middleware_1 = 'setuserv_scrapy.setuproxy.ProxyCrawlerMiddleware'
proxy_middleware = 'setuserv_scrapy.setuproxy.SetuProxyManager'
monitor_middleware = 'setuserv_scrapy.httpmonitor.MonitorMiddleware'

DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    proxy_middleware: 100,
    monitor_middleware: 1000,
}

SPIDER_MIDDLEWARES = {
    'scrapy.contrib.spidermiddleware.referer.RefererMiddleware': True,
}

COMMANDS_MODULE = 'setuserv_scrapy.commands'

# Lets access per system constant also
config = configparser.ConfigParser()
config.read(os.path.expanduser('~/.setuserv/scraping_scrapy_app.ini'))
scraping_config = config['Scraping']
WEBHOSE_TOKEN = scraping_config['WEBHOSE_TOKEN']
BROKER_URL = scraping_config['CELERY_BROKER']
LOG_DIR = scraping_config['LOG_DIR']
PARSE_LOG_DIR = scraping_config['PARSE_LOG_DIR']
CC_EMAIL = scraping_config['CC_EMAIL']
REPORTING_URL = 'https://act.mineforinsights.com/api'

config = configparser.ConfigParser()
config.read(os.path.expanduser('~/.setuserv/kafka.ini'))
kafka_config = config['Kafka']
KAFKA_CLUSTER = kafka_config['KAFKA_SERVER']

config_django = configparser.ConfigParser()
config_django.read(os.path.expanduser('~/.setuserv/scraping_django_app.ini'))
django_app_config = config_django['Scraping Django App']
SCRAPY_SERVER = django_app_config['SCRAPY_SERVER']

SMARTPROXY_USER = "sp36b98541"
SMARTPROXY_PASSWORD = "AlphaBeta108"
SMARTPROXY_ENDPOINT = "gate.dc.smartproxy.com"
SMARTPROXY_PORT = 20000 #random.randint(20001, 37960)

STORMPROXY_ENDPOINT = "51.159.2.110"#"207.180.216.144"
STORMPROXY_PORT = 19003

LUMINATI_ENDPOINT = "zproxy.lum-superproxy.io"
LUMINATI_PORT = 22225
LUMINATI_USER = "lum-customer-c_e39c3bc0-zone-static"
LUMINATI_PASSWORD = "9wq61jykqtcv"

LUMINATI_USER_2 = "lum-customer-c_e39c3bc0-zone-zone_dc_2"
LUMINATI_PASSWORD_2 = "99ctgshg7tz1"

CRAWLERA_ENABLED = False
