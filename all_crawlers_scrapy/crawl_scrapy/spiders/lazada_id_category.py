import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
import scrapy
from scrapy.http import FormRequest
from .setuserv_spider import SetuservSpider
from urllib.parse import urlsplit


class LazadaCategoryIdSpider(SetuservSpider):
    name = 'lazada-category_id-scraper'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Lazada Products Scraping starts")
        assert self.source == 'lazada_category'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            country_code = urlsplit(product_url).netloc[-2:]
            media_entity = {'url': product_url, 'id': product_id, 'country_code': country_code}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            # yield scrapy.Request(url="https://www.lazada.vn/sua-cho-tre-tu-1-3-tuoi/?page=2",
            #                      callback=self.parse_category,
            #                      errback=self.err, dont_filter=True,
            #                      meta={'media_entity': media_entity, 'page': page})
            path =  self.get_category_url(product_url,page, country_code)

            yield scrapy.Request(url=self.get_category_url(product_url, page, country_code),
                                 callback=self.parse_category,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url, path),
                                 meta={'media_entity': media_entity,'page': page})

    def parse_category(self, response):
        print("*************************")
        #print(response.text)
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        product_url = media_entity["url"]
        country_code = media_entity["country_code"]
        page = response.meta["page"]
        #res = json.loads(response.text)

        if 'listItems' not in response.text:
            print('failllllllllllllllll')
            print("failed response", response.text)
            path =  self.get_category_url(product_url,page, country_code)
            yield scrapy.Request(url=self.get_category_url(product_url,page, country_code),
                                 callback=self.parse_category,
                                 errback=self.err, dont_filter=True,
                                 headers=self.get_headers(product_url, path),
                                 meta={'media_entity': media_entity,'page': page})

        # elif
        else:
            print("##############passsssssssssssss")
            print(response.text)
            res = json.loads(response.text)

            if res['mods']['listItems'] and (len(res['mods']['listItems'])>0):
                for item in res['mods']['listItems']:
                    media = {
                        "client_id": str(self.client_id),
                        "media_source": str(self.source),
                        "category_url": product_url,
                        "product_url": "https:" + item["itemUrl"],
                        "media_entity_id": item['itemId'],
                        "type": "product_details",
                        "propagation": self.propagation,
                        "created_date": datetime.utcnow()
                    }
                    try:
                        actual_price = item['originalPriceShow']
                        discount_percentage = item['discount']
                        print("discount_percentage@@@@@@@@@")
                        print(type(discount_percentage))
                        discount_percentage = str(discount_percentage)
                        discount_percentage = discount_percentage.split("-")
                        discount_percentage= "-".join(discount_percentage[1:])
                    except:
                        actual_price = item['priceShow']
                        discount_percentage = '-'
                    try:
                        list_of_dicts = item['icons']
                        json_string = json.dumps(list_of_dicts)
                        if "lazMall" in json_string:
                            mall_type = "lazMall"
                        else:
                            mall_type = "No Mall"
                    except:
                        mall_type = "No Mall"

                    try:
                        units_sold = item['itemSoldCntShow']
                    except:
                        units_sold = '-'
                    if item['review'] == '':
                        review_count = 0
                    else:
                        review_count = item['review']

                    if item['inStock']:
                        product_availability = "In stock"
                    else:
                        product_availability = "Out of Stock"
                    
                    rating = item['ratingScore']
                    rating = round(float(rating), 1)

                    path =  self.get_category_url(product_url,page, country_code)
                    yield self.yield_category_details(category_url=media['category_url'],
                                                      product_url=media['product_url'],
                                                      product_id=media['media_entity_id'],
                                                      page_no = page,
                                                      product_name = item['name'],
                                                      product_availability= product_availability,
                                                      rating = rating,
                                                      actual_price = actual_price,
                                                      discount_percentage = discount_percentage,
                                                      discounted_price = item['priceShow'],
                                                      units_sold = units_sold,
                                                      brand_name = item['brandName'],
                                                      mall_type=mall_type,
                                                      seller_name=item['sellerName'],
                                                      seller_id= item['sellerId'],
                                                      review_count= review_count,
                                                      extra_info='')

                page += 1
                if page <= 200:
                    path =  self.get_category_url(product_url,page, country_code)
                    print("*********get_category_url", self.get_category_url(product_url,page, country_code))
                    yield scrapy.Request(url=self.get_category_url(product_url,page, country_code),
                                         callback=self.parse_category,
                                         errback=self.err, dont_filter=True,
                                         headers=self.get_headers(product_url, path),
                                         meta={'media_entity': media_entity,'page': page})
                    self.logger.info(f"{page} is going")

    @staticmethod
    def get_category_url(product_url,page, country_code):
        if country_code:
            cat_url = (product_url.split(country_code)[1:])
            cat_url = f"{country_code}".join(cat_url)
            keyword = cat_url.split("?")[0]
            print(keyword)
            extra_par = cat_url.split(keyword)[1]
            extra_param = extra_par.replace("?", "&")

            if country_code in {'my', 'vn', 'ph', 'id', 'sg', 'th'}:
                return {
                    'my': f"https://www.lazada.com.my{keyword}?ajax=true&page={page}{extra_param}",
                    'vn': f"https://www.lazada.vn{keyword}?ajax=true&page={page}{extra_param}",
                    'ph': f"https://www.lazada.com.ph{keyword}?ajax=true&page={page}{extra_param}",
                    'id': f"https://www.lazada.co.id{keyword}?ajax=true&page={page}{extra_param}",
                    'sg': f"https://www.lazada.sg{keyword}?ajax=true&page={page}{extra_param}",
                    'th': f"https://www.lazada.co.th{keyword}?ajax=true&page={page}{extra_param}"
                }[country_code]

    @staticmethod
    def get_headers(product_url, path):

        # :authority: www.lazada.vn
        # :method: GET
        # :scheme: https
        # accept: application/json, text/plain, */*
        # accept-encoding: gzip, deflate, br
        # accept-language: en-GB,en-US;q=0.9,en;q=0.8
        # cookie: __wpkreporterwid_=f1166693-64a2-49e2-bdeb-65ccc1cf8bab; t_fv=1662885270382; t_uid=dhgG15PndKOIM5nLFX4mahq19epqnDl1; cna=yrWgG3o2rHQCAbdSKTL8+8/s; lzd_cid=bb3d6e27-4da0-4ed2-92d7-8c47c32c44d2; _bl_uid=LzlCk7hnxnR210yaqiygs22bIhmj; lzd_sid=14e9055852a146503cec8f071efe63c2; _tb_token_=e535da605e5b3; dsa_category_disclaimer=true; _gcl_au=1.1.2005182689.1664023363; _fbp=fb.1.1665206357804.341655542; AMCVS_126E248D54200F960A4C98C6%40AdobeOrg=1; cto_axid=-lLgbLXkM7xlxMiwtQg_Z5cPyNqTXERR; pdp_sfo=1; cto_bundle=e8LCEV9XbDdQV3NZd1pTU0FkZ01MJTJGbDIlMkZzbG52OWFySUxHa1BJOXZNJTJGZ3Z3bmRrTnVHdjAxVlJoS0xmUHNhMWJVM2RiVlN6QkxDMFhlRVB6Nkg5WHNZVmE2Z3ZmajZrMENaR3Y4a2lraExWSFk1WVc2blRhV1JaY201Y3F4UFglMkJ2ZVRBbE02dGVMZmg4NmFYOUJYbXNKTzFvTjU5ek5XbGpuTkZaOUVTRHd3ZXFDNFN5REdhT3EwSlp3aUpoM0FaNmNUOA; hng=VN|vi|VND|704; hng.sig=EmlYr96z9MQGc5b9Jyf9txw1yLZDt_q0EWkckef954s; _clck=19atpmx|1|f5t|0; _ga=GA1.2.869824777.1666621722; _uetvid=c0dfa7b046c811ed85ad9fcfe4ab78b5; AMCV_126E248D54200F960A4C98C6%40AdobeOrg=-1124106680%7CMCIDTS%7C19294%7CMCMID%7C32348046939783283531894176258510408780%7CMCAAMLH-1667563168%7C12%7CMCAAMB-1667563168%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1666965568s%7CNONE%7CvVersion%7C5.2.0; t_sid=gkyGvyhs66Ni9YgU3mf9ZyRLyTrRHQ0W; utm_channel=NA; _m_h5_tk=f2cef426ca3db69a2785749b627a5b53_1667071428448; _m_h5_tk_enc=bc6d62f35eb89d17e055e09f7bebfdce; x5sec=7b22617365727665722d6c617a6164613b32223a223363623661366332643062353862623763366436363733343933306634323539434c2b35395a6f47454b4f42736f444238717965446a43633463657941304144227d; tfstk=cA3VBAxjKPDWlMic8zUZ8VqHMLuAaJunWTP_mdmk2p854MZULsfdB70QdurCaOqc.; l=eBNkLaCnTPYy9jrUBO5a-urza77T6IObz1FzaNbMiIncC6YFMPJMOhtQm2JYcLKRJWXcMrYB4mAqCk9TnFi0-PHjgpcp-Y4N1YMDBef..; isg=BBISxsjgpfAlD9nMkU3f7gUHY970Ixa9Fsb8q9xqRkVN77DpxLFKzF1JXlNT2o5V
        # referer: https://www.lazada.vn/sua-cho-tre-tu-1-3-tuoi/?page=3
        # user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36
        headers = {
            'authority': 'www.lazada.vn',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'referer': product_url,
            'path': path,
            # 'origin': 'www.' + product_url.split('/')[2],
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/72.0.3626.121 Safari/537.36',
            'cookie': '__wpkreporterwid_=2300142b-2ae6-41a2-2744-689f532e341b; lzd_cid=5a833a98-e93a-4962-bfcb-1ebb4d7e0260; t_uid=5a833a98-e93a-4962-bfcb-1ebb4d7e0260; t_fv=1662633930049; cna=yrWgG3o2rHQCAbdSKTL8+8/s; lzd_sid=1158141785e8efff73eab35d34bc3802; anon_uid=6bcd6d04fd81fb5eb371212148fd0750; _tb_token_=e31706bbe6e5; _bl_uid=tbl9X7Ups2UxXwbImbXpinzaFOaR; age_limit=18Y; _gcl_au=1.1.608638538.1664124298; hng=SG|en-SG|SGD|702; userLanguageML=en; pdp_sfo=1; AMCVS_126E248D54200F960A4C98C6%40AdobeOrg=1; _fbp=fb.1.1666093956887.1931664763; _ga=GA1.2.1163203179.1666154442; EGG_SESS=S_Gs1wHo9OvRHCMp98md7Ox8RitX_Plc8Uh8duuX-xPGhD3XIDxTYwIjSWFttPM9qqzlMY-xoEPBWtGgZsxFKIB6qFnC6ZxjYk84brfjTSS31XlSMh-Qfuvwxd1pq9agIN8EyYNygir_zOSab8fwLGIXN-KdOhxj0m9WOgKIw4M=; _gid=GA1.2.1236102813.1666248228; AMCV_126E248D54200F960A4C98C6%40AdobeOrg=-1124106680%7CMCIDTS%7C19286%7CMCMID%7C32348046939783283531894176258510408780%7CMCAAMLH-1666853031%7C12%7CMCAAMB-1666853031%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1666255431s%7CNONE%7CvVersion%7C5.2.0; _clck=1ri94hz|1|f5v|0; _m_h5_tk=f6d8ac9b638b157fba80baaa77b892b4_1666270984420; _m_h5_tk_enc=0477556ca7a55cca2cbc7361bc9ac180; t_sid=5WLHCPyFlkBleqvudIjOTk410JlsXiL5; utm_channel=NA; _uetsid=8ca1f860504211ed9de88f4a97d120c0; _uetvid=5b4dd0b04edb11ed8e9df36cedbc4d97; _clsk=1g8ut13|1666265961722|1|1|e.clarity.ms/collect; tfstk=cvJVBPcX-OYSKYd0LTBwU71mPTXAZeJHBzS1i2HPZRE_JiCciDUOqp_Ca6ZzSsf..; l=eBaC9UTuTNsTkiphBOfZlurza77OSIRv6uPzaNbMiOCP9iCp5rSOW6yb0FL9C3MNh6RkR3oDgAZBBeYBcIxMt0_6E-T-pvHmn; isg=BMHBPUpvRkH8Z6o9VAz0TIEk0Avb7jXgAYMvviMWvUgnCuHcaz5FsO8M7GZMTc0Y'

        }

        return headers
