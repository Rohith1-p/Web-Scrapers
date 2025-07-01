import scrapy
import json
import time
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class ProductListCostco(SetuservSpider):
    name = 'costco-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("costco_product_list scraping process start")
        assert self.source == 'costco_product_list'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            if ' ' in product_id:
                product_id = product_id.replace(' ', '+')
            query_url = self.get_product_url(product_id, page)
            print("Query URL", self.source, query_url)
            yield scrapy.Request(url='https://www.costco.com/CatalogSearch?currentPage=1&pageSize=24&keyword=Blue+River',
                                 callback=self.parse_product_list,
                                 headers=self.get_headers(query_url),
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        page = response.meta["page"]
        print(response.text)
        self.dump(response, 'html', 'rev_response', self.source, sub_brand, str(page))

        _res = BeautifulSoup(response.text, 'html.parser')
        res = _res.findAll("div", {"class": "col-xs-6 col-lg-4 col-xl-3 product"})

        if res:
            for item in res:
                if item:
                    try:
                        product_url = item.find("span", {"class": "description"}).find("a").get("href")
                        product_id = product_url.split('product.')[1].split('.')[0]
                        product_name = item.find("span", {"class": "description"}).find("a").text.strip()
                        try:
                            review_count = item.find("meta", {"itemprop": "reviewCount"}).get("content")
                        except:
                            review_count = 0

                        self.yield_products_list \
                            (sub_brand=sub_brand,
                             product_url=product_url,
                             product_id=product_id,
                             product_name=product_name,
                             review_count=review_count)
                    except:
                        pass



            # page += 1
            # query_url = self.get_product_url(product_id, page)
            # print("Query URL", self.source, query_url)
            # yield scrapy.Request(url=query_url,
            #                      callback=self.parse_product_list,
            #                      headers=self.get_headers(query_url),
            #                      errback=self.err,
            #                      dont_filter=True,
            #                      meta={'media_entity': media_entity,
            #                            'page': page})

    @staticmethod
    def get_product_url(sub_brand, page_count):
        url = f"https://www.costco.com/CatalogSearch?currentPage={page_count}" \
              f"&dept=All&pageSize=24&keyword={sub_brand}"
        return url

    @staticmethod
    def get_headers(query_url):
        headers = {
            'authority': 'www.costco.com',
            'method': 'GET',
            'path': '/CatalogSearch?currentPage=1&pageSize=24&keyword=Blue+River',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cookie': 'akaas_AS01=2147483647~rv=7~id=1a0e3253f8c2d9e5dfd2582042b788ba; bm_sz=B3D40B13048F7B98AD5A0ECD84EB80DB~YAAQxnMsMdl5VZ5/AQAAjFaPtQ946zxClTSRrNarEngzwZrS5BosudHy1tBMSsZrU+jJG5IkIqshVt9R0GkUzhxhB7lPdqf7KSBUDRzy668m6mTl2ywpY/fnz1VkVyGusWs3xqoTD/Eiz2VcRekuJBy+chBgMTR90zxS0uOprzfHRvJxTcboWG+vrHpcnb1XI3Fp1xjRv0DPsKZOY8JYo7AjLk0E03kz0c2Dktm/98mCd35WcGs4xjvzyHTXhkoPJovb01TvkqS2fxvkBdSAkBuMJlgmEE0hStisWexrm6LaS+A=~4407861~3360067; AKA_A2=A; bm_mi=72393F8D454CBF14702D6A7653E77DC4~Zb1wbsS8Kvpy7667KXwU03E4X8NTzZ4qoCZmrGgiq5zkKbI8FzH3Qp7+c66HqjRkvdosJWhgUuE2AHZxClSrKrzJyamKJ7orsq+vo8IjmYmjuJg1newSaRdyKHr083qSMld3mjrswQg/Hpu7KM5gGnqaD0JLp6G6cPrGlwWxz6aDMG/6pVeKb+rfdSYUAzWQjXZGQbA9EoFaZ1d4zsBOSFOHanFluqom17kwlQ1dqdDD3C7xXd7Iyc6at1fpU+A/bNavv1j8NvixKxREqqXe5A==; selectedLanguage=-1; AMCVS_97B21CFE5329614E0A490D45%40AdobeOrg=1; _abck=85CD05B661D1D1327F780E13DF111C39~0~YAAQxnMsMVd6VZ5/AQAAdWePtQeejIGPJXW0koDUBYtcEcH+kzp1uygjwoNZM9kOwENyW5tMguIz1uHQEuG2CFKQa/QL/y9ITNVV4AIZUuhv9P8V3pFd6aunJV6t3mt5vzjtnNKjCliHOy8o9uQaetMqMnhaYjk/2jbd61KtiywEIKbjP039wDWEC5XG3cirb81rGKzr+nPXML3mAb7m9fbljOJh5ufSwnsFr9p37WfEHjuVHfvsoZJ11zzdOZF01qyUzJfvf0WY+0xJqQZp+mzW6dPLZ4NC7MugdUa5595ZYXlaIxfxERAoZ9Mf41NugSKSbImNMO5wIIRPEYyyPyXdCKK4o2i+7b0O1I/AXFlrzbR1agopJRDBsgi7tUoNdmMzHFXxEJi7lRxunDOA0n9jcM+CfPO7~-1~-1~-1; s_ecid=MCMID%7C59863970083918511843253767686986400906; ak_bmsc=9BD989FF1658F6E808376333CD82C238~000000000000000000000000000000~YAAQfMEzuAEpI51/AQAAJmiPtQ9QWUa0Cjy2NJSmHYoGDNTzF6zzOuKYOyHjOam329Bp0b67mnsyyuKUjnnDLKkPjJgaMYZBYXBfKSdIkqor6PR5C6osKwlnNaXbFzUa1D3Ir6XFIYqtDQyXlamw4EAe29dtPjo6fW2tn2gNjQZRLkDrPZthDK1ol0v6DM0Z/iVFLj6U/CEToNfoztafCGmihU1+7bCuo+Fm0fzBgRD55u79Yfyu589KQtugm0FD8Hk2lLZeVE34dDnww/Or3696ZMYkA76gGp/7IRLfIj3c1K+rlc13YY4BgjYF5uLiFrptHE3Hos6R1XUXMfGInnEkfVAkGtdkXPezu0sBKVpDGEikV7MC/Qp/0VrQxzHBihr3B9APLVmeD0+p7DfhZ+7BZKuwovR8rXHHQ3s0P0IOv3VHm/h8Ptyc5JsXCggXqy/SBlJSqqKGo4pPn2rDwhu0pzBKvB9Qan599QIU5oD51PLn+qRGXSHMLRmJ+3U=; AMCV_97B21CFE5329614E0A490D45%40AdobeOrg=-1124106680%7CMCMID%7C59863970083918511843253767686986400906%7CMCAAMLH-1648623348%7C12%7CMCAAMB-1648623348%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1648025748s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.2.0; _gcl_au=1.1.896413944.1648018549; at_check=true; CriteoSessionUserId=33098f03-689e-4c8b-a4cb-9345f83e3ceb; invCheckPostalCode=98101; WAREHOUSEDELIVERY_WHS=%7B%22distributionCenters%22%3A%5B%221250-3pl%22%2C%221321-wm%22%2C%22283-wm%22%2C%22561-wm%22%2C%22725-wm%22%2C%22731-wm%22%2C%22757-wm%22%2C%22758-wm%22%2C%22759-wm%22%2C%22794-wm%22%2C%22847_0-cor%22%2C%22847_0-cwt%22%2C%22847_0-edi%22%2C%22847_0-ehs%22%2C%22847_0-membership%22%2C%22847_0-mpt%22%2C%22847_0-spc%22%2C%22847_0-wm%22%2C%22847_1-edi%22%2C%22847_NA-cor%22%2C%22847_NA-pharmacy%22%2C%22847_NA-wm%22%2C%22951-wm%22%2C%22952-wm%22%2C%229847-wcs%22%5D%2C%22groceryCenters%22%3A%5B%22115-bd%22%5D%2C%22shipToLocation%22%3A%2298101%22%7D; invCheckStateCode=WA; invCheckCity=Seattle; ajs_anonymous_id_2=0abb5950-535b-4589-acaa-b25d939a2fd4; s_cc=true; JSESSIONID=0000UWarvklhJka0tYslX1y8ksB:1cq6lcun9; WC_SESSION_ESTABLISHED=true; WC_PERSISTENT=rak7nTjHQmcyR3EWsGcYwLHb1RQ%3D%0A%3B2022-03-22+23%3A56%3A03.02_1648018563001-932770_10301_-1002%2C-1%2CUSD%2CXa1tU7PGEui0gJzAB4OHeRglehY4z%2BPAV0aBNWEj67ITurDfwKOzTPLq6wASIIBwyblKJqFCR6N5q39yFXycJQ%3D%3D_10301; WC_AUTHENTICATION_-1002=-1002%2C5M9R2fZEDWOZ1d8MBwy40LOFIV0%3D; WC_ACTIVEPOINTER=-1%2C10301; WC_USERACTIVITY_-1002=-1002%2C10301%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C%2FVTIb13hu2f14c829gWmxX4AjGAt57v4SLuF%2BZWnNeh%2BTIP%2BVNJNVN6rZv2J9p3wnTvh6qbaUkfFAntUkffloceoBef3TqJkd6gX4juh2%2FHezHaqPANexR0%2F2LVk3eoo3FehCtHoKNnQr1PvdBpdYLLO5HG7aR%2BYjVFpuwU%2BT49n8i2Ms8DU2yfWs1QjeK9oVc7zMDfuMgjfIogedJm0bw%3D%3D; WC_GENERIC_ACTIVITYDATA=[24236788753%3Atrue%3Afalse%3A0%3Aho8DQ7JE3t4NRj8UxAaYA%2BofOHM%3D][com.ibm.commerce.context.entitlement.EntitlementContext|120577%253B120572%253B120563%253B120565%253B120570%253B120571%253B120567%253B120568%253B120569%253B120566%253B120757%253B120754%253B120752%253B120758%253B120753%253B120756%253B120755%253B120751%253B120765%253B120762%253B120763%253B120761%253B120573%253B120574%253B4000000000000101005%253B60501%253B4000000000000001002%26null%26null%26-2000%26null%26null%26null][com.ibm.commerce.context.audit.AuditContext|1648018563001-932770][com.costco.pharmacy.commerce.common.context.PharmacyCustomContext|null%26null%26null%26null%26null%26null][com.ibm.commerce.context.globalization.GlobalizationContext|-1%26USD%26-1%26USD][com.ibm.commerce.store.facade.server.context.StoreGeoCodeContext|null%26null%26null%26null%26null%26null][com.ibm.commerce.catalog.businesscontext.CatalogContext|10701%26null%26false%26false%26false][com.ibm.commerce.context.experiment.ExperimentContext|null][com.ibm.commerce.context.ExternalCartContext|null][CTXSETNAME|Store][com.ibm.commerce.context.base.BaseContext|10301%26-1002%26-1002%26-1][com.ibm.commerce.giftcenter.context.GiftCenterContext|null%26null%26null]; _fbp=fb.1.1648018564651.1310301842; C_LOC=MH; mbox=PC#26530a9ce17b4fa0a5b60d28553c4a5d.31_0#1711265346|session#e463eaab6ba74046a56e448b9968ca60#1648022405; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Mar+23+2022+12%3A59%3A06+GMT%2B0530+(India+Standard+Time)&version=6.24.0&isIABGlobal=false&hosts=&consentId=ac99c824-ad9b-4cad-b120-dcb55dd083f3&interactionCount=1&landingPath=NotLandingPage&groups=BG12%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1%2CSPD_BG%3A1%2CC0004%3A1&AwaitingReconsent=false; cto_bundle=9WMbU19UbEclMkIxTjY2d2dNTjV0Z3gzMG0zNE1VRzVQRVolMkJMeTIlMkZPZ00lMkZhRTJQZHFvTGVzVVUyYUgzd2JkcHVQVEtvQzFFU3l6MU5GWGVEUmJTZlNvb01VU2IxN0RQWEV4YVJuWXhTQzRDaFRlNFhhdWlzQjMlMkZoU2lQd2lWRGI0VjhRdzNGeGNGeEpObm0zWkg0eWl2Z2dxaFFmSU1wUVBGT05TUSUyQjloUjdIMHBVR0FnQUhNYms4ZnB2Yk9zanRHY2JNQnA; akavpau_zezxapz5yf=1648020879~id=b3d2725bab66e3e2257e30e5ebf70662; bm_sv=2064C53602F376CFC890C2FAB0224CF5~oGpofKG26OWdZE2qrpBq/XS8Vd+NgrIuENtJxvxN483XPWAuSAOmVhGU4NlQmW0qvg+4J88cPCSj73JRE0LVr5DBh3OwGL9Xp7vkxwAjNA3ZAV8nZndola4bg94iAr/mtJMQ2gZjgYCdJ0q0bLSdMlcvbtJ3PF9CcObGU/NSj6k=; s_sq=cwcostcocomprod%3D%2526c.%2526a.%2526activitymap.%2526page%253Dhttps%25253A%25252F%25252Fwww.costco.com%25252FCatalogSearch%25253FcurrentPage%25253D0%252526keyword%25253DBlue%25252BRiver%2526link%253DNext%252520Page%2526region%253Dsearch-results%2526.activitymap%2526.a%2526.c%2526pid%253Dhttps%25253A%25252F%25252Fwww.costco.com%25252FCatalogSearch%25253FcurrentPage%25253D0%252526keyword%25253DBlue%25252BRiver%2526oid%253Dhttps%25253A%25252F%25252Fwww.costco.com%25252FCatalogSearch%25253FcurrentPage%25253D1%252526pageSize%25253D24%252526keyword%25253DBlue%25252BRiver%2526ot%253DA; RT="z=1&dm=www.costco.com&si=6d73bd11-2f5b-4659-8ba6-2c9ecc9cf7a7&ss=l137ovqj&sl=a&tt=1la7&bcn=%2F%2F684d0d4a.akstat.io%2F&obo=2&nu=10krjlc3z&cl=17r53&ul=17r64',
            'referer': 'https://www.costco.com/CatalogSearch?currentPage=0&keyword=Blue+River',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36'
        }

        return headers
