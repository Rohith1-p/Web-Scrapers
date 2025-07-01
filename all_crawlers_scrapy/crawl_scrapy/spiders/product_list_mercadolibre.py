import scrapy
from bs4 import BeautifulSoup

from .setuserv_spider import SetuservSpider


class ProductListMercadolibre(SetuservSpider):
    name = 'mercadolibre-products-list'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("mercadolibre products list scraping process start")
        assert self.source == 'mercadolibre_ar' or 'mercadolibre_br' or 'mercadolibre_mx'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            query_url = self.get_product_url(self.source.split('_')[1], product_id)
            print("Query URL", self.source, query_url)
            yield scrapy.Request(url=query_url,
                                 callback=self.parse_product_list,
                                 errback=self.err,
                                 dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_product_list(self, response):
        media_entity = response.meta["media_entity"]
        sub_brand = media_entity["id"]
        page = response.meta["page"]

        _res = BeautifulSoup(response.text, 'html.parser')
        res = _res.findAll("li", {"class": "ui-search-layout__item"})

        if res:
            for item in res:
                if item:
                    if 'br' in self.source:
                        product_url = item.find("a", {"class": "ui-search-link"}).get('href')
                        product_name = item.find("a", {"class": "ui-search-link"}).get('title')
                    else:
                        product_url = item.find("div",
                                                {"class": "ui-search-item__group ui-search-item__group--title"}).find(
                            'a').get('href')
                        product_name = item.find("div",
                                                 {"class": "ui-search-item__group ui-search-item__group--title"}).find(
                            'a').get('title')
                    try:
                        review_count = item.find("span", {"class": "ui-search-reviews__amount"}).text
                    except:
                        review_count = 0
                    product_id = product_url

                    self.yield_products_list \
                        (sub_brand=sub_brand,
                         product_url=product_url,
                         product_id=product_id,
                         product_name=product_name,
                         review_count=review_count)

            if 'title="Siguiente"' in response.text or 'title="Seguinte"' in response.text:
                try:
                    print(f"here {page + 1} page coming")
                    if '_br' in self.source:
                        query_url = _res.find("a", {"title": "Seguinte"}).get('href')
                    else:
                        print("mx/ar is coming")
                        query_url = _res.find("a", {"title": "Siguiente"}).get('href')
                    print("Query URL", self.source, query_url)
                    yield scrapy.Request(url=query_url,
                                         callback=self.parse_product_list,
                                         errback=self.err,
                                         dont_filter=True,
                                         meta={'media_entity': media_entity,
                                               'page': page})
                except:
                    print(f"Error in page {page + 1} & {self.source} {sub_brand}")

    @staticmethod
    def get_product_url(source, sub_brand):
        if source in {'mx', 'ar', 'br'}:
            return {
                'mx': f"https://listado.mercadolibre.com.mx/{sub_brand}",
                'ar': f"https://listado.mercadolibre.com.ar/{sub_brand}",
                'br': f"https://lista.mercadolivre.com.br/{sub_brand}"
            }[source]
