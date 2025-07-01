import dateparser
from datetime import datetime
import re
import time
from bs4 import BeautifulSoup
import scrapy
from .setuserv_spider import SetuservSpider


class ScholarGoogleSpider(SetuservSpider):
    name = 'scholar-google-articles'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Scholar-google process start")
        assert self.source == 'scholar_google'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 0
            yield scrapy.Request(url=self.get_review_url(product_id, page),
                                 callback=self.ol_info,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity,
                                       'page': page})

    def ol_info(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity["url"]
        product_id = media_entity["id"]
        page = response.meta["page"]
        print('response_response_response_response_response',response.text)

        if 'There are no articles in this profile.' not in response.text:
            ol_name = response.css('div[id="gsc_prf_in"]::text').extract_first()
            _designation = response.css('div[class="gsc_prf_il"]').extract_first()
            designation = ' '.join(BeautifulSoup(_designation, "html.parser").stripped_strings)
            keys_list = response.css('td[class="gsc_rsb_std"]::text').extract()
            _citations_values = response.css('span[class="gsc_g_al"]::text').extract()
            citations_values = _citations_values[-8::]
            created_date = self.start_date

            articles_list = response.css('tr.gsc_a_tr')
            if articles_list:
                for item in articles_list:
                    if item:
                        article_url = 'https://scholar.google.co.in' + \
                                      item.css('a[class="gsc_a_at"]::attr(href)').extract_first()
                        article_title = item.css('a[class="gsc_a_at"]::text').extract_first()
                        _created_date = item.css('span[class="gsc_a_h gsc_a_hc gs_ibl"]::text').extract_first()
                        created_date = dateparser.parse(str(_created_date) + '-01-01')
                        if created_date is not None:
                            extra_info = {'article_url': article_url,
                                          'article_title': article_title,
                                          'keys_list': keys_list,
                                          'citations_values': citations_values,
                                          'ol_name': ol_name,
                                          'designation': designation}

                            if self.start_date <= created_date <= self.end_date:
                                time.sleep(2)
                                yield scrapy.Request(url=article_url,
                                                     callback=self.parse_articles,
                                                     errback=self.err, dont_filter=True,
                                                     meta={'media_entity': media_entity,
                                                           'extra_info': extra_info})
                                self.logger.info(f"Scraping article data for {article_url}")
                        else:
                            self.logger.info(f"No published date for {article_url}")

                if created_date >= self.start_date:
                    page += 20
                    time.sleep(10)
                    yield scrapy.Request(url=self.get_review_url(product_id, page),
                                         callback=self.ol_info,
                                         errback=self.err, dont_filter=True,
                                         meta={'media_entity': media_entity,
                                               'page': page})
                    self.logger.info(f"Request is going for {product_url} and {page}")
        else:
            self.logger.info(f"Pages exhausted for {product_url} and {page}")

    def parse_articles(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        extra_info = response.meta["extra_info"]
        article_url = extra_info['article_url']

        keys_list = extra_info['keys_list']
        citations_values = extra_info['citations_values']
        raw_data = response.css('div.gs_scl')
        info_dict = {}

        for item in raw_data:
            key = item.css('div.gsc_oci_field::text').extract_first()
            val = item.css('div.gsc_oci_value').extract_first()
            if key == 'Total citations':
                val = item.css('div.gsc_oci_value a').extract_first()
            soup = BeautifulSoup(val, 'html.parser')
            for s in soup(['script', 'style']):
                s.decompose()
            val = ' '.join(soup.stripped_strings)
            info_dict.update({key: val})

        authors_list = info_dict.get('Authors')
        description = info_dict.get('Description')
        article_citations = info_dict.get('Total citations')
        if article_citations is not None:
            article_citations = re.findall(r'\d+', article_citations)[0]
        else:
            article_citations = 0
        published_date = info_dict.get('Publication date')
        published_date = dateparser.parse(published_date)
        publisher = info_dict.get('Publisher')
        try:
            citations_2014 = citations_values[-8]
        except:
            citations_2014 = 0
        try:
            citations_2015 = citations_values[-7]
        except:
            citations_2015 = 0
        try:
            citations_2016 = citations_values[-6]
        except:
            citations_2016 = 0
        try:
            citations_2017 = citations_values[-5]
        except:
            citations_2017 = 0
        try:
            citations_2018 = citations_values[-4]
        except:
            citations_2018 = 0
        try:
            citations_2019 = citations_values[-3]
        except:
            citations_2019 = 0
        try:
            citations_2020 = citations_values[-2]
        except:
            citations_2020 = 0
        try:
            citations_2021 = citations_values[-1]
        except:
            citations_2021 = 0

        if self.start_date <= published_date <= self.end_date:
            self.yield_hcp_profile_articles \
                (product_url=product_url,
                 product_id=product_id,
                 article_id=article_url.split('citation_for_view=')[1],
                 article_url=article_url,
                 ol_name=extra_info['ol_name'],
                 designation=extra_info['designation'],
                 publications_count='',
                 citations=keys_list[0],
                 citations_since_2016=keys_list[1],
                 h_index=keys_list[2],
                 h_index_since_2016=keys_list[3],
                 i10_index=keys_list[4],
                 i10_index_since_2016=keys_list[5],
                 citations_2014=citations_2014,
                 citations_2015=citations_2015,
                 citations_2016=citations_2016,
                 citations_2017=citations_2017,
                 citations_2018=citations_2018,
                 citations_2019=citations_2019,
                 citations_2020=citations_2020,
                 citations_2021=citations_2021,
                 authors_list=authors_list,
                 description=description,
                 article_citations=article_citations,
                 published_date=published_date,
                 publisher=publisher,
                 article_title=extra_info['article_title']
                 )

    @staticmethod
    def get_review_url(product_id, page):
        url = f'https://scholar.google.co.in/citations?hl=en&user={product_id}&sortby=pubdate' \
              f'&cstart={page}&pagesize=20'
        return url
