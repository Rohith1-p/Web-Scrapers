import requests
import scrapy
import dateparser, datetime
from bs4 import BeautifulSoup
from .setuserv_spider import SetuservSpider


class OncologytubeSpider(SetuservSpider):
    name = 'oncologytube-product-reviews'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("Oncologytube process start")
        assert self.source == 'oncologytube'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            page = 1
            yield scrapy.Request(url=f"{product_url}/page/{page}?showOnly=dateAddedOrder",
                                 callback=self.parse_response,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity, 'page': page})

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']
        page = response.meta['page']
        created_date = self.start_date
        articles = response.css('div[class=" col-lg-3 col-md-6 col-sm-6 col-xs-12 galleryVideo thumbsImage fixPadding"]')

        if articles:
            for item in articles:
                if item:
                    created_date = item.css('div[class="galeryDetails"] div').extract()[2]
                    created_date = ' '.join(BeautifulSoup(created_date, "html.parser").findAll(text=True)).strip()
                    username = item.css('div[class="galeryDetails"] div').extract()[3]
                    username = ' '.join(BeautifulSoup(username, "html.parser").findAll(text=True)).strip()
                    created_date = dateparser.parse(created_date)
                    title = item.css('strong[class="title"]::text').extract_first()
                    _id = item.css("a::attr(videos_id)").extract_first()
                    url = item.css("a::attr(href)").extract_first()
                    views_count = item.css('div[class="galeryDetails"] span[itemprop="interactionCount"]::text'
                                           ).extract_first().strip().split(' ')[0]
                    article_info = {'url': url, 'created_date': created_date, '_id': _id,
                                    'username': username, 'title': title, 'views_count': views_count}

                    if self.start_date <= created_date <= self.end_date:
                        yield scrapy.Request(url=url, callback=self.parse_article,
                                             errback=self.err, dont_filter=True,
                                             meta={'media_entity': media_entity, 'article_info': article_info})
                        self.logger.info(f"Scraping article_url {url}")

            #                     if self.start_date <= created_date <= self.end_date:
            #                         response = requests.get(url)
            #                         res = BeautifulSoup(response.text, 'html.parser')
            #                         description = res.find("meta", {"name": "description"}).get("content")
            #                         self.yield_article(
            #                             article_id=_id,
            #                             product_id=product_id,
            #                             created_date=created_date,
            #                             username=username,
            #                             description=description,
            #                             title=title,
            #                             full_text=str(title) + '. ' + str(description),
            #                             url=url,
            #                             disease_area='',
            #                             medicines='',
            #                             trial='',
            #                             views_count=views_count
            #                         )

            if created_date >= self.start_date:
                page += 1
                yield scrapy.Request(url=f"{product_url}/page/{page}?showOnly=dateAddedOrder",
                                     callback=self.parse_response,
                                     errback=self.err, dont_filter=True,
                                     meta={'media_entity': media_entity, "page": page})
        else:
            print('Pages exhausted / Non 200 response', response.text)

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        article_info = response.meta["article_info"]

        res = BeautifulSoup(response.text, 'html.parser')
        if 'descriptionAreaContent' in response.text:
            description = res.find('div', {"class": "descriptionAreaContent"}).text.strip()
        else:
            description = 'Blocked description'
        self.yield_article(
            article_id=article_info['_id'],
            product_id=product_id,
            created_date=article_info['created_date'],
            username=article_info['username'],
            description=description,
            title=article_info['title'],
            full_text=str(article_info['title']) + '. ' + str(description),
            url=article_info['url'],
            disease_area='',
            medicines='',
            trial='',
            views_count=article_info['views_count']
        )