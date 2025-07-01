import json
from scrapy.http import FormRequest

import dateparser
from .setuserv_spider import SetuservSpider


class YoutubePharmaSpider(SetuservSpider):
    name = 'youtube-pharma-videos'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        assert (self.source == 'youtube')
        self.access_key = 'AIzaSyDoK8nNkbD6DVgRbXjGCSjjAzzW4R8Ye-s'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            url = 'https://www.googleapis.com/youtube/v3/search'
            params = {'key': self.access_key, 'part': 'snippet', 'q': str(product_id), 'maxResults': str(50),
                      'type': 'video', 'order': 'relevance'}
            published_after = self.start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            published_before = self.end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            if published_after:
                params.setdefault('publishedAfter', published_after)
            if published_before:
                params.setdefault('publishedBefore', published_before)

            yield FormRequest(url=url,
                              method='GET',
                              formdata=params,
                              dont_filter=True,
                              callback=self.parse_response,
                              errback=self.err,
                              meta={'media_entity': media_entity,
                                    'url': url})
            self.logger.info(f"Generating videos data for {product_id}")

    def parse_response(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        url = response.meta['url']
        created_date = self.start_date
        data = json.loads(response.text)
        data = data['items']

        if data:
            for item in data:
                if item:
                    video_id = item['id']['videoId']
                    _created_date = item['snippet']['publishedAt']
                    channel_id = item['snippet']['channelId']
                    created_date = dateparser.parse(_created_date).strftime("%Y-%m-%d")
                    created_date = dateparser.parse(created_date)

                    if self.start_date <= created_date <= self.end_date and channel_id == 'UC1GIzE1emRH6dEyP72N_htQ':
                        video_url = 'https://www.googleapis.com/youtube/v3/videos'
                        params = {'key': self.access_key, 'part': 'snippet,statistics', 'id': video_id}

                        yield FormRequest(url=video_url,
                                          method='GET',
                                          formdata=params,
                                          dont_filter=True,
                                          callback=self.get_post_details,
                                          errback=self.err,
                                          meta={'media_entity': media_entity,
                                                'video_id': video_id,
                                                'created_date': created_date})

            if created_date >= self.start_date:
                params = {'key': self.access_key, 'part': 'snippet', 'q': str(product_id), 'maxResults': str(50),
                          'type': 'video', 'order': 'relevance'}
                published_after = self.start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                published_before = created_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                if published_after:
                    params.setdefault('publishedAfter', published_after)
                if published_before:
                    params.setdefault('publishedBefore', published_before)

                yield FormRequest(url=url,
                                  method='GET',
                                  formdata=params,
                                  dont_filter=True,
                                  callback=self.parse_response,
                                  errback=self.err,
                                  meta={'media_entity': media_entity,
                                        'url': url})

    def get_post_details(self, response):
        media_entity = response.meta["media_entity"]
        product_id = media_entity['id']
        video_id = response.meta['video_id']
        created_date = response.meta['created_date']

        video_data = json.loads(response.text)
        video_data = video_data['items']
        self.logger.info(f"Getting video response{video_id}")

        if video_data:
            for item in video_data:
                if item:
                    title = item['snippet']['title']
                    description = item['snippet']['description'].replace('\n', ' ')
                    views_count = item['statistics']['viewCount']
                    url = 'https://www.youtube.com/watch?v=' + str(video_id)
                    full_text = str(title) + '. ' + str(description)

                    if full_text:
                        self.yield_article(
                            article_id=video_id,
                            product_id=product_id,
                            created_date=created_date,
                            username='',
                            description=description,
                            title=title,
                            full_text=full_text,
                            url=url,
                            disease_area='',
                            medicines='',
                            trial='',
                            views_count=views_count
                        )
