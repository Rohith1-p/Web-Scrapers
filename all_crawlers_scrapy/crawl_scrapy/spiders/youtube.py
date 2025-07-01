import pymongo
import scrapy
import urllib
import json
from datetime import datetime
from bson import ObjectId
from setuserv_scrapy.items import YoutubeCommentsItem
from .setuserv_spider import SetuservSpider
import requests
import time
from pymongo import UpdateOne
from celery import Celery
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class YoutubeSpider(SetuservSpider):
    name = 'youtube'
    BASE_URL = 'https://www.googleapis.com/youtube/v3'

    def __init__(self, mongo_host='localhost', mongo_port=27017, mongo_db='sample-1', mongo_collection='config', document_id='5c2f28c663ca8df2f144f4f8'):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        assert (self.source == 'youtube')
        self.keys = self.load_access_keys()
        self.access_key = self.keys.pop()

        # Use celery
        self.celery = Celery()
        self.celery.config_from_object('setuserv_scrapy.settings')
        # self.access_key = 'AIzaSyCO7CDooMDjc1Wdf0OPk8TLtJM4uvyc8dM'
        # self.video_id = 'oPUpC8uXMlk'

    def load_access_keys(self):
        f = open('access_keys.txt', 'r')
        keys = f.read().splitlines()
        f.close()
        return keys

    def start_requests(self):
        if self.config_doc['type'] == 'newData':
            self.get_posts_comments()
        elif self.config_doc['type'] == 'updatePosts':
            self.update_posts_comments()
        elif self.config_doc['type'] == 'updateInteractions':
            self.update_posts_interactions()

        yield
        #

        # params = {
        #     'key': self.access_key,
        #     'part': 'snippet,replies',
        #     'videoId': self.video_id,
        #     'maxResults': 100
        # }
        # url = f'{self.BASE_URL}/commentThreads/?{urllib.parse.urlencode(params)}'
        # request = scrapy.Request(url, callback=self.parse)
        # request.meta['params'] = params
        # return [request]

    def update_posts_interactions(self):
        print("In update interactions")
        # listener.authenticate_()
        post_list = self.get_posts_by_client_id(self.config_doc['company_id'], self.config_doc['since'], self.config_doc['until'])
        self.scrape_interactions_for_posts(post_list)
        self.mongo_db.config.update_one({'_id': self.config_doc["_id"]}, {'$set': {'completed_date': datetime.utcnow()}})

    def scrape_interactions_for_posts(self, item):
        post_id = item[0]
        self.update_interactions(post_id)

    def update_interactions(self, post_id):
        post_details = self.get_post_details(post_id)
        if post_details is not None:
            self.mongo_db.media.update_one(
                {
                    "client_id":self.client_id,
                    "media_source": "youtube",
                    "id": post_id
                },
                {
                    "$set": {
                        "updated_at": datetime.utcnow(),
                        "interactions": post_details['interactions']
                    }
                }
            )
            self.celery.send_task('filter_media.push_interactions', (post_id, "youtube", "media", str(self.execution_config_id)))
        else:
            self.logger.warn("Unable to update interactions for post id "+post_id)

    def update_posts_comments(self):
        print("In update post comments")
        self.mongo_db.config.update_one({'_id': self.config_doc["_id"]}, {'$set': {'start_date': datetime.utcnow()}})
        access_key_file = open('access_keys.txt', 'r')
        # listener.authenticate_()
        post_list = self.get_posts_by_client_id(self.config_doc['company_id'], self.config_doc['posts_since'], self.config_doc['posts_until'])
        self.scrape_comments_for_posts(post_list)
        self.mongo_db.config.update_one({'_id': self.config_doc["_id"]}, {'$set': {'completed_date': datetime.utcnow()}})

    def get_posts_by_client_id(self, client_id, since, until):
        posts = self.mongo_db.media_filtered.find({'client_id': client_id, "created_date": {"$gte": since, "$lte": until}})
        post_list = []
        for post in posts:
            post_list.append((post["id"], post["keywords"]))
        return post_list

    def authenticate_(self):
        if len(self.access_keys) == 0:
            self.logger.error("All the access keys got exhausted")
            log_doc = {"message":'All the access keys got exhausted',"execution_config_id":self.config_doc ,"type":"error"}
            self.mongo_db.log.insert_one(log_doc)
            raise Exception("All the access keys got exhausted")
        else:
            self.access_key = self.keys.pop()
            self.logger.info("Updated new access key "+self.access_key)
            log_doc = {"message":"Updated new access key "+self.access_key,"execution_config_id":self.config_doc ,"type":"info"}
            self.mongo_db.log.insert_one(log_doc)

    def get_posts_comments(self):
        self.mongo_db['config'].update_one({'_id': self.config_doc["_id"]}, {'$set': {'start_date': datetime.utcnow()}})
        tuple_list = []
        for keyword in self.config_doc["keywords"]:
            tuple_list.append(("keyword", keyword))

        for channelId in self.config_doc["channels"]:
            tuple_list.append(("channelId", channelId))

        for username in self.config_doc["usernames"]:
            tuple_list.append(("username", username))

        for item in tuple_list:
            self.scrape_posts_comments(item)

    def scrape_posts_comments(self, item):
        source = item[0]
        search_term = item[1]
        if source == 'keyword':
            print('alok2')
            search_term = search_term.encode().decode()
            self.add_page(search_term, self.config_doc["company_id"], False)
            self.get_videos_by_keyword(search_term, self.config_doc["company_id"], self.config_doc["since"], self.config_doc["until"])
            self.get_comments(search_term, self.config_doc["until"], self.config_doc["since"], self.config_doc["company_id"])
        elif source == 'channelId':
            print('Processing channelId:', search_term)
            self.add_page(search_term, self.config_doc["company_id"])

            self.get_video_by_channel_id(search_term, self.config_doc["company_id"], self.config_doc["since"], self.config_doc["until"])
            self.get_comments(search_term, self.config_doc["until"], self.config_doc["since"], self.config_doc["company_id"])
        else:
            print('Invalid type:', source)

    def get_video_by_channel_id(self, channel_id, client_id, published_after=None, published_before=None):
        url = self.BASE_URL + '/search'
        params = {'key': self.access_key, 'part': 'snippet,id', 'channelId': channel_id, 'maxResults': 50, 'type': 'video','order':'relevance'}
        published_after = published_after.strftime('%Y-%m-%dT%H:%M:%S+0000')
        published_before = published_before.strftime('%Y-%m-%dT%H:%M:%S+0000')
        if published_after:
            params.setdefault('publishedAfter', published_after)
        if published_before:
            params.setdefault('publishedBefore', published_before)
        next_page_token = None
        documents = []
        count = 0
        media_count = 0
        all_video_ids = set()
        while True:
            count += 1
            if next_page_token:
                params['pageToken'] = next_page_token
            r = requests.get(url, params=params)
            data = r.json()
            if self.check_rate_limit(r):
                data = requests.get(url, params=params).json()
            self.logger.info('page:' + str(count) + ' items:' + str(len(data.get("items", []))))
            if len(data.get("items",[])) == 0:
                break
            video_ids = []
            for item in data.get('items', []):
                video_id = item['id']['videoId']
                if video_id not in all_video_ids:
                    video_ids.append(video_id)
                    all_video_ids.add(video_id)
            start_time_new = int(round(time.time() * 1000))
            for video_id in video_ids:
                try:
                    video = self.get_video_from_db(video_id, client_id)
                    if video:
                        log_doc = {"type": "info",
                                   "message": "Not processing video with id " + str(
                                       video_id) + " for keyword " + channel_id + " as it is already present other media entity ",
                                   "execution_config_id": self.config_doc}
                        self.mongo_db.log.insert_one(log_doc)
                        del video['media_entity_id']
                        del video['updated_at']
                        if 'keywords' in video:
                            del video['keywords']
                    elif video is None:
                        video = self.get_post_details(video_id)
                        media_count = media_count + 1
                    video['media_entity_id'] = ""
                    video['client_id'] = client_id
                    documents.append(
                        UpdateOne(
                            {   'client_id': client_id,
                                "media_source": video["media_source"],
                                "id": video['id']
                            },
                            {
                                "$setOnInsert": video,
                                "$set": {
                                    "updated_at": datetime.utcnow()
                                },
                                "$addToSet": {
                                    "keywords": {
                                        "$each": [channel_id]
                                    }
                                }
                            }, upsert=True
                        )
                    )
                except Exception as err:
                    self.logger.error("Exception occured while fetching details of video "+str(video_id)+" "+str(err))
                    log_doc = {"exception":str(err),"message":'Exception occured while details of video '+ str(video_id) +" " +str(err),
                           "execution_config_id":self.config_doc, "type":"error"}
                    self.mongo_db.log.insert_one(log_doc)
                    if str(err) == 'All the access keys got exhausted':
                        raise
            end_time_new = int(round(time.time() * 1000))
            self.logger.info("Time taken for all videos in milli seconds "+str(end_time_new - start_time_new))
            next_page_token = data.get('nextPageToken', None)
            if not next_page_token:
                break
        if len(documents) > 0:
            self.mongo_db.media.bulk_write(documents,ordered = False)
        for video_id in all_video_ids:
            self.celery.send_task('filter_media.filter_youtube_media', (video_id,"youtube","media"))
        self.mongo_db.media_entity.update_one({'id':channel_id},{'$set': {'media_count':media_count,'updated_at' : datetime.utcnow()}})

    def get_video_from_db(self,videoId, client_id):
        return self.mongo_db.media.find_one({'client_id': client_id, "media_source":"youtube","id":videoId})

    def get_comments(self, channel_id_or_keyword, start_date, end_date, client_id):
        channel_video_ids = self.mongo_db.media.find({'client_id': client_id,  "keywords": channel_id_or_keyword, "created_date": {"$gte": end_date, "$lte": start_date}}, {'id': 1, '_id': 0})
        for video_id in list(channel_video_ids):
            video_id = video_id['id']
            try:
                self.get_comment_threads(video_id, [channel_id_or_keyword], client_id, start_date, end_date)
            except Exception as e:
                self.logger.error('Exception occured while fetching comments for videoid '+ str(video_id) +" " +str(e))
                log_doc = {"exception": str(e),
                           "message": 'Exception occured while fetching comments for videoid ' + str(video_id) + " " + str(e),
                           "execution_config_id": self.config_doc,
                           "type": "error"}
                self.mongo_db.log.insert_one(log_doc)
                if str(e) == 'All the access keys got exhausted':
                    raise

    def get_request_session(self):
        session = requests.Session()
        retry = Retry(total=5, backoff_factor=0.3)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    # TODO(alok): use this to make spider async instead of get_comment_threads
    def get_comment_threads_async(self, video_id, channel_id_or_keyword, client_id, start_date=None, end_date=None):
        params = {
            'key': self.access_key,
            'part': 'snippet,replies',
            'videoId': video_id,
            'maxResults': 100
        }
        url = f'{self.BASE_URL}/commentThreads/?{urllib.parse.urlencode(params)}'
        request = scrapy.Request(url, callback=self.parse, dont_filter=True)
        request.meta['params'] = params
        request.meta['video_id'] = video_id
        yield request

    def get_comment_threads(self, video_id, channel_id_or_keyword, client_id, start_date=None, end_date=None):
        self.logger.info('Ready to fetch comments and reply for videoId:'+ str(video_id))
        url = self.BASE_URL + '/commentThreads'
        params = {'key': self.access_key, 'part': 'snippet,replies', 'videoId': video_id, 'maxResults': 100}
        next_page_token = None
        s = self.get_request_session()
        count = 0
        comment_ids = set()
        while True:
            count += 1
            if next_page_token:
                params['pageToken'] = next_page_token
            r = s.get(url, params=params)
            data = r.json()
            if self.check_rate_limit(r):
                data = requests.get(url, params=params).json()
            next_page_token = data.get('nextPageToken', None)
            documents = []
            for item in data.get('items', []):
                created_date = item['snippet']['topLevelComment']['snippet']['publishedAt']
                _created_date = datetime.strptime(created_date, '%Y-%m-%dT%H:%M:%S.000Z')
                if start_date >= _created_date >= end_date:
                    pass
                elif _created_date > start_date:
                    continue
                else:
                    next_page_token = None
                    break
                document = {}
                document['created_date'] = _created_date
                document['body'] = item['snippet']['topLevelComment']['snippet']['textOriginal']
                document['creator_name'] = item['snippet']['topLevelComment']['snippet'].get('authorDisplayName', {})
                document['id'] = item['snippet']['topLevelComment']['id']
                document['url'] = 'https://www.youtube.com/watch?v={video_id}&lc={commentId}'.format(video_id=video_id, commentId=document['id'])
                document['creator_id'] = item['snippet']['topLevelComment']['snippet'].get('authorChannelId', {}).get('value', '')
                document['parent_id'] = item['snippet']['topLevelComment']['snippet']['videoId']
                document['parent_type'] = 'media'
                document['media_source'] = 'youtube'
                interactions = {'like_count': item['snippet']['topLevelComment']['snippet']['likeCount']}
                document['interactions'] = interactions
                document['created_at'] = datetime.utcnow()
                comment_ids.add(document['id'])
                document['media_entity_id'] = ""
                document['client_id'] = client_id
                documents.append(
                    UpdateOne(
                        {
                            'client_id': client_id,
                            "media_source": document['media_source'],
                            "id": document['id']
                        },
                        {
                            "$setOnInsert": document,
                            "$set": {
                                "updated_at": datetime.utcnow()
                            },
                            "$addToSet": {
                                "keywords": {
                                    "$each": channel_id_or_keyword
                                }
                            }
                        }, upsert=True
                    )
                )
                total_reply_count = item['snippet']['totalReplyCount']
                self.logger.info('replyCount for video: {} , comment: {} is {}'.format(video_id, document['id'], total_reply_count))
                if total_reply_count and item.get('replies'):
                    for reply in item['replies']['comments']:
                        created_date = reply['snippet']['publishedAt']
                        _created_date = datetime.strptime(created_date, '%Y-%m-%dT%H:%M:%S.000Z')
                        if start_date >= _created_date >= end_date:
                            pass
                        elif _created_date > start_date:
                            continue
                        else:
                            break
                        reply_obj = {}
                        reply_obj['id'] = reply['id']
                        reply_obj['url'] = 'https://www.youtube.com/watch?v={video_id}&lc={replyId}'.format(video_id=video_id, replyId=reply_obj['id'])
                        reply_obj['creator_name'] = reply['snippet']['authorDisplayName']
                        reply_obj['creator_id'] = reply['snippet']['authorChannelId']['value']
                        reply_obj['authorChannelUrl'] = reply['snippet']['authorChannelUrl']
                        reply_obj['videoId'] = reply['snippet']['videoId']
                        reply_obj['body'] = reply['snippet']['textOriginal']
                        reply_obj['created_date'] = _created_date
                        reply_interactions = {'like_count': reply['snippet']['likeCount']}
                        reply_obj['interactions'] = reply_interactions
                        reply_obj['parent_id'] = document['id']
                        reply_obj['parent_type'] = 'comment'
                        reply_obj['media_source'] = 'youtube'
                        reply_obj['created_at'] = datetime.utcnow()
                        comment_ids.add(reply_obj['id'])
                        reply_obj['media_entity_id'] = ""
                        reply_obj['client_id'] = client_id
                        documents.append(
                            UpdateOne(
                                {
                                    'client_id': client_id,
                                    "media_source": document['media_source'],
                                    'id': reply_obj['id'],
                                },
                                {
                                    "$setOnInsert": reply_obj,
                                    "$set": {
                                        "updated_at": datetime.utcnow()
                                    },
                                    "$addToSet": {
                                        "keywords": {
                                            "$each": channel_id_or_keyword
                                        }
                                    }
                                }, upsert=True
                            )
                        )
            start_time = int(round(time.time() * 1000))
            #self.lock.acquire()
            if len(documents) > 0:
                self.mongo_db.comments.bulk_write(documents,ordered = False)
            #self.lock.release()
            end_time = int(round(time.time() * 1000))
            self.logger.info("Time take in milli seconds "+str(end_time - start_time))
            self.logger.info("No of comments inserted are "+ str(len(documents)))
            if not next_page_token:
                break
        for commentId in comment_ids:
            self.celery.send_task('filter_media.filter_youtube_media', (commentId,"youtube","comments"))

    def get_videos_by_keyword(self, keyword, client_id, published_after=None, published_before=None):
        url = self.BASE_URL + '/search'
        params = {'key': self.access_key, 'part': 'snippet', 'q': keyword, 'maxResults': 50, 'type': 'video', 'order': 'relevance'}
        published_after = published_after.strftime('%Y-%m-%dT%H:%M:%S+0000')
        published_before = published_before.strftime('%Y-%m-%dT%H:%M:%S+0000')
        if published_after:
            params.setdefault('publishedAfter', published_after)
        if published_before:
            params.setdefault('publishedBefore', published_before)
        next_page_token = None
        documents = []
        count = 0
        media_count = 0
        all_video_ids = set()
        while True:
            count += 1
            if next_page_token:
                params['pageToken'] = next_page_token
            r = requests.get(url, params=params)
            data = r.json()
            if self.check_rate_limit(r):
                data = requests.get(url, params=params).json()
            self.logger.info('page:' + str(count) + ' items:' + str(len(data.get("items", []))))
            if len(data.get("items",[])) == 0:
                break
            video_ids = []
            for item in data.get('items', []):
                video_id = item['id']['videoId']
                if video_id not in all_video_ids:
                    video_ids.append(video_id)
                    all_video_ids.add(video_id)
            start_time_new = int(round(time.time() * 1000))
            video = None
            for video_id in video_ids:
                try:
                    video = self.get_video_from_db(video_id, client_id)
                    if video:
                        log_doc = {"type": "info",
                                   "message": "Not processing video with id " + str(
                                       video_id) + " for keyword " + keyword + " as it is already present other media entity ",
                                   "execution_config_id": self.config_doc}
                        self.mongo_db.log.insert_one(log_doc)
                        del video['media_entity_id']
                        del video['updated_at']
                        if 'keywords' in video:
                            del video['keywords']
                    elif video is None:
                        video = self.get_post_details(video_id)
                        media_count = media_count + 1
                    video['media_entity_id'] = ""
                    video['client_id'] = client_id
                    documents.append(
                        UpdateOne(
                            {
                                "client_id": client_id,
                                "media_source": video["media_source"],
                                "id": video['id']
                            },
                            {
                                "$setOnInsert": video,
                                "$set": {
                                    "updated_at": datetime.utcnow()
                                },
                                "$addToSet": {
                                    "keywords": {
                                        "$each": [keyword]
                                    }
                                }
                            }, upsert=True
                        )
                    )
                except Exception as err:
                    self.logger.error("Exception occured while fetching details of video "+str(video_id)+" "+str(err))
                    log_doc = {"exception":str(err),"message":'Exception occured while details of video '+ str(video_id) +" " +str(err), "execution_config_id":self.config_doc, "type":"error"}
                    self.mongo_db.log.insert_one(log_doc)
                    if str(err) == 'All the access keys got exhausted':
                        raise
            end_time_new = int(round(time.time() * 1000))
            self.logger.info("Time taken for all videos in milli seconds "+str(end_time_new - start_time_new))
            next_page_token = data.get('nextPageToken', None)
            if not next_page_token:
                break
        if len(documents) > 0:
            self.mongo_db.media.bulk_write(documents,ordered = False)
        for video_id in all_video_ids:
            self.celery.send_task('filter_media.filter_youtube_media', (video_id,"youtube","media"))
        self.mongo_db.media_entity.update_one({'id':keyword},{'$set': {'media_count':media_count,'updated_at' : datetime.utcnow()}})

    def get_post_details(self, videoId):
        '''
        To get video details
        '''
        url = self.BASE_URL + '/videos'
        params = {'key': self.access_key, 'part': 'snippet,statistics', 'id': videoId}
        start_time = int(round(time.time() * 1000))
        r = requests.get(url, params=params)
        end_time = int(round(time.time() * 1000))
        self.logger.info("Time take in milli seconds to get post details"+str(end_time - start_time))
        data = r.json()
        if self.check_rate_limit(r):
            data = requests.get(url, params=params).json()
        details = {}
        item = data.get('items', {})
        if item:
            item = item[0]
        else:
            self.logger.warn('bad url '+r.url)
            log_doc = {"message":'bad url '+r.url,"execution_config_id":self.config_doc, "type":"warn"}
            self.mongo_db.log.insert_one(log_doc)
            return None
        details['id'] = item['id'] # videoId
        details['title'] = item['snippet']['title']
        details['body'] = item['snippet']['description']
        details['url'] = 'https://www.youtube.com/watch?v=' + details['id']
        details['creator_id'] = item['snippet']['channelId']
        details['creator_name'] = item['snippet']['channelTitle']
        interactions = {}
        interactions['dislike_count'] = item['statistics'].get('dislikeCount', 0)
        interactions['like_count'] = item['statistics'].get('likeCount', 0)
        interactions['view_count'] = item['statistics'].get('viewCount',0)
        interactions['favorite_count'] = item['statistics'].get('favoriteCount',0)
        interactions['comment_count'] = item['statistics'].get('commentCount',0)
        details['interactions'] = interactions
        details['created_date'] = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%S.000Z')
        details['media_source'] = 'youtube'
        details['created_at'] = datetime.utcnow()
        return details

    def check_rate_limit(self,response):
        data = response.json()
        if "error" in data:
            for error in data.get("error").get("errors",[]):
                if error["reason"] == 'dailyLimitExceeded' or error["reason"] == 'quotaExceeded':
                    self.authenticate_()
                    return True
                else:
                    log_doc = {"message":error["reason"],"execution_config_id":self.config_doc ,"type":"error","url":response.url}
                    self.mongo_db.log.insert_one(log_doc)
                    return True
        return False

    def add_page(self, channelIdOrKeyword, client_id, isChannel=True):
        '''
        To add channel or keyword as page
        '''
        entity = self.mongo_db.media_entity.find_one({'id': channelIdOrKeyword, 'entity_source': "youtube"})
        if entity:
            self.logger.info(channelIdOrKeyword + ' this channelId already exits')
            entity["updated_at"] = datetime.utcnow()
        else:
            entity = {}
            if isChannel:
                entity = self.get_page_details(channelIdOrKeyword)
                entity['entity_type'] = 'youtube_channel'
            else:
                entity['entity_type'] = 'youtube_keyword'
                entity['id'] = channelIdOrKeyword
                entity['name'] = channelIdOrKeyword

            entity["entity_source"] = 'youtube'
            entity['created_at'] = datetime.utcnow()
            entity['updated_at'] = datetime.utcnow()
            entity['client_id'] = self.client_id
        self.mongo_db.media_entity.update_one({'id': channelIdOrKeyword, 'entity_source': "youtube"}, {"$setOnInsert": entity}, upsert=True)

    def get_page_details(self, channelId):
        '''
        To get channel details
        '''
        url = self.BASE_URL + '/channels'
        params = {'key': self.access_key, 'part': 'snippet,statistics', 'id': channelId}
        r = requests.get(url, params=params)
        data = r.json()
        if self.check_rate_limit(r):
            data = requests.get(url, params=params).json()
        # print("url is ",url)
        details = {}
        print(data)
        try:
            items = data.get('items', {})
            item = {}
            if items:
                item = items[0]
            details['id'] = item['id']  # channelId
            details['name'] = item['snippet']['localized']['title']  # title
            details['media_count'] = item['statistics']['videoCount']  # videoCount
            details['follower_count'] = item['statistics']['subscriberCount']
            details['about'] = item['snippet']['localized']['description']
            details['created_date'] = item['snippet']['publishedAt']
            details['url'] = item['snippet'].get('customUrl', '')  # TODO
        except Exception as e:
            self.logger.error('error' + str(e) + " " + channelId)
            log_doc = {"exception": str(e), "message": 'error' + str(e) + " " + channelId,
                       "execution_config_id": self.config_doc, "type": "error"}
            self.mongo_db.log.insert_one(log_doc)

        return details

    def parse(self, response):
        video_id = response.meta['video_id']
        data = json.loads(response.body)
        next_page_token = data.get('nextPageToken', None)
        # lets collect comment and reply
        items = data.get('items', [])
        for item in items:
            created_date = item['snippet']['topLevelComment']['snippet']['publishedAt']
            _created_date = datetime.datetime.strptime(created_date, '%Y-%m-%dT%H:%M:%S.000Z')
            if self.start_date >= _created_date >= self.end_date:
                pass
            elif _created_date > self.start_date:
                continue
            else:
                next_page_token = None
                break
            id = item['snippet']['topLevelComment']['id']
            interactions = {'like_count': item['snippet']['topLevelComment']['snippet']['likeCount']}
            record = {
                'created_date': _created_date,
                'body': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'creator_name': item['snippet']['topLevelComment']['snippet'].get('authorDisplayName', {}),
                'id': id,
                'url': f'https://www.youtube.com/watch?v={self.video_id}&lc={id}',
                'creator_id': item['snippet']['topLevelComment']['snippet'].get('authorChannelId', {}).get('value', ''),
                'parent_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                'parent_type': 'media',
                'media_source': 'youtube',
                'interactions': interactions,
                'created_at': datetime.datetime.utcnow(),
                'media_entity_id': '',
                'client_id': self.client_id,
            }
            yield record

            total_reply_count = item['snippet']['totalReplyCount']

            if total_reply_count and item.get('replies'):
                for reply in item['replies']['comments']:
                    created_date = reply['snippet']['publishedAt']
                    _created_date = datetime.strptime(created_date, '%Y-%m-%dT%H:%M:%S.000Z')
                    if self.start_date >= _created_date >= self.end_date:
                        pass
                    elif _created_date > self.start_date:
                        continue
                    else:
                        break
                    reply_interactions = {'like_count': reply['snippet']['likeCount']}
                    reply_record = {
                        'created_date': _created_date,
                        'body': reply['snippet']['textOriginal'],
                        'creator_name': reply['snippet']['authorDisplayName'],
                        'id': reply['id'],
                        'url': 'https://www.youtube.com/watch?v={video_id}&lc={replyId}'.format(video_id=video_id, replyId=reply_record['id']),
                        'creator_id': reply['snippet']['authorChannelId']['value'],
                        'parent_id': record['id'],
                        'parent_type': 'comment',
                        'media_source': 'youtube',
                        'interactions': reply_interactions,
                        'created_at': datetime.utcnow(),
                        'media_entity_id': '',
                        'client_id': self.client_id,
                        'authorChannelUrl': reply['snippet']['authorChannelUrl'],
                        'videoId': reply['snippet']['videoId'],
                    }
                    yield reply_record

        # lets paginate if next page is available for more comments
        if next_page_token:
            params = response.meta['params']
            params['pageToken'] = next_page_token
            url = f'{self.BASE_URL}/commentThreads/?{urllib.parse.urlencode(params)}'
            request = scrapy.Request(url, callback=self.parse, dont_filter=True)
            request.meta['params'] = params
            request.meta['video_id'] = video_id
            yield request
