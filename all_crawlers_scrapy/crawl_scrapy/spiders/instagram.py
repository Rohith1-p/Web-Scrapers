import datetime
import hashlib
import json
import random

import pymongo as pm
# importing required packages
import scrapy
from bson.objectid import ObjectId
from scrapy.utils.project import get_project_settings
from setuserv_scrapy.items import InstagramMediaIem
from .setuserv_spider import SetuservSpider
settings = get_project_settings()
settings['CRAWLERA_ENABLED'] = True

# reload(sys)
# sys.setdefaultencoding('utf8')


USER_AGENT_LIST = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/602.4.8 (KHTML, like Gecko) Version/10.0.3 Safari/602.4.8",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/603.2.5 (KHTML, like Gecko) Version/10.1.1 Safari/603.2.5",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.6.17 (KHTML, like Gecko) Version/9.1.1 Safari/601.6.17",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/604.5.6 (KHTML, like Gecko) Version/11.0.3 Safari/604.5.6", ]


class InstaScraperWithoutFilter(SetuservSpider):
    """
    Spider to crawl data from insta hashtags
    """
    name = 'insta_spider_without_filter'
    # default variables

    allowed_domains = []
    append_url = []
    start_urls = []

    # custom_settings = {
    #    'MONGO_DATABASE': "trinetra",
    # }

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)

        self.logger.info("Instagram process start")

        self.scrapper_name = "instagram_scrapper_without_filtering"
        self.user_agent = random.choice(USER_AGENT_LIST)
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_client = pm.MongoClient(mongo_host, int(mongo_port))
        self.db = self.mongo_client[mongo_db]
        mongo_collection = self.db[mongo_collection]
        if mongo_collection:
            self.config_doc = mongo_collection.find_one({"_id": ObjectId(document_id)})
            self.db_name = self.config_doc['db_name']
            self.db_uri = self.config_doc['db_host']
            self.is_crawlera = self.config_doc['is_crawlera']
            self.client_id = self.config_doc['company_id']
            self.scrape_data_since = self.config_doc['since']
            self.scrape_data_until = self.config_doc['until']
            self.media_source = self.config_doc['media_source']
            self.type = self.config_doc['type']
            self.propagation = self.config_doc.get('propagation', 'filtering')

    def start_requests(self):
        """
        This method spins off all the requests
        """
        if self.type == "updatePosts":
            self.logger.info(f"Starting requests for type {type}")
            self.posts_since = self.config_doc['posts_since']
            self.posts_until = self.config_doc['posts_until']
            self.scrape_comment_since = self.scrape_data_since
            self.scrape_comment_until = self.scrape_data_until
            # self.log("parsing updatedate")

            post_ids = self.get_posts_by_client_id(self.client_id, self.posts_since, self.posts_until)
            self.logger.info(f"Post id's are {post_ids}")
            for post_id in post_ids:
                yield scrapy.Request(
                    url='https://www.instagram.com/p/{}/?__a=1'.format(post_id),
                    callback=self.parse_interactions_user_profile_data_and_comments,
                    dont_filter=True, errback=self.err,
                    headers={'USER_AGENT': self.user_agent, 'X-Crawlera-Profile': self.is_crawlera},
                    meta={
                        'is_initial_page': True,
                        'shortcode': post_id,
                        'non_recent_comment_count': 0,
                        'name': self.post_info[post_id]["keywords"],
                        'category': self.post_info[post_id]["category"], "updatePosts": True
                    }
                )

        elif self.type == "updateInteractions":
            # The posts are read from media_filtered collection as it is too expensive to update the posts in media collection
            # The posts we are updating interactions for should have already present in media collection otherwise it may get inconsistencies
            self.logger.info(f"Starting requests for type {type}")
            self.posts_since = self.config_doc['posts_since']
            self.posts_until = self.config_doc['posts_until']
            posts = self.get_filtered_posts_by_client_id(self.client_id, self.posts_since,
                                                              self.posts_until)
            for post in posts:
                post['client_id'] = self.client_id
                post['propagation'] = self.propagation
                # The below method cleans the attributes of media_filtered collection so that they can be inserted in media
                self.delete_fields_not_in_media(post)
                # 'config_doc_id' instead of 'config_doc_ids'. In the Db it is again getting stored as 'config_doc_ids'
                yield scrapy.Request(
                    url='https://www.instagram.com/p/{}/?__a=1'.format(post['id']),
                    callback=self.parse_interactions,
                    dont_filter=True, errback=self.err,
                    meta={'post': post},
                    headers={'USER_AGENT': self.user_agent, 'X-Crawlera-Profile': self.is_crawlera})

        else:
            self.scrape_data_for_links = self.config_doc['links']
            self.scrape_comment_since = self.scrape_data_since
            self.scrape_comment_until = self.scrape_data_until
            for link in self.scrape_data_for_links:
                self.logger.info(f"Scraping data for link {link}")
                # Use a custom user agent every time
                self.user_agent = random.choice(USER_AGENT_LIST)
                if "explore" in link:
                    yield scrapy.Request(
                        url=link,
                        callback=self.parse_hashtag, dont_filter=True, errback=self.err,
                        headers={'USER_AGENT': self.user_agent,
                                 'X-Crawlera-Profile': self.is_crawlera}
                    )

                else:
                    yield scrapy.Request(
                        url=link,
                        callback=self.parse_profiles, dont_filter=True, errback=self.err,
                        headers={'USER_AGENT': self.user_agent,
                                 'X-Crawlera-Profile': self.is_crawlera}
                    )

    def delete_fields_not_in_media(self, post):
        self.logger.info("Deleting posts")
        del post['updated_at']
        del post["is_filtered_for"]
        del post["filtered_for"]
        del post['batch_ids']
        del post['_id']
        if "is_relevent_2" in post:
            del post["is_relevent_2"]
        if "is_relevent_3" in post:
            del post["is_relevent_3"]

    def get_posts_by_client_id(self, client_id, since, until):
        self.logger.info("Collecting posts by client-id")

        posts = self.db.media.find(
            {"client_id": client_id, "created_date": {"$gte": since, "$lte": until}})
        post_ids = []
        self.post_info = {}
        for post in posts:
            post_ids.append(post["id"])
            self.post_info[post["id"]] = {"keywords": post["keywords"], "category": post["category"]}
        # self.log(post_ids)
        return post_ids

    def get_filtered_posts_by_client_id(self, client_id, since, until):
        self.logger.info("Filtering posts by client-id")
        posts = self.db.media_filtered.find({"client_id": client_id, "created_date": {"$gte": since, "$lte": until}})
        return list(posts)

    def parse_profiles(self, response):
        """
        Parse profiles for brands & influences by user names/profiles
        :param response:
        :return:
        """
        self.logger.info("Parsing profiles for brands & influences by user names/profiles")
        non_recent_count = 0
        channel_url = response.url
        category = "profile"
        home_intial_json = self.get_json_data_from_initial_page(response)
        graphql_data = home_intial_json["entry_data"]["ProfilePage"][0]["graphql"]
        has_next = graphql_data["user"]["edge_owner_to_timeline_media"]["page_info"]["has_next_page"]
        end_cursor = graphql_data["user"]["edge_owner_to_timeline_media"]["page_info"]["end_cursor"]
        user_id = graphql_data["user"]["id"]
        edge_followed_by = graphql_data["user"]["edge_followed_by"]["count"]
        edge_follow = graphql_data["user"]["edge_follow"]["count"]
        edge_owner_to_timeline_media = graphql_data["user"]["edge_owner_to_timeline_media"]["count"]
        biography = graphql_data["user"]["biography"]
        name = graphql_data["user"]["username"]
        posts = graphql_data["user"]["edge_owner_to_timeline_media"]["edges"]
        yield {
            'type': 'page',
            'category': 'profile',
            'id': channel_url,
            'entity_source': 'instagram',
            'url': channel_url,
            'name': name,
            'media_count': edge_owner_to_timeline_media,
            'follower_count': edge_followed_by,
            'about': biography,
            'following_count': edge_follow,
            'entity_type': 'channel_id',
            'created_at': datetime.datetime.utcnow(),
        }
        self.logger.info("Parsing posts")
        self.parse_posts(posts, channel_url, name, has_next, end_cursor, non_recent_count, category, user_id)

    def parse_hashtag(self, response):
        """
        Parse profiles for hashtags
        """
        self.logger.info("Generating hashtags")
        non_recent_count = 0
        channel_url = response.url
        self.logger.info(f"Parsing for link {channel_url}")
        category = "hashtag"
        home_initial_json = self.get_json_data_from_initial_page(response)
        graphql_data = home_initial_json["entry_data"]["TagPage"][0]["graphql"]
        name = str(graphql_data["hashtag"]["name"])
        edge_hashtag_to_media = graphql_data["hashtag"]["edge_hashtag_to_media"]["count"]
        posts = graphql_data["hashtag"]["edge_hashtag_to_media"]["edges"]
        user_id = ''
        end_cursor = graphql_data["hashtag"]["edge_hashtag_to_media"]["page_info"]["end_cursor"]
        has_next = graphql_data["hashtag"]["edge_hashtag_to_media"]["page_info"]["has_next_page"]
        # self.lo(graphql_data["hashtag"]["edge_hashtag_to_media"]["edges"])
        yield {
            'type': 'page',
            'category': 'hashtag',
            'id': channel_url,
            'entity_source': 'instagram',
            'url': channel_url,
            'name': name,
            'media_count': edge_hashtag_to_media,
            'follower_count': '',
            'about': '',
            'following_count': '',
            'entity_type': 'channel_id',
            'created_at': datetime.datetime.utcnow(),
            "client_id": self.client_id,
            "propagation": self.propagation,
        }

        self.logger.info(f"There are total {len(posts)} posts for the keyword {name} on the first page")

        self.parse_posts(posts, channel_url, name, has_next, end_cursor, non_recent_count, category, user_id)

    def preprocess_posts_json(self, response):
        """
        Parse posts from jsons (typically from second page onwards)
        :param response:
        :return:
        """

        channel_url = response.meta['channel_url']
        name = response.meta["name"]
        user_id = response.meta['user_id']
        category = response.meta["category"]
        non_recent_count = response.meta["non_recent_count"]
        json_data = json.loads(response.text)
        graphql_data = json_data["data"]
        if category == "hashtag":
            posts = graphql_data["hashtag"]["edge_hashtag_to_media"]["edges"]
            end_cursor = graphql_data["hashtag"]["edge_hashtag_to_media"]["page_info"]["end_cursor"]
            has_next = graphql_data["hashtag"]["edge_hashtag_to_media"]["page_info"]["has_next_page"]
        else:
            posts = graphql_data["user"]["edge_owner_to_timeline_media"]["edges"]
            end_cursor = graphql_data["user"]["edge_owner_to_timeline_media"]["page_info"]["end_cursor"]
            has_next = graphql_data["user"]["edge_owner_to_timeline_media"]["page_info"]["has_next_page"]
        self.parse_posts(posts, channel_url, name, has_next, end_cursor, non_recent_count, category, user_id)

    def get_json_data_from_initial_page(self, response):
        """
        Parse initial page response to get json data powering the page
        :param response:
        :return:
        """
        self.logger.info("Converting json data in a dictionary-format")
        k = response.css('body script[type="text/javascript"]').extract_first()
        l = k.split('>window._sharedData = ')
        g = l[1].split(';</script>')[0]
        home_intial_json = json.loads(g)
        return home_intial_json

    def parse_interactions(self, response):
        post = response.meta['post']
        self.logger.info(f"Updating interactions for the post {post['id']}")
        json_initial_page = json.loads(response.body)
        post['interactions']['like_count'] = json_initial_page["graphql"]["shortcode_media"][
            "edge_media_preview_like"]["count"]
        post['interactions']['comment_count'] = json_initial_page["graphql"]["shortcode_media"][
            "edge_media_to_comment"]["count"]
        yield post
        self.celery.send_task(
            'filter_media.push_interactions', (post["id"], "instagram", "media")
        )

    def parse_posts(self, posts, channel_url, name, has_next, end_cursor, non_recent_count, category, user_id):
        """
        Parse posts into the scraping database.
        :param posts:
        :param channel_url:
        :param name:
        :param has_next:
        :param end_cursor:
        :param non_recent_count:
        :param category:
        :param user_id:
        :return:
        """
        # self.log("called parse_posts function")
        # iterating through hashtags

        for post in posts:
            self.user_agent = random.choice(USER_AGENT_LIST)
            shortcode = post["node"]["shortcode"]
            creator_id = post["node"]["owner"]["id"]
            post_id = post["node"]["id"]
            is_video = post["node"]["is_video"]
            if not is_video:
                media_type = "photo"
            else:
                media_type = "video"
            time_stamp = post["node"]["taken_at_timestamp"]
            edge_media_to_comment = post["node"]["edge_media_to_comment"]["count"]
            edge_media_preview_like = post["node"]["edge_media_preview_like"]["count"]
            if post["node"]["edge_media_to_caption"]["edges"]:
                title_text = post["node"]["edge_media_to_caption"]["edges"][0]["node"]["text"]
            else:
                title_text = ''
            review_date = datetime.datetime.fromtimestamp(int(time_stamp))  # .strftime('%Y-%m-%d %H:%M:%S')
            if self.scrape_data_since <= review_date <= self.scrape_data_until:
                post_dict = {
                    'type': 'post',
                    'category': category,
                    'id': shortcode,
                    'body': title_text,
                    'media_source': 'instagram',
                    'media_type': media_type,
                    'url': 'https://www.instagram.com/p/{}/'.format(shortcode),
                    'source_url': channel_url,
                    'interactions': {'like_count': edge_media_preview_like, 'comment_count': edge_media_to_comment},
                    'created_date': review_date,
                    'keywords': [name],
                    'created_at': datetime.datetime.utcnow(),
                    'creator_id': creator_id,
                    'creator_name': '',
                    "client_id": self.client_id,
                    "propagation": self.propagation,
                    "media_entity_id": "",
                }

                yield scrapy.Request(
                    url='https://www.instagram.com/p/{}/?__a=1'.format(shortcode),
                    callback=self.parse_interactions_user_profile_data_and_comments,
                    dont_filter=True, errback=self.err,
                    meta={
                        'shortcode': shortcode,
                        'name': [name],
                        'link': channel_url,
                        'non_recent_comment_count': 0,
                        'post_dict': post_dict,
                        "is_initial_page": True,
                        'category': category
                    },
                    headers={'USER_AGENT': self.user_agent, 'X-Crawlera-Profile': self.is_crawlera})
                self.logger.info("Parsing individual posts")

        if non_recent_count <= 20 and has_next:
            rhx_gis = "ba0f1715575e9c4bf9826b44ffa9d7fc"
            if category == "hashtag":
                variables = '{"tag_name":"' + name + '","first":12,"after":"' + end_cursor + '"}'
            else:
                variables = '{"id":"' + user_id + '","first":12,"after":"' + end_cursor + '"}'
            values = "%s:%s" % (
                rhx_gis,
                variables
            )
            x_instagram_gis = hashlib.md5(values.encode()).hexdigest()
            headers = {
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36',
                'x-instagram-gis': x_instagram_gis,
                'X-Crawlera-Profile': self.is_crawlera
            }
            if category == "hashtag":
                yield scrapy.Request(
                    url='https://www.instagram.com/graphql/query/?query_hash=ded47faa9a1aaded10161a2ff32abb6b&variables=%7B%22tag_name%22%3A%22{}%22%2C%22first%22%3A12%2C%22after%22%3A%22{}%22%7D'.format(
                        name, end_cursor
                    ),
                    callback=self.preprocess_posts_json,
                    dont_filter=True,
                    headers=headers, errback=self.err,
                    meta={
                        'channel_url': channel_url,
                        'name': name,
                        'non_recent_count': non_recent_count,
                        'category': category,
                        'user_id': user_id
                    }
                )
            else:
                yield scrapy.Request(
                    url='https://www.instagram.com/graphql/query/?query_hash=42323d64886122307be10013ad2dcc44&variables=%7B%22id%22%3A%22{}%22%2C%22first%22%3A12%2C%22after%22%3A%22{}%22%7D'.format(
                        user_id, end_cursor
                    ),
                    callback=self.preprocess_posts_json,
                    dont_filter=True, errback=self.err,
                    headers=headers,
                    meta={
                        'channel_url': channel_url,
                        'name': name,
                        'non_recent_count': non_recent_count,
                        'category': category,
                        'user_id': user_id
                    }
                )

    def parse_interactions_user_profile_data_and_comments(self, response):
        """
        Parses posts for interactions, user profile data and comments
        :param response:
        :return:
        """
        try:
            is_initial_page = response.meta['is_initial_page']
        except KeyError:
            is_initial_page = False
        # self.log("inside the parse intial page")
        self.user_agent = random.choice(USER_AGENT_LIST)
        short_code = response.meta["shortcode"]
        name = response.meta["name"]
        category = response.meta["category"]
        non_recent_comment_count = response.meta["non_recent_comment_count"]
        json_initial_page = json.loads(response.text)
        if is_initial_page:
            try:
                update_posts = response.meta['updatePosts']
            except KeyError:
                update_posts = False

            if update_posts:
                pass
            else:
                post_dict = response.meta["post_dict"]
                post_creator = json_initial_page["graphql"]["shortcode_media"]["owner"]["username"]
                post_creator_id = json_initial_page["graphql"]["shortcode_media"]["owner"]["id"]
                fields = {
                    'type': post_dict["type"],
                    'category': post_dict['category'],
                    'id': post_dict["id"],
                    'body': post_dict["body"],
                    'media_source': post_dict["media_source"],
                    'media_type': post_dict["media_type"],
                    'url': post_dict["url"],
                    'source_url': post_dict["source_url"],
                    'interactions': post_dict["interactions"],
                    'created_date': post_dict["created_date"],
                    'keywords': post_dict["keywords"],
                    'creator_name': post_creator,
                    'creator_id': post_creator_id,
                    'created_at': post_dict["created_at"],
                    "client_id": self.client_id,
                    "propagation": self.propagation,
                    "media_entity_id": post_dict["media_entity_id"]
                }
                yield InstagramMediaIem(**fields)

            initial_comments_list = json_initial_page["graphql"]["shortcode_media"]["edge_media_to_comment"]["edges"]
            end_cursor = json_initial_page["graphql"]["shortcode_media"]["edge_media_to_comment"]["page_info"][
                "end_cursor"]
            has_next = json_initial_page["graphql"]["shortcode_media"]["edge_media_to_comment"]["page_info"][
                "has_next_page"]
        else:
            has_next = json_initial_page["data"]["shortcode_media"]["edge_media_to_comment"]["page_info"][
                "has_next_page"]
            end_cursor = json_initial_page["data"]["shortcode_media"]["edge_media_to_comment"]["page_info"][
                "end_cursor"]
            initial_comments_list = json_initial_page["data"]["shortcode_media"]["edge_media_to_comment"]["edges"]

        self.parse_comments(
            initial_comments_list, short_code, name, has_next, end_cursor, category, non_recent_comment_count
        )

    def parse_comments(self, initial_comments_list, short_code, name, has_next, end_cursor, category,
                       non_recent_comment_count):
        """

        :param has_next:
        :param non_recent_comment_count:
        :param initial_comments_list:
        :param short_code:
        :param name:
        :param end_cursor:
        :param category:
        :return:
        """
        self.logger.info("Parsing comments on a particular post")

        for comment in initial_comments_list:
            created_at = comment["node"]["created_at"]
            review_date = datetime.datetime.fromtimestamp(int(created_at))

            if self.scrape_comment_since <= review_date <= self.scrape_comment_until:
                fields = {
                    'type': 'comment',
                    'category': category,
                    'body': comment["node"]['text'],
                    'creator_name': comment["node"]["owner"]["username"],
                    'created_date': review_date,
                    'id': comment["node"]["id"],
                    'url': 'https://www.instagram.com/p/{}/'.format(short_code),
                    'creator_id': comment["node"]["owner"]["id"],
                    'parent_id': short_code,
                    'parent_type': 'media',
                    'media_source': 'instagram',
                    'created_at': datetime.datetime.utcnow(),
                    'media_entity_id': "",
                    "client_id": self.client_id,
                    "propagation": self.propagation,
                    "keywords": name,
                }
                yield InstagramMediaIem(**fields)
            else:
                pass
            if self.scrape_comment_since > review_date:
                non_recent_comment_count += 1
            else:
                non_recent_comment_count = 0

        if non_recent_comment_count <= 20 and has_next:
            rhx_gis = "ba0f1715575e9c4bf9826b44ffa9d7fc"
            variables = '{"shortcode":"' + short_code + '","first":34,"after":"' + end_cursor + '"}'
            values = "%s:%s" % (
                rhx_gis,
                variables)
            x_instagram_gis = hashlib.md5(values.encode()).hexdigest()
            headers = {
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.117 Safari/537.36',
                'x-instagram-gis': x_instagram_gis,
                'X-Crawlera-Profile': self.is_crawlera
            }
            yield scrapy.Request(
                url='https://www.instagram.com/graphql/query/?query_hash=33ba35852cb50da46f5b5e889df7d159&variables=%7B%22shortcode%22%3A%22{}%22%2C%22first%22%3A34%2C%22after%22%3A%22{}%22%7D'.format(
                    short_code, end_cursor
                ),
                callback=self.parse_interactions_user_profile_data_and_comments,
                dont_filter=True,
                headers=headers,
                meta={
                    'shortcode': short_code,
                    'name': name,
                    'category': category,
                    'non_recent_comment_count': non_recent_comment_count
                }
            )
