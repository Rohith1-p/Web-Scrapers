# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class MediaItems(scrapy.Item):
    id = scrapy.Field()
    created_date = scrapy.Field()
    body = scrapy.Field()
    rating = scrapy.Field()
    parent_type = scrapy.Field()
    url = scrapy.Field()
    media_source = scrapy.Field()
    type = scrapy.Field()
    creator_id = scrapy.Field()
    creator_name = scrapy.Field()
    media_entity_id = scrapy.Field()
    title = scrapy.Field()
    config_doc_id = scrapy.Field()
    client_id = scrapy.Field()
    propagation = scrapy.Field()
    extra_info = scrapy.Field()

class SetuservScrapyItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class InfluensterEntityItems(scrapy.Item):
    id = scrapy.Field()
    type = scrapy.Field()
    entity_type = scrapy.Field()

class InfluensterMediaItems(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    parent_id = scrapy.Field()
    type = scrapy.Field()
    media_source = scrapy.Field()
    rating = scrapy.Field()

class InfluensterItems(scrapy.Item):
    productName = scrapy.Field()
    productUrl = scrapy.Field()
    productCategory = scrapy.Field()
    productBrand = scrapy.Field()
    reviewCount = scrapy.Field()
    
class InfluensterItems_v2(scrapy.Item):
    productName = scrapy.Field()
    originalUrl = scrapy.Field()
    newUrl = scrapy.Field()
    breadcrumb = scrapy.Field()
    productBrand = scrapy.Field()
    
class InfluensterCommentItems(scrapy.Item):
    created_date = scrapy.Field()
    body = scrapy.Field()
    rating = scrapy.Field()
    url = scrapy.Field()
    parent_type = scrapy.Field()
    media_source = scrapy.Field()
    type = scrapy.Field() 
    creator_id = scrapy.Field()
    creator_name = scrapy.Field()
    media_entity_id = scrapy.Field()
    parent_id = scrapy.Field()
    
class SephoraItems(scrapy.Item):
    productName = scrapy.Field()
    productUrl = scrapy.Field()
    productCategory = scrapy.Field()
    productPrice = scrapy.Field()
    productReviewCount = scrapy.Field()

class SephoraProductListItems(scrapy.Item):
    productBrandName = scrapy.Field()
    productDisplayName = scrapy.Field()
    productUrl = scrapy.Field()
    productCategory = scrapy.Field()
    reviewCount = scrapy.Field()
    
class SephoraReviewItem(scrapy.Item):
    productUrl = scrapy.Field()
    reviewId = scrapy.Field()
    title = scrapy.Field()
    rating = scrapy.Field()
    reviewText = scrapy.Field()
    reviewDate = scrapy.Field()

class UltaProductListItems(scrapy.Item):
    productName = scrapy.Field()
    productUrl = scrapy.Field()
    productCategory = scrapy.Field()
    reviewCount = scrapy.Field()

class SephoraEntityItems(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    type = scrapy.Field()
    entity_source = scrapy.Field()
    rating = scrapy.Field()
    name = scrapy.Field()
    interactions = scrapy.Field()
    brand = scrapy.Field()
    config_doc_id = scrapy.Field()


class ReviewSitesMediaEntityItem(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    type = scrapy.Field()
    entity_source = scrapy.Field()
    rating = scrapy.Field()
    name = scrapy.Field()
    interactions = scrapy.Field()
    brand = scrapy.Field()
    config_doc_id = scrapy.Field()


class ReviewSitesMediaItem(MediaItems):
    pass

class WebhoseItems(MediaItems):
    pass

class InstagramMediaIem(scrapy.Item):
    type = scrapy.Field()
    category = scrapy.Field()
    id = scrapy.Field()
    body = scrapy.Field()
    config_doc_id = scrapy.Field()
    media_source = scrapy.Field()
    media_type = scrapy.Field()
    url = scrapy.Field()
    source_url = scrapy.Field()
    interactions = scrapy.Field()
    created_date = scrapy.Field()
    keywords = scrapy.Field()
    creator_name = scrapy.Field()
    creator_id = scrapy.Field()
    created_at = scrapy.Field()
    client_id = scrapy.Field()
    propagation = scrapy.Field()
    media_entity_id = scrapy.Field()
    parent_type = scrapy.Field()
    parent_id = scrapy.Field()
    extra_info = scrapy.Field()


class YoutubeCommentsItem(scrapy.Item):
    created_date = scrapy.Field()
    body = scrapy.Field()
    creator_name = scrapy.Field()
    id = scrapy.Field()
    url = scrapy.Field()
    creator_id = scrapy.Field()
    parent_id = scrapy.Field()
    parent_type = scrapy.Field()
    media_source = scrapy.Field()
    interactions = scrapy.Field()
    created_at = scrapy.Field()
    media_entity_id = scrapy.Field()
    client_id = scrapy.Field()
