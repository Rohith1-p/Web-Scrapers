import scrapy
import dateparser
import base64
import json
from bs4 import BeautifulSoup
import pandas as pd
from .setuserv_spider import SetuservSpider
from oauth2client.service_account import ServiceAccountCredentials

class ClinicalTrialsSpider(SetuservSpider):
    name = 'clinicaltrials-articles'

    first_posted_date = ''
    last_updated_date = ''

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("ClinicalTrials process start")
        assert self.source == 'clinicaltrials'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            yield scrapy.Request(url=product_url, callback=self.parse_article,
                                 errback=self.err, dont_filter=True,
                                 meta={'media_entity': media_entity})
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_article(self, response):
        media_entity = response.meta["media_entity"]
        product_url = media_entity['url']
        product_id = media_entity['id']

        data_dict = json.loads(response.text)

        studies_list = data_dict['FullStudiesResponse']['FullStudies']

        for study in studies_list:
            protocol_section = study['Study']['ProtocolSection']

            last_updated_post = protocol_section['StatusModule']['LastUpdatePostDateStruct']['LastUpdatePostDate']

            last_updated_post = dateparser.parse(last_updated_post)

            if last_updated_post >= self.start_date and last_updated_post <= self.end_date:

                print('last_updated_post===>', last_updated_post)

                id = protocol_section['IdentificationModule']['NCTId']
                title = protocol_section['IdentificationModule']['BriefTitle']
                title_link = 'https://clinicaltrials.gov/ct2/show/'
                title_link = title_link + id
                status = protocol_section['StatusModule']['OverallStatus']

                first_post = protocol_section['StatusModule']['StudyFirstPostDateStruct']['StudyFirstPostDate']

                full_text = ''

                if 'DescriptionModule' in protocol_section:
                    if 'BriefSummary' in protocol_section['DescriptionModule']:
                        brief_summary = protocol_section['DescriptionModule']['BriefSummary']
                        full_text = brief_summary + ' '
                    if 'DetailedDescription' in protocol_section['DescriptionModule']:
                        detailed_description = protocol_section['DescriptionModule']['DetailedDescription']
                        full_text = full_text + ' ' + detailed_description

                try:
                    conditions_list = protocol_section['ConditionsModule']['ConditionList']['Condition']
                except:
                    conditions_list = []

                try:
                    arm_group_list = protocol_section['ArmsInterventionsModule']['ArmGroupList']['ArmGroup']
                    interventions_list = []
                    for group in arm_group_list:
                        if 'ArmGroupInterventionList' in group:
                            for intervention in group['ArmGroupInterventionList']['ArmGroupInterventionName']:
                                interventions_list.append(intervention)
                except:
                    arm_group_list = []

                try:
                    resp_locations_list = protocol_section['ContactsLocationsModule']['LocationList']['Location']
                    locations_list = []
                    for loc in resp_locations_list:
                        location = ''
                        if 'LocationFacility' in loc:
                            location = loc['LocationFacility'] + ', '
                        if 'LocationCity' in loc:
                                location = location + loc['LocationCity'] + ', '
                        if 'LocationState' in loc:
                                location = location + loc['LocationState'] + ', '
                        if 'LocationCountry' in loc:
                                location = location + loc['LocationCountry']
                        locations_list.append(location)

                except:
                    locations_list = []


                conditions = self.append_list_items(conditions_list)
                Interventions = self.append_list_items(interventions_list)
                Locations = self.append_list_items(locations_list)

                extra_info = {'status': status, 'conditions': conditions, 'Interventions': Interventions,
                          'Locations': Locations, 'First Posted': first_post}

                self.yield_research_sources(
                    article_id=id,
                    product_id=product_id,
                    created_date=last_updated_post,
                    author_name=product_id,
                    description="",
                    full_text=full_text,
                    product_url=product_url,
                    title=title,
                    url=title_link,
                    article_link= title_link,
                    extra_info= extra_info,
                )


    def append_list_items(self, item_list):
        item_list_len = len(item_list)-1
        items_appended = ''
        for item in item_list:
            if item_list_len == 0:
                items_appended += item
                break
            if item_list.index(item) == item_list_len:
                items_appended += item
            else:
                current_item = item + '|'
                items_appended += current_item
        return items_appended
