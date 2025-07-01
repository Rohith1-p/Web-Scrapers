import scrapy, json
import requests
from .setuserv_spider import SetuservSpider


class QuoraSpider(SetuservSpider):
    name = 'quora-answers'

    def __init__(self, mongo_host, mongo_port, mongo_db, mongo_collection, document_id, env):
        super().__init__(mongo_host, mongo_port, mongo_db, mongo_collection, document_id, self.name, env)
        self.logger.info("quora-answers process start")
        assert self.source == 'quora'

    def start_requests(self):
        self.logger.info("Starting requests")
        for product_url, product_id in zip(self.start_urls, self.product_ids):
            media_entity = {'url': product_url, 'id': product_id}
            media_entity = {**media_entity, **self.media_entity_logs}
            keyword = product_id
            url = 'https://www.quora.com/graphql/gql_para_POST?q=SearchResultsListQuery'
            page = 9
            str_page = str(page)
            payload = self.get_payload()
            payload = payload.replace("keyword", keyword)
            payload = payload.replace("page", str_page)
            headers = self.get_headers()
            response = requests.request("POST", url, headers=headers, data=payload)
            self.parse_response(page, keyword, response, media_entity)
            self.logger.info(f"Generating reviews for {product_url} and {product_id}")

    def parse_response(self, page, keyword, response,media_entity):
        product_id = media_entity['id']
        product_url = media_entity['url']
        res = json.loads(response.text)
        res = res['data']['searchConnection']['edges']
        if res:
            for item in res:
                _id=item['node']['objectId']
                try:
                    questions = item['node']['question']['title']
                    questions = json.loads(questions)
                    questions = questions['sections'][0]['spans'][0]['text']
                except:
                    questions = ''
                try:
                    answer_list = ''
                    answers = item['node']['previewAnswer']['content']
                    answers = json.loads(answers)
                    for i in range(len(answers['sections'])):
                        sent = answers['sections'][i]['spans'][0]['text']
                        answer_list += sent + " "
                        answer_list = answer_list[:49998]
                except:
                    answer_list = ''
                try:
                    first_name = item['node']['previewAnswer']['author']['names'][0]['givenName']
                    last_name = item['node']['previewAnswer']['author']['names'][0]['familyName']
                    names = first_name + " " + last_name
                except:
                    names = ''
                try:
                    links= item['node']['question']['facebookSharePermalink']
                except:
                    links = ''
                try:
                    view   = item['node']['previewAnswer']['numViews']
                except:
                    view =  ''
                try:
                    upvote = item['node']['previewAnswer']['numUpvotes']
                except:
                    upvote = ''
                try:
                    aid = item['node']['previewAnswer']['aid']
                    print("aid   ******* ",aid)
                    comment_url = "https://www.quora.com/graphql/gql_para_POST?q=AnswerCommentLoaderInnerQuery"
                    comment_headers = self.comments_headers()
                    comment_payload = self.comments_payload()
                    comment_payload = comment_payload.replace('change_payload',str(aid))
                    print("comment_payload ",comment_payload)
                    response = requests.request("POST", comment_url, headers=comment_headers, data=comment_payload)
                    response = response.text
                    print(type(response))
                    response = json.loads(response)
                    comments = response['data']['answer']['allCommentsConnection']['edges']
                    list_comments = []
                    for main in range(len(comments)):
                        # print(len(comments))
                        comment = comments[main]['node']['content']
                        comment = json.loads(comment)
                        comment = comment['sections'][0]['spans'][0]['text']
                        print(comment)
                        list_comments.append(comment)
                        node = comments[main]['node']
                        # print(node)
                        list_comments = self.replies(node, list_comments)
                        print(list_comments)

                except:
                    aid = ''
                    list_comments = ''



                self.yield_quora(
                    product_id=product_id,
                    product_url=product_url,
                    _id=_id,
                    questions=questions,
                    answers=answer_list,
                    links=links,
                    names=names,
                    views = view,
                    upvotes = upvote,
                    answer_id=aid,
                    comments=list_comments)

            self.logger.info("Sending request for next page")
            keyword = product_id
            url = 'https://www.quora.com/graphql/gql_para_POST?q=SearchResultsListQuery'
            page += 10
            str_page = str(page)
            payload = self.get_payload()
            payload = payload.replace("keyword", keyword)
            payload = payload.replace("page", str_page)
            headers = self.get_headers()
            response = requests.request("POST", url, headers=headers, data=payload)
            self.parse_response(page, keyword, response, media_entity)

    def replies(self,node, list_comments):
        # print(node["repliesConnection"]['edges'])
        # print('inthe replies')

        # print(node)
        print(len(node["repliesConnection"]['edges']))
        try:
            if len(node["repliesConnection"]['edges']) != 0:
                print('in the if ')
                sub_replies = node["repliesConnection"]['edges']
                for i in range(len(sub_replies) + 1):
                    print("in the for loop")
                    content = sub_replies[i]['node']['content']
                    comment = json.loads(content)

                    comment = comment['sections'][0]['spans'][0]['text']
                    # print("in the function ",comment)
                    list_comments.append(comment)
                    # print(list_comments)

                    next_node = sub_replies[i]['node']
                    list_comments = self.replies(next_node, list_comments)
            return list_comments
        except:
            return list_comments

    @staticmethod
    def get_payload():
        payload = '''{"queryName":"SearchResultsListQuery","extensions":
        {"hash":"9783e54a95b3cb7683b4af15e712ca4f0d54a819a37850136f0c58e8d4f38551"},
        "variables":{"query":"keyword","disableSpellCheck":null,"resultType":"all_types",
        "author":null,"time":"all_times","first":10,"after":"page","tribeId":null}}'''
        return payload

    @staticmethod
    def get_headers():
        headers = {
            'authority': 'www.quora.com',
            'content-type': 'application/json',
            'cookie': 'm-b=ICVd-l_o4FlHW7UzZ5HaCw==; m-b_lax=ICVd-l_o4FlHW7UzZ5HaCw==; m-b_strict=ICVd-l_o4FlHW7UzZ5Ha'
                      'Cw==; m-s=BxnOakAmMKxl_AHKv8bY7w==; m-dynamicFontSize=regular; _fbp=fb.1.1649124897945.11610274'
                      '46; G_ENABLED_IDPS=google; __gads=ID=a6af18db50b0de1d:T=1649124898:S=ALNI_Mbx-EqyjG59CGwInJE_0k'
                      'QaWgAYRQ; m-sa=1; m-ans_frontend_early_version=3fe00c6550d0a922; __aaxsc=2; __stripe_mid=006230'
                      '53-ac49-40a6-9b32-1f2c8f2ca3bf9c1a83; g_state={"i_l":0}; m-lat=5IWPvZdznvvl_F5MhbpFdg==; m-logi'
                      'n=1; m-uid=1827736423; m-theme=light; _gcl_au=1.1.2103918153.1650954607; _scid=7314cf4b-6307-49'
                      'e3-8acb-ad240da13c27; G_AUTHUSER_H=0; _sctr=1|1650911400000; __gpi=UID=00000505bfb22614:T=16509'
                      '06240:RT=1651210861:S=ALNI_MYQU12OXcPYndLMChsYGcsvJ8Sz6g; aasd=1%7C1651217187265; __stripe_sid='
                      'c71f24f5-b4f3-493a-a0c9-8ddda3f42f1c51a364',
            'quora-formkey': '1ba9eb8486b6d60254e69a1cf83e285b',
                   }

        return headers

    @staticmethod
    def comments_payload():
        payload = '''{"queryName":"AnswerCommentLoaderInnerQuery","extensions":{"hash":"f9fd6b699e3c60821b9c08c9b195b542f68d8d7b677c784ff2f39da31294f843"},"variables":{"aid":change_payload,"first":10}}'''
        return payload

    @staticmethod
    def comments_headers():

        headers = {
            'authority': 'www.quora.com',
            'content-type': 'application/json',
            'cookie': 'm-b=ICVd-l_o4FlHW7UzZ5HaCw==; m-b_lax=ICVd-l_o4FlHW7UzZ5HaCw==; m-b_strict=ICVd-l_o4FlHW7UzZ5HaCw==; m-s=BxnOakAmMKxl_AHKv8bY7w==; m-dynamicFontSize=regular; _fbp=fb.1.1649124897945.1161027446; G_ENABLED_IDPS=google; __gads=ID=a6af18db50b0de1d:T=1649124898:S=ALNI_Mbx-EqyjG59CGwInJE_0kQaWgAYRQ; m-sa=1; m-ans_frontend_early_version=3fe00c6550d0a922; __stripe_mid=00623053-ac49-40a6-9b32-1f2c8f2ca3bf9c1a83; g_state={"i_l":0}; m-lat=5IWPvZdznvvl_F5MhbpFdg==; m-login=1; m-uid=1827736423; m-theme=light; _gcl_au=1.1.2103918153.1650954607; _scid=7314cf4b-6307-49e3-8acb-ad240da13c27; _sctr=1|1650911400000; G_AUTHUSER_H=0; __aaxsc=2; aasd=1%7C1652261600214; __stripe_sid=52843ef4-69a9-4a0e-97d6-d5f9987688c0e964d5; __cf_bm=tUxKKYMXCSjRWTugLLvx9ZpvzYGjLWo93lnzWS1hyFc-1652334519-0-ASPB6kkL+2/LTx4oWvVCXYCx4LTXrLW9iyrje3ZWr/b3MEmxsDnK80huofEfG0QYmmUv1YkcslOVY91ccef0Rhk=; __gpi=UID=00000505bfb22614:T=1650906240:RT=1652334912:S=ALNI_MYQU12OXcPYndLMChsYGcsvJ8Sz6g; m-b=ICVd-l_o4FlHW7UzZ5HaCw==; m-b_lax=ICVd-l_o4FlHW7UzZ5HaCw==; m-b_strict=ICVd-l_o4FlHW7UzZ5HaCw==; m-s=BxnOakAmMKxl_AHKv8bY7w==',
            'quora-formkey': '1ba9eb8486b6d60254e69a1cf83e285b',
        }
        return headers
