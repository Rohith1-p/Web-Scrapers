import requests
import json
import pandas as pd
import datetime

def twitter_data():
    twitter_handles_df = pd.read_excel('twitter_companies.xlsx')
    twitter_companies = twitter_handles_df['handles']
    print('twitter_companies',twitter_companies[:10])
    data_dict = {'Twitter Handle':[],'Name':[],'Screen Name':[],'Description':[],'Website':[],'Location':[],"Followers":[],"Friends":[],'Joined Date':[],'Category':[]}
    for i in twitter_companies:
        handle = i.split('/')[-1]
        print('handle---> ',handle)
        url = f"https://twitter.com/i/api/graphql/CgrrOldPft4MOIWoMlHW8w/UserByScreenName?variables=%7B%22screen_name%22%3A%22{handle}%22%2C%22withSafetyModeUserFields%22%3Atrue%7D&features=%7B%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Atrue%2C%22subscriptions_verification_info_verified_since_enabled%22%3Atrue%2C%22highlights_tweets_tab_ui_enabled%22%3Atrue%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%7D"

        payload = {}
        headers = {
          'Authority': 'twitter.com',
          'Method': 'GET',
          'Path': f'/i/api/graphql/CgrrOldPft4MOIWoMlHW8w/UserByScreenName?variables=%7B%22screen_name%22%3A%22{handle}%22%2C%22withSafetyModeUserFields%22%3Atrue%7D&features=%7B%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Atrue%2C%22subscriptions_verification_info_verified_since_enabled%22%3Atrue%2C%22highlights_tweets_tab_ui_enabled%22%3Atrue%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%7D',
          'Scheme': 'https',
          'Accept': '*/*',
          'Accept-Encoding': 'gzip, deflate, br',
          'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7,zh;q=0.6,nl;q=0.5',
          'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA',
          'Content-Type': 'application/json',
          'Cookie': '_ga=GA1.2.1588629361.1685425907; _gid=GA1.2.424062713.1685425907; guest_id=v1%3A168542590683419525; guest_id_marketing=v1%3A168542590683419525; guest_id_ads=v1%3A168542590683419525; g_state={"i_l":0}; kdt=2xV6ks23wtCPIQrBD4CJvWjrRQbgvbQdqdy29Sad; auth_token=3e046146f5a6e6a0ae6953f30db8ce0cc81cbb6a; ct0=6b89c894664d64be6a029aa9f99ac99f623b4a0091cda83b46bc83dd40d213a77232d5cdd31a3d599ad8cac532a27ed10d532def24168542b8efb53539f00f962af9f72887d9eeba89b76e027eccc718; twid=u%3D1514625477218287617; lang=en; external_referer=padhuUp37zh%2BakC7OqDpELiW7RSN8JEP|0|8e8t2xd8A2w%3D; personalization_id="v1_1IpyG0hx1BLkZBSbiSLVzQ=="; ct0=4e00b24bad87f669ab7ea81cff367e7e41543cc7800ff95acb4c264164242e064f78df88ff3ecd617ccf6dab91cc24e8eea79b3b849654cc3c992617fff26ee23bf8a3d86c8e58a5f0cb3e2256017042; guest_id=v1%3A168546064796180477; guest_id_ads=v1%3A168546064796180477; guest_id_marketing=v1%3A168546064796180477; personalization_id="v1_XlgxHzjj0ZD0XE1U84OnjA=="',
          'Dnt': '1',
          'Referer': 'https://twitter.com/0xKYCinc',
          'Sec-Ch-Ua': '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
          'Sec-Ch-Ua-Mobile': '?0',
          'Sec-Ch-Ua-Platform': '"macOS"',
          'Sec-Fetch-Dest': 'empty',
          'Sec-Fetch-Mode': 'cors',
          'Sec-Fetch-Site': 'same-origin',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
          'X-Csrf-Token': '6b89c894664d64be6a029aa9f99ac99f623b4a0091cda83b46bc83dd40d213a77232d5cdd31a3d599ad8cac532a27ed10d532def24168542b8efb53539f00f962af9f72887d9eeba89b76e027eccc718',
          'X-Twitter-Active-User': 'yes',
          'X-Twitter-Auth-Type': 'OAuth2Session',
          'X-Twitter-Client-Language': ''
        }


        response = requests.request("GET", url, headers=headers, data=payload)

    #     print(response.text)
        data = json.loads(response.text)
        print('data',data)
        try:    
            desc = data['data']['user']['result']['legacy']['description']
        except:
            continue

        print(handle, "-----> ",desc)

        try:
            joined_data = data['data']['user']['result']['legacy']['created_at']
        except:
            joined_data = '-'

        try:
            website_link = data['data']['user']['result']['legacy']['entities']['url']['urls'][0]['expanded_url']
        except:
            website_link = '-'

        try:
            location = data['data']['user']['result']['legacy']['location']
        except:
            location = '-'
        try:
            followers = data['data']['user']['result']['legacy']['followers_count']
        except:
            followers = '-'

        try:
            friends = data['data']['user']['result']['legacy']['friends_count']
        except:
            friends = '-'
        try:
            name = data['data']['user']['result']['legacy']['name']
        except:
            name = '-'

        try:
            screen_name = data['data']['user']['result']['legacy']['screen_name']
            screen_name = '@'+screen_name
        except:
            screen_name = '-'

        try:
            category = data['data']['user']['result']['professional']['category'][0]['name']
        except:
            category = '-'




        data_dict['Twitter Handle'].append(i)
        data_dict['Name'].append(name) 
        data_dict['Description'].append(desc) 
        data_dict['Website'].append(website_link) 
        data_dict['Location'].append(location) 
        data_dict['Followers'].append(followers)
        data_dict['Friends'].append(friends)
        data_dict['Joined Date'].append(joined_data)
        data_dict['Screen Name'].append(screen_name)
        data_dict['Category'].append(category)

    
    
    # current_datetime = datetime.datetime.now()

    # # Format the date and time as a string
    # timestamp = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

    # Create the file name with the timestamp
    # file_name = f"twitter_profiles_data.xlsx"



    
    data_df = pd.DataFrame(data_dict)
    # data_df.to_excel(file_name)
    return data_df 
