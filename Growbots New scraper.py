import requests
import json  
import pandas as pd

children_naics_list = ['111', '112', '113', '114', '115', '211', '212', '213', '221', '236', '237', '238', '311', '312', '313', '314', '315', '316', '321', '322', '323', '324', '325', '326', '327', '331', '332', '333', '334', '335', '336', '337', '339', '423', '424', '425', '441', '442', '443', '444', '445', '446', '447', '448', '451', '452', '453', '454', '481', '482', '483', '484', '485', '486', '487', '488', '491', '492', '493', '511', '512', '515', '517', '518', '519', '521', '522', '523', '524', '525', '531', '532', '533', '541', '551', '561', '562', '611', '621', '622', '623', '624', '711', '712', '713', '721', '722', '811', '812', '813', '814', '921', '922', '923', '924', '925', '926', '927', '928']

children_naics_sub_list = ['1111', '1112', '1113', '1114', '1119', '1121', '1122', '1123', '1124', '1125', '1129', '1131', '1132', '1133', '1141', '1142', '1151', '1152', '1153', '2111', '2121', '2122', '2123', '2131', '2211', '2212', '2213', '2361', '2362', '2371', '2372', '2373', '2379', '2381', '2382', '2383', '2389', '3111', '3112', '3113', '3114', '3115', '3116', '3117', '3118', '3119', '3121', '3122', '3131', '3132', '3133', '3141', '3149', '3151', '3152', '3159', '3161', '3162', '3169', '3211', '3212', '3219', '3221', '3222', '3231', '3241', '3251', '3252', '3253', '3254', '3255', '3256', '3259', '3261', '3262', '3271', '3272', '3273', '3274', '3279', '3311', '3312', '3313', '3314', '3315', '3321', '3322', '3323', '3324', '3325', '3326', '3327', '3328', '3329', '3331', '3332', '3333', '3334', '3335', '3336', '3339', '3341', '3342', '3343', '3344', '3345', '3346', '3351', '3352', '3353', '3359', '3361', '3362', '3363', '3364', '3365', '3366', '3369', '3371', '3372', '3379', '3391', '3399', '4231', '4232', '4233', '4234', '4235', '4236', '4237', '4238', '4239', '4241', '4242', '4243', '4244', '4245', '4246', '4247', '4248', '4249', '4251', '4411', '4412', '4413', '4421', '4422', '4431', '4441', '4442', '4451', '4452', '4453', '4461', '4471', '4481', '4482', '4483', '4511', '4512', '4522', '4523', '4531', '4532', '4533', '4539', '4541', '4542', '4543', '4811', '4812', '4821', '4831', '4832', '4841', '4842', '4851', '4852', '4853', '4854', '4855', '4859', '4861', '4862', '4869', '4871', '4872', '4879', '4881', '4882', '4883', '4884', '4885', '4889', '4911', '4921', '4922', '4931', '5111', '5112', '5121', '5122', '5151', '5152', '5173', '5174', '5179', '5182', '5191', '5211', '5221', '5222', '5223', '5231', '5232', '5239', '5241', '5242', '5251', '5259', '5311', '5312', '5313', '5321', '5322', '5323', '5324', '5331', '5411', '5412', '5413', '5414', '5415', '5416', '5417', '5418', '5419', '5511', '5611', '5612', '5613', '5614', '5615', '5616', '5617', '5619', '5621', '5622', '5629', '6111', '6112', '6113', '6114', '6115', '6116', '6117', '6211', '6212', '6213', '6214', '6215', '6216', '6219', '6221', '6222', '6223', '6231', '6232', '6233', '6239', '6241', '6242', '6243', '6244', '7111', '7112', '7113', '7114', '7115', '7121', '7131', '7132', '7139', '7211', '7212', '7213', '7223', '7224', '7225', '8111', '8112', '8113', '8114', '8121', '8122', '8123', '8129', '8131', '8132', '8133', '8134', '8139', '8141', '9211', '9221', '9231', '9241', '9251', '9261', '9271', '9281']

for naics_code in children_naics_list:
    data_dict = {'Contacted':[],	'Fields':[],	'facebook_link':[],	'linkedin_link':[],	'twitter_link':[],	'company_name':[],	'description':[],	'email_domain':[],	'funding_rounds':[],	'geolocation':[], 'headquarters_address':[],	'headquarters_phone':[],	'industries':[],	'naics_codes':[],	'logo_url':[],	'number_of_employees':[],	'size':[],	'specialties':[],	'technologies':[],	'revenue':[],	'total_funding':[],	'website':[],	'profile_id':[]}
    print("nacis_code ----->", naics_code)
    url = "https://app.growbots.com/api/v1/profile_viewer/companies/_search"
    
    page = 1
    offset = 0
    pages = 49
    while page <= pages:
    # for page in range(50):
        print("page -->", page)
        payload = json.dumps({
          "query": {"naics":[naics_code],
            "companySize": {
              "from": 1,
              "to": 200
            },
            "numberOfLocations": {},
            "revenue": {
              "from": 1
            },
            "foundingYear": {},
            "technologies": [],
            "wwwRank": {
              "from": 1
            },
            "companyAdvancedQuery": [],
            "companyLocation": [],
            "headquartersLocation":[{"value":{"city":'null',"administrativeArea":'null',"country":"United States","negated":'false'}},{"value":{"city":'null',"administrativeArea":'null',"country":"China","negated":'false'}},{"value":{"city":'null',"administrativeArea":'null',"country":"Japan","negated":'false'}},{"value":{"city":"New Germany","administrativeArea":"Nova Scotia","country":"Canada","negated":'false'}},{"value":{"city":'null',"administrativeArea":'null',"country":"India","negated":'false'}},{"value":{"city":"Frances","administrativeArea":"South Australia","country":"Australia","negated":'false'}},{"value":{"city":'null',"administrativeArea":'null',"country":"Brazil","negated":'false'}},{"value":{"id":"Russia","country":"Russia","city":'null',"administrativeArea":'null',"negated":'false'}}],
            "funding": {
              "lastAmount": {},
              "totalAmount": {},
              "lastDate": {},
              "lastStage": []
            },
            "companyName": []
          },
          "count": 200,
          "offset": offset
        })
    
        offset += 200
        page +=1
        headers = {
          'authority': 'app.growbots.com:',
          'method': 'POST',
          'path': '/api/v1/profile_viewer/companies/_search:',
          'scheme': 'https:',
          'Accept': '*/*',
          'Accept-Encoding': 'gzip, deflate, br',
          'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7,zh;q=0.6,nl;q=0.5',
          'Cache-Control': 'no-cache',
          'Content-Length': '332',
          'Content-Type': 'application/json',
          'Dnt': '1',
          'Gb-Request-Id': 'f18ca91790164a728bfcf770b7257fe0',
          'Origin': 'https://app.growbots.com',
          'Pragma': 'no-cache',
          'Referer': 'https://app.growbots.com/',
          'Sec-Ch-Ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
          'Sec-Ch-Ua-Mobile': '?0',
          'Sec-Ch-Ua-Platform': '"macOS"',
          'Sec-Fetch-Dest': 'empty',
          'Sec-Fetch-Mode': 'cors',
          'Sec-Fetch-Site': 'same-origin',
          'Sentry-Trace': '5b37bba88fec405483820ac68e44f0ec-97aa8f10b88932a6-1',
          'Session-Id': '3947e8f31a0b4628bf9833c681901d9a',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
        }
        
        response = requests.request("POST", url, headers=headers, data=payload)
        
        print(response)
    
        response_json = json.loads(response.text)
        companies_data= response_json['companies']
        total_companies = response_json['total']
        pages = total_companies//200
        print("#######Total Page ##########  ",pages)
        if pages > 49:
            pages = 49
        else:
            pass
        
        print(len(companies_data))
        
        for row in companies_data:
        
            contacted = row['contacted']
            all_industries = row['fields']['all_industries']
            company_facebook = row['fields']['company_facebook']
            company_linkedin = row['fields']['company_linkedin']
            company_name = row['fields']['company_name']
            company_twitter = row['fields']['company_twitter']
            description = row['fields']['description']
            email_domain = row['fields']['email_domain']
            funding_rounds = row['fields']['funding_rounds']
            geolocation = row['fields']['geolocation']
            headquarters_address = row['fields']['headquarters_address']
            headquarters_phone= row['fields']['headquarters_phone']
            industries = row['fields']['industries']
            logo_url = row['fields']['logo_url']
            naics_codes = row['fields']['naics_codes']
            number_of_employees = row['fields']['number_of_employees']
            revenue = row['fields']['revenue']
            size = row['fields']['size']
            specialties = row['fields']['specialties']
            technologies = row['fields']['technologies']
            total_funding = row['fields']['total_funding']
            website = row['fields']['website']
            profileId = row['profileId']
        
            
            data_dict['Contacted'].append(contacted)
            data_dict['Fields'].append(all_industries)
            data_dict['facebook_link'].append(company_facebook)
            data_dict['linkedin_link'].append(company_linkedin)
            data_dict['twitter_link'].append(company_twitter)
            data_dict['company_name'].append(company_name)
            data_dict['description'].append(description)
            data_dict['email_domain'].append(email_domain)
            data_dict['funding_rounds'].append(funding_rounds)
            data_dict['geolocation'].append(geolocation)
            data_dict['headquarters_address'].append(headquarters_address)
            data_dict['headquarters_phone'].append(headquarters_phone)
            data_dict['industries'].append(industries)
            data_dict['naics_codes'].append(naics_codes)
            data_dict['logo_url'].append(logo_url)
            data_dict['number_of_employees'].append(number_of_employees)
            data_dict['size'].append(size)
            data_dict['specialties'].append(specialties)
            data_dict['technologies'].append(technologies)
            data_dict['revenue'].append(revenue)
            data_dict['total_funding'].append(total_funding)
            data_dict['website'].append(website)
            data_dict['profile_id'].append(profileId)

    
    import datetime
    # Get the current date and time
    current_datetime = datetime.datetime.now()
    
    # Format the date and time into a string (e.g., "2023-09-27_14-35-15")
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create a file name using the formatted date and time
    file_name = f"growbots_companies_naics_code_{naics_code}_{formatted_datetime}.csv"
    
    print("Generated file name:", file_name)
    pd.DataFrame(data_dict).to_csv(file_name)    
        
