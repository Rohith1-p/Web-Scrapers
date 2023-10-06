import unicodedata
import re
import pandas as pd
from scrapy.selector import Selector
import requests
import urllib.parse
import datetime





#  Install the Python Requests library:
# `pip install requests`


def send_request(url):
    print('in send_request')
    print('url',url)
    
    url = 'https://api.crawlbase.com/?token=Your crawl Base token='+ url

    response = requests.request("GET", url)#, headers=headers, data=payload)
    print('Response HTTP Status Code: ', response.status_code)
#     print('Response HTTP Response Body: ', response.content)
#     file_path = 'vovo1.html'
    return response



def remove_emoji(string):
    print('remove_emoji@@@@@@@@@',string)
    string = string.strip()
    # Remove emoji-like characters using regular expressions
    clean_string = re.sub(r'[^\w\s]', '', string)
#     clean_string = re.sub(r'\s', '', clean_string)
    pattern = re.compile("["
                         u"\U0001F600-\U0001F64F"  # emoticons
                         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                         u"\U0001F680-\U0001F6FF"  # transport & map symbols
                         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                         u"\U00002500-\U00002BEF"  # chinese characters
                         u"\U00002702-\U000027B0"
                         u"\U00002702-\U000027B0"
                         u"\U000024C2-\U0001F251"
                         u"\U0001f926-\U0001f937"
                         u"\U00010000-\U0010ffff"
                         u"\u200d"
                         u"\u2640-\u2642"
                         u"\u2600-\u2B55"
                         u"\u23cf"
                         u"\u23e9"
                         u"\u231a"
                         u"\u3030"
                         u"\ufe0f"
                         "]+", flags=re.UNICODE)

    return pattern.sub(r'', clean_string)


def get_crunchbase_data():
    from scrapy.selector import Selector
    company_names_df = pd.read_excel('Data Files/Twitter_data.xlsx')
    company_names = company_names_df['Name']
    print(company_names)

    screen_names = list(company_names_df['Screen Name'])

    crunchbase_links = {}
    for name in company_names:
    #     name = name.replace(',','')
    #     name = name.replace('.','')
    #     name = name.replace(' ','')
        try:
            if '|' in name:
                try:
                    val = name.split('|')
                    name = val[0]
                except:
                    continue
        except:
            continue
        try:
            if '-' in name:
                try:
                    val = name.split('-')
                    name = val[0]
                except:
                    continue
        except:
            continue
            

        output_string = remove_emoji(name)
        print("output_string -----> ",output_string)
        company_query = '+'.join(output_string.split(' '))+'+crunchbase'
        google_query = 'https://www.google.com/search?q='+company_query
        print(google_query)
        google_res = send_request(google_query)
        resp = Selector(text=google_res.text)
        crunch_link = resp.css('div[class="v7W49e"] div[class="MjjYud"] ::attr(href)').extract()
        missing = resp.css('div[class="TXwUJf"] ::text').extract()
        print(missing)
        
        

        url = google_query
        word_list = missing

        # Parsing the query string from the URL
        query_string = urllib.parse.urlparse(url).query

        # Extracting the value after 'q=' in the query string
        search_terms = urllib.parse.parse_qs(query_string)['q'][0]
        print('search_terms',search_terms)
        # Removing "Missing:", spaces, and "Must include:" from the word list
        cleaned_word_list = [word.strip() for word in word_list if word.strip() not in ['Missing:', '', 'Must include:']]

        # Splitting the search terms into individual words
        words = search_terms.split("+")

        # Checking if any of the final words (excluding "crunchbase") are present in the search terms
        print('cleaned_word_list',cleaned_word_list)
        if len(cleaned_word_list) !=0:
            for word in cleaned_word_list:
                print(word)
                if word in search_terms and word != "crunchbase":
                    print("crunchbase link not found.")
                    crunchbase_links[output_string] = "Company not found in Crunchbase"
                    break

                else:
                    try:
                        crunchbase_links[output_string] = crunch_link[0]
                    except:
                        crunchbase_links[output_string] = ''

        else:
            try:
                print("in the esle condition cleaned_word_list",cleaned_word_list)
                crunchbase_links[output_string] = crunch_link[0]
            except:
                crunchbase_links[output_string] = ''

                

                

    crunchbase_data = {'Cruncbase url':[],'Screen Name':[],'Company Name':[],'Geo Location':[],'Funding Amount':[],'Industries':[],'Founder Names':[],'Founder Links':[]}#,'Financial Summary':[]}
    for index, (name,url) in enumerate(crunchbase_links.items()):
        if url != 'Company not found in Crunchbase':
            print(name,'url',url)
            response = send_request(url)
    #         print(response.text)
            from scrapy.selector import Selector
            res = Selector(text = response.text)
            print(res)
            location = res.css('.icon_and_value .ng-star-inserted:nth-child(1) label-with-icon ::text').extract()
            geo_location = ''.join(location)
            funding = res.css('.spacer:nth-child(1) .link-primary ::text').extract()
            funds_raised = res.css('div[class="info"] field-formatter[linkcolor="primary"] span[class="component--field-formatter field-type-money ng-star-inserted"] ::text').extract_first()
            val = ''.join(funding)
            print(val)
            industries = res.css("chips-container ::text").extract()
            industries = ', '.join(industries)


            founders_names = res.css('.ng-star-inserted:nth-child(5) .field-type-identifier-multi .ng-star-inserted ::text').extract()
            founders = res.css('.ng-star-inserted:nth-child(5) .field-type-identifier-multi .ng-star-inserted ::attr(href)').extract()
            if len(founders_names) == 0:
                founders_names = res.css('.text_and_value .ng-star-inserted~ .ng-star-inserted+ .ng-star-inserted .field-type-identifier-multi .ng-star-inserted ::text').extract()
                founders = res.css('.text_and_value .ng-star-inserted~ .ng-star-inserted+ .ng-star-inserted .field-type-identifier-multi .ng-star-inserted ::attr(href)').extract()


            print('founders',founders)
            founders_dict = {}
            for i in range(len(founders)):
                ceo_link = 'https://www.crunchbase.com'+ founders[i]
                founders_res = send_request(ceo_link)
                founders_res_sc = Selector(text= founders_res.text)
                founder_profile_links = founders_res_sc.css('fields-card[class="ng-star-inserted"] link-formatter ::attr(href)').extract()
                founders_dict[founders_names[i]] = founder_profile_links
            print('founders_dict',founders_dict)
            crunchbase_data['Company Name'].append(name)
        #     crunchbase_data['About'].append()
            crunchbase_data['Geo Location'].append(geo_location)
            crunchbase_data['Funding Amount'].append(funds_raised)
            crunchbase_data['Industries'].append(industries)
            crunchbase_data['Founder Names'].append(founders_names)
            crunchbase_data['Founder Links'].append(founders_dict)
            crunchbase_data['Cruncbase url'].append(url)
            crunchbase_data['Screen Name'].append(screen_names[index])
            print(crunchbase_data)
        else:
            crunchbase_data['Cruncbase url'].append('Company not found in Crunchbase')
            crunchbase_data['Company Name'].append(name)
            crunchbase_data['Geo Location'].append(' ')
            crunchbase_data['Funding Amount'].append(' ')
            crunchbase_data['Industries'].append(' ')
            crunchbase_data['Founder Names'].append(' ')
            crunchbase_data['Founder Links'].append(' ')
            crunchbase_data['Screen Name'].append(screen_names[index])
            print('in else',crunchbase_data)

    current_datetime = datetime.datetime.now()

    # # Format the date and time as a string
    timestamp = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
    df = pd.DataFrame(crunchbase_data)
    filename = f'crunchbasedata{timestamp}.xlsx'
    df.to_excel(filename)
    return  df   
