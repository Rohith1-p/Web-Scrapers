import requests
from scrapy import Selector

url = "https://www.beamline.fund/portfolio"

payload = {}
headers = {
  'Cookie': 'XSRF-TOKEN=1699899543|Fqkxrsc7nhTm; ssr-caching=cache#desc=hit#varnish=hit_miss#dc#desc=fastly_g'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response)

resp = Selector(text=response.text)


data_dict = {'Company Name':[],'Website':[],'Description1':[],'Description2:':[],'Logo Url':[]}
for i in resp.css('div[data-testid="inline-content"] div[data-testid="mesh-container-content"] div[tabindex="0"]'):
    image_url = i.css('wow-image[class^="HlRz5e"] img ::attr(src)').extract_first()
    try:
      image_url = image_url.replace("blur_3,", "")
    except:
       image_url = ''

    description1 = i.css('span[style="color:#19171E;"] ::text').extract()
    desp_str = ''.join(description1)
    desp2 = i.css('span[style="color:#605E5E;"] ::text').extract()
    desp2_str = ''.join(desp2[:-1])

    
    print("%%%%%%%%%%%%%%%%%%%")
    if len(desp2_str) <4:
       print('desp2_str',desp2_str)
       print("in desp2")
       desp2_str = i.css('span[style="color:#626262;"] ::text').extract()
       desp2_str = ''.join(desp2_str)



    website_url = i.css('a[target="_blank"] ::attr(href)').extract_first()
    company_title = i.css('span[style="color:#000000;"] ::text').extract_first()
    if not company_title:
       company_title = i.css('span[style="color:rgb(0, 0, 0); font-family:din-next-w01-light, din-next-w02-light, din-next-w10-light, sans-serif;"] ::text').extract_first()

    data_dict['Company Name'].append(company_title)
    data_dict['Website'].append(website_url)
    data_dict['Description1'].append(desp_str)
    data_dict['Description2:'].append(desp2_str)
    data_dict['Logo Url'].append(image_url)

import pandas as pd

df = pd.DataFrame(data_dict)
df.to_excel('beamline_Data.xlsx')
