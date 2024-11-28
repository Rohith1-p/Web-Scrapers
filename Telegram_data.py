from telethon.sync import TelegramClient, connection
from telethon.tl.types import ChannelParticipantsAdmins
import asyncio
import pandas as pd

async def check_admin(group_name):
    api_id = #Enter your api_id
    api_hash = 'Enter your hash key'


 
    async with TelegramClient('session_name', api_id, api_hash) as client:
        try:
            group_entity = await client.get_entity(group_name)
        except:
            group_entity = 'No group'
        
        tele_link_list = []
        try:
            participants = await client.get_participants(group_entity, filter=ChannelParticipantsAdmins)

            
            for participant in participants:
                # print('participant.username----->',participant.username)
                tele_link_list.append(participant.username)

            # print("User", username, "is not an admin in", group_name)
        except:
            tele_link_list.append('Telegram group restricted to scrape')
            pass

        return tele_link_list



# channel_urls_df = pd.read_excel('twitter_data5Jun.xlsx')
# channel_urls = list(channel_urls_df['Telegram'])
# names = list(channel_urls_df['Screen Name'])

# handles = {'Name':[],'account':[],'profiles':[]}
# # channel_urls = [i for i in channel_urls if str(i) != 'nan']
# print('channel_urls',channel_urls)
# for i in range(len(channel_urls)):
#     if 'https://t.me' in str(channel_urls[i]):
#        tele_link =  asyncio.run(check_admin(channel_urls[i]))
#        print('tele_link',tele_link)
#        handles['Name'].append(names[i])
#        handles['account'].append(channel_urls[i])
#        handles['profiles'].append(tele_link)
#     else:
#         handles['Name'].append(names[i])
#         handles['account'].append(channel_urls[i])
#         handles['profiles'].append(' ')


# handles_df = pd.DataFrame(handles)
# handles_df.to_excel('Telegram_groups_admins_links.xlsx')

