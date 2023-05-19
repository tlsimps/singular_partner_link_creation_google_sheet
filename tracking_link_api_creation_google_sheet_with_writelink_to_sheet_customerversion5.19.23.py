#Author: Travis Simpson
#last update 2023/05/19
#Tracking link creatiion - partner link types only

#Pulls data from https://docs.google.com/spreadsheets/d/11nXbVf3LymFzu5vfdWQKaes2NzoJiS_xDIHYe6rxZyY/edit#gid=1659652832 sheet 
#Use Case - I have a new app(s) that I want to launch. First set up partner configurations for all partners. Add App info to the Sheet. Run Script. 
#Code pulls Apps and Bundle IDs from Google sheet - then checks to see which partners are configured in Singular's Partner Configuration
#will Create Partner links for all Partners set up in Singular's Partner Configuration for the provided App/Bundle IDs
#Created links are written to the Generated Links tab
#Shows code example of how to get IDs from Helper APIs to build Tracking Link Creation API request -- NOT MEANT FOR USE IN PRODUCTION - this is meant to be used as an example only



import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import requests
import json
import yaml
import datetime

# Google Sheet Auth
service_account_file = "/Users/your/path/client_secret.json"
sheet_file_key = "11nXbVf3LymFzu5vfdWQKaes2NzoJiS_xDIHYe6rxZyY"
creds = Credentials.from_service_account_file(service_account_file, scopes=['https://www.googleapis.com/auth/spreadsheets'])
sheets_service = build("sheets", "v4", credentials=creds)

# Get data from the Google Sheet
sheet_data = sheets_service.spreadsheets().values().get(spreadsheetId=sheet_file_key, range='Tracking Link Creation Tool!A5:G').execute()
data = sheet_data.get("values", [])
columns = data[0]

# Load to df and clean
df = pd.DataFrame(data[1:], columns=columns)
df_jc = ['App Name', 'Platform', 'Bundle ID']
df[df_jc] = df[df_jc].apply(lambda x: x.str.lower())
#print(df[['App Name', 'Platform', 'Bundle ID']])

#links api
api_key = '<your api key>'
create_link_url = "https://api.singular.net/api/v1/singular_links/links"
domains_url = "https://api.singular.net/api/v1/singular_links/domains"
apps_url = "https://api.singular.net/api/v1/singular_links/apps"
configured_partners_url = "https://api.singular.net/api/v1/singular_links/configured_partners"
partner_details_url = "https://api.singular.net/api/v1/singular_links/all_partners"

# Get app data and join to Sheet df
apps_response = requests.get(url=apps_url, headers={'Authorization': api_key})
app_data = apps_response.json()
available_apps_df = pd.DataFrame(app_data['available_apps'])
avail_apps_jc = ['app', 'app_platform', 'app_longname']
available_apps_df[avail_apps_jc] = available_apps_df[avail_apps_jc].apply(lambda x: x.str.lower())
#print(available_apps_df[['app', 'app_platform', 'app_longname']])

merged_df = df.merge(available_apps_df, left_on = df_jc, right_on = avail_apps_jc)
#print(merged_df)

#link subdomain mapping -- maybe handle in Google sheet instead?
app_subdomain = {
    'Travis Singular Test App' : 'se',
    'Android Sample App' : 'se'
}
for app in app_data['available_apps']:
    if app['app'] in app_subdomain:
        app_subdomain[int(app['app_id'])] = app_subdomain.pop(app['app'])

#Get configured partners - we want to generate links for all partners, for apps in the sheet
params = {
    "app_site_id": merged_df['app_site_id']
}
configured_partners_response = requests.request("GET", configured_partners_url, headers={'Authorization': api_key})
configured_partners = yaml.safe_load(configured_partners_response.text)['available_partners']
print("configured_partners: %s" % configured_partners)

#get partner details
params = {
    "partner_id": ''
}
partner_details = requests.get(partner_details_url, params=params, headers={'Authorization': api_key})
partner_details_data = yaml.safe_load(partner_details.text)['partners']
print(partner_details_data)

#get link domains -- don't really need this with the links dictionary above
domains_response = requests.get(url=domains_url, headers={'Authorization': api_key})
links_data = domains_response.json()

df_result = pd.DataFrame()
for partner in configured_partners:
    partner_id = partner['singular_partner_id']
    partner_name = partner['singular_partner_display_name']
    app_site_id = partner['app_site_id']
    app_id = partner['app_id']
    # SANs does not support links
    if partner_name in ['Facebook', 'applesearchads', 'Snapchat', 'Twitter', 'AdWords', 'Yahoo Gemini', 'TikTok for Business (DO NOT MODIFY)']: 
        continue
    for pid in partner_details_data:
        if partner_id == pid['singular_partner_id']:
            support_reengagement = pid['support_reengagement']
            support_multiple_os = pid['support_multiple_os']   
    for index, row in merged_df.iterrows():
        if row['app_site_id'] == app_site_id:
            print("creating link for app %s(%s), for: %s(%s)" % (row['app'], row['app_id'], partner_name, partner_id))
            platform_key = "ios_redirection" if row['app_platform'] == "ios" else "android_redirection"
            payload = {
                "app_id": row['app_id'],
                "partner_id": partner_id,
                "link_type": "partner",
                "tracking_link_name": partner_name.replace(" ","") + "_" + row['Tracker Name'],
                "link_subdomain": app_subdomain[app_id],
                "link_dns_zone": "sng.link",
                "destination_fallback_url": "https://www.example.com/",
                platform_key: {
                    "app_site_id": app_site_id,
                    "destination_url": row['store_url'], 
                    "destination_deeplink_url": row['deep link'],
                    "destination_deferred_deeplink_url": row['deep link']
                },
                "enable_reengagement": support_reengagement
            }
            payload = json.dumps(payload)
            #print(payload)
            response = requests.request("POST", create_link_url, headers={'Authorization': api_key}, data=payload)
            response_data = yaml.safe_load(response.text)
            print(response_data)

            df_result = df_result.append({
                'app_name': row['app'],
                'platform': row['app_platform'],
                'bundle_id': row['app_longname'],
                'app_site_id': app_site_id,
                'app_id': app_id,
                'partner_id': partner_id,
                'partner_name': partner_name, 
                'tracking_link_id': response_data['tracking_link_id'] if 'error' not in response_data else None,
                'tracking_link_name': response_data['tracking_link_name'] if 'error' not in response_data else None,
                'click_tracking_link': response_data['click_tracking_link'] if 'error' not in response_data else None,
                'impression_tracking_link': response_data['impression_tracking_link'] if 'error' not in response_data else None,
                'error_code': response_data['error']['code'] if 'error' in response_data else None,
                'error_message': response_data['error']['message'] if 'error' in response_data else None,
                'creation_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }, ignore_index=True)
            print(df_result)
            # Write df_result to Google Sheet
            values = df_result.T.reset_index().T.values.tolist()
            body = {
            "values": values,
            }
            sheets_service.spreadsheets().values().update(
                spreadsheetId=sheet_file_key, range="Generated Links", valueInputOption="RAW", body=body
            ).execute()
