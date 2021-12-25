import requests
import pandas as pd
import re
import cloudscraper
import json

def get_access_token():
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    }
    r = requests.get("https://bukalapak.com", headers=headers)
    access_token_search = re.search("\"access_token\":\"(.*?)\"",r.text)
    access_token = access_token_search.group()[16:-1]
    return access_token

def get_product(params, access_token, 
                page = 60, URL="https://api.bukalapak.com/multistrategy-products"):

payload = {
    "category_id": 304,
    "offset": 0,
    "facet": True,
    "page": 1,
    "access_token": access_token
}

URL="https://api.bukalapak.com/multistrategy-products"
scraper = cloudscraper.create_scraper()

res = scraper.get(URL, params=payload)

df_item_list=[]
body = res.json()
df_main = pd.json_normalize(body)
dataitem_json = json.loads(pd.Series.to_json(df_main["data"]))
df_item = pd.json_normalize(dataitem_json, record_path="0")
df_item_list.append(df_item)


print(df_item_list)