import requests
import pandas as pd
import re
import cloudscraper
import json
import random
import time

SCHEMA = {
    "condition":"object",
    "created_at":"datetime64[ns]",
    "id":"object",
    "max_quantity":"object",
    "merchant_return_insurance":"bool",
    "min_quantity":"Int64",
    "name":"object",
    "price":"Int64",
    "rush_delivery":"bool",
    "sku_id":"object",
    "state":"object",
    "stock":"Int64",
    "weight":"Int64",
    "category_url":"object",
    "rating_average_rate":"float64",
    "rating_user_count":"Int64",
    "sla_type":"object",
    "sla_value":"object",
    "specs_brand":"object",
    "stats_interest_count":"Int64",
    "stats_sold_count":"Int64",
    "stats_view_count":"Int64",
    "stats_waiting_payment_count":"float64",
    "store_address_city":"object",
    "store_address_province":"object",
    "store_brand_seller":"bool",
    "store_delivery_time":"object",
    "store_id":"object",
    "store_description":"object",
    "store_level_name":"object",
    "store_name":"object",
    "store_premium_level":"object",
    "store_premium_top_seller":"bool",
    "store_rejection_recent_transactions":"Int64",
    "store_rejection_rejected":"Int64",
    "store_reviews_negative":"Int64",
    "store_reviews_positive":"Int64",
    "store_sla_type":"object",
    "store_sla_value":"Int64",
    "store_subscriber_amount":"Int64",
    "store_url":"object",
    "warranty_cheapest":"bool",
    "deal_applied_date":"datetime64[ns]",
    "deal_discount_price":"Int64",
    "deal_expired_date":"datetime64[ns]",
    "deal_original_price":"Int64",
    "deal_percentage":"Int64",
    "url":"object",
    "timestamp":"datetime64[ns]"
}

ADD_COL_TYPE = {
    'datetime': ['timestamp','created_at','deal_applied_date','deal_expired_date'],
    'bool_str': ['merchant_return_insurance', 'rush_delivery', 'store_brand_seller', 'store_premium_top_seller', 'warranty_cheapest'],
}

def get_access_token():
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    }
    r = requests.get("https://bukalapak.com", headers=headers)
    access_token_search = re.search("\"access_token\":\"(.*?)\"",r.text)
    access_token = access_token_search.group()[16:-1]
    return access_token

def get_products(params, access_token, 
                page = 60, URL="https://api.bukalapak.com/multistrategy-products"):
    df_item_list = []
    index = 1
    scraper = cloudscraper.create_scraper()
    while index <= page:
        payload = {
            "offset": ((index-1)*30),
            "page": index,
            "access_token": access_token,
            **params
        }
    res = scraper.get(URL, params=payload)

    sleep_time = random.randint(10, 50)
    time.sleep(sleep_time/1000)
    if res.status_code == 200:
        body = res.json()
        df_main = pd.json_normalize(body)
        dataitem_json = json.loads(pd.Series.to_json(df_main["data"]))
        df_item = pd.json_normalize(dataitem_json, record_path="0")
        df_item_list.append(df_item)
    if res.status_code != 200:
        raise ValueError("Error returned: {}".format(res.status_code))
    index = index + 1

    df_scraper = pd.concat(df_item_list, ignore_index=True)
    return df_scraper

def clean_df(df_scraper, timestr, SCHEMA=SCHEMA, ADD_COL_TYPE=ADD_COL_TYPE):
    df_scraper.columns = [col.replace(' ', '_').replace('.', '_') for col in df_scraper.columns]
    df_scraper['timestamp'] = pd.to_datetime(timestr, format='%Y-%m-%d')

    df_scraper_clean = pd.DataFrame()
    for col in SCHEMA:
        df_scraper_clean[col] = df_scraper[col]
    
    for col in ADD_COL_TYPE['bool_str']:
        df_scraper_clean[col] = df_scraper_clean[col].astype('bool')

    for col in ADD_COL_TYPE['datetime']:
        df_scraper_clean[col] = pd.to_datetime(df_scraper_clean[col],
                                format="%Y-%m-%dT%H:%M:%SZ")
    
    df_scraper_clean = df_scraper_clean.astype(SCHEMA).reset_index(drop=True)
    return df_scraper_clean