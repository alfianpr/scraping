from bukalapak import *
import pandas as pd

page = 2
category = 2269

params = {
    "prambanan_override" : "true",
    "category_id" : category,
    "sort" : "bestselling",
    "limit" : 30,
    "facet" : "true",
    # "brand": "true"
}

access_token = get_access_token()
df_scraper = get_products(params,access_token, page=page)
df_scraper_final = clean_df(df_scraper, timestr)

df_scraper_final.to_excel("skincare.xlsx")