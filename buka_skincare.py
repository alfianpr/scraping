from bukalapak import get_access_token, get_products, clean_df
import pandas as pd
from prefect import task, Flow
from prefect.run_configs import LocalRun
from prefect.tasks.core.function import FunctionTask
from datetime import datetime
from utils import to_excel

get_access_token = FunctionTask(get_access_token)
get_products = FunctionTask(get_products)
clean_df = FunctionTask(clean_df)
to_excel = FunctionTask(to_excel)

page = 10
category = "2269"

params = {
    "prambanan_override" : "true",
    "category_id" : category,
    "sort" : "bestselling",
    "limit" : 30,
    "facet" : "true",
    # "brand": "true"
}

def prefect_flow():
    with Flow("buka_skincare", run_config=LocalRun()) as flow:
        timestr = datetime.today()
        access_token = get_access_token()
        df_scraper = get_products(params,access_token, page=page)
        df_scraper_final = clean_df(df_scraper, timestr)
        to_excel(df_scraper_final, name_file="buka_skincare")
    return flow

flow = prefect_flow()
#flow.run()
flow.register(project_name="scraping")