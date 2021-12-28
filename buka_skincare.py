from lib_bukalapak import get_access_token, get_products, clean_df
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

final_page = 350
category = "2269"

def prefect_flow():
    with Flow("buka_skincare", run_config=LocalRun()) as flow:
        timestr = datetime.today()
        access_token = get_access_token()
        df_scraper = get_products(category, final_page, access_token)
        df_scraper_final = clean_df(df_scraper, timestr)
        df_csv = to_excel(df_scraper_final, name_file="buka_skincare")
    return flow

flow = prefect_flow()
flow.run()
#flow.register(project_name="scraping")