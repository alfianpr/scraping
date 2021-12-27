import pandas as pd
from datetime import datetime

def to_csv(df, name_file):
    data = df.to_csv(f'data_scrape/{name_file}_{int(datetime.now().timestamp())}.csv')
    return data