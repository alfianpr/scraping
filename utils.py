import pandas as pd
from datetime import datetime

def to_excel(df, name_file):
    data = df.to_excel(f'data_scrape/{name_file}_{int(datetime.now().timestamp())}.xlsx')
    return data

def to_csv(df, name_file):
    data = df.to_csv(f'data_scrape/{name_file}_{int(datetime.now().timestamp())}.csv')
    return data