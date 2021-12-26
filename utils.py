import pandas as pd

def to_excel(df, name_file):
    data = df.to_excel("{name_file}.xlsx")
    return data