import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine
import difflib
import re

# basedata = pd.read_csv('basedata.csv')
# meidata = pd.read_csv('meiaadata.csv')

engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server")

def read_from_database(table):
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(sql=query, con=engine)
    cols = data.columns
    if "Date" in cols:
        data["Date"] = data["Date"].astype('datetime64[ns]')
    if "date" in cols:
        data["date"] = data["date"].astype('datetime64[ns]')
    return data.apply(lambda x: pd.to_numeric(x.astype(str).str.strip('"').str.strip('$').str.replace(',',''), errors='coerce')).fillna(data)


canal_list = read_from_database('canals_list')
mine = read_from_database('mine')
port = read_from_database('port')
vessel = read_from_database('vessel')
currency = read_from_database('currency')

# to dict
canal_store = dict(canal_list.loc[:, ['canals_id', 'canals']].values)
mine_store = dict(mine.loc[:, ['mine_id', 'mine']].values)
port_store = dict(port.loc[:, ['port_id', 'port']].values)
vessel_store = dict(vessel.loc[:, ['vessel_id', 'vessel_class']].values)
currency_store = dict(currency.loc[:, ['currency_id', 'currency']].values)

def restruct():
    store = {**canal_store, **mine_store, **port_store, **vessel_store, **currency_store}
    xl = pd.ExcelFile('cbix2input.xlsx')
    sheets = xl.sheet_names[:-4]
    dfs = {}
    for sh in sheets:
        name = f"cbix_{'_'.join(sh.split(' '))}"
        name = re.compile('[^a-zA-Z_]').sub('', name)
        name = "cbix2" + name[4:]
        print(name)
        data = read_from_database(name)
        data = data.replace(store)
        # data.loc[0] = data.loc[0].astype(float, errors='ignore').astype(int, errors='ignore').fillna(data.loc[0])
        # print(data.loc[0].values)
        vals = [str(a).replace('\n', ' ').replace('\r', ' ') if not a is None else a for a in data.loc[0].values]
        data.columns = vals
        data = data[1:]
        data.drop(columns=["creation_date", "updation_date"], inplace=True)
        data = data.replace({None: np.nan})
        cols = data.columns
        if "Date" in cols:
            data["Date"] = data["Date"].astype('datetime64[ns]')
        if "date" in cols:
            data["date"] = data["date"].astype('datetime64[ns]')
        data.reset_index(drop=True, inplace=True)
        # data.to_csv(f"outs/{name}.csv")
        dfs[name] = data
    return dfs




