import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine
import difflib
import re

engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

def read_from_database(table):
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(sql=query, con=engine)
    cols = data.columns
    # if "Date" in cols:
    #     data["Date"] = data["Date"].astype('datetime64[ns]')
    # if "date" in cols:
    #     data["date"] = data["date"].astype('datetime64[ns]')
    return data.apply(lambda x: pd.to_numeric(x.astype(str).str.strip('"').str.strip('$').str.replace(',',''), errors='coerce')).fillna(data)




all_foreignkeys = [ 'country', 'port', 'region']
store = {}
for a in all_foreignkeys:
    d = read_from_database(a)
    c = [f for f in d.columns if f[-3:] == '_id'][0]
    if c != 'group_id':
        s = dict(d.loc[:, [c, c[:-3]]].values)
    else:
        s = dict(d.loc[:, [c, 'groups']].values)
    store[a] = s
    # store = {**store, **s}

month = dict(read_from_database('month_id').loc[:, ['month', "month_id"]].values)
store["month"] = month


all_fk_maps = {
    "monthly_tradedata": {'Country': store['country'], 'Region': store['region'], "Port": store['port'], 'Customer': store['region']},
}   



pivoted_tables = [
    "monthly_summary_bauxite_requirement_supply",
    "monthly_alumina_by_province",
    "monthly_domestic_price_dmt",
    "monthly_import_by_country_dmt",
    "imported_bauxite_stocks",
    "monthly_avg_cfr_price",
    "monthly_average_viu_cfr_price"
]


all_names = [
    "monthly_fxrates",
    "monthly_rawdata",
    "monthly_cbixchart",
    "monthly_tradedata",
    "monthly_insurance_rate_in_cif",
]   


def dropIfExists(df, cols):
    for c in cols:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)
    return df

def rev_meltdown(df, idx=None, val='value', column='variable'):
    if idx is None:
        df = dropIfExists(df, ['creation_date', 'updation_date'])
        idx = list(df.columns)
        idx.remove(val)
    # return df.pivot(index= (idx)[val].aggregate('mean').unstack().reset_index()
    result = df.pivot(values=val, index=idx[:-1], columns=column).reset_index()
    return result


def final_touch(df, name):
    data = df.copy()
    data = data.replace({None: np.nan, "Not available": np.nan})
    cols = data.columns
    lo = []
    try:
        if "Date" in cols and not name in lo :
            data["Date"] = data["Date"].astype('datetime64[ns]')
        if "date" in cols and not name in lo:
            data["date"] = data["date"].astype('datetime64[ns]')
    except:
        print("A date exception here")
    data.reset_index(drop=True, inplace=True)
    # data.columns = pd.Series(data.columns).dt.normalize()
    return data

def convert_date_columns(data):
    temp_g = pd.DataFrame(list(data.columns), columns=['T'])
    temp_g['T'] = pd.to_datetime(temp_g['T'], errors='coerce').fillna(temp_g['T'])
    data.columns = temp_g['T'].values
    print(data.columns)
    return data

def restruct():
    dfs = {}
    tnc = ['imported_bauxite_stocks']
    for sh in all_names:
        name = sh
        print(name)
        data = read_from_database(name)
        # data = data.replace(store)
        if name in all_fk_maps:
            print(name)
            for f in all_fk_maps[name]:
                data[f] = data[f].apply(lambda x: all_fk_maps[name][f].get(x,x))
        data.drop(columns=["creation_date", "updation_date", f"{name}_id"], inplace=True)
        data = convert_date_columns(data) if name in tnc else data
        dfs[name] = final_touch(data, name)
        # data.to_csv(f"outs/{name}.csv",index=False )
    for c in pivoted_tables:
        name = c
        print(name)
        data = read_from_database(name)
        # data = data.replace(store)
        if name in all_fk_maps:
            print(name)
            for f in all_fk_maps[name]:
                data[f] = data[f].apply(lambda x: all_fk_maps[name][f].get(x,x))
        data = rev_meltdown(data)
        data.drop(columns=[f"{name}_id"], inplace=True)
        # print(data.columns)
        data = convert_date_columns(data) 
        dfs[name] = final_touch(data, name)
        # data.to_csv(f"outs/{name}.csv",index=False )
    return dfs


# rest = restruct()



"""
self.fxrate = rest["monthly_fxrates"]
self.rawdata = rest["monthly_rawdata"]
self.cbixchart = rest["monthly_cbixchart"]
self.tradedata  = rest["monthly_tradedata"]
self.insuranceratelookup   = rest["monthly_insurance_rate_in_cif"]
self.bauxite_reqrmt_supply   = rest["monthly_summary_bauxite_requirement_supply"]
self.alumina_by_brovince     = rest["monthly_alumina_by_province"]
self.monthly_domestic_price_dmt      = rest["monthly_domestic_price_dmt"]
self.monthly_import_by_country_dmt   = rest["monthly_import_by_country_dmt"]
self.imported_bauxite_stocks = rest["imported_bauxite_stocks"]
self.MONTHLY_AVERAGE_CFR_PRICE      = rest["monthly_avg_cfr_price"]
self.MONTHLY_AVERAGE_VIU_CFR_PRICE  = rest["monthly_average_viu_cfr_price"]
"""
