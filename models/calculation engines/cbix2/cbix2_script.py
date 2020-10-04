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




store = {}

canal_list = read_from_database('canals_list')
mine = read_from_database('mine')
port = read_from_database('port')
vessel = read_from_database('vessel')
currency = read_from_database('currency')
technology = read_from_database('technology')
country = read_from_database('country')


canal_store = dict(canal_list.loc[:, ['canals_id', 'canals']].values)
mine_store = dict(mine.loc[:, [ 'mine_id', 'mine']].values)
port_store = dict(port.loc[:, [ 'port_id', 'port']].values)
vessel_store = dict(vessel.loc[:, [ 'vessel_id', 'vessel_class']].values)
currency_store = dict(currency.loc[:, [ 'currency_id', 'currency']].values)
technology_store = dict(technology.loc[:, [ 'technology_id', 'technology']].values)
country_store = dict(country.loc[:, [ 'country_id', 'country']].values)
store = {'canal': canal_store, 'mine': mine_store, 'port': port_store, 'vessel': vessel_store, 'currency': currency_store, 'technology': technology_store, 'country': country_store}


all_fk_maps = {
   "Indices_Mines_Exporting_Port": {'Mine': store['mine']},
    "processing_factors": {'Processing Regime': store['technology']},
    "china_price_series": {},
    "Ship_Time_Charter_Rates": {'Vesssel class':  store['vessel']},
    "ship_speed": {'Vesssel class':  store['vessel']},
    "Trade_Details": {'Mine': store['mine'], 'Importing Port': store['port'], 'Old CBIX type Calc': store['port']},
    "special_leg_shipping": {'Mine': store['mine']},
    "Canals_Class": {'Class': store['vessel']},
    "vessel_class": {'Class': store['vessel']},
    "Port_Linkages": {'Exporting Port': store['port'], 'Importing Port': store['port'], 'VLOC – Applicable Fuel Region': store['country'], 'Capesize – Applicable Fuel Region': store['country'], 'Suezmax – Applicable Fuel Region': store['country'], 'NeoPanamax – Applicable Fuel Region': store['country'], 'Panamax – Applicable Fuel Region': store['country'], 'Supramax – Applicable Fuel Region': store['country'], 'Handysize – Applicable Fuel Region': store['country']},
    "Ship_Fuel_Prices": {'Fuel Region': store['country']},
    "China_Importing_Ports": {'Currency': store['country']},
    "Canal": {'Currency': store['country'], 'Canal': store['canal']},
    "Actual_Price_determination_from_CBIX_price": {'Mine': store['mine'], 'Importing Port': store['port']},
    "Indices_Mines_Exporting_Port_2": {'Mine': store['mine']},
    "freights_perwmt_tradedata": {'Mine': store['mine'], 'Importing Port': store['port']},
    "cbix_coefficients_determination": {'Mine': store['mine'], 'Importing Port': store['port']},
}   
 


all_names = [
    "Indices_Mines_Exporting_Port",
    "processing_factors",
    "china_price_series",
    "global_factors",
    "Ship_Time_Charter_Rates",
    "ship_speed",
    "Trade_Details",
    "special_leg_shipping",
    "MRN_Juruti_max_cargo",
    "fxrates_withdates",
    "Canals_Class",
    "vessel_class",
    "Port_Linkages",
    "Ship_Fuel_Prices",
    "China_Importing_Ports",
    "Canal",
    "Master_date_cell",
    "Target_CBIX_Price",
    "Actual_Price_determination_from_CBIX_price",
    "Data_table_of_forecast_prices",
    "Indices_Mines_Exporting_Port_2",
    "Freights_table_selector",
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
    ex = ["MRN_Juruti_max_cargo", "Data_table_of_forecast_prices"]
    data = data.replace({None: np.nan, "Not available": np.nan}) if not name in ex  else data
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
    temp_g['T'] = pd.to_datetime(temp_g['T'], errors='coerce').dt.normalize().fillna(temp_g['T'])
    data.columns = temp_g['T'].values
    print(data.columns)
    return data

def restruct():
    dfs = {}
    tnc = []
    for sh in all_names:
        name = sh
        print(name)
        data = read_from_database(name)
        # data = data.replace(store)
        if "model_id" in data.columns:
            data = data.loc[data["model_id"] == "model_9"]
            data.drop(columns=["model_id"], inplace=True)
        if name in all_fk_maps:
            print(name)
            for f in all_fk_maps[name]:
                data[f] = data[f].apply(lambda x: all_fk_maps[name][f].get(x,x))
        data.drop(columns=["creation_date", "updation_date", f"{name}_id"], inplace=True)
        data = convert_date_columns(data) if name in tnc else data
        exempts = ["Port_Linkages", "Canal", "global_factors"]
        if name not in exempts:
            data = data.replace({0: np.nan, None: np.nan})
            print(data)
        dfs[name] = final_touch(data, name)
        # data.to_csv(f"outs/{name}.csv",index=False )
    return dfs

# rest = restruct()
