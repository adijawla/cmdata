import pandas as pd
import numpy as np

# basedata = pd.read_csv('basedata.csv')
# meidata = pd.read_csv('meiaadata.csv')

meidata_store = {"province_1": "Guangxi", "province_13": "Guizhou", "province_6": "Henan", "province_7": "Shanxi", "province_218": "Other"}
years = [*map(str,range(2005, 2032))]
g_cols = ['Y_1', 'Y_2', 'Y_3', 'Y_4', 'Y_5', 'Y_6', 'Y_7', 'Y_8', 'Y_9', 'Y_10', 'Y_11',
 'Y_12', 'Y_13', 'Y_14', 'Y_15', 'Y_16', 'Y_17', 'Y_18', 'Y_19', 'Y_20', 'Y_21',
 'Y_22', 'Y_23', 'Y_24', 'Y_25', 'Y_26', 'Y_27']

yg = dict(zip(g_cols, years))
# print(yg)

def restruct(data, ind, col, val):
    print(data)
    cols = data[col].unique()
    result = pd.DataFrame(columns=[ind, *cols])
    indexes =  data.loc[0::len(cols), ind].values
    result[ind] = indexes 
    values = data[val].values.reshape(-1, len(cols))
    result.loc[:, cols] = list(values)
    result = result.rename(columns=yg)
    result = result.replace(meidata_store)
    return result.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce')).fillna(result)

    

# basedata_r = restruct(basedata, 'ind', 'year_id', 'value')
# print(basedata_r)
# meidata_r = restruct(meidata, 'province_id', 'year_id', 'Value').replace(meidata_store)
# meidata_r = meidata_r.rename(columns={'province_id': 'province'})
# print(meidata_r)
