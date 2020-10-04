import difflib
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server")
def read_tables():
    query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
            """
    data = pd.read_sql_query(sql=query, con=engine)
    return data

# list_tb_1 = ["cbix_data", "changjiang_lme_daily_inputs", "mines_bx_production", "platts_vs_cmaax_inputs"]
# db_table_names = read_tables()
# tablenames = [difflib.get_close_matches(a, db_table_names) for a in list_tb_1]
# print(tablenames)

def read_from_database(table):
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(sql=query, con=engine)
    if "Date" in data.columns:
        data["Date"] = data["Date"].astype('datetime64[ns]')
    if "date" in data.columns:
        data["date"] = data["date"].astype('datetime64[ns]')
    return data.apply(lambda x: pd.to_numeric(x.astype(str).str.strip('"').str.strip('$').str.replace(',',''), errors='coerce')).fillna(data)


all_foreignkeys = ['technology', 'refinery', 'province', 'year', 'company', 'ownership', 'groups', 'country', 'mine' ]
store = {}
for a in all_foreignkeys:
    d = read_from_database(a)
    c = [f for f in d.columns if f[-3:] == '_id'][0]
    if c != 'group_id':
        s = dict(d.loc[:, [c, c[:-3]]].values)
    else:
        s = dict(d.loc[:, [c, 'groups']].values)
    store = {**store, **s}


print(store)
