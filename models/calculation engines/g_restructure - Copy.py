import os
import pandas as pd
import numpy as np
import pyodbc


list_tb = ['diesel_price_ex_incl_vat','diesel_price_rebate',#'labour_price_rebates',
           'quartrly_price_labour','Water_Price_ex_inc_vat','Currency_n_FX',
           'Electricity_Explosives_Gasoline_LPG_Water_Rebates','main_qrtly_input',
           'prices_including_excluding_vat','VAT_Rates']

server   = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'T@lst0y50'
driver   = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect(f'DRIVER={driver};PORT=1433;SERVER={server};PORT=1443;DATABASE={database};UID={username};PWD={password}')



fsql = '''SELECT
    fk.name 'FK Name',
    tp.name 'Parent_table',
    cp.name 'name_p'
    , cp.column_id,
    tr.name 'Refrenced_table',
    cr.name 'name_r', cr.column_id
FROM 
    sys.foreign_keys fk
INNER JOIN 
    sys.tables tp ON fk.parent_object_id = tp.object_id
INNER JOIN 
    sys.tables tr ON fk.referenced_object_id = tr.object_id
INNER JOIN 
    sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
INNER JOIN 
    sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
INNER JOIN 
    sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
ORDER BY
    tp.name, cp.column_id'''




def sql_join(table, orig, lookups):
    #print(table, orig, lookups)
    first = [f"{table}.{a}," for a in orig]
    all_lookups = []
    for lookup in lookups:
        all_lookups.append(f"inner join {lookup[0]} on {table}.{lookup[2]} = {lookup[0]}.{lookup[1]}\n")
    second = [f"{lookup[0]}.{lookup[3]} as {lookup[2]}," for lookup in lookups]
    third = f'from {table}'
    sel = f"from {table}"
    final = f"select {''.join(first)}{''.join(second)[:-1]}\n{sel}\n{''.join(all_lookups)}"
    #print(final)
    return final
    
redf = pd.read_sql_query(fsql,conn)

def col(inputname):
    inputdb = pd.read_sql_query("select * from "+str(inputname),conn)
    alll = list(inputdb.columns)
    alll.remove('creation_date')
    alll.remove('updation_date')
    info = redf.loc[redf.Parent_table==inputname].reset_index()
    scol = info["name_p"].tolist()
    tcol = []
    for i in range(len(scol)):
        alll.remove(scol[i])
        m = []
        tcol.append([info["Refrenced_table"][i],info["name_r"][i],info["name_p"][i],info["Refrenced_table"][i]])
         
    return alll,tcol
list_dfs = []
for i in list_tb:
    
    a,b =  col(i)
    q =sql_join(i,a,b)
    df = pd.read_sql_query(q,conn)
    list_dfs.append(df)
    #df.to_csv(r"C:\Users\magmarkd1\Downloads\quartly mining model\testing\\"+i+'.csv')


    


    
