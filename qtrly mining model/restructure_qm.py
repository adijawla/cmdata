import os
import pandas as pd
import numpy as np
import pyodbc


list_tb = ['diesel_price_ex_incl_vat','diesel_price_rebate','labour_price_rebates',
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
    try:
        alll.remove('creation_date')
        alll.remove('updation_date')
    except Exception as e:
        print(e)
    info = redf.loc[redf.Parent_table==inputname].reset_index()
    scol = info["name_p"].tolist()
    tcol = []
    for i in range(len(scol)):
        alll.remove(scol[i])
        m = []
        tcol.append([info["Refrenced_table"][i],info["name_r"][i],info["name_p"][i],info["Refrenced_table"][i]])
         
    return alll,tcol

def rev(s):
	return s[::-1]

list_dfs = []
for i in list_tb:
    
    a,b =  col(i)
    q =sql_join(i,a,b)
    
    df = pd.read_sql_query(q,conn)
    list_dfs.append(df)
    
    #df.to_csv(r"C:\Users\magmarkd1\Downloads\quartly mining model\testing\\"+i+'.csv')

df1,df2,df3,df4,df5,df6,df7,df8,df9,df10 = list_dfs

df11 = pd.read_sql_query('''
                select other_controls.quarter_switch,year.year as year_id
                from other_controls
                inner join year on other_controls.quarterly_year_switch = year.year_id''',conn)


##diesel_price_ex_incl_vat
#df1_ex = df1.drop('diesel_price_including_vat',axis=1)
df1_ex = pd.pivot_table(df1,index=['line','denomination_per_litre'],values='diesel_prices_excluding_vat',columns=['quarter','year_id'],aggfunc='sum')
df1_ex.columns = df1_ex.columns.map(rev).map('_'.join)

df1_inc = pd.pivot_table(df1,index=['line','denomination_per_litre'],values='diesel_price_including_vat',columns=['quarter','year_id'],aggfunc='sum')
df1_inc.columns = df1_inc.columns.map(rev).map('_'.join )



#diesel_price_rebate
df2_dpr =  pd.pivot_table(df2,index=['rebate_denomination_per_litre'],values='diesel_price_rebates',columns=['quarter','year_id'],aggfunc='sum')
df2_dpr.columns = df2_dpr.columns.map(rev).map('_'.join )

#labour_price_rebates
df3_lpr =  pd.pivot_table(df3,index=['currency_id'],values='labour_price_rebates',columns=['quarter','year_id'],aggfunc='sum')
df3_lpr.columns = df3_lpr.columns.map(rev).map('_'.join )

#quartrly_price_labour
df4_ex = pd.pivot_table(df4,index=['country_id','currency_id'],values='labour_prices_excluding_vat',columns=['quarter','year_id'],aggfunc='sum')
df4_ex.columns = df4_ex.columns.map(rev).map('_'.join)

df4_inc = pd.pivot_table(df4,index=['country_id','currency_id'],values='labour_prices_including_vat',columns=['quarter','year_id'],aggfunc='sum')
df4_inc.columns = df4_inc.columns.map(rev).map('_'.join )

#Water_Price_ex_inc_vat
df5_ex = pd.pivot_table(df5,index=['line','denomination_per_m_3'],values='water_prices_excluding_vat',columns=['quarter','year_id'],aggfunc='sum')
df5_ex.columns = df5_ex.columns.map(rev).map('_'.join)

df5_inc = pd.pivot_table(df5,index=['line','denomination_per_m_3'],values='water_prices_including_vat',columns=['quarter','year_id'],aggfunc='sum')
df5_inc.columns = df5_inc.columns.map(rev).map('_'.join )

#Currency_n_FX
df6_cfx = pd.pivot_table(df6,index=['line','currency_id'],values='Value',columns=['qurter','year_id'],aggfunc='sum')
df6_cfx.columns = df6_cfx.columns.map(rev).map('_'.join)

#Electricity_Explosives_Gasoline_LPG_Water_Rebates
df7_ele =  pd.pivot_table(df7,index=['rebate_denomination_per_litre'],values='electricity_rebates',columns=['quarter','year_id'],aggfunc='sum')
df7_ele.columns = df7_ele.columns.map(rev).map('_'.join )

df7_exp =  pd.pivot_table(df7,index=['rebate_denomination_per_litre'],values='explosive_rebates',columns=['quarter','year_id'],aggfunc='sum')
df7_exp.columns = df7_exp.columns.map(rev).map('_'.join )

df7_gas =  pd.pivot_table(df7,index=['rebate_denomination_per_litre'],values='gasoline_rebates',columns=['quarter','year_id'],aggfunc='sum')
df7_gas.columns = df7_gas.columns.map(rev).map('_'.join )

df7_lpg =  pd.pivot_table(df7,index=['rebate_denomination_per_litre'],values='lpg_rebates',columns=['quarter','year_id'],aggfunc='sum')
df7_lpg.columns = df7_lpg.columns.map(rev).map('_'.join )

df7_wat =  pd.pivot_table(df7,index=['rebate_denomination_per_litre'],values='water_rebates',columns=['quarter','year_id'],aggfunc='sum')
df7_wat.columns = df7_wat.columns.map(rev).map('_'.join )

#main_qrtly_input

print(df8)
print(df11)


#prices_including_excluding_vat
df9_ex_el = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='electricity_price_excluding_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_ex_el.columns = df9_ex_el.columns.map(rev).map('_'.join)
df9_inc_el = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='electricity_price_including_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_inc_el.columns = df9_inc_el.columns.map(rev).map('_'.join )

df9_ex_ex = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='explosive_price_excluding_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_ex_ex.columns = df9_ex_ex.columns.map(rev).map('_'.join)
df9_inc_ex = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='explosive_price_including_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_inc_ex.columns = df9_inc_ex.columns.map(rev).map('_'.join )

df9_ex_ga = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='gasoline_price_excluding_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_ex_ga.columns = df9_ex_ga.columns.map(rev).map('_'.join)
df9_inc_ga = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='gasoline_price_including_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_inc_ga.columns = df9_inc_ga.columns.map(rev).map('_'.join )

df9_ex_lp = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='lpg_price_excluding_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_ex_lp.columns = df9_ex_lp.columns.map(rev).map('_'.join)
df9_inc_lp = pd.pivot_table(df9,index=['line','denomination_per_litre'],values='lpg_price_including_vat',columns=['quarter','year_id'],aggfunc='sum')
df9_inc_lp.columns = df9_inc_lp.columns.map(rev).map('_'.join )

#VAT_Rates
df10_vat =  pd.pivot_table(df10,index=['line','units'],values='Value',columns=['quarter','year_id'],aggfunc='sum')
df10_vat.columns = df10_vat.columns.map(rev).map('_'.join )


