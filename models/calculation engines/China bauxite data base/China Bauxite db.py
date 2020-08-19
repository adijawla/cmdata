
import pyodbc
import pandas as pd
server = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'T@lst0y50'
driver= '{ODBC Driver 17 for SQL Server}'
       
conn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
df1=pd.read_sql_query("SELECT * FROM [dbo].[china_bx_data2]", conn)
df1.drop(columns =["creation_date","updation_date"], inplace = True)
df2=pd.read_sql_query("SELECT * FROM [dbo].[china_bx_data_grade]", conn)
df2.drop(columns =["creation_date","updation_date"], inplace = True)
province=pd.read_sql_query("SELECT * FROM [dbo].[province]", conn)
province.drop(columns =["creation_date","updation_date"], inplace = True)
county=pd.read_sql_query("SELECT * FROM [dbo].[county]", conn)
county.drop(columns =["creation_date","updation_date"], inplace = True)


'''China bx Data db'''
province=province[['province_id','province']]
join1=df1.merge(province, on='province_id', how='left')

join1=join1.merge(county, on='county_id', how='left')



join1=join1.drop(['province_id','county_id'], axis = 1) 
cols = list(join1.columns)
cols = [cols[-1]] + cols[:-1]
join1 = join1[cols]
cols = list(join1.columns)
cols = [cols[-1]] + cols[:-1]
join1 = join1[cols]
join1=join1.rename(columns={ 'province': 'Province' })
join1.to_csv("China bx Data db Output1.csv",index=False)


'''china_bx_data_gradedb'''

join2=df2.merge(province, on='province_id', how='left')

join2=join2.merge(county, on='county_id', how='left')



join2=join2.drop(['province_id','county_id'], axis = 1) 
cols = list(join2.columns)
cols = [cols[-1]] + cols[:-1]
join2 = join2[cols]
cols = list(join2.columns)
cols = [cols[-1]] + cols[:-1]
join2 = join2[cols]
join2=join2.rename(columns={ 'province': 'Province' })

join2.to_csv("china_bx_data_gradedb Output1.csv",index=False)