import pandas as pd 
import numpy as np
import time, os
from flatdbconverter import Flatdbconverter
#import uploadtodbll as uploadtodb
from outputdb import uploadtodb

import pyodbc

server = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'Ins201799'
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
join1.to_csv("china_bx_data.csv",index=False)


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

join2.to_csv("china_bx_data_grade.csv", index=False)

# #########################################
db_conv = Flatdbconverter("China bauxite data base")

dbflat_time = time.perf_counter()


# input_data = "Inputs/china_bx_data.csv"
main_data = join1
# print(main_data)

county_col = ['Province', 'County', 'Inventory \'000 t', 'Geological\nEndowment \'000 t']
province_col = ['Province', 'Inventory \'000 t', 'Geological\nEndowment \'000 t']

county_df = pd.DataFrame(columns=county_col)
province_df = pd.DataFrame(columns=province_col)

curr_province = ""
curr_county = ""
pro_index = 0
count_index = 0

for index, data in main_data.iterrows():

    if (data["Province"] != curr_province):
        print("\n")

        provSum_for_Inv = main_data.loc[(main_data["Province"] == data['Province'])].iloc[:, 4].str.replace("-", "0").astype(float).sum()
        provSum_for_Geo = main_data.loc[(main_data["Province"] == data['Province'])].iloc[:, 5].str.replace("-", "0").astype(float).sum()

        province_df.loc[pro_index] = [data['Province'], provSum_for_Inv, provSum_for_Geo]

        curr_province = data["Province"]
        pro_index = pro_index + 1


    if (data["county"] != curr_county):
        countSum_for_Inv = main_data.loc[(main_data["Province"] == data['Province']) & (main_data["county"] == data['county'])].iloc[:, 4].str.replace("-", "0").astype(float).sum()
        countSum_for_Geo = main_data.loc[(main_data["Province"] == data['Province']) & (main_data["county"] == data['county'])].iloc[:, 5].str.replace("-", "0").astype(float).sum()

        county_df.loc[count_index] = [data['Province'], data['county'], countSum_for_Inv, countSum_for_Geo]

        curr_county = data["county"]
        count_index = count_index + 1

    
    print("Province => "+data['Province']+" \t|-| County => "+str(data['county'])+" \t|-| Inventory => "+str(countSum_for_Inv)+" \t|-| Geological => "+str(countSum_for_Geo))


print("Task One (1) Completed")
print("\n+\n+\n\n+\n+\n")

c1_proddata1 = db_conv.single_year_mult_out(province_df, 'Province Model')
c1_proddata2 = db_conv.single_year_mult_out(county_df, 'County Model')


snapshot_output_data = pd.DataFrame(columns=['snapshot_id','output_set','output_id', 'outputrow','output_label','output_value','model_id', 'overriden'])

dblist_names = [
    'County Model',
    'Province Model',
]

dblist = [
    c1_proddata1,
    c1_proddata2,
]

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)

print("Task Two (2) Completed")
print(time.perf_counter() - dbflat_time)

snapshot_output_data=snapshot_output_data
uploadtodb.upload(snapshot_output_data)
