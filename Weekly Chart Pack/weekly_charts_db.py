# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 14:00:32 2020

@author: JOHN
"""


import pyodbc
import pandas as pd
from datetime import datetime


server = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'T@lst0y50'
driver = '{ODBC Driver 17 for SQL Server}'

# tables not found in input_db
cbixrollingdata = pd.read_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Input db\cbixrollingdata.csv')
cmaax_rolling_data = pd.read_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Input db\cmaax_rolling_data.csv')
frieghtrollingdata = pd.read_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Input db\frieghtrollingdata.csv')


conn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server +
                      ';PORT=1443;DATABASE='+database+';UID='+username+';PWD=' + password)


daily_price_data = pd.read_sql_query(
    "SELECT * FROM [dbo].[daily_price_data]", conn)
week_id = pd.read_sql_query("SELECT * FROM [dbo].[week_id]", conn)
year = pd.read_sql_query("SELECT * FROM [dbo].[year]", conn)
cmaaxdata1 = pd.read_sql_query("SELECT * FROM [dbo].[cmaaxdata1]", conn)
cmaaxdata2 = pd.read_sql_query("SELECT * FROM [dbo].[cmaaxdata2]", conn)
frieght_data = pd.read_sql_query("SELECT * FROM [dbo].[frieght_data]", conn)
month_id = pd.read_sql_query("SELECT * FROM [dbo].[month_id]", conn)
province = pd.read_sql_query("SELECT * FROM [dbo].[province]", conn)
domestic_price_data = pd.read_sql_query(
    "SELECT * FROM [dbo].[domestic_price_data]", conn)
domestic_price_raw_data = pd.read_sql_query(
    "SELECT * FROM [dbo].[domestic_price_raw_data]", conn)
checked_causticpricedata = pd.read_sql_query(
    "SELECT * FROM [dbo].[checked_causticpricedata]", conn)
day_id = pd.read_sql_query("SELECT * FROM [dbo].[day_id]", conn)
# = pd.read_sql_query("SELECT * FROM [dbo].[]", conn)
# = pd.read_sql_query("SELECT * FROM [dbo].[]", conn)
# = pd.read_sql_query("SELECT * FROM [dbo].[]", conn)

# daily price date


def excel_date(d):
    date_in = 0
    try:
        date_in = datetime.strptime(d, '%d-%m-%Y')
    except:
        date_in = datetime.strptime(d, '%d/%m/%Y')
    temp = datetime(1899, 12, 30)
    delta = date_in - temp
    return float(delta.days) + (float(delta.seconds)/86400)


daily_price_data['elementdatayear'] = [(datetime.strptime(
    daily_price_data.loc[i, 'date'], '%d-%m-%Y').year) for i in daily_price_data.index]
daily_price_data['elementdatamonth'] = [(datetime.strptime(
    daily_price_data.loc[i, 'date'], '%d-%m-%Y').month) for i in daily_price_data.index]
daily_price_data['elementdataday'] = [(datetime.strptime(
    daily_price_data.loc[i, 'date'], '%d-%m-%Y').day) for i in daily_price_data.index]
daily_price_data['date'] = [excel_date(d) for d in daily_price_data['date']]
daily_price_data.drop(['year_id', 'month_id', 'day_id','creation_date', 'updation_date' ], axis=1, inplace=True)

for col in frieght_data.columns[:-3]:
    frieght_data.rename(columns={
        col: col.replace('_', ',')}, inplace=True)
d = {week_id['week_id'][x]: week_id['week'][x] for x in week_id.index}
frieght_data['week,id'] = [d[frieght_data.loc[i, 'week,id']]
                           for i in frieght_data.index]
frieght_data.rename(columns={
    'week,id': 'week'}, inplace=True)
frieght_data['week'] = [excel_date(i) for i in frieght_data['week']]
frieght_data.drop(columns=['creation_date','updation_date'], axis=1, inplace=True)


domestic_price_data.rename(columns={
    'shanxi_4_5__5_0': 'Shanxi 4.5 - 5.0',
    'henan_4_0__5_0': 'Henan 4.0 - 5.0',
    'guizhou_5_5__6_5': 'Guizhou 5.5 - 6.5'}, inplace=True)
domestic_price_data.drop(columns=['creation_date', 'updation_date'], axis=1, inplace=True)

domestic_price_raw_data.rename(columns={
    'shanxi4_5': 'shanxi4.5',
    'guizhou5_5': 'guizhou5.5',
    'guizhou6_5': 'guizhou6.5'
}, inplace=True)

for col in checked_causticpricedata.columns[1:-2]:
    col_n = col.capitalize().replace('_', ' ')
    checked_causticpricedata.rename(columns={
        col: col_n[:-2]+col_n[-2:].capitalize()
    }, inplace=True)
# dexer new update to date columns
checked_causticpricedata.rename(columns={
    'DaTe': 'date'
}, inplace=True)
values = []
for i in checked_causticpricedata['date']:
    values.append(excel_date(i))
checked_causticpricedata['date'] = values
checked_causticpricedata.drop(columns=['creation_date','updation_date'],axis=1, inplace=True)

cbixrollingdata.rename(columns={
    'day_id': 'Day'}, inplace=True)
d = {day_id['day_id'][x]: day_id['day'][x] for x in day_id.index}
cbixrollingdata['Day'] = [d[cbixrollingdata.loc[i, 'Day']]
                          for i in cbixrollingdata.index]

cmaax_rolling_data.rename(columns={
    'day_id': 'Day',
    'nax': 'NAX',
    'sax': 'SAX'}, inplace=True)
d = {day_id['day_id'][x]: day_id['day'][x] for x in day_id.index}
cmaax_rolling_data['Day'] = [(d[cmaax_rolling_data.loc[i, 'Day']])
                             for i in cmaax_rolling_data.index]


frieghtrollingdata.rename(columns={
    'week_id': 'week'}, inplace=True)
d = {week_id['week_id'][x]: week_id['week'][x] for x in week_id.index}
frieghtrollingdata['week'] = [int(b.split('_')[1]) for b in frieghtrollingdata['week']]
'''
frieghtrollingdata['week'] = [
    (d[frieghtrollingdata.loc[i, 'week']]) for i in frieghtrollingdata.index]
frieghtrollingdata['week'] = [excel_date(d) for d in frieghtrollingdata['week']]
'''

cmaaxdata1.drop(columns=['creation_date','updation_date'], axis=1, inplace=True)
cmaaxdata2.drop(columns=['creation_date','updation_date'], axis=1, inplace=True)

# outputs
domestic_price_data.to_csv(
    r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\domesticpricedata1.csv", index=False)
checked_causticpricedata.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\causticpricedata.csv', index=False)
cbixrollingdata.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\cbixrollingdata.csv', index=False)
cmaaxdata1.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\cmaaxdata1.csv', index=False)
cmaaxdata2.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\cmaaxdata2.csv', index=False)
cmaax_rolling_data.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\cmaaxrollingdata.csv', index=False)
daily_price_data.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\dailypricedata.csv', index=False)
frieght_data.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\frieghtdata.csv', index=False)
frieghtrollingdata.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\frieghtrollingdata.csv', index=False)
domestic_price_raw_data.to_csv(
    r'c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\\Inputs\domesticpricerawdata1.csv', index=False)
