#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  5 19:36:24 2020

@author: jondexter
"""

import pyodbc
import pandas as pd
from datetime import datetime


server = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'T@lst0y50'
driver = '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server +
                      ';PORT=1443;DATABASE='+database+';UID='+username+';PWD=' + password)

#converters
country = pd.read_sql_query("SELECT * FROM [dbo].[country]", conn)
dcountry = {country['country_id'][x]:country['country'][x] for x in country.index}

mine = pd.read_sql_query("SELECT * FROM [dbo].[mine]", conn)
dmine = {mine['mine_id'][x]:mine['mine'][x] for x in mine.index}

province = pd.read_sql_query("SELECT * FROM [dbo].[province]", conn)
dprovince =  {province['province_id'][x]:province['province'][x] for x in province.index}

refineries = pd.read_sql_query("SELECT * FROM [dbo].[refinery]", conn)
drefineries = {refineries['refinery_id'][x]:refineries['refinery'][x] for x in refineries.index}

chinese_name=  pd.read_sql_query("SELECT * FROM [dbo].[chinese_name]", conn)
dchinese = {chinese_name['chinese_name_id'][x]:chinese_name['chinese_name'][x] for x in chinese_name.index}

sub_chinese_name=  pd.read_sql_query("SELECT * FROM [dbo].[sub_chinese_name]", conn)
dsubChinese = {sub_chinese_name['sub_chinese_name_id'][x]:sub_chinese_name['sub_chinese_name'][x] for x in sub_chinese_name.index}

category=  pd.read_sql_query("SELECT * FROM [dbo].[category]", conn)
dcat = {category['category_id'][x]:category['category'][x] for x in category.index}

ownership=  pd.read_sql_query("SELECT * FROM [dbo].[ownership]", conn)
downership= {ownership['ownership_id'][x]:ownership['ownership'][x] for x in ownership.index}

tech = pd.read_sql_query("SELECT * FROM [dbo].[technology]", conn)
dtech = {tech['technology_id'][x]:tech['technology'][x] for x in tech.index}

owner = pd.read_sql_query("SELECT * FROM [dbo].[owner]", conn)
dOwner = {owner['owner_id'][x]:owner['owner'][x] for x in owner.index}

bauxite = pd.read_sql_query("SELECT * FROM [dbo].[bauxite]", conn)
dbauxite = {bauxite['bauxite_id'][x]:bauxite['bauxite'][x] for x in bauxite.index}

year = pd.read_sql_query("SELECT * FROM [dbo].[year]", conn)
dyear = {year['year_id'][x]:year['year'][x] for x in year.index}

port = pd.read_sql_query("SELECT * FROM [dbo].[port]", conn)
dport = {port['port_id'][x]:port['port'][x] for x in port.index}

smelter = pd.read_sql_query("SELECT * FROM [dbo].[smelter]", conn)
dsmelter = {smelter['smelter_id'][x]:smelter['smelter'][x] for x in smelter.index}

ch_smelter = pd.read_sql_query("SELECT * FROM [dbo].[ch_smelter]", conn)
dch_smelter = {ch_smelter['ch_smelter_id'][x]:ch_smelter['ch_smelter'][x] for x in ch_smelter.index}

smelter_short_name = pd.read_sql_query("SELECT * FROM [dbo].[smelter_short_name]", conn)
dssn = {smelter_short_name['smelter_short_name_id'][x]:smelter_short_name['smelter_short_name'][x] for x in smelter_short_name.index}

groups = pd.read_sql_query("SELECT * FROM [dbo].[groups]",conn)
dgroup = {groups['group_id'][x]:groups['groups'][x] for x in groups.index}

continent = pd.read_sql_query("SELECT * FROM [dbo].[continent]", conn)
dcontinent = {continent['continent_id'][x]:continent['continent'][x] for x in continent.index}

currency = pd.read_sql_query("SELECT * FROM [dbo].[currency]",conn)
dcurrency = {currency['currency_id'][x]:currency['currency'][x] for x in currency.index}

#general imports
data_engine = pd.read_sql_query( "SELECT * FROM [dbo].[data_engine]", conn)
capitalcost = pd.read_sql_query( "SELECT * FROM [dbo].[capitalcost]", conn)
AlF3_Price = pd.read_sql_query( "SELECT * FROM [dbo].[AlF3_Price]", conn)
carbon_price = pd.read_sql_query( "SELECT * FROM [dbo].[carbon_price]", conn)
carbon_source = pd.read_sql_query( "SELECT * FROM [dbo].[carbon_source]", conn)
captive_power_cost = pd.read_sql_query( "SELECT * FROM [dbo].[captive_power_cost]", conn)
electricity_consumption = pd.read_sql_query( "SELECT * FROM [dbo].[electricity_consumption]", conn)
electricity_grid = pd.read_sql_query( "SELECT * FROM [dbo].[electricity_grid]", conn)
naturalgasprice  = pd.read_sql_query( "SELECT * FROM [dbo].[naturalgasprice ]", conn)
salary = pd.read_sql_query( "SELECT * FROM [dbo].[salary]", conn)
currency_input_id = pd.read_sql_query( "SELECT * FROM [dbo].[currency_input_id]", conn)
smelter_capacity = pd.read_sql_query( "SELECT * FROM [dbo].[smelter_capacity]", conn)
smelter_production= pd.read_sql_query( "SELECT * FROM [dbo].[smelter_production]", conn)

selfsuppliedaacost = pd.read_sql_query( "SELECT * FROM [dbo].[selfsuppliedaacost]", conn)
selfsuppliedrefinary = pd.read_sql_query( "SELECT * FROM [dbo].[selfsuppliedrefinary]", conn)
# = pd.read_sql_query( "SELECT * FROM [dbo].[]", conn)
aa_source= pd.read_sql_query( "SELECT * FROM [dbo].[aa_source]", conn)
aasource_table_2= pd.read_sql_query( "SELECT * FROM [dbo].[aasource_table_2]", conn)
aa_web3 = pd.read_sql_query( "SELECT * FROM [dbo].[aa_web3]", conn)
aa_capital_cost = pd.read_sql_query( "SELECT * FROM [dbo].[aa_capital_cost]", conn)

aa_selfsupply_ratio = pd.read_sql_query( "SELECT * FROM [dbo].[aa_selfsupply_ratio]", conn)
electricity_source = pd.read_sql_query( "SELECT * FROM [dbo].[electricity_source]", conn)
#electricity_source_aaselfsupplyratio = pd.read_sql_query( "SELECT * FROM [dbo].[electricity_source_aaselfsupplyratio]", conn)
#aa_source

data_engine.drop(['data_engine_id','creation_date','updation_date'], axis=1,inplace=True)
data_engine.rename(columns={
    'ref':'Ref',
    'smelter_id':'Smelter',
    'ch_smelter_id':'Ch Smelter',
    'smelter_short_name_id':'Smelter Short Name',
    'group_id':'Group',
    'continent_id':'Continent',
    'country_id':'Country',
    'technology_id':'Technoloy',
    'province_id':'State/Province',
    'ownership_id':'Ownership',
    'average_cell_life_day':'Average Cell Life,day',
    'Cells_Below_300':'Cells Below 300',
    'Operating_cells_Below_300':'Operating cells Below 300'},inplace=True)
for cols in [ 'Headcount_(workshop)','Cells_300','Cells_350','Cells_400','Cells_420','Cells_500','Cells_600',
             'Operating_cells_300', 'Operating_cells_350', 'Operating_cells_400','Operating_cells_420', 'Operating_cells_500', 'Operating_cells_600' ]:
    data_engine.rename(columns={
        cols:cols.replace('_',' ').capitalize()
    },inplace=True)
for col in data_engine.columns[10:]:
    try:
        data_engine[col] = [float(i) for i in data_engine[col]]
    except:
        print(col)

data_engine['Smelter'] = [dsmelter[i] for i in data_engine['Smelter']]
data_engine['Ch Smelter'] = [ dch_smelter[i] for i in data_engine['Ch Smelter'] ]
data_engine['Smelter Short Name'] = [dssn[i] for i in data_engine['Smelter Short Name']]
data_engine['Group'] = [dgroup[i] for i in data_engine['Group']]
data_engine['Continent'] = [dcontinent[i] for i in data_engine['Continent']]
data_engine['Country'] = [dcountry[i] for i in data_engine['Country']]
data_engine['Technoloy'] = [dtech[i] for i in data_engine['Technoloy']]
data_engine['State/Province'] = [dprovince[i] for i in data_engine['State/Province']]
data_engine['Ownership'] = [downership[i] for i in data_engine['Ownership']]


capitalcost.drop(['capitalcost_id','creation_date','updation_date'],axis=1, inplace=True)
capitalcost.rename(columns={
    'province_id':'State/Province'}, inplace=True)
capitalcost['State/Province'] = [dprovince[i] for i in capitalcost['State/Province']]


temp = AlF3_Price.loc[(AlF3_Price['quarter']=='Q1')&(AlF3_Price['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['State/Province'] = [dprovince[i] for i in temp['province_id'] ]
data['UNIT'] = 'RMB/t'
for year in list(dict.fromkeys( AlF3_Price['year_id'] )):
    for quarter in (list(dict.fromkeys( AlF3_Price['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = AlF3_Price.loc[(AlF3_Price['quarter']==quarter)&(AlF3_Price['year_id']==year),'AIF3_price'].astype(float).to_list()
        except:
            print('alf3-price missing cols: ',col_name)
AlF3_Price = data


temp = carbon_price.loc[(carbon_price['quarter']=='Q1')&(carbon_price['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['State/Province'] = [dprovince[i] for i in temp['province_id'] ]
data['UNIT'] = 'RMB/t'
for year in list(dict.fromkeys(carbon_price['year_id'] )):
    for quarter in (list(dict.fromkeys( carbon_price['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = carbon_price.loc[(carbon_price['quarter']==quarter)&(carbon_price['year_id']==year),'carbon_price'].astype(float).to_list()
        except:
            print('carbon-price missing cols: ',col_name)
carbon_price = data


temp = carbon_source.loc[(carbon_source['quarter']=='Q1')&(carbon_source['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Ref'] =  list(range(1,len(temp)+1))
data['Smelter'] = [dsmelter[i] for i in temp['smelter_id'] ]
data['Country'] = [dcountry[i] for i in temp['country_id']]
data['UNIT'] = '%'
for year in list(dict.fromkeys(carbon_source['year_id'] )):
    for quarter in (list(dict.fromkeys( carbon_source['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = carbon_source.loc[(carbon_source['quarter']==quarter)&(carbon_source['year_id']==year),'carbon_source'].astype(float).to_list()
        except:
            print('carbon-sourcce missing cols: ',col_name)
carbon_source = data


temp = captive_power_cost.loc[(captive_power_cost['quarter']=='Q1')&(captive_power_cost['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Ref'] =  list(range(1,len(temp)+1))
data['Smelter'] = [dsmelter[i] for i in temp['smelter_id'] ]
data['Captive Power Source'] = temp['captive_power_source'].to_list()
data['Province'] = [dprovince[i] for i in temp['province_id']]
data['CM Adjustment for Grid'] = [float(i) for i in temp['cm_adjustment_for_grid']]
data['UNIT'] = 'RMB/kW.h'
for year in list(dict.fromkeys(captive_power_cost['year_id'] )):
    for quarter in (list(dict.fromkeys( captive_power_cost['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = captive_power_cost.loc[(captive_power_cost['quarter']==quarter)&(captive_power_cost['year_id']==year),'captive_power_cost'].astype(float).to_list()
        except:
            print('captive_power_cost missing cols: ',col_name)
captive_power_cost = data


temp = electricity_consumption.loc[(electricity_consumption['quarter']=='Q1')&(electricity_consumption['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Ref'] =  list(range(1,len(temp)+1))
data['Smelter'] = [dsmelter[i] for i in temp['smelter_id'] ]
data['Country'] = [dcountry[i] for i in temp['country_id']]
data['UNIT'] = 'kW.h/t.Al'
for year in list(dict.fromkeys(electricity_consumption['year_id'] )):
    for quarter in (list(dict.fromkeys( electricity_consumption['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = electricity_consumption.loc[(electricity_consumption['quarter']==quarter)&(electricity_consumption['year_id']==year),'electricity_consumption'].astype(float).to_list()
        except:
            print('electricity consup missing cols: ',col_name)
electricity_consumption = data


temp = electricity_grid.loc[(electricity_grid['quarter']=='Q1')&(electricity_grid['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Grid'] = [dprovince[i] for i in temp['province_id'] ]
data['UNIT'] = 'RMB/kWh'
for year in list(dict.fromkeys(electricity_grid['year_id'] )):
    for quarter in (list(dict.fromkeys( electricity_grid['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = electricity_grid.loc[(electricity_grid['quarter']==quarter)&(electricity_grid['year_id']==year),'electricity_grid'].astype(float).to_list()
        except:
            print('elec grid missing cols: ',col_name)
electricity_grid = data

temp = naturalgasprice.loc[(naturalgasprice['quarter']=='Q1')&(naturalgasprice['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
result = []
for i in temp['province_id']:
    try:
        result.append(dprovince[i])
    except:
        result.append(float('nan'))
data['State/Province'] = result #[dprovince[i] for i in result]
data['UNIT'] = 'RMB/m3'
for year in list(dict.fromkeys(naturalgasprice['year_id'] )):
    for quarter in (list(dict.fromkeys( naturalgasprice['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = naturalgasprice.loc[(naturalgasprice['quarter']==quarter)&(naturalgasprice['year_id']==year),'natural_gas'].astype(float).to_list()
        except:
            print('natural_gas missing cols: ',col_name)
naturalgasprice = data


temp = salary.loc[(salary['quarter']=='Q1')&(salary['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['NBS'] = [dprovince[i] for i in temp['province_id'] ]
data['Survey'] =[float(i) for i in temp['survey'] ]
data['Adjustment'] = [float(i) for i in temp['adjustment'] ]
data['UNIT'] = temp['unit']
for year in list(dict.fromkeys(salary['year_id'] )):
    for quarter in (list(dict.fromkeys( salary['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = salary.loc[(salary['quarter']==quarter)&(salary['year_id']==year),'salary'].astype(float).to_list()
        except:
            print('salary missing cols: ',col_name)
salary = data


temp = currency_input_id.loc[(currency_input_id['quarter']=='Q1')&(currency_input_id['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Country'] = [dcountry[i] for i in temp['country_id'] ]
data['Currency'] = [dcurrency[i] for i in temp['currency_id']]
data['UNIT'] = [i for i in temp['unit']]
for year in list(dict.fromkeys(currency_input_id['year_id'] )):
    for quarter in (list(dict.fromkeys( currency_input_id['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = currency_input_id.loc[(currency_input_id['quarter']==quarter)&(currency_input_id['year_id']==year),'curreny_input'].astype(float).to_list()
        except:
            print('currency missing cols: ',col_name)
currency_input_id = data


temp = smelter_capacity.loc[(smelter_capacity['quarter']=='Q1')&(smelter_capacity['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Ref'] =  list(range(1,len(temp)+1))
data['Smelter'] = [dsmelter[i] for i in temp['smelter_id']]
data['Country'] = [dcountry[i] for i in temp['country_id'] ]
data['UNIT'] = 'Ktpa'
vals= []
for i in temp['year_of_commence']:
    try:
        vals.append(float(i))
    except:
        vals.append(0)
data['Year of commence']  =vals #[float(i) for i in temp['year_of_commence']]
for year in list(dict.fromkeys(smelter_capacity['year_id'] )):
    for quarter in (list(dict.fromkeys( smelter_capacity['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = smelter_capacity.loc[(smelter_capacity['quarter']==quarter)&(smelter_capacity['year_id']==year),'value'].astype(float).to_list()
        except:
            print('smelter cap missing cols: ',col_name)
smelter_capacity = data
smelter_capacity.rename(columns={'2020 Q1':'2019 Q3'},inplace=True)
smelter_capacity['2019 Q4'] = smelter_capacity['2019 Q3']
smelter_capacity['2020 Q1'] = smelter_capacity['2019 Q4']


temp = smelter_production.loc[(smelter_production['quarter']=='Q1        ')&(smelter_production['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Ref'] =  list(range(1,len(temp)+1))
data['Smelter'] = [dsmelter[i] for i in temp['smelter_id']]
data['Country'] = [dcountry[i] for i in temp['country_id'] ]
data['UNIT'] = 'K'
for year in list(dict.fromkeys(smelter_production['year_id'] )):
    for quarter in (list(dict.fromkeys( smelter_production['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = smelter_production.loc[(smelter_production['quarter']==quarter)&(smelter_production['year_id']==year),'value'].astype(float).to_list()
        except:
            print('smelter prod missing cols: ',col_name)
smelter_production = data
for col in smelter_production:
    smelter_production.rename(columns={
        col:col.strip()}, inplace=True)



selfsuppliedaacost.drop(columns=['creation_date', 'updation_date'],axis=1, inplace=True)
for col in selfsuppliedaacost.columns.to_list():
    selfsuppliedaacost.rename(columns={
        col:col.replace('_',' ').capitalize()
    }, inplace=True)
selfsuppliedaacost.rename(columns={
    'Selfsuppliedaacost id':'Ref',
    'Smelter id':'Smelter',
    'Ch smelter id':'Ch Smelter',
    'Self supply':'Self-supply',
    'Refinery id':'Refinery1',
    'Refinery id1':'Refinery2',
    'Inner mongolia':'Inner Mongolia'}, inplace=True)
selfsuppliedaacost['Smelter']   = [dsmelter[i] for i in selfsuppliedaacost['Smelter']]
selfsuppliedaacost['Refinery1'] = [drefineries[i] for i in selfsuppliedaacost['Refinery1'] ]
selfsuppliedaacost['Refinery2'] = [drefineries[i] for i in selfsuppliedaacost['Refinery2'] ]
selfsuppliedaacost['Ch Smelter'] = [dch_smelter[i] for i in selfsuppliedaacost['Ch Smelter']]


temp = selfsuppliedrefinary.loc[(selfsuppliedrefinary['quarter']=='Q1')&(selfsuppliedrefinary['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Refinary'] = [drefineries [i] for i in temp['refinery_id'] ]
for year in list(dict.fromkeys(selfsuppliedrefinary['year_id'] )):
    for quarter in (list(dict.fromkeys( selfsuppliedrefinary['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = selfsuppliedrefinary.loc[(selfsuppliedrefinary['quarter']==quarter)&(selfsuppliedrefinary['year_id']==year),'value'].astype(float).to_list()
        except:
            print('selfsuppliedrefinary missing cols: ',col_name)
selfsuppliedrefinary = data


aa_source.drop(['creation_date', 'updation_date'],axis=1, inplace=True)
for col in aa_source.columns:
    aa_source.rename(columns={
        col:col.replace('_',' ').capitalize()
    },inplace=True)
aa_source.rename(columns={
    'Aa source id':'Ref',
    'Smelter id':'Smelter',
    'Province id':'State/Province',
    'Ch smelter id':'Ch_Smelter',
    'Inner mongolia':'Inner Mongolia',
    'Inner mongolia self':'Inner Mongolia self'},inplace=True)
aa_source['Smelter'] = [dsmelter[i] for i in aa_source['Smelter']]
aa_source['State/Province'] = [dprovince[i] for i in aa_source['State/Province']]
aa_source['Ch_Smelter'] = [dch_smelter[i] for i in aa_source['Ch_Smelter']]
for col in aa_source.columns:
    try:
        aa_source[col] = [floar(i) for i in aa_source[col]]
    except:
        pass



for col in aasource_table_2.columns:
    aasource_table_2.rename(columns={
        col:col.replace('_',' ').capitalize() }, inplace=True)
aasource_table_2.rename(columns={
    'Province id':'State/Province',
    'Inner mongolia':'Inner Mongolia'},inplace=True)
aasource_table_2.drop(['Creation date','Updation date'], axis=1, inplace=True)
aasource_table_2['State/Province'] = [dprovince[i] for i in aasource_table_2['State/Province']]
for col in aasource_table_2.columns[1:]:
    aasource_table_2[col] = [float(i.replace('-','0')) for i in aasource_table_2[col]]



temp = electricity_source.loc[(electricity_source['quarter']=='Q1')&(electricity_source['year_id']=='Y_9')]
data = pd.DataFrame(columns=[])
data['Ref'] = list(range(1,len(temp)+1))
data['County'] = [dcountry[i] for i in temp['country_id'] ]
res = []
for i in temp['ch_smelter_id']:
    try:
        res.append(dch_smelter[i])
    except:
        res.append('-')
data['Smelter'] = [dsmelter[i] for i in temp['smelter_id']]
data['Ch_Smelter'] = res #[dch_smelter[i] for i in temp['ch_smelter_id']]
data['Energy Source'] = [i for i in temp['energy_source']]
data['UNIT'] = '%'
for year in list(dict.fromkeys(electricity_source['year_id'] )):
    for quarter in (list(dict.fromkeys( electricity_source['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            data[col_name] = electricity_source.loc[(electricity_source['quarter']==quarter)&(electricity_source['year_id']==year), 'electricity_source'].astype(float).to_list()
        except:
            print('electricity_source missing cols: ',col_name)
electricity_source = data          


tempp = aa_selfsupply_ratio.loc[(aa_selfsupply_ratio['quarter']=='Q1')&(aa_selfsupply_ratio['year_id']=='Y_9')]
dataa = pd.DataFrame(columns=[])
dataa['Ref'] = list(range(1,len(tempp)+1))
dataa['County'] = [dcountry[i] for i in tempp['country_id'] ]
res = []
for i in tempp['ch_smelter_id']:
    try:
        res.append(dch_smelter[i])
    except:
        res.append('-')
dataa['Smelter'] = [dsmelter[i] for i in tempp['smelter_id']]
dataa['Ch_Smelter'] = res #[dch_smelter[i] for i in tempp['ch_smelter_id']]
dataa['Energy Source'] = [i for i in tempp['energy_source']]
dataa['UNIT'] = '%'
for year in list(dict.fromkeys(aa_selfsupply_ratio['year_id'] )):
    for quarter in (list(dict.fromkeys( aa_selfsupply_ratio['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            dataa[col_name] = aa_selfsupply_ratio.loc[(aa_selfsupply_ratio['quarter']==quarter)&(aa_selfsupply_ratio['year_id']==year), 'selfsuppply_ratio'].astype(float).to_list()
        except:
            print('aa_selfsupply_ratio missing cols: ',col_name)


tem =aa_web3.loc[(aa_web3['quarter']=='Q1')&(aa_web3['year_id']=='Y_9')]
web3 = pd.DataFrame(columns=[])
web3['State/Province'] =[dprovince[i] for i in tem['province_id']]
web3['UNIT'] = 'RMB/yr'
for year in list(dict.fromkeys(aa_web3['year_id'] )):
    for quarter in (list(dict.fromkeys( aa_web3['quarter'] ))):
        year_value = dyear[year]
        col_name = year_value  +' '+ quarter
        try:
            web3[col_name] = aa_web3.loc[(aa_web3['quarter']==quarter)&(aa_web3['year_id']==year), 'Value'].astype(float).to_list()
        except:
            pass


aa_capital_cost.drop(['aa_capital_cost','updation_date', 'creation_date'], axis =1, inplace=True)
aa_capital_cost.rename(columns={
    'reference_number':'Ref',
    'smelter':'Smelter',
    'capital_cost':'Capital Cost',
    'greenfield_or_brownfield':'Greenfield/Brownfield',
    'construction_period':'Construction period',
    'finance_charge':'Finance charge (Intererest rate)'}, inplace=True)
aa_capital_cost['Smelter'] = [dsmelter[i] for i in aa_capital_cost['Smelter']]
for col in ['Capital Cost','Construction period','Finance charge (Intererest rate)']:
    aa_capital_cost[col] = [float(i) for i in aa_capital_cost[col]]
aa_capital_cost = aa_capital_cost.set_index('Ref').T.reset_index().rename(columns={'index':'Ref'})

writer = pd.ExcelWriter('_AL cost model.xlsx')
data_engine.to_excel(writer, sheet_name='Data Engine', index=False)
aa_capital_cost.to_excel(writer, sheet_name='Capital Cost', index=False)
AlF3_Price.to_excel(writer, sheet_name='AlF3_Price', index=False)
carbon_price.to_excel(writer, sheet_name='carbon price', index=False)
carbon_source.to_excel(writer, sheet_name='Carbon Source', index=False)
capitalcost.to_excel(writer, sheet_name='CapitalCost', index=False)
captive_power_cost.to_excel(writer, sheet_name='Captive power cost', index=False)
electricity_consumption.to_excel(writer, sheet_name='Electricity Consumption', index=False)
electricity_grid.to_excel(writer, sheet_name='Electricity Grid', index=False)
electricity_source.to_excel(writer, sheet_name='Electricity Source', index=False)###
naturalgasprice.to_excel(writer, sheet_name='NaturalGasPrice', index=False)
salary.to_excel(writer, sheet_name='Salary', index=False)
currency_input_id.to_excel(writer, sheet_name='Currency', index=False)
smelter_capacity.to_excel(writer, sheet_name='Smelter Capacity', index=False)
smelter_production.to_excel(writer, sheet_name='Smelter Production', index=False)
aa_source.to_excel(writer, sheet_name='Aa Source', index=False)
web3.to_excel(writer, sheet_name='web3', index=False)
selfsuppliedaacost.to_excel(writer, sheet_name='SelfsuppliedAacost', index=False)
dataa.to_excel(writer, sheet_name='AaselfsupplyRatio', index=False)##
selfsuppliedrefinary.to_excel(writer, sheet_name='SelfSuppliedRefinary', index=False)
aasource_table_2.to_excel(writer, sheet_name='AaSource', index=False)
dataa.to_excel(writer, sheet_name='Aa SelfSupply Ratio', index=False)##
writer.save()







