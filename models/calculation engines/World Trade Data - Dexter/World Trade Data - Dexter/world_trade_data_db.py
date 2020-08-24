# -*- coding: utf-8 -*-
"""
Created on Tue Aug 18 11:17:22 2020

@author: JOHN
"""


import pandas as pd
import string
import pyodbc
import re

server = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'T@lst0y50'
driver= '{ODBC Driver 17 for SQL Server}'

conn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)

country = pd.read_sql_query("SELECT * FROM [dbo].[country]", conn)
dcountry = {country['country_id'][x]:country['country'][x] for x in country.index}

year = pd.read_sql_query("SELECT * FROM [dbo].[year]", conn)
dyear = {year['year_id'][x]:year['year'][x] for x in year.index}
month_id = pd.read_sql_query("SELECT * FROM [dbo].[month_id]", conn)
dmonth = {month_id['month_id'][x]:month_id['month'][x] for x in month_id.index}
class world_trade_db():
    def __init__(self):
        self.final_processing_lastquarter_lastyear = pd.read_sql_query("SELECT * FROM [dbo].[final_processing_lastquarter_lastyear]", conn)
        self.importer_exporter_combination = pd.read_sql_query("SELECT * FROM [dbo].[importer_exporter_combination]", conn)
        self.directinput_worldtradedata = pd.read_sql_query("SELECT * FROM [dbo].[directinput_worldtradedata]", conn)
        self.declared_exporter = pd.read_sql_query("SELECT * FROM [dbo].[declared_exporter]", conn)
        self.declared_importer = pd.read_sql_query("SELECT * FROM [dbo].[declared_importer]", conn)
        self.cm_china_import_data = pd.read_sql_query("SELECT * FROM [dbo].[cm_china_import_data]", conn)
        self.exporter_importer_combination = pd.read_sql_query("SELECT * FROM [dbo].[exporter_importer_combination]", conn)
        # = pd.read_sql_query("SELECT * FROM [dbo].[]", conn)
        # = pd.read_sql_query("SELECT * FROM [dbo].[]", conn)
        # = pd.read_sql_query("SELECT * FROM [dbo].[]", conn)
        
        self.Final_Processing_Last_Quarter = pd.DataFrame(columns=[], index=self.final_processing_lastquarter_lastyear.index)
        self.Final_Processing_Last_Year = pd.DataFrame(columns=[], index=self.final_processing_lastquarter_lastyear.index)
        self.Final_Table_Final_Processing = pd.DataFrame(columns=[], index=self.final_processing_lastquarter_lastyear.index)
        self.exporter = pd.DataFrame(columns=[])
        self.rowexporter =pd.DataFrame(columns=[])
        self.importer = pd.DataFrame(columns=[])
        self.rowimporter =pd.DataFrame(columns=[])
        self.cm_table =pd.DataFrame(columns=[]) 
        self.DM = pd.DataFrame(columns=[])


    def calcall(self):
        #self.final_processing_lastquarter_lastyear
        self.Final_Processing_Last_Quarter['Exporter'] = [ dcountry[ self.final_processing_lastquarter_lastyear.loc[i,'exporter_id'] ] for i in  self.Final_Processing_Last_Quarter.index]
        self.Final_Processing_Last_Quarter['Importer'] = [ dcountry[ self.final_processing_lastquarter_lastyear.loc[i,'importer_id'] ] for i in  self.Final_Processing_Last_Quarter.index]
        self.Final_Processing_Last_Quarter['Mln t'] = self.final_processing_lastquarter_lastyear['Mlnt_last_quarter']
       
        
        self.Final_Processing_Last_Year['Exporter'] = [ dcountry[ self.final_processing_lastquarter_lastyear.loc[i,'exporter_id'] ] for i in  self.Final_Processing_Last_Quarter.index]
        self.Final_Processing_Last_Year['Importer'] = [ dcountry[ self.final_processing_lastquarter_lastyear.loc[i,'importer_id'] ] for i in  self.Final_Processing_Last_Quarter.index]
        self.Final_Processing_Last_Year['Mln t'] = self.final_processing_lastquarter_lastyear['Mlnt_last_year']
        
        
        self.Final_Table_Final_Processing['Exporter'] = self.Final_Processing_Last_Year['Exporter']
        self.Final_Table_Final_Processing['Importer'] = self.Final_Processing_Last_Year['Importer']
        self.Final_Table_Final_Processing['Mln t'] = float('nan')
        
        self.importer_exporter_combination.rename(columns={
            'importer_exporter_combination':'Importer.Exporter Combination'},inplace=True)
        self.importer_exporter_combination = self.importer_exporter_combination.drop(['creation_date', 'updation_date'], axis=1)
        
        self.directinput_worldtradedata.rename(columns={
            'exporter_id':'Exporter',
            'importer_id':'Importer',
            'total_for_quarter':'Total for Quarter',
            'percentage_of_total':"% of total",
            'annualised':'Annualised'
            }, inplace=True)
        self.directinput_worldtradedata['Exporter'] = [ dcountry[self.directinput_worldtradedata['Exporter'][i]] for i in self.directinput_worldtradedata.index]
        self.directinput_worldtradedata['Importer'] = [ dcountry[self.directinput_worldtradedata['Importer'][i]] for i in self.directinput_worldtradedata.index]
        self.directinput_worldtradedata = self.directinput_worldtradedata.drop(['directinput_worldtradedata_id','creation_date','updation_date'], axis=1)
        
        self.declared_importer.rename(columns={
            'year_id':'YEAR',
            'month_id':'MONTH',
            'reporting_importer_country':'REPORTING Importer COUNTRY',
            'unit':'UNIT',
            'partner_exporter_country':'PARTNER exporter COUNTRY',
            'quantity':'QUANTITY'
            }, inplace=True)
        self.declared_importer['YEAR'] = [ dyear[self.declared_importer['YEAR'][i]] for i in self.declared_importer.index ]
        self.declared_importer['MONTH'] = [ dmonth[self.declared_importer['MONTH'][i]] for i in self.declared_importer.index ]
        self.declared_importer['REPORTING Importer COUNTRY'] = [ dcountry[self.declared_importer.loc[i, 'REPORTING Importer COUNTRY']] for i in self.declared_importer.index]
        self.declared_importer['PARTNER exporter COUNTRY'] = [ dcountry[self.declared_importer.loc[i, 'PARTNER exporter COUNTRY']] for i in self.declared_importer.index]
        self.declared_importer = self.declared_importer.drop(['declared_importer_id','creation_date', 'updation_date'], axis=1)
        
        self.declared_exporter.rename(columns={
            'year_id':'YEAR',
            'month_id':'MONTH',
            'reporting_export_country':'REPORTING exporter COUNTRY',
            'unit':'UNIT',
            'partner_importer_country':'PARTNER importer COUNTRY',
            'quantity':'QUANTITY'},inplace=True)
        res = []
        for i in self.declared_exporter.index:
            try:
                res.append(dyear[self.declared_exporter['YEAR'][i]] )
            except:
                res.append(float('nan'))
        self.declared_exporter['YEAR'] = res
        self.declared_exporter['MONTH'] = [ dmonth[self.declared_exporter['MONTH'][i]] for i in self.declared_exporter.index ]
        self.declared_exporter['REPORTING exporter COUNTRY'] = [ dcountry[self.declared_exporter.loc[i, 'REPORTING exporter COUNTRY']] for i in self.declared_exporter.index]
        self.declared_exporter['PARTNER importer COUNTRY'] = [ dcountry[self.declared_exporter.loc[i, 'PARTNER importer COUNTRY']] for i in self.declared_exporter.index]
        self.declared_exporter = self.declared_exporter.drop(['declared_exporter_id','creation_date','updation_date'], axis=1)

        self.cm_china_import_data.rename(columns={
            'exporter_id':'Exporter',
            'year_id':'year',
            'month_id':'month',
            'cm_china_import_data':'data'}, inplace=True)
        self.cm_china_import_data['Exporter'] = [ dcountry[self.cm_china_import_data.loc[i, 'Exporter']] for i in self.cm_china_import_data.index]
        self.cm_china_import_data['year'] = [ dyear[self.cm_china_import_data.loc[i, 'year']] for i in self.cm_china_import_data.index]
        self.cm_china_import_data['month'] = [ dmonth[self.cm_china_import_data.loc[i, 'month']] for i in self.cm_china_import_data.index]
        self.cm_china_import_data = self.cm_china_import_data.drop(['cm_china_import_data_id','creation_date', 'updation_date'], axis=1)
        self.cm_table['Exporter'] =sorted(list(dict.fromkeys(self.cm_china_import_data['Exporter'])))
        self.cm_table['2019_10'] = [self.cm_china_import_data['data'][i] for i in range(0,len(self.cm_china_import_data) ,3)]
        self.cm_table['2019_11'] = [self.cm_china_import_data['data'][i] for i in range(1,len(self.cm_china_import_data) ,3)]
        self.cm_table['2019_12'] = [self.cm_china_import_data['data'][i] for i in range(2,len(self.cm_china_import_data) ,3)]
        self.cm_china_import_data = self.cm_table
        
        expp = [(re.split('_', i)[1]) for i in self.importer_exporter_combination['Importer.Exporter Combination']]
        self.exporter['Exporter'] = sorted(list(dict.fromkeys(expp)))
        self.rowexporter = self.exporter.copy()
        
        impp = [(re.split('_', i)[0]) for i in self.importer_exporter_combination['Importer.Exporter Combination']]
        self.importer['Importer'] =sorted(list(dict.fromkeys(impp)))
        rImp = self.importer['Importer'].copy().to_list()
        rImp.pop(rImp.index('China'))
        self.rowimporter['Importer'] = rImp
        
        exp = [ dcountry[self.exporter_importer_combination.loc[i, 'exporter_id']] for i in self.exporter_importer_combination.index]
        imp = [ dcountry[self.exporter_importer_combination.loc[i, 'importer_id']] for i in self.exporter_importer_combination.index]
        self.DM['Exporter.Importer Combination'] = [exp[i]+'_'+imp[i] for i in range(len(self.exporter_importer_combination))]
        #self.DM['Exporter.Importer Combination'] = sorted(self.DM['Exporter.Importer Combination'] =.to_list())
        
wd = world_trade_db()
wd.calcall()

writer = pd.ExcelWriter('World Trade Model.xlsx')
wd.importer_exporter_combination.to_excel(writer, sheet_name='Importer.ExporterCombination', index=False)
wd.importer.to_excel(writer, sheet_name='Importer', index=False)
wd.directinput_worldtradedata.to_excel(writer, sheet_name='Direct Input D26   H424', index=False)
wd.rowexporter.to_excel(writer, sheet_name='RowExporter', index=False)
wd.rowimporter.to_excel(writer, sheet_name='RowImporter', index=False)
wd.exporter.to_excel(writer, sheet_name='Exporter', index=False)
wd.declared_importer.to_excel(writer, sheet_name='DeclaredImporters', index=False)
wd.declared_exporter.to_excel(writer, sheet_name='DeclaredExporters', index=False)
wd.cm_china_import_data.to_excel(writer, sheet_name='CM China Import Data', index=False)
wd.Final_Table_Final_Processing.to_excel(writer, sheet_name='Final Table Final Processing', index=False)
wd.Final_Processing_Last_Quarter.to_excel(writer, sheet_name='Final Processing Last Quarter', index=False)
wd.Final_Processing_Last_Year.to_excel(writer, sheet_name='Final Processing Last Year', index=False)
wd.DM.to_excel(writer, sheet_name='DM', index=False)
writer.save()
