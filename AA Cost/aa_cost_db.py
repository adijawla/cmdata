#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  3 20:12:42 2020

@author: jondexter
"""

import pandas as pd
import numpy as np
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
downer = {ownership['ownership_id'][x]:ownership['ownership'][x] for x in ownership.index}

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

#refs_refs = pd.read_csv('ref_refineries.csv')
#drefs = {self.capbase['Refinery'][x]:self.capbase['Ref. No.'][x] for x in self.capbase.index}
class AA_Cost_DB():
    def __init__(self):
        self.import_bx_tonnes_cfr = pd.read_sql_query("SELECT * FROM [dbo].[import_bx_tonnes_cfr]", conn)
        self.Imported_Bx_CharacteristicsV2 = pd.read_sql_query("SELECT * FROM [dbo].[Imported_Bx_CharacteristicsV2]", conn)
        self.refineries_reacting_values =  pd.read_sql_query("SELECT * FROM [dbo].[refineries_reacting_values]", conn)
        
        self.aacost_standard_digestion_factor =  pd.read_sql_query("SELECT * FROM [dbo].[aacost_standard_digestion_factor]", conn)
        self.aacost_freight_prices =  pd.read_sql_query("SELECT * FROM [dbo].[aacost_freight_prices]", conn)
        self.aacost_calcningtechnology =  pd.read_sql_query("SELECT * FROM [dbo].[aacost_calcningtechnology]", conn)
        self.aacost_exchange_rate =  pd.read_sql_query("SELECT * FROM [dbo].[aacost_exchange_rate]", conn)
        self.aacost_calorificfactor =  pd.read_sql_query("SELECT * FROM [dbo].[aacost_calorificfactor]", conn)
        self.pdt_cost1input =  pd.read_sql_query("SELECT * FROM [dbo].[pdt_cost1input]", conn)
        
        self.pdt_cost2input =  pd.read_sql_query("SELECT * FROM [dbo].[pdt_cost2input]", conn)
        
        self.capbase =  pd.read_sql_query("SELECT * FROM [dbo].[capbase]", conn)
        self.proddatah =  pd.read_sql_query("SELECT * FROM [dbo].[proddatah]", conn)
        
        self.bauxite_pdtdata =  pd.read_sql_query("SELECT * FROM [dbo].[refineries_pdtdata]", conn)
        self.transport_fee_of_import_bauxite =  pd.read_sql_query("SELECT * FROM [dbo].[transport_fee_of_import_bauxite]", conn)
        
        self.Base_capacity_2_base_production_2 =  pd.read_sql_query("SELECT * FROM [dbo].[Base_capacity_2_base_production_2]", conn)
        
        self.base_capprod_split =  pd.read_sql_query("SELECT * FROM [dbo].[base_capprod_split]", conn)
        self.imported_bx_cfr_technology_codes =  pd.read_sql_query("SELECT * FROM [dbo].[imported_bx_cfr_technology_codes]", conn)
        #self. =  pd.read_sql_query("SELECT * FROM [dbo].[]", conn)
        
        self.data = pd.DataFrame(columns=[])
        self.data2 = pd.DataFrame(columns=[])
        self.imported_bx_xtics = pd.DataFrame(columns=[])
        
        #self.drefs = {self.capbase['Refinery'][x]:self.capbase['Ref. No.'][x] for x in self.capbase.index}
    def callall(self):
        self.import_bx_tonnes_cfr.rename(columns={
            'No':'No.',
            'show':'Show',
            'country_id':'Country',
            'mine_id':'Mine / Product',
            'item':'Item',
            'units':'Units'},inplace=True)
        self.import_bx_tonnes_cfr['Country'] = [dcountry[x] for x in self.import_bx_tonnes_cfr['Country']]
        self.import_bx_tonnes_cfr['Mine / Product'] = [dmine[x] for x in self.import_bx_tonnes_cfr['Mine / Product']]
      
        imp= aa.import_bx_tonnes_cfr.loc[aa.import_bx_tonnes_cfr['refinery_reference']=='1.1']
        data = self.data
        data['No.'] = imp['No.']
        data['Show'] = imp['Show']
        data['Source'] = imp['Country'] + '-'+imp['Mine / Product']
        data['Country'] = imp['Country']
        data['Mine / Product'] = imp['Mine / Product']
        data['Item'] = imp['Item']
        data['Units'] = imp['Units']
        refs = (list(dict.fromkeys(self.import_bx_tonnes_cfr['refinery_reference'])))
        for ref in refs:
            data[ref] = self.import_bx_tonnes_cfr.loc[self.import_bx_tonnes_cfr['refinery_reference']==ref,'Value'].to_list()
            #data[ref].replace(0, np.nan, inplace=True)
        ##here
        self.data2['Ref. No.'] = self.imported_bx_cfr_technology_codes['ref_no']
        self.data2['Digestion technology'] = self.imported_bx_cfr_technology_codes['Digestion_technology']
        
        
        self.imported_bx_xtics['No.'] = self.Imported_Bx_CharacteristicsV2['Imported_Bx_CharacteristicsV2_id']
        self.imported_bx_xtics['Show'] = self.Imported_Bx_CharacteristicsV2['show']
        self.imported_bx_xtics['Country'] = [dcountry[x] for x in self.Imported_Bx_CharacteristicsV2['country_id']]
        self.imported_bx_xtics['Mine / Product'] =[dmine[x] for x in self.Imported_Bx_CharacteristicsV2['mine_id']]
        self.imported_bx_xtics['Source'] = self.imported_bx_xtics['Country']+'-'+ self.imported_bx_xtics['Mine / Product']
        self.imported_bx_xtics['Moisture'] = self.Imported_Bx_CharacteristicsV2['Moisture']
        for col in self.Imported_Bx_CharacteristicsV2.columns[5:-2]:
            col_new = col[:-2].replace('_', ' ')+col[-2:].replace('_','.')
            self.imported_bx_xtics[col_new] = self.Imported_Bx_CharacteristicsV2[col]
        
        for col in self.refineries_reacting_values.columns:
            self.refineries_reacting_values.rename(columns={
                col:col.capitalize()}, inplace=True)
        
        #lookup tabs
        self.aacost_standard_digestion_factor.rename(columns={
            'keys':'Key',
            'digestion_technology':'Digestion Technology',
            'dsp_aa_sio2':'DSP AA:SiO2',
            'dsp_na_si':'DSP Na:Si',
            'lime_rate':'Lime rate',
            'required_steam_raising_energy':'Required Steam raising energy',
            'required_calcining_energy':'Required Calcining energy',
            'calcining_energy_gas':'Calcining energy (gas)',
            'extraction_efficiency':'Extraction Efficiency',
            'caustic_wash_loss':'Caustic wash loss',
            'limestone_rate':'Limestone rate',
            'alumina_quality':'Alumina quality',
            'required_electrical_energy':'Required Electrical energy'},inplace=True)
        self.aacost_standard_digestion_factor = self.aacost_standard_digestion_factor.drop(['creation_date', 'updation_date'], axis=1)
        self.aacost_standard_digestion_factor.drop_duplicates(keep='first', inplace=True)
        #convert values to float
        for col in self.aacost_standard_digestion_factor.columns:
            try:
                self.aacost_standard_digestion_factor[col] = self.aacost_standard_digestion_factor[col].astype(float)
            except:
                pass

     
        self.aacost_freight_prices = self.aacost_freight_prices.drop(['aacost_freight_prices_id','creation_date', 'updation_date'], axis=1)
        data=pd.DataFrame(columns=[])
        new_cols = ['Province Prices & Rates - Current',
         'Grid Electricity List Price',
         'Self-Supplied Electricity List Price',
         'Labor General List Price',
         'Labor Multiplier to All up Costs for Private Co.',
         'Labor Multiplier to All up Costs for SOE',
         'Price - Domestic Bx FAW',
         'Caustic Soda List Price',
         'Sodium Carbonate List Price',
         'Lime List Price',
         'Flocculent List Price',
         'Lignitous Coal List Price FAW',
         'Anthracite List Price',
         'Limestone List Price',
         'Lig. Coal Calorific Value',
         "A'cite Calorific Value",
         'Freight rate (trucking)',
         'Gas Price Delivered',
         'Gas Calorific Value',
         'Chlorine Price',
         'Red Mud Disposal Cost']
        old_cols = self.aacost_freight_prices.columns.to_list()
        for i in range(self.aacost_freight_prices.shape[1]):
            try:
                data[new_cols[i]] = self.aacost_freight_prices[old_cols[i]].astype(float)
            except:
                data[new_cols[i]] = self.aacost_freight_prices[old_cols[i]]
        self.aacost_freight_prices = data
        self.aacost_freight_prices['Province Prices & Rates - Current'] = [dprovince[x] for x in self.aacost_freight_prices['Province Prices & Rates - Current']]
        
        
        #all correct
        
        data = pd.DataFrame(columns=[])
        data['Fuel Source']= self.aacost_calcningtechnology['fuel_source']
        data['Energy requirement (GJ/t_aa)']= self.aacost_calcningtechnology['energy_requirement']
        self.aacost_calcningtechnology = data
        
        data = pd.DataFrame(columns=[])
        data['Exchange Rate']= [dcountry[x] for x in self.aacost_exchange_rate['exchange_rate']]
        data['USD/Local']= self.aacost_exchange_rate['usd_local']
        self.aacost_exchange_rate = data
        self.aacost_calorificfactor.rename(columns={
            'caloriific_value_conversion_factor':'Caloriific value conversion factor'},inplace=True)
        self.aacost_calorificfactor = self.aacost_calorificfactor.drop([ 'creation_date', 'updation_date'],axis=1)
        
        #PDT cost current1
        self.pdt_cost1input = self.pdt_cost1input.drop(['pdt_cost1input_id','creation_date', 'updation_date'], axis=1)
        data = pd.DataFrame(columns=[])
        col_new = ['Ref. No.',
                    'Summary Name ',
                    'Chinese Name',
                    'Sub Chinese Name',
                    'Prov - Category',
                    'Exchange Rate',
                    'Capacity - count totals',
                    'Capacity - main and labour cals',
                    'Bauxite 1 - Loading + Unloading Fee',
                    'Bauxite 1 - Customisation multiplier',
                    'Bauxite 2 - Loading + Unloading Fee',
                    'Bauxite 2 - Customisation multiplier',
                    'Bauxite 3 - Loading + Unloading Fee',
                    'Bauxite 3 - Customisation multiplier',
                    'Caustic Purchased - List Price Cus mulplier',
                    'Caustic Purchased - caustic strength transported',
                    'Caustic Purchased - transport distance',
                    'Caustic Purchased - Freight rate(trucking)',
                    'Caustic Purchased - [Loading + Unloading] Total Fee',
                    'Caustic Purchased - Customisation multiplier',
                    'Caustic Self supplied - Electricity Caustic Prod',
                    'Caustic Self supplied - Electricity Caustic Prod.1',
                    'Caustic Self supplied - Customisation multiplier 1',
                    'Caustic Self supplied - Customisation multiplier 2',
                    'Caustic Self supplied - Total Elec for prodn',
                    'Caustic Self supplied - Other Costs',
                    'Caustic Self supplied - tonnes per NaOH',
                    'Caustic Self supplied - Caustic Strenght as transported',
                    'Caustic Self supplied - Transport distance',
                    'Caustic Self supplied -Freight rate (trucking)',
                    'Caustic Self supplied - [Loading + Unloading] Total Fee',
                    'Caustic Self supplied - Customisation multiplier 3',
                    'Caustic Self supplied - Domestic Purchased',
                    'Energy St.R - sourcing lignitous Coal ',
                    'Energy St.R - Lignitous Coal - Customisation multiplier 1',
                    'Energy St.R - Lignitous Coal - Transport distance',
                    'Energy St.R - Lignitous Coal - Loading + Unloading Fee',
                    'Energy St.R - Lignitous Coal - Customisation multiplier 2',
                    'Energy St.R - Natural gas - Customisation multiplier 1',
                    'Energy for Calcining - Sourcing - Anthracite Coal',
                    'Energy for Calcining -  Anthracite Coal - Customistion multiplier 1',
                    'Energy for Calcining -  Anthracite Coal - Transport distance',
                    'Energy for Calcining -  Anthracite Coal - Loading + Unloading Fee',
                    'Energy for Calcining -  Anthracite Coal - Customistion multiplier 2',
                    'Energy for Calcining - Natural gas - Customistion multiplier',
                    'Energy for Calcining - Elec  Purchased- Customistion multiplier',
                    'Lime - custom mul 1',
                    'Lime - Transport distance',
                    'Lime - Loading + unloading Fee',
                    'Lime - custom mul 2',
                    'Limestone - custom mul 1',
                    'Limestone - Transport distance',
                    'Limestone - loading + unloading fee',
                    'Limestone - custom mul 2',
                    'Labour - custom mul',
                    'Labour - hours worked per year',
                    'Consumables - custom mul 1',
                    'Consumables - Transport distance',
                    'Consumables - Loading + Unloading Fee',
                    'Consumables - custom mul 2',
                    'Bauxite - Usage - Handling losses',
                    'Caustic - Final sodium',
                    'Energy -Usage -cus mul 1',
                    'Energy -Usage -cus mul 2',
                    'Energy -Usage -cus mul 3',
                    'Lime - Usage -Custom mul',
                    'Limestone - Usage -Custom mul',
                    'Labour - Usage -Custom',
                    'Consumables -Usage -Usage rate',
                    'Consumables -Usage -customise',
                    'Maintenance - Portion of capex',
                    'Other costs - Packaging',
                    'Updated_Caustic_Customisation_Multiplier']
        col_old = self.pdt_cost1input.columns.to_list()
        for i in range(len(col_new)):
            try:
                data[col_new[i]] = [float(i) for i in  self.pdt_cost1input[col_old[i]]]
            except:
                try:
                    data[col_new[i]] = [(float(i.strip('%'))/100) for i in  self.pdt_cost1input[col_old[i]]]
                except:
                    try:
                        data[col_new[i]] = [float(i.replace(',','')) for i in  self.pdt_cost1input[col_old[i]]]
                    except:
                        data[col_new[i]] =  self.pdt_cost1input[col_old[i]]
                        
        data['Summary Name '] = [ drefineries[x] for x in data['Summary Name ']]
        ch=[]
        for x in data['Chinese Name']:
            try:
                ch.append(dchinese[x])
            except:
                ch.append(float('nan'))
        data['Chinese Name'] = ch
        data['Sub Chinese Name'] = [ dsubChinese[x] for x in data['Sub Chinese Name']]
        data['Prov - Category'] = [ dcat[x] for x in data['Prov - Category']]
        self.pdt_cost1input = data
        
        
        
        ###pdt 2
        self.pdt_cost2input = self.pdt_cost2input.drop(['creation_date','updation_date'],axis=1)
        data = pd.DataFrame(columns=[])
        col_new = ['Ref. No.',
         'Summary Name',
         'Ownership',
         'Capacity - count totals',
         'Capacity - cap maintenance calcs',
         'Alumina Produced',
         'Bauxite 1 - mining Dressing (FAW)',
         'Bauxite 1 - final freight to ref',
         'Bauxite 2 - mining Dressing (FAW)',
         'Bauxite 2 - final freight to ref',
         'Caustic - Purchased - List Price Custom mul',
         'Energy - Sourcing Lig Coal',
         'Energy - Lignitous Coal Custom mul',
         'Energy - Lignitous Coal final freight',
         'Energy - natural gas custom mul',
         'Energy Calcing - sourcing anth',
         'Energy Calcing - anth cos mul',
         'Energy Calcing - anth final frieght',
         'Energy Calcing - Natural gas custom mul',
         'Energy Calcing - Electricity custom mul',
         'Lime - custom mul',
         'Lime - Final freight',
         'Limestone - custom mul',
         'Limestone - Final freight',
         'Labour - custom mul',
         'Labour - hours worked',
         'Labour - all up costs Private',
         'Labour - all up costs SOE',
         'Labour - final multiplier',
         'Flocculent - custom mul',
         'Flocculent - final freight',
         'Usage - Bauxite -Handling loss',
         'Usage - Sodium Carbonate use rate',
         'Usage - Energy -custom mul 1',
         'Usage - Energy -custom mul 2',
         'Usage - Energy -custom mul 3',
         'Usage - Lime Custom mul',
         'Usage - Limestone Custom mul',
         'Usage -Labour Customise',
         'Usage - Flocculent - usage rate',
         'Usage - Flocculent - customise',
         'C&M Formulae Capital',
         'Maintenance Portion of Capex',
         'Other costs - allowance',
         'Other costs - Packaging']
        col_old = self.pdt_cost2input.columns.to_list()
        for i in range(len(col_new)):
            try:
                data[col_new[i]] = [float(i) for i in  self.pdt_cost2input[col_old[i]]]
            except:
                try:
                    data[col_new[i]] = [(float(i.strip('%'))/100) for i in  self.pdt_cost2input[col_old[i]]]
                except:
                    try:
                        data[col_new[i]] = [float(i.replace(',','')) for i in  self.pdt_cost2input[col_old[i]]]
                    except:
                        data[col_new[i]] =  self.pdt_cost2input[col_old[i]]
        data['Summary Name'] = [dcountry[x] for x in data['Summary Name']]
        data['Ownership'] = [downer[x] for x in data['Ownership']]
        data['Ref. No.'] = [i+150 for i in data['Ref. No.']]
        self.pdt_cost2input = data
        
        self.yy = pd.DataFrame(columns=[])
        self.yy['Year'] = [2020]
        
        
        
        #ref cap and production
        cap = pd.DataFrame(columns=[])
        #cap['Ref. No.']
        imp = self.capbase.loc[self.capbase['year_id']=='Y_1']
        cap['Ref. No.'] = imp['refinery_reference']
        cap['Refinery'] = [drefineries[x] for x in imp['refinery_id']]
        cap['Province'] = [dprovince[x] for x in imp['province_id']]
        cap['Prov - Category'] = [dcat[x] for x in imp['category_id']]
        cap['Technology'] =  [dtech[x] for x in imp['technology_id']]
        cap['Ownership'] = [downer[x] for x in imp['ownership_id']]
        cap['Owner'] = [dOwner[x] for x in imp['owner_id']]
        cap['Bauxite Previous'] = [dbauxite[x] for x in imp['bauxite_id']]
        cap['Bauxite Now'] = cap['Bauxite Previous'] 
        for year in list(dict.fromkeys(self.capbase['year_id'])):
            year_name = dyear[year]
            cap[year_name] = self.capbase.loc[self.capbase['year_id']==year,'capacity'].to_list()
        self.capbase = cap
        #[ drefs[x] for x in self.capbase['Refinery']]
        
        pro = pd.DataFrame(columns=[])
        imp = self.proddatah.loc[self.proddatah['year_id']=='Y_1']
        pro['Ref. No.'] = imp['refinery_reference']
        pro['Refinery'] = [drefineries[x] for x in imp['refinery_id']]
        pro['Province'] = [dprovince[x] for x in imp['province_id']]
        pro['Prov - Category'] = [dcat[x] for x in imp['category_id']]
        pro['Technology'] =  [dtech[x] for x in imp['technology_id']]
        pro['Ownership'] = [downer[x] for x in imp['ownership_id']]
        pro['Owner'] = [dOwner[x] for x in imp['owner_id']]
        pro['Bauxite Previous'] = [dbauxite[x] for x in imp['bauxite_id']]
        pro['Bauxite Now'] = pro['Bauxite Previous']
        for year in list(dict.fromkeys(self.proddatah['year_id'])):
            year_name = dyear[year]
            pro[year_name] = self.proddatah.loc[self.proddatah['year_id']==year,'production'].to_list()
        self.proddatah = pro
        self.proddatah = self.proddatah.fillna(0)
        #[ drefs[x] for x in self.proddatah['Refinery']]
        
        bcbp = self.Base_capacity_2_base_production_2.loc[self.Base_capacity_2_base_production_2['year_id']=='Y_1']
        self.bc2= pd.DataFrame(columns=[])
        self.bp2= pd.DataFrame(columns=[])
        self.bc2['Ref. No.'] = bcbp['refinery_reference']
        self.bc2['Refinery'] = [drefineries[x] for x in bcbp['refinery_id']]
        self.bc2['Province'] =[dprovince[x] for x in bcbp['province_id']]
        self.bc2['Prov - Category'] = [dcat[x] for x in bcbp['category_id']]
        self.bc2['Technology'] = [dtech[x] for x in bcbp['technology_id']]
        self.bc2['Ownership'] = [downer[x] for x in bcbp['ownership_id']]
        self.bc2['Owner'] = [dOwner[x] for x in bcbp['owner_id']]
        self.bc2['Bauxite Previous'] = bcbp['bauxite_previous'].fillna(0)
        self.bc2['Bauxite Now'] = bcbp['bauxite_now'].fillna(0)
        
        self.bp2 = self.bc2.copy()
        for year in list(dict.fromkeys(self.Base_capacity_2_base_production_2['year_id'])):
            year_name = dyear[year]
            self.bc2[year_name] = pd.Series(self.Base_capacity_2_base_production_2.loc[self.Base_capacity_2_base_production_2['year_id']==year,'base_capacity'].to_list()).fillna(0)
            self.bp2[year_name] = pd.Series(self.Base_capacity_2_base_production_2.loc[self.Base_capacity_2_base_production_2['year_id']==year,'base_production'].to_list()).fillna(0)
        
        self.base_capprod_split
        self.bpros = pd.DataFrame(columns=[])
        self.bcaps = pd.DataFrame(columns=[])
        bb = self.base_capprod_split.loc[self.base_capprod_split['Attribute']=='Y_1']
        self.bpros['Ref. No.'] = bb['ref_no']
        self.bpros['Refinery'] = [drefineries[x] for x in bb['refinery_id']]
        self.bpros['Province'] =[dprovince[x] for x in bb['province_id']]
        self.bpros['Prov - Category'] = [dcat[x] for x in bb['category_id']]
        self.bpros['Technology'] = [dtech[x] for x in bb['technology_id']]
        self.bpros['Ownership'] = [downer[x] for x in bb['ownership_id']]
        self.bpros['Owner'] = [dOwner[x] for x in bb['owner_id']]
        self.bpros['Bauxite Previous'] = bb['bauxite_previous'].fillna(0)
        self.bpros['Bauxite Now'] = bb['bauxite_now'].fillna(0)
        self.bcaps = self.bpros
        
        for year in list(dict.fromkeys(self.base_capprod_split['Attribute'])):
            year_name = dyear[year]
            self.bcaps[year_name] = pd.Series(self.base_capprod_split.loc[self.base_capprod_split['Attribute']==year,'base_cap_splits'].to_list()).fillna(0)
            self.bpros[year_name] = pd.Series(self.base_capprod_split.loc[self.base_capprod_split['Attribute']==year,'base_prod_splits'].to_list()).fillna(0)
        self.bcaps = self.bcaps.fillna(0)
        self.bpros = self.bpros.fillna(0)
        self.bp2 = self.bp2.fillna(0)
        self.bc2 = self.bc2.fillna(0)   
        
        
        
        #refineries pdt data
        self.bauxite_pdtdata = self.bauxite_pdtdata.drop(['creation_date', 'updation_date'],axis=1)
        col_new = ['Ref. No.',
         'Refinery',
         'Ownership',
         'Alumina Produced',
         'Digestion Technology Code',
         'Bauxite1 - Aa Grade (Reacting)',
         'Bauxite1 - A/S ratio (Reacting)',
         'Bauxite1 - Moisture',
         'Bauxite1 - Mining-Dressing (FAW) Cost',
         'Bauxite1 - Transport distance',
         'Bauxite 2 - Aa Grade (Reacting)',
         'Bauxite 2 - A/S ratio (Reacting)',
         'Bauxite 2 - Moisture',
         'Bauxite 2 - Transport distance',
         'Bauxite 3 - Transport distance',
         'Bauxite 3 - Transport fee',
         'Bauxite Sourcing - Domestic Self Supply',
         'Bauxite Sourcing - Domestic Purchased',
         'Cus Mul - BAR',
         'Cus Mul - Caustic']
        
        col_old = ['refinery_reference',
         'refinery_id',
         'ownership_id',
         'alumina_produced',
         'digestion_technology_code',
         'bauxite1_aa_grade_reacting',
         'bauxite1_as_ratio_reacting',
         'bauxite1_moisture',
         'bauxite1_mining_dressing_faw_cost',
         'bauxite1_transport_distance',
         'bauxite_2_aa_grade_reacting',
         'bauxite_2_as_ratio_reacting',
         'bauxite_2_moisture',
         'bauxite_2_transport_distance',
         'bauxite_3_transport_distance',
         'bauxite_3_transport_fee',
         'bauxite_sourcing_domestic_self_supply',
         'bauxite_sourcing_domestic_purchased',
         'cus_mul_bar',
         'cus_mul_caustic']
        
        bax = pd.DataFrame(columns=[])
        for i in range(len(col_new)):
            bax[col_new[i]] = self.bauxite_pdtdata[col_old[i]]
        bax['Refinery'] = [drefineries[x] for x in bax['Refinery']]
        bax['Ownership'] = [downer[x] for x in bax['Ownership']]
        self.bauxite_pdtdata = bax
        try:
           self.bauxite_pdtdata['Bauxite Sourcing - Domestic Self Supply']= [(float(i.strip('%'))/100) for i in self.bauxite_pdtdata['Bauxite Sourcing - Domestic Self Supply']]
        except:
            pass
        try:
            self.bauxite_pdtdata['Bauxite Sourcing - Domestic Purchased'] = [(float(i.strip('%'))/100) for i in self.bauxite_pdtdata['Bauxite Sourcing - Domestic Purchased']  ]
        except:
            pass
        
        #transport_fee_of_import_bauxite
        self.ref_options = pd.DataFrame(columns=[])
        ff = self.refineries_reacting_values.loc[7:,:]
        ff = ff.reset_index(drop=True)
        self.ref_options['Option 1 - Port'] = pd.Series([dport[x] for x in self.transport_fee_of_import_bauxite['port_id']])
        self.ref_options['Option 1 - Railway (km)'] = float('nan')
        self.ref_options['Option 1 - Road (km)']    =  pd.Series(self.transport_fee_of_import_bauxite['option_1_road_km'])
        self.ref_options['Option 1 - Waterway (km)'] = pd.Series(self.transport_fee_of_import_bauxite['option_1_waterway_km'])
        self.ref_options['Option 1 - Freight']       = pd.Series(self.transport_fee_of_import_bauxite['option_1_freight'])
        self.ref_options['Option 2 - Port']         = pd.Series([dport[x] for x in self.transport_fee_of_import_bauxite['port_id2']])
        self.ref_options['Option 2 - Railway (km)'] = pd.Series(self.transport_fee_of_import_bauxite['option_2_railway_km'])
        self.ref_options['Option 2 - Road (km)'] = pd.Series(self.transport_fee_of_import_bauxite['option_2_road_km'])
        self.ref_options['Option 2 - Freight']  = pd.Series(self.transport_fee_of_import_bauxite['option_2_freight'])
        self.ref_options['Option']              = pd.Series(self.transport_fee_of_import_bauxite['option'])
        #self.ref_options['Freight']             = pd.Series(self.transport_fee_of_import_bauxite['freight'])
        self.ref_options['Freight - FS']             = pd.Series(self.transport_fee_of_import_bauxite['freight'])
        self.ref_options['Port Handling incl']  = pd.Series(self.transport_fee_of_import_bauxite['port_handling_incl'])
        self.ref_options['Total Transport ']    = pd.Series(self.transport_fee_of_import_bauxite['total_transport'])
        self.ref_options['Factor'] = pd.Series(ff['Factor'])
        self.ref_options['Value'] = pd.Series(ff['Value'])
    
        
        
aa = AA_Cost_DB()
aa.callall()

writer1 = pd.ExcelWriter('inputs//_Import Bx Tonnes & CFR.xlsx')
aa.data2.to_excel(writer1, sheet_name='Sheet1', index=False)
aa.data.to_excel(writer1, sheet_name='Table1', index=False)
writer1.save()

writer2 = pd.ExcelWriter('inputs//_Imported Bx Characteristics.xlsx')
aa.imported_bx_xtics.to_excel(writer2, sheet_name='Imported Bx Characteristics', index=False)
aa.refineries_reacting_values.to_excel(writer2, sheet_name='Reacting Values', index=False)
writer2.save()

writer3 = pd.ExcelWriter('inputs//_Look up tables.xlsx')
aa.aacost_standard_digestion_factor.to_excel(writer3, sheet_name='Table 1', index=False)
aa.aacost_freight_prices.to_excel(writer3, sheet_name='Table 2', index=False)
aa.aacost_calcningtechnology.to_excel(writer3, sheet_name='Table 3', index=False)
aa.aacost_exchange_rate.to_excel(writer3, sheet_name='Table 4', index=False)
aa.aacost_calorificfactor.to_excel(writer3, sheet_name='Table 5', index=False)
writer3.save()

writer4 = pd.ExcelWriter('inputs//_PDT cost current.xlsx')
aa.yy.to_excel(writer4, sheet_name='Year', index=False)
aa.pdt_cost1input.to_excel(writer4, sheet_name='PDT Cost 1', index=False)
aa.pdt_cost2input.to_excel(writer4, sheet_name='PDT Cost 2', index=False)
writer4.save()

writer5 = pd.ExcelWriter('inputs//_Refineries Capacity and Production.xlsx')
aa.capbase.to_excel(writer5, sheet_name='Base Capacity', index=False)
aa.bcaps.to_excel(writer5, sheet_name='Base Cap Splits', index=False)
aa.proddatah.to_excel(writer5, sheet_name='Base Production', index=False)
aa.bpros.to_excel(writer5, sheet_name='Base Prod Splits', index=False)
aa.bc2.to_excel(writer5, sheet_name='Base Capacity 2', index=False)
aa.bp2.to_excel(writer5, sheet_name='Base Production 2', index=False)
writer5.save()

writer6 = pd.ExcelWriter('inputs//_Refineries PDT Data.xlsx')
aa.bauxite_pdtdata.to_excel(writer6, sheet_name='Bauxites', index=False)
aa.ref_options.to_excel(writer6, sheet_name='Options', index=False)
writer6.save()