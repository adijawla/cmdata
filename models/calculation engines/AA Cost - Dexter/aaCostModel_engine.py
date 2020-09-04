import numpy as np
import pandas as pd
import time
import inspect
import datetime
import csv
from flatdbconverter import Flatdbconverter
import connect
from outputdb import uploadtodb

from aa_cost_db import AA_Cost_DB

ma_conv = Flatdbconverter("Aa Cost Model (CHN)")

#global imports
import_bx_tonnes_filename = 'inputs/_Import Bx Tonnes & CFR.xlsx'
imported_bx_xtcs_filename = 'inputs/_Imported Bx Characteristics.xlsx'
pdt_cost_current_filename ='inputs/_PDT cost current.xlsx'
lookup_filename = 'inputs/_Look up tables.xlsx'
refineriesPDT_filename = 'inputs/_Refineries PDT Data.xlsx'
refCapProduction_filename = 'inputs/_Refineries Capacity and Production.xlsx'

class Import_Bx_Tonnes():
    def __init__ (self):
        self.imp_bx_main = pd.read_excel(import_bx_tonnes_filename, sheet_name='Table1')
        self.table2 = pd.DataFrame(columns=[])
        self.table3 = pd.DataFrame(columns=[])
        self.table4 = pd.DataFrame(columns=[])
        self.table5 = pd.DataFrame(columns=[])
    
    def calc_all(self):
        self.cal_1()
        self.cal_2()
        self.cal_3()
        self.cal_4()
        
    #Table 2: Splitting out Tonnages only
    def cal_1(self):
        for col in self.imp_bx_main.columns:
            self.table2[col] = [self.imp_bx_main[col][x] for x in range(0, self.imp_bx_main.shape[0],2) ]
    
    #Table 3: Splitting out Prices only
    def cal_2(self):
        for col in self.imp_bx_main.columns:
            self.table3[col] = [self.imp_bx_main[col][x] for x in range(1, self.imp_bx_main.shape[0]+1,2) ]
        self.table3['Source'] = self.table2['Source']
        self.table3['Country'] = self.table2['Country']
        self.table3['Mine / Product'] = self.table2['Mine / Product']
        
    #Table 4: tonnages blank detection
    def cal_3(self):
        for col in self.table2.columns[:7].to_list():
            self.table4[col] = self.table2[col]
        for col in self.table2.columns[7:].to_list():
            self.table4[col] =  pd.isna(self.table2[col])
            
    #Table 5: Prices blank detection
    def cal_4(self):
        for col in self.table3.columns[:7].to_list():
            self.table5[col] = self.table3[col]
        for col in self.table3.columns[7:].to_list():
            self.table5[col] =  pd.isna(self.table3[col])
            
        
        

class PDT_Cost_Current():
    def __init__(self, bxTonnes):
        self.refPDT1 = pd.read_excel(refineriesPDT_filename, sheet_name='Bauxites')
        self.refPDT2= pd.read_excel(refineriesPDT_filename, sheet_name='Options')
        self.ref_base_cap = pd.read_excel(refCapProduction_filename, sheet_name='Base Capacity')
        self.ref_base_cap_regions = pd.read_excel(refCapProduction_filename, sheet_name='Base Cap Splits')
        self.ref_base_prod = pd.read_excel(refCapProduction_filename, sheet_name='Base Production')
        self.ref_base_prod_regions = pd.read_excel(refCapProduction_filename, sheet_name='Base Prod Splits')
        
        self.ImpBxSummary = pd.read_excel(import_bx_tonnes_filename, sheet_name='Sheet1').copy()
        self.ImportedBx = pd.read_excel(imported_bx_xtcs_filename, sheet_name='Imported Bx Characteristics')
        self.bxMain = bxTonnes
        self.pdt_cost_1 = pd.read_excel(pdt_cost_current_filename,  sheet_name = 'PDT Cost 1' )
        self.pdt_cost_2 = pd.read_excel(pdt_cost_current_filename,  sheet_name = 'PDT Cost 2' )
        self.lookup1 = pd.read_excel(lookup_filename, sheet_name='Table 1')
        self.lookup2 = pd.read_excel(lookup_filename, sheet_name='Table 2')
        self.lookup3 = pd.read_excel(lookup_filename, sheet_name='Table 3')
        self.lookup4 = pd.read_excel(lookup_filename, sheet_name='Table 4')
        self.lookup5 = pd.read_excel(lookup_filename, sheet_name='Table 5')
        self.macro_economic = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.cap_production = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.pdt_global = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.bauxite_1 = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.bauxite_2 = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.bauxite_3 = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.bauxite_sourcing_mix= pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.bauxite_final_xtics = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.caustic_purchase = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.caustic_selfsupplied = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.energy_for_steam = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.energy_calcining_alumina = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.lime = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.limestone = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.labour = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.consumables = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.usg_bauxite = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.usg_caustic = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.usg_energy = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.usg_lime = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.usg_limestone = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.usg_labour = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.usg_consumables = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.bauxite_cost = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.caustic_cost = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.energy_cost = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.lime_cost = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.labour_cost = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.consumables_cost = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.maintenance_cost = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.other_cost= pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.PDT1summmary = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.pdt1_headline = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        self.pdt_1_costSum = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        #self. = pd.DataFrame(columns=[], index=self.pdt_cost_1.index)
        
    def calc_all(self):
        self.cal_refPDT_all()
        self.refCapProd_calcs()
        self.importBx_summary()
        
        self.calc_macroEconomic()
        self.calc_capacityProduction()
        self.calc_global()
        self.calc_bauxite_1()  
        self.calc_bauxite_2() 
        self.calc_bauxite_3() 
        self.calc_bauxite_sourcing_mix()
        self.calc_bauxite_final_xtics()
        
        self.calc_caustic_purchased()
        self.calc_caustic_self_supplied()
        self.calc_energy_sourcing_mix()
        self.calc_energy_lignitous_coal()
        self.calc_energy_natural_gas()
        self.calc_energy_calcing_sourcing_mix()
        self.calc_energy_calcing_anthracite_coal()
        self.calc_energy_calcing_natural_gass()
        self.calc_energy_calcing_electicity()
        self.calc_lime_prices_mix()
        self.calc_limestone_prices_mix()
        self.calc_labour_prices_mix()
        self.calc_consumables_prices_mix()
        self.calc_usage_bauxite()
        self.calc_usage_caustic()
        self.calc_usage_energy()
        self.calc_usage_lime_limestone()
        self.calc_usage_labour()
        self.calc_usage_consumables()
        self.calc_costs_bauxite()
        self.calc_costs_caustic()
        self.calc_costs_energy()
        self.calc_costs_lime()
        self.calc_costs_labour()
        self.calc_costs_consumables()
        self.calc_costs_maintenance()
        self.calc_costs_other()
        self.calc_pdt_summary()
        self.calc_pdt_summary2()
        
    #refineries capcity production 
    def refCapProd_calcs(self):
        for col in self.ref_base_cap_regions.columns[9:]:
            for row in self.ref_base_cap_regions.index:
                val1 = self.ref_base_cap.loc[(self.ref_base_cap['Prov - Category']==self.ref_base_cap_regions.loc[row,'Prov - Category'])&(self.ref_base_cap['Bauxite Now']==self.ref_base_cap_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                val2 = self.ref_base_cap.loc[(self.ref_base_cap['Technology']==self.ref_base_cap_regions.loc[row,'Technology'])&(self.ref_base_cap['Bauxite Now']==self.ref_base_cap_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                self.ref_base_cap_regions.loc[row,col] =  val1+val2
                
        for col in self.ref_base_cap_regions.columns[9:]:
            for row in self.ref_base_prod_regions.index:
                val1 = self.ref_base_prod.loc[(self.ref_base_prod['Prov - Category']==self.ref_base_prod_regions.loc[row,'Prov - Category'])&(self.ref_base_prod['Bauxite Now']==self.ref_base_prod_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                val2 = self.ref_base_prod.loc[(self.ref_base_prod['Technology']==self.ref_base_prod_regions.loc[row,'Technology'])&(self.ref_base_prod['Bauxite Now']==self.ref_base_prod_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                self.ref_base_prod_regions.loc[row,col] =  val1+val2
      
    #so the import bx tonnes
    def importBx_summary(self):
        self.imp_summary_1()
        self.imp_summary_2()
        self.imp_summary_3()
        self.imp_summary_4()
        self.imp_summary_5()
        self.imp_summary_6()
        
    def imp_summary_1(self):
        d={self.ref_base_cap['Ref. No.'][x]:self.ref_base_cap['Refinery'][x] for x in self.ref_base_cap.index}
        for row in self.ImpBxSummary.index:
            try:
                self.ImpBxSummary.at[row,'Refinery'] = d[self.ImpBxSummary.loc[row, 'Ref. No']]
                self.ImpBxSummary.at[row,'tonnes'] = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']].astype(float).sum()
            except:
                self.ImpBxSummary.at[row,'tonnes'] = 0
                self.ImpBxSummary.at[row,'Refinery'] = 0
    def imp_summary_2(self):  
        for row in self.ImpBxSummary.index:
            value = 0
            if self.ImpBxSummary.loc[row,'tonnes']<=0:
                value =0
            else:
                value = self.bxMain.table3[self.ImpBxSummary.loc[row, 'Ref. No']] * self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']]
                value = value.sum()/self.ImpBxSummary.loc[row,'tonnes']
                
            self.ImpBxSummary.at[row,'Avg CFR Price'] = value
            
    def imp_summary_3(self):
        for row in self.ImpBxSummary.index:
            value = 0
            if self.ImpBxSummary.loc[row,'tonnes']<=0:
                value = 0
            else:
                value = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']]  *  self.ImportedBx['Moisture']
                value = value.sum()/self.ImpBxSummary.loc[row,'tonnes']
            self.ImpBxSummary.at[row,'Moisture'] = value
        
    def imp_summary_4(self):
        for row in self.ImpBxSummary.index:
            res = 0
            if self.ImpBxSummary.loc[row,'tonnes'] <= 0:
                res = 0
            elif self.ImpBxSummary.loc[row,'Digestion technology'] == 'LT':
                ans = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']] * self.ImportedBx['LT Bayer Refining.2']
                res = ans.sum()/self.ImpBxSummary.loc[row,'tonnes']
            elif self.ImpBxSummary.loc[row,'Digestion technology'] == 'MT':
                ans = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']] * self.ImportedBx['MT Bayer Refining.2']
                res = ans.sum()/self.ImpBxSummary.loc[row,'tonnes']
            elif self.ImpBxSummary.loc[row,'Digestion technology'] == 'HT':
                ans = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']] * self.ImportedBx['HT Bayer & Sinter.2']
                res = ans.sum()/self.ImpBxSummary.loc[row,'tonnes']
            else:
                res = 'error'
            self.ImpBxSummary.at[row, 'Silica Grade(Reacting)']  = res
    def imp_summary_5(self):
        for row in self.ImpBxSummary.index:
            res = 0
            if self.ImpBxSummary.loc[row,'tonnes'] <= 0:
                res = 0
            elif self.ImpBxSummary.loc[row,'Digestion technology'] == 'LT':
                ans = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']] * self.ImportedBx['LT Bayer Refining.1']
                res = ans.sum()/self.ImpBxSummary.loc[row,'tonnes']
            elif self.ImpBxSummary.loc[row,'Digestion technology'] == 'MT':
                ans = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']] * self.ImportedBx['MT Bayer Refining.1']
                res = ans.sum()/self.ImpBxSummary.loc[row,'tonnes']
            elif self.ImpBxSummary.loc[row,'Digestion technology'] == 'HT':
                ans = self.bxMain.table2[self.ImpBxSummary.loc[row, 'Ref. No']] * self.ImportedBx['HT Bayer & Sinter.1']
                res = ans.sum()/self.ImpBxSummary.loc[row,'tonnes']
            else:
                res = 'error'
            self.ImpBxSummary.at[row, 'Alumina Grade(Reacting']  = res
            
    def imp_summary_6(self):
        for row in self.ImpBxSummary.index:
            res = 0
            if self.ImpBxSummary.loc[row,'tonnes'] <= 0:
                res = 0
            else:
                if self.ImpBxSummary.loc[row,'Silica Grade(Reacting)'] <= 0:
                    ans = 0
                else:
                    ans =self.ImpBxSummary.loc[row, 'Alumina Grade(Reacting'] / self.ImpBxSummary.loc[row,'Silica Grade(Reacting)']
                res = ans
            self.ImpBxSummary.at[row,'A/S ratio(Reacting/Reacting)'] = res
        

        
    #im going to do refineries pdt here
    def cal_refPDT_all(self):
        for i in range(self.refPDT2.shape[0]):
            self.refPDT_cal_1(i)
        self.refPDT_cal_2()
            
    def refPDT_cal_1(self, row):
        road_rate = float(self.refPDT2.loc[self.refPDT2['Factor']=='Road Rate','Value'])
        rail_rate = float(self.refPDT2.loc[self.refPDT2['Factor']=='Rail Rate','Value'])
        waterway_rate = float(self.refPDT2.loc[self.refPDT2['Factor']=='Waterway Rate','Value'])
        rail_loading = float(self.refPDT2.loc[self.refPDT2['Factor']=='Rail Loading','Value'])
        port_handling = float(self.refPDT2.loc[self.refPDT2['Factor']=='Port Handling','Value'])
     
        if float(self.refPDT2.loc[row,'Option 1 - Road (km)'])> 0:
            self.refPDT2.loc[row,'Option 1 - Freight'] = float(self.refPDT2.loc[row,'Option 1 - Road (km)']) * road_rate + float(self.refPDT2.loc[row,'Option 1 - Waterway (km)']) * waterway_rate
        elif float(self.refPDT2.loc[row,'Option 1 - Waterway (km)'])> 0:
            self.refPDT2.loc[row,'Option 1 - Freight'] = float(self.refPDT2.loc[row,'Option 1 - Waterway (km)']) * waterway_rate
        else:
            self.refPDT2.loc[row,'Option 1 - Freight'] = ''
            
        if float(self.refPDT2.loc[row,'Option 2 - Railway (km)'])> 0:
            self.refPDT2.loc[row,'Option 2 - Freight'] = rail_loading + float(self.refPDT2.loc[row,'Option 2 - Railway (km)']) * rail_rate + float(self.refPDT2.loc[row,'Option 2 - Road (km)']) * road_rate
        elif float(self.refPDT2.loc[row,'Option 2 - Road (km)'])> 0:
            self.refPDT2.loc[row,'Option 2 - Freight'] = float(self.refPDT2.loc[row,'Option 2 - Road (km)']) * road_rate
        else:
            self.refPDT2.loc[row,'Option 2 - Freight'] = ''
            
        rows = [3,4,5,6,7,8,9,10,12,15,16,17,18, 19,23, 35, 36,39,40,  43,44,45,46,47, 50,51, 66, 82,  90,91, 94]
        for Row in rows:
            self.refPDT2.loc[Row,'Freight'] = self.refPDT2.loc[Row,'Option 1 - Freight'] if float(self.refPDT2.loc[Row,'Option']) ==1 else self.refPDT2.loc[Row,'Option 2 - Freight']
        
        try:
            res = 0
            if self.refPDT2.loc[row,'Freight'] == '':
                res = 'FALSE'
            else:
                if float(self.refPDT2.loc[row,'Port Handling incl']) == 1:
                    res = self.refPDT2.loc[row,'Freight']
                else:
                    res = float(self.refPDT2.loc[row,'Freight']) + float(port_handling)
            self.refPDT2.loc[row,'Total Transport '] = res
        except:
            print(row)    
    
    def refPDT_cal_2(self):
        self.refPDT1['Bauxite 3 - Transport fee'][:97] = self.refPDT2['Total Transport '][:97].copy()
     
    
            
    #im going to do refineries pdt above
        
        
    
        
    def calc_macroEconomic(self):
        d = {self.ref_base_cap['Ref. No.'][x]:self.ref_base_cap['Province'][x] for x in self.ref_base_cap.index}
        d2= {self.ref_base_cap['Ref. No.'][x]:self.ref_base_cap['Refinery'][x] for x in self.ref_base_cap.index}
        d3= {self.ref_base_cap['Ref. No.'][x]:self.ref_base_cap['Owner'][x] for x in self.ref_base_cap.index}
        self.macro_economic['Province'] = [d[self.pdt_cost_1['Ref. No.'][x]] for x in self.pdt_cost_1.index]
        self.macro_economic['Sub Name - technology variant'] = [d2[self.pdt_cost_1['Ref. No.'][x]] for x in self.pdt_cost_1.index]
        self.macro_economic['Owner'] = [d3[self.pdt_cost_1['Ref. No.'][x]] for x in self.pdt_cost_1.index]
        self.macro_economic['Exchange Rate'] = self.pdt_cost_1.loc[0, 'Exchange Rate']
    
    def calc_capacityProduction(self):
        year = '2020'
        self.cap_production['Count in current capacity totals'] = self.pdt_cost_1['Capacity - count totals']
        self.cap_production['Capacity - for maintenance and labour'] =  self.pdt_cost_1['Capacity - main and labour cals']
        for row in self.pdt_cost_1.index:
            self.cap_production.at[row,'Capacity -sub module'] = float(self.ref_base_cap.loc[self.ref_base_cap['Ref. No.']==self.pdt_cost_1.loc[row,'Ref. No.'], year])*1000
            self.cap_production.at[row,'Production - sub module'] =float(self.ref_base_prod.loc[self.ref_base_cap['Ref. No.']==self.pdt_cost_1.loc[row,'Ref. No.'], year]) *1000
        l=[]
        for row in self.pdt_cost_1.index:
            if self.cap_production.loc[row,'Capacity -sub module']>0:
                l.append( self.cap_production.loc[row,'Production - sub module']/self.cap_production.loc[row,'Capacity -sub module'])
            else:
                l.append(0)
        self.cap_production['Utilization - sub module']=  l
    def calc_global(self):
        self.pdt_global['Ownership'] = self.func_1_bauxites('Ownership')
        self.pdt_global['Alumina Produced'] = self.func_1_bauxites('Alumina Produced')
        self.pdt_global['Digestion Technology code'] = self.func_1_bauxites('Digestion Technology Code')
        d= {self.lookup1['Key'][x]:self.lookup1['Digestion Technology'][x]  for x in self.lookup1.index}
        l = []
        for _ in self.pdt_global['Digestion Technology code']:
            try:
                l.append(d[_])
            except:
                l.append(0)
        self.pdt_global['Refinery Digestion Technology'] = l#[d[int(self.pdt_global['Digestion Technology code'][x]) ] for x in self.pdt_global.index]
        
    def calc_bauxite_1(self):
        self.bauxite_1['Alumina Grade(Reacting)'] = self.func_1_bauxites('Bauxite1 - Aa Grade (Reacting)')
        self.bauxite_1['A/S ratio(Reacting/Reacting)'] = self.func_1_bauxites('Bauxite1 - A/S ratio (Reacting)')
        self.bauxite_1['Moisture'] = self.func_1_bauxites('Bauxite1 - Moisture')
        self.bauxite_1['Silica Grade(Reacting)'] = [(self.bauxite_1['Alumina Grade(Reacting)'][x]/self.bauxite_1['A/S ratio(Reacting/Reacting)'][x] if self.bauxite_1['A/S ratio(Reacting/Reacting)'][x] >0 else 0) for x in self.pdt_cost_1.index]
        
        self.bauxite_1['Mining-Dressing (FAW)'] = self.func_1_bauxites('Bauxite1 - Mining-Dressing (FAW) Cost')
        
        self.bauxite_1['Transport distance']        = self.func_1_bauxites('Bauxite1 - Transport distance')
        self.bauxite_1['Freight rate (trucking)']   = self.func_2_lk2('Freight rate (trucking)')
        self.bauxite_1['Loading + Unloading Fee']   = self.pdt_cost_1['Bauxite 1 - Loading + Unloading Fee']
        self.bauxite_1['Total Freight']             = self.bauxite_1['Transport distance']*self.bauxite_1['Freight rate (trucking)']+self.bauxite_1['Loading + Unloading Fee']
        self.bauxite_1['Customisation multiplier']  = self.pdt_cost_1['Bauxite 1 - Customisation multiplier']
        #custom multiply not good at all
        #self.bauxite_1['Customisation multiplier'] = [float(x.strip('%'))/100 for x in self.bauxite_1['Customisation multiplier']]
        self.bauxite_1['Final Freight to refinery'] = [(0 if self.bauxite_1['Transport distance'][x]<= 0 else self.bauxite_1['Total Freight'][x]*self.bauxite_1['Customisation multiplier'][x] ) for x in self.pdt_cost_1.index]
        
        self.bauxite_1['Total Delivered 1'] = self.bauxite_1['Mining-Dressing (FAW)']  + self.bauxite_1['Final Freight to refinery']
        self.bauxite_1['Total Delivered 2'] = [(self.bauxite_1['Total Delivered 1'][x]/(1-self.bauxite_1['Moisture'][x])) for x in self.pdt_cost_1.index]
        
    def calc_bauxite_2(self):
        self.bauxite_2['Alumina Grade(Reacting)'] = self.func_1_bauxites('Bauxite 2 - Aa Grade (Reacting)')
        self.bauxite_2['A/S ratio(Reacting/Reacting)'] = self.func_1_bauxites('Bauxite 2 - A/S ratio (Reacting)')
        self.bauxite_2['Moisture'] = self.func_1_bauxites('Bauxite 2 - Moisture')
        self.bauxite_2['Silica Grade(Reacting)'] = [(self.bauxite_2['Alumina Grade(Reacting)'][x]/self.bauxite_2['A/S ratio(Reacting/Reacting)'][x] if self.bauxite_2['A/S ratio(Reacting/Reacting)'][x] >0 else 0) for x in self.pdt_cost_1.index]
        
        self.bauxite_2['Price - Domestic Bx FAW'] = self.func_2_lk2('Price - Domestic Bx FAW')
        
        self.bauxite_2['Transport distance']        = self.func_1_bauxites('Bauxite 2 - Transport distance')
        self.bauxite_2['Freight rate (trucking)']   = self.func_2_lk2('Freight rate (trucking)')
        self.bauxite_2['Loading + Unloading Fee']   = self.pdt_cost_1['Bauxite 2 - Loading + Unloading Fee']
        self.bauxite_2['Total Freight']             = self.bauxite_2['Transport distance']*self.bauxite_2['Freight rate (trucking)']+self.bauxite_2['Loading + Unloading Fee']
        self.bauxite_2['Customisation multiplier']  = self.pdt_cost_1['Bauxite 2 - Customisation multiplier']
        #
        #self.bauxite_2['Customisation multiplier'] = [float(x.strip('%'))/100 for x in self.bauxite_2['Customisation multiplier']]
        self.bauxite_2['Final Freight to refinery'] = [(0 if self.bauxite_2['Transport distance'][x]<= 0 else self.bauxite_2['Total Freight'][x]*self.bauxite_2['Customisation multiplier'][x] ) for x in self.pdt_cost_1.index]
        
        self.bauxite_2['Total Delivered 1'] = self.bauxite_2['Price - Domestic Bx FAW']  + self.bauxite_2['Final Freight to refinery']
        self.bauxite_2['Total Delivered 2'] = [(self.bauxite_2['Total Delivered 1'][x]/(1-self.bauxite_2['Moisture'][x])) for x in self.pdt_cost_1.index]
    
    
    def calc_bauxite_3(self):
        self.bauxite_3['Alumina Grade(Reacting)'] = self.func_3_iBX('Alumina Grade(Reacting')
        self.bauxite_3['A/S ratio(Reacting/Reacting)'] = self.func_3_iBX('A/S ratio(Reacting/Reacting)')
        self.bauxite_3['Moisture'] = self.func_3_iBX('Moisture')
        self.bauxite_3['Silica Grade(Reacting)'] = [(self.bauxite_3['Alumina Grade(Reacting)'][x]/self.bauxite_3['A/S ratio(Reacting/Reacting)'][x] if self.bauxite_3['A/S ratio(Reacting/Reacting)'][x] >0 else 0) for x in self.pdt_cost_1.index]
        
        self.bauxite_3['Purchase - CFR'] = self.func_3_iBX('Avg CFR Price')
        
        self.bauxite_3['Total Transport Fee'] = self.func_1_bauxites('Bauxite 3 - Transport fee')
    
        self.bauxite_3['Transport distance']        = self.func_1_bauxites('Bauxite 3 - Transport distance')
        self.bauxite_3['Freight rate (trucking)']   = self.func_2_lk2('Freight rate (trucking)')
        self.bauxite_3['Loading + Unloading Fee']   = self.pdt_cost_1['Bauxite 3 - Loading + Unloading Fee']
        self.bauxite_3['Total Freight']             = self.bauxite_3['Transport distance']*self.bauxite_3['Freight rate (trucking)']+self.bauxite_3['Loading + Unloading Fee']
        self.bauxite_3['Customisation multiplier']  = self.pdt_cost_1['Bauxite 3 - Customisation multiplier']
        #
        #self.bauxite_3['Customisation multiplier'] = [float(x.strip('%'))/100 for x in self.bauxite_3['Customisation multiplier']]
        self.bauxite_3['Final Freight to refinery'] = [(0 if self.bauxite_3['Transport distance'][x]<= 0 else self.bauxite_3['Total Freight'][x]*self.bauxite_3['Customisation multiplier'][x] ) for x in self.pdt_cost_1.index]
        l = []
        for _ in self.bauxite_3['Total Transport Fee']:
            l.append(0) if type(_) == str else l.append(_)
        self.bauxite_3['Total Delivered 1'] = self.bauxite_3['Purchase - CFR'] *self.macro_economic['Exchange Rate'] *(1-self.bauxite_3['Moisture']) + l
        self.bauxite_3['Total Delivered 2'] = [(self.bauxite_3['Total Delivered 1'][x]/(1-self.bauxite_3['Moisture'][x])) for x in self.pdt_cost_1.index]
    
    def calc_bauxite_sourcing_mix(self):
        self.bauxite_sourcing_mix['Domestic Self Supply'] = self.func_1_bauxites('Bauxite Sourcing - Domestic Self Supply')
        self.bauxite_sourcing_mix['Domestic Purchased'] = self.func_1_bauxites('Bauxite Sourcing - Domestic Purchased')
        self.bauxite_sourcing_mix['Domestic Self Supply'] = self.bauxite_sourcing_mix['Domestic Self Supply'].astype(float)
        self.bauxite_sourcing_mix['Domestic Purchased']= self.bauxite_sourcing_mix['Domestic Purchased'].astype(float)
        self.bauxite_sourcing_mix['Import Purchased'] = 1-(self.bauxite_sourcing_mix['Domestic Self Supply']+self.bauxite_sourcing_mix['Domestic Purchased'])
        self.bauxite_sourcing_mix['Total'] = self.bauxite_sourcing_mix['Domestic Self Supply'].astype(float)+self.bauxite_sourcing_mix['Domestic Purchased'] +self.bauxite_sourcing_mix['Import Purchased']
        
    def calc_bauxite_final_xtics(self):
        val1 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite_1['Alumina Grade(Reacting)'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite_2['Alumina Grade(Reacting)'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite_3['Alumina Grade(Reacting)']
        val2 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite_1['Moisture'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite_2['Moisture'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite_3['Moisture']
        val3 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite_1['Silica Grade(Reacting)'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite_2['Silica Grade(Reacting)'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite_3['Silica Grade(Reacting)']
        
        self.bauxite_final_xtics['Alumina Grade (Reacting)'] = val1
        self.bauxite_final_xtics['Moisture'] =val2
        self.bauxite_final_xtics['Silica Grade (Reacting)']=val3
        self.bauxite_final_xtics['A/S ratio (Reacting / Reacting)'] = [ (self.bauxite_final_xtics['Alumina Grade (Reacting)'][x]/self.bauxite_final_xtics['Silica Grade (Reacting)'][x] if self.bauxite_final_xtics['Silica Grade (Reacting)'][x] > 0 else 0) for x in self.pdt_cost_1.index]
        
        val4 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite_1['Total Delivered 2'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite_2['Total Delivered 2'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite_3['Total Delivered 2']
        self.bauxite_final_xtics['Total Delivered'] = val4 #some condition dey here
    
    def calc_caustic_purchased(self):
        self.caustic_purchase['Caustic Soda List Price'] = self.func_2_lk2('Caustic Soda List Price')
        self.caustic_purchase['List Price Customisation multiplier'] = self.pdt_cost_1['Caustic Purchased - List Price Cus mulplier']
        #
        #self.caustic_purchase['List Price Customisation multiplier'] = [float(x.strip('%'))/100 for x in self.caustic_purchase['List Price Customisation multiplier']]
        self.caustic_purchase['Customised List Price'] = self.caustic_purchase['Caustic Soda List Price']*self.caustic_purchase['List Price Customisation multiplier'] 
        
        self.caustic_purchase['Caustic Strenght'] = self.pdt_cost_1['Caustic Purchased - caustic strength transported']
        self.caustic_purchase['Transport distance'] = self.pdt_cost_1['Caustic Purchased - transport distance']
        self.caustic_purchase['Freight rate (trucking)'] = self.pdt_cost_1['Caustic Purchased - Freight rate(trucking)']
        self.caustic_purchase['[Loading + Unloading] Total Fee'] = self.pdt_cost_1['Caustic Purchased - [Loading + Unloading] Total Fee']
        self.caustic_purchase['Total Freight - Before cus'] = self.caustic_purchase['Transport distance']*self.caustic_purchase['Freight rate (trucking)']+self.caustic_purchase['[Loading + Unloading] Total Fee']
        self.caustic_purchase['Customisation Multiplier'] = self.pdt_cost_1['Caustic Purchased - Customisation multiplier']
        #
        #self.caustic_purchase['Customisation Multiplier'] = [float(x.strip('%'))/100 for x in self.caustic_purchase['Customisation Multiplier']]
        self.caustic_purchase['Final Frieght to refinery 1'] = self.caustic_purchase['Total Freight - Before cus']*self.caustic_purchase['Customisation Multiplier']
        self.caustic_purchase['Final Frieght to refinery 2'] = [ (0 if self.caustic_purchase['Transport distance'][x]<=0 else self.caustic_purchase['Final Frieght to refinery 1'][x]/self.caustic_purchase['Caustic Strenght'][x]) for x in self.pdt_cost_1.index]
        self.caustic_purchase['Final Price - purchased caustic'] = self.caustic_purchase['Customised List Price'] + self.caustic_purchase['Final Frieght to refinery 2']
    
    def calc_caustic_self_supplied(self):
        self.caustic_selfsupplied['Electricity for Prod'] = self.pdt_cost_1['Caustic Self supplied - Electricity Caustic Prod']
        self.caustic_selfsupplied['Grid Electricity List Price'] = self.func_2_lk2('Grid Electricity List Price')
        self.caustic_selfsupplied['Custom multiplier 1'] = self.pdt_cost_1['Caustic Self supplied - Customisation multiplier 1']
        self.caustic_selfsupplied['Grid Electricity final price'] = self.caustic_selfsupplied['Grid Electricity List Price']*self.caustic_selfsupplied['Custom multiplier 1']
        
        self.caustic_selfsupplied['Self-Supplied Electricity List Price'] = self.func_2_lk2('Self-Supplied Electricity List Price')
        self.caustic_selfsupplied['Custom multiplier 2'] = self.pdt_cost_1['Caustic Self supplied - Customisation multiplier 2']
        self.caustic_selfsupplied['Self-Supplied Electricity Final Price'] = self.caustic_selfsupplied['Self-Supplied Electricity List Price']*self.caustic_selfsupplied['Custom multiplier 2']
        
        self.caustic_selfsupplied['Total Electricity NaOH'] = self.pdt_cost_1['Caustic Self supplied - Total Elec for prodn']
        val_1 = self.caustic_selfsupplied['Grid Electricity final price']*self.caustic_selfsupplied['Total Electricity NaOH']*self.caustic_selfsupplied['Electricity for Prod']
        val_2 = self.caustic_selfsupplied['Self-Supplied Electricity Final Price'] *self.caustic_selfsupplied['Total Electricity NaOH'] *(1-self.caustic_selfsupplied['Electricity for Prod'])
        self.caustic_selfsupplied['Electricity cost 1 ECU'] = val_1+val_2
        
        self.caustic_selfsupplied['Other Costs'] = self.pdt_cost_1['Caustic Self supplied - Other Costs']
        self.caustic_selfsupplied['Total Manufacturing cost'] = self.caustic_selfsupplied['Electricity cost 1 ECU']+ self.caustic_selfsupplied['Other Costs']
        self.caustic_selfsupplied['Chlorine Price'] = self.func_2_lk2('Chlorine Price')
        
        self.caustic_selfsupplied['Cost to be apportioned to NaOH'] =self.caustic_selfsupplied['Total Manufacturing cost'] - self.caustic_selfsupplied['Chlorine Price']
        self.caustic_selfsupplied['tonnes pure NaOH'] = self.pdt_cost_1['Caustic Self supplied - tonnes per NaOH']
        self.caustic_selfsupplied['Cost per tonne pure NaOH'] = self.caustic_selfsupplied['Cost to be apportioned to NaOH']/self.caustic_selfsupplied['tonnes pure NaOH'] 
        
        self.caustic_selfsupplied['Caustic Strength'] = self.pdt_cost_1['Caustic Self supplied - Caustic Strenght as transported']
        self.caustic_selfsupplied['Transport distance'] = self.pdt_cost_1['Caustic Self supplied - Transport distance']
        self.caustic_selfsupplied['Freight rate (trucking)'] =self.pdt_cost_1['Caustic Self supplied -Freight rate (trucking)']
        self.caustic_selfsupplied['[Loading + Unloading] Total Fee'] =self.pdt_cost_1['Caustic Self supplied - [Loading + Unloading] Total Fee']
        self.caustic_selfsupplied['Total Freight - Before Customisation'] =self.caustic_selfsupplied['Transport distance']*self.caustic_selfsupplied['Freight rate (trucking)']+self.caustic_selfsupplied['[Loading + Unloading] Total Fee']
        self.caustic_selfsupplied['Customisation multiplier 3'] = self.pdt_cost_1['Caustic Self supplied - Customisation multiplier 3']
        self.caustic_selfsupplied['Final Freight to refinery 1'] = self.caustic_selfsupplied['Total Freight - Before Customisation']*self.caustic_selfsupplied['Customisation multiplier 3']
        self.caustic_selfsupplied['Final Freight to refinery 2'] = [(0 if self.caustic_selfsupplied['Transport distance'][x]<=0 else self.caustic_selfsupplied['Final Freight to refinery 1'][x]/self.caustic_selfsupplied['Caustic Strength'][x] ) for x in self.pdt_cost_1.index]
        self.caustic_selfsupplied['Final Price - Self-Supplied caustic'] =self.caustic_selfsupplied['Cost per tonne pure NaOH']+self.caustic_selfsupplied['Final Freight to refinery 2']
        self.caustic_selfsupplied['Domestic Purchased'] =self.pdt_cost_1['Caustic Self supplied - Domestic Purchased']
        self.caustic_selfsupplied['Final Price'] =self.caustic_selfsupplied['Domestic Purchased']*self.caustic_purchase['Final Price - purchased caustic'] + (1-self.caustic_selfsupplied['Domestic Purchased'])*self.caustic_selfsupplied['Final Price - Self-Supplied caustic']
        
    def calc_energy_sourcing_mix(self):
        self.energy_for_steam['% from Lignitous Coal'] = self.pdt_cost_1['Energy St.R - sourcing lignitous Coal ']
        self.energy_for_steam['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] = 1-self.energy_for_steam['% from Lignitous Coal']
       
    def calc_energy_lignitous_coal(self):
        self.energy_for_steam['Lignitous Coal List Price FAW']                      = self.func_2_lk2('Lignitous Coal List Price FAW')
        self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 1'] = self.pdt_cost_1['Energy St.R - Lignitous Coal - Customisation multiplier 1']
        self.energy_for_steam['Final Price FAW']                                    = self.energy_for_steam['Lignitous Coal List Price FAW'] * self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 1']
        self.energy_for_steam['Transport distance']                                 = self.pdt_cost_1['Energy St.R - Lignitous Coal - Transport distance']
        self.energy_for_steam['Freight rate (trucking)']                            = self.func_2_lk2('Freight rate (trucking)')
        self.energy_for_steam['Loading + Unloading Fee']                            = self.pdt_cost_1['Energy St.R - Lignitous Coal - Loading + Unloading Fee']  
        self.energy_for_steam['Total Freight']                                      = self.energy_for_steam['Transport distance'] * self.energy_for_steam['Freight rate (trucking)'] + self.energy_for_steam['Loading + Unloading Fee']
        self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 2']      =self.pdt_cost_1['Energy St.R - Lignitous Coal - Customisation multiplier 2']     
        self.energy_for_steam['Final Freight'] = [(0 if self.energy_for_steam['Transport distance'][x]<=0 else self.energy_for_steam['Total Freight'][x] * self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 2'][x]) for x in self.pdt_cost_1.index ]
        self.energy_for_steam['Delivered price'] = self.energy_for_steam['Final Freight'] + self.energy_for_steam['Final Price FAW'] 
        
        self.energy_for_steam['Calorific value conversion factor'] = 0.004184
        self.energy_for_steam['Lig. Coal Calorific Value']                          = self.func_2_lk2('Lig. Coal Calorific Value')
        self.energy_for_steam['Lig. Coal Calorific Value2']                         = self.energy_for_steam['Calorific value conversion factor'] * self.energy_for_steam['Lig. Coal Calorific Value']
        self.energy_for_steam['Final Price'] =  [(self.energy_for_steam['Delivered price'][_]/self.energy_for_steam['Lig. Coal Calorific Value2'][_]) for _ in self.energy_for_steam.index]
    
    def calc_energy_natural_gas(self):
        self.energy_for_steam['Gas Price Delivered']                                = self.func_2_lk2('Gas Price Delivered')
        self.energy_for_steam['Customisation multiplier']                           = self.pdt_cost_1['Energy St.R - Natural gas - Customisation multiplier 1']
        self.energy_for_steam['Natural Gas Final Price']                                        = self.energy_for_steam['Gas Price Delivered'] * self.energy_for_steam['Customisation multiplier']
        
        self.energy_for_steam['Gas Calorific Value']                                = self.func_2_lk2('Gas Calorific Value')
        self.energy_for_steam['Final Price2'] = self.energy_for_steam['Natural Gas Final Price'] /self.energy_for_steam['Gas Calorific Value'] 
        self.energy_for_steam['Steam raising fuel - final price'] = (self.energy_for_steam['% from Lignitous Coal']*self.energy_for_steam['Final Price']) + (self.energy_for_steam['% from Natural Gas / Coal Seam Gas / Coke Oven Gas']*self.energy_for_steam['Final Price2'])
        
    def calc_energy_calcing_sourcing_mix(self):
        self.energy_calcining_alumina['% from Anthracite Coal'] = self.pdt_cost_1['Energy for Calcining - Sourcing - Anthracite Coal']
        self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] = 1 - self.energy_calcining_alumina['% from Anthracite Coal'] 
               
    def calc_energy_calcing_anthracite_coal(self):
        self.energy_calcining_alumina['Anthracite List Price'] =  self.func_2_lk2('Anthracite List Price')
        self.energy_calcining_alumina['Customization Multiplier 1'] = self.pdt_cost_1['Energy for Calcining -  Anthracite Coal - Customistion multiplier 1']
        self.energy_calcining_alumina['Final Price FAW'] = self.energy_calcining_alumina['Anthracite List Price']*self.energy_calcining_alumina['Customization Multiplier 1']
       
        self.energy_calcining_alumina['Transport distance'] = self.pdt_cost_1['Energy for Calcining -  Anthracite Coal - Transport distance']
        self.energy_calcining_alumina['Freight rate (trucking)'] = self.func_2_lk2('Freight rate (trucking)')
        self.energy_calcining_alumina['Loading + Unloading Fee'] = self.pdt_cost_1['Energy for Calcining -  Anthracite Coal - Loading + Unloading Fee']
        self.energy_calcining_alumina['Total Freight'] = self.energy_calcining_alumina['Transport distance']*self.energy_calcining_alumina['Freight rate (trucking)']+ self.energy_calcining_alumina['Loading + Unloading Fee']
        self.energy_calcining_alumina['Customization Multiplier 2'] = self.pdt_cost_1['Energy for Calcining -  Anthracite Coal - Customistion multiplier 2']
        self.energy_calcining_alumina['Final Freight'] = [(0 if self.energy_calcining_alumina['Transport distance'][x]<=0 else self.energy_calcining_alumina['Total Freight'][x] * self.energy_calcining_alumina['Customization Multiplier 2'][x]) for x in self.energy_for_steam.index ]
        
        self.energy_calcining_alumina['Delivered price'] =self.energy_calcining_alumina['Final Price FAW']+self.energy_calcining_alumina['Final Freight']
                
        self.energy_calcining_alumina['Calorific value conversion factor'] = 0.004184
        self.energy_calcining_alumina["A'cite Calorific Value"] =  self.func_2_lk2("A'cite Calorific Value")
        self.energy_calcining_alumina["A'cite Calorific Value 2"] =  self.energy_calcining_alumina['Calorific value conversion factor'] * self.energy_calcining_alumina["A'cite Calorific Value"] 
        self.energy_calcining_alumina['Final Price'] = self.energy_calcining_alumina['Delivered price']/self.energy_calcining_alumina["A'cite Calorific Value 2"]
        
    def calc_energy_calcing_natural_gass(self):
        self.energy_calcining_alumina['Gas Price Delivered'] = self.func_2_lk2('Gas Price Delivered')
        self.energy_calcining_alumina['Natural Gas Customization Multiplier'] = self.pdt_cost_1['Energy for Calcining - Natural gas - Customistion multiplier']
        self.energy_calcining_alumina['Natural Gas Final Price'] = self.energy_calcining_alumina['Gas Price Delivered']  *  self.energy_calcining_alumina['Natural Gas Customization Multiplier'] 
        
        self.energy_calcining_alumina['Gas Calorific Value'] = self.func_2_lk2('Gas Calorific Value')
        self.energy_calcining_alumina['Natural Gas Final Price 2'] = self.energy_calcining_alumina['Natural Gas Final Price']/self.energy_calcining_alumina['Gas Calorific Value']
        self.energy_calcining_alumina['Alumina calcining energy - final price'] = (self.energy_calcining_alumina['% from Anthracite Coal'] * self.energy_calcining_alumina['Final Price'] )+(self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] * self.energy_calcining_alumina['Natural Gas Final Price 2'])
       
    def calc_energy_calcing_electicity(self):
        self.energy_calcining_alumina['Grid Electricity List Price'] =  self.func_2_lk2('Grid Electricity List Price')
        self.energy_calcining_alumina['Electricity Customisation multiplier'] = self.pdt_cost_1['Energy for Calcining - Elec  Purchased- Customistion multiplier']
        self.energy_calcining_alumina['Electricity Final Price'] = self.energy_calcining_alumina['Grid Electricity List Price'] * self.energy_calcining_alumina['Electricity Customisation multiplier']
    
    def calc_lime_prices_mix(self):
        self.lime['Lime List Price'] =  self.func_2_lk2('Lime List Price')
        self.lime['Customisation multiplier 1'] = self.pdt_cost_1['Lime - custom mul 1']
        self.lime['Customised price'] = self.lime['Lime List Price'] *self.lime['Customisation multiplier 1']
        
        self.lime['Transport distance']         = self.pdt_cost_1['Lime - Transport distance']
        self.lime['Freight rate(trucking)']     = self.func_2_lk2('Freight rate (trucking)')
        self.lime['Loading + Unloading Fee']    = self.pdt_cost_1['Lime - Loading + unloading Fee']
        self.lime['Total Freight']              = self.lime['Transport distance'] * self.lime['Freight rate(trucking)'] + self.lime['Loading + Unloading Fee']
        self.lime['Customisation multiplier 2'] = self.pdt_cost_1['Lime - custom mul 2']
        self.lime['Final Freight']              = [(0 if self.lime['Transport distance'][x]<=0 else self.lime['Total Freight'][x] * self.lime['Customisation multiplier 2'][x]) for x in self.energy_for_steam.index ]
        
        self.lime['Final Price']                = self.lime['Customised price'] + self.lime['Final Freight']
        
    def calc_limestone_prices_mix(self):
        self.limestone['Limestone List Price']  = self.func_2_lk2('Limestone List Price')
        self.limestone['Customisation multiplier 1'] = self.pdt_cost_1['Limestone - custom mul 1']
        self.limestone['Customised price']      =self.limestone['Limestone List Price'] * self.limestone['Customisation multiplier 1']
        
        self.limestone['Transport distance']        = self.pdt_cost_1['Limestone - Transport distance']
        self.limestone['Freight rate(trucking)']    = self.func_2_lk2('Freight rate (trucking)')
        self.limestone['Loading + Unloading Fee']   = self.pdt_cost_1['Limestone - loading + unloading fee']
        self.limestone['Total Freight']             = self.limestone['Transport distance'] * self.limestone['Freight rate(trucking)'] + self.limestone['Loading + Unloading Fee']
        self.limestone['Customisation multiplier 2']= self.pdt_cost_1['Limestone - custom mul 2']
        self.limestone['Final Freight']             = [(0 if self.limestone['Transport distance'][x]<=0 else self.limestone['Total Freight'][x] * self.limestone['Customisation multiplier 2'][x]) for x in self.energy_for_steam.index ]
        
        self.limestone['Final Price']               = self.limestone['Customised price'] + self.limestone['Final Freight']
    
    def calc_labour_prices_mix(self):
        self.labour['Labor General List Price']         = self.func_2_lk2('Labor General List Price')
        self.labour['Customisation multiplier']         = self.pdt_cost_1['Labour - custom mul']
        self.labour['Customised Price']                 = self.labour['Labor General List Price']  * self.labour['Customisation multiplier']
        self.labour['hours worked per year']            = self.pdt_cost_1['Labour - hours worked per year']
        self.labour['Labor Multiplier to All up Costs for Private Co.'] = self.func_2_lk2('Labor Multiplier to All up Costs for Private Co.')
        self.labour['Labor Multiplier to All up Costs for SOE']         = self.func_2_lk2('Labor Multiplier to All up Costs for SOE')
        self.labour['Final multiplier to all up costs'] =  [(self.labour['Labor Multiplier to All up Costs for Private Co.'][x] if self.pdt_global['Ownership'][x]=='Private' else self.labour['Labor Multiplier to All up Costs for SOE'][x] ) for x in self.pdt_cost_1.index]
        self.labour['Final Price - all up costs']       = self.labour['Customised Price'] * 12/self.labour['hours worked per year'] * self.labour['Final multiplier to all up costs']
  
    def calc_consumables_prices_mix(self):
        self.consumables['Flocculent List Price'] = self.func_2_lk2('Flocculent List Price')
        self.consumables['Customisation multiplier 1'] =self.pdt_cost_1['Consumables - custom mul 1']
        self.consumables['Customised Price'] = self.consumables['Flocculent List Price']*self.consumables['Customisation multiplier 1']
        
        self.consumables['Transport distance'] =self.pdt_cost_1['Consumables - Transport distance']
        self.consumables['Freight rate (trucking)'] = self.func_2_lk2('Freight rate (trucking)')
        self.consumables['Loading + Unloading Fee'] =self.pdt_cost_1['Consumables - Loading + Unloading Fee']
        self.consumables['Total Freight'] = self.consumables['Transport distance']*self.consumables['Freight rate (trucking)']+self.consumables['Loading + Unloading Fee']
        self.consumables['Customisation multiplier 2'] =self.pdt_cost_1['Consumables - custom mul 2']
        self.consumables['Final Freight'] = [(0 if self.consumables['Transport distance'][x] <=0 else self.consumables['Total Freight'][x]*self.consumables['Customisation multiplier 2'] ) for x in self.pdt_cost_1.index]
        
        self.consumables['Final Price'] =self.consumables['Customised Price'] +self.consumables['Final Freight']
    
    def calc_usage_bauxite(self):
        self.usg_bauxite['Handling losses'] = self.pdt_cost_1['Bauxite - Usage - Handling losses']
        self.usg_bauxite['Alumina qaulity'] = self.func_4_lk1('Alumina quality')
        self.usg_bauxite['DSP AA:SiO2']     = self.func_4_lk1('DSP AA:SiO2')
        self.usg_bauxite['Extrcation efficiency'] =self.func_4_lk1('Extraction Efficiency')
        result = []
        res = 0
        for row in self.pdt_cost_1.index:
            if (self.bauxite_final_xtics.loc[row,'Alumina Grade (Reacting)']-self.usg_bauxite.loc[row,'DSP AA:SiO2']*self.bauxite_final_xtics.loc[row,'Silica Grade (Reacting)']) <=0 or (1-self.usg_bauxite.loc[row,'Handling losses'])<=0 or(1-self.usg_bauxite.loc[row,'Extrcation efficiency']) <=0:
                res = 0
                result.append(res)
            else:
                res = 1/(self.bauxite_final_xtics.loc[row,'Alumina Grade (Reacting)']-self.usg_bauxite.loc[row,'DSP AA:SiO2']*self.bauxite_final_xtics.loc[row,'Silica Grade (Reacting)'])
                res = res/(1-self.usg_bauxite.loc[row,'Handling losses'])/self.usg_bauxite.loc[row,'Extrcation efficiency']*self.usg_bauxite.loc[row,'Alumina qaulity']
                result.append(res)
        self.usg_bauxite['Bauxite Use']     = result
        self.usg_bauxite['Customise multiplier'] =self.func_1_bauxites('Cus Mul - BAR')
        self.usg_bauxite['Final bauxite'] = self.usg_bauxite['Bauxite Use']*self.usg_bauxite['Customise multiplier']
    
    def calc_usage_caustic(self):
        self.usg_caustic['DSP Na:Si'] = self.func_4_lk1('DSP Na:Si')
        self.usg_caustic['Caustic Wash loss'] = self.func_4_lk1('Caustic wash loss')
        value = self.usg_caustic['DSP Na:Si']* self.bauxite_final_xtics['Silica Grade (Reacting)']*self.usg_bauxite['Final bauxite'] + self.usg_caustic['Caustic Wash loss']
        self.usg_caustic['Caustic use'] = value
        self.usg_caustic['Customise multiplier'] = self.func_1_bauxites('Cus Mul - Caustic')
        self.usg_caustic['Final caustic use rate'] = self.usg_caustic['Caustic use']*self.usg_caustic['Customise multiplier']
        self.usg_caustic['Final Sodium Carbonate use rate'] = self.pdt_cost_1['Caustic - Final sodium']
        
    def calc_usage_energy(self):
        self.usg_energy['Required Steam raising energy'] =self.func_4_lk1('Required Steam raising energy')
        self.usg_energy['Customisation multiplier 1'] = self.pdt_cost_1['Energy -Usage -cus mul 1']
        self.usg_energy['Steam energy'] = self.usg_energy['Required Steam raising energy']*self.usg_energy['Customisation multiplier 1']
        
        self.usg_energy['If Anthracite'] =6.5
        self.usg_energy['If Natural gas/Coal Steam'] = 3.2
        val = self.energy_calcining_alumina['% from Anthracite Coal']*self.usg_energy['If Anthracite'] + self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas']*self.usg_energy['If Natural gas/Coal Steam']
        self.usg_energy['Final before cusmising'] = val
        self.usg_energy['Customisation multiplier 2'] = self.pdt_cost_1['Energy -Usage -cus mul 2']
        self.usg_energy['Calcining energy'] =self.usg_energy['Final before cusmising']*self.usg_energy['Customisation multiplier 2']
        
        self.usg_energy['Required Electrical energy'] = self.func_4_lk1('Required Electrical energy')
        self.usg_energy['Customisation multiplier 3'] = self.pdt_cost_1['Energy -Usage -cus mul 3']
        self.usg_energy['Electrical energy - final'] = self.usg_energy['Required Electrical energy']*self.usg_energy['Customisation multiplier 3']
        
    def calc_usage_lime_limestone(self):
        self.usg_lime['Lime rate'] = self.func_4_lk1('Lime rate')
        self.usg_lime['Custom multiplier'] = self.pdt_cost_1['Lime - Usage -Custom mul']
        self.usg_lime['Final Lime rate'] = self.usg_lime['Lime rate']*self.usg_lime['Custom multiplier']
        
        self.usg_limestone['Limestone 1 rate'] = self.func_4_lk1('Limestone rate')
        self.usg_limestone['Limestone 2 rate'] = self.usg_limestone['Limestone 1 rate'] *self.bauxite_final_xtics['Silica Grade (Reacting)']
        self.usg_limestone['Custom multiplier'] =self.pdt_cost_1['Limestone - Usage -Custom mul']
        self.usg_limestone['Customed Limestone rate'] =self.usg_limestone['Limestone 2 rate']*self.usg_limestone['Custom multiplier']
        self.usg_limestone['Final Limestone rate'] =self.usg_limestone['Customed Limestone rate']*self.usg_bauxite['Final bauxite']
        
    def calc_usage_labour(self):
        val1 = [(10*pow(self.cap_production['Capacity - for maintenance and labour'][x],-0.2) if (float(self.cap_production['Capacity - for maintenance and labour'][x]))>1 else 0) for x in self.pdt_cost_1.index]
        val2 = [(1.5 if self.pdt_global['Digestion Technology code'][x]==2 or self.pdt_global['Digestion Technology code'][x]==3  else 1) for x in self.pdt_cost_1.index]
        self.usg_labour['Workforce Productivity'] = [val1[_]*val2[_] for _ in self.pdt_cost_1.index]
        self.usg_labour['Customise'] = self.pdt_cost_1['Labour - Usage -Custom']
        self.usg_labour['Final Workforce Productivity'] =self.usg_labour['Workforce Productivity'] * (1 + self.usg_labour['Customise'] )
    
    def calc_usage_consumables(self):
        self.usg_consumables['Red Mud Make - scaling factor'] = [max(1, self.usg_bauxite['Final bauxite'][x]+ self.usg_lime['Final Lime rate'][x] + self.usg_limestone['Final Limestone rate'][x]-1) for x in self.pdt_cost_1.index]
        #flocculent
        self.usg_consumables['Usage rate'] = self.pdt_cost_1['Consumables -Usage -Usage rate']
        self.usg_consumables['customise']  = self.pdt_cost_1['Consumables -Usage -customise']
        self.usg_consumables['Customised usage rate'] =  [(self.usg_consumables['Usage rate'][x] * (1+self.usg_consumables['customise'][x] )) for x in self.pdt_cost_1.index ]
        self.usg_consumables['Usage'] = self.usg_consumables['Customised usage rate'] * self.usg_consumables['Red Mud Make - scaling factor']/1000
       
        
    def calc_costs_bauxite(self):
        self.bauxite_cost['bauxite cost'] = self.bauxite_final_xtics['Total Delivered'] * self.usg_bauxite['Final bauxite']
        
    def calc_costs_caustic(self):
        self.caustic_cost['Caustic cost'] = self.usg_caustic['Final caustic use rate'] * self.caustic_selfsupplied['Final Price']
        self.caustic_cost['Sodium Carbonate cost'] = 0
        self.caustic_cost['Total caustic cosr'] = self.caustic_cost['Caustic cost'] + self.caustic_cost['Sodium Carbonate cost']

    def calc_costs_energy(self):
        self.energy_cost['Lignitious coal'] = self.usg_energy['Steam energy'] *  self.energy_for_steam['Final Price']
        self.energy_cost['Anthracite/Gas']  = self.usg_energy['Calcining energy'] * self.energy_calcining_alumina['Alumina calcining energy - final price']
        self.energy_cost['Electricity']     = self.usg_energy['Electrical energy - final'] * self.energy_calcining_alumina['Electricity Final Price']
        self.energy_cost['Total energy cost']   = self.energy_cost['Lignitious coal']  + self.energy_cost['Anthracite/Gas'] +  self.energy_cost['Electricity']
        
    def calc_costs_lime(self):
        self.lime_cost['Lime']              = self.usg_lime['Final Lime rate'] * self.lime['Final Price'] 
        self.lime_cost['Limestone']         = self.usg_limestone['Final Limestone rate'] * self.limestone['Final Price']
        self.lime_cost['Total lime cost']   = self.lime_cost['Lime'] + self.lime_cost['Limestone']

    def calc_costs_labour(self):
        self.labour_cost['Labour'] = self.labour['Final Price - all up costs'] * self.usg_labour['Final Workforce Productivity']
        
    def calc_costs_consumables(self):
        self.consumables_cost['Flocculent']         = self.usg_consumables['Usage'] * self.consumables['Final Price']
        val1 = [(250*pow(self.cap_production['Production - sub module'][x],-0.33) if (float(self.cap_production['Production - sub module'][x]))>1 else 0) for x in self.pdt_cost_1.index]
        val2 = [(1.5 if self.pdt_global['Digestion Technology code'][x]==2 or self.pdt_global['Digestion Technology code'][x]==3  else 1) for x in self.pdt_cost_1.index]
        self.consumables_cost['Allowance for other Consumables']  = [val1[_]*val2[_] for _ in self.pdt_cost_1.index]
        self.consumables_cost['Total consumable cost'] = self.consumables_cost['Flocculent'] +  self.consumables_cost['Allowance for other Consumables']
        
    def calc_costs_maintenance(self):
        val1 = [(4500*pow(self.cap_production['Capacity - for maintenance and labour'][x],-0.09) if (float(self.cap_production['Capacity - for maintenance and labour'][x]))>1 else 0) for x in self.pdt_cost_1.index]
        val2 = [(1.5 if self.pdt_global['Digestion Technology code'][x]==2 or self.pdt_global['Digestion Technology code'][x]==3  else 1) for x in self.pdt_cost_1.index]
        self.maintenance_cost['C&M Formulae Capital RMB/t'] = [val1[_]*val2[_] for _ in self.pdt_cost_1.index]
        self.maintenance_cost['C&M Formulae Capital US$/t'] = [(self.maintenance_cost['C&M Formulae Capital RMB/t'][x]/self.macro_economic['Exchange Rate'][x] if self.maintenance_cost['C&M Formulae Capital RMB/t'][x]>0 else ' ') for x in self.pdt_cost_1.index]
        self.maintenance_cost['Maintenance - Portion of Capex'] = self.pdt_cost_1['Maintenance - Portion of capex']
        self.maintenance_cost['Maintenance - Maintenance Cost'] =self.maintenance_cost['C&M Formulae Capital RMB/t'] * self.maintenance_cost['Maintenance - Portion of Capex']
        
    def calc_costs_other(self):
        self.other_cost['Other cost allowance'] =  [(65*pow(self.cap_production['Production - sub module'][x],-0.1) if (float(self.cap_production['Production - sub module'][x]))>1 else 0) for x in self.pdt_cost_1.index]
        
        val = self.usg_bauxite['Final bauxite']+self.usg_caustic['Final caustic use rate']+self.usg_caustic['Final Sodium Carbonate use rate']+self.usg_lime['Final Lime rate']+self.usg_limestone['Final Limestone rate']
        self.other_cost['Red mud quantity'] = val -1
        
        self.other_cost['Red Mud Disposal Cost 1'] = self.func_2_lk2('Red Mud Disposal Cost')
        self.other_cost['Red Mud Disposal Cost 2'] = [max(0, self.other_cost['Red mud quantity'][x]*self.other_cost['Red Mud Disposal Cost 1'][x] ) for x in self.pdt_cost_1.index]
        
        self.other_cost['Packaging'] = self.pdt_cost_1['Other costs - Packaging']
        
    def calc_pdt_summary(self):
        self.PDT1summmary['Bauxite']             = self.bauxite_cost['bauxite cost'] 
        self.PDT1summmary['Caustic']             = self.caustic_cost['Total caustic cosr']
        self.PDT1summmary['Lime']                = self.lime_cost['Total lime cost']
        self.PDT1summmary['Energy']              = self.energy_cost['Total energy cost']
        self.PDT1summmary['Labour']              = self.labour_cost['Labour']
        self.PDT1summmary['Consumables']         = self.consumables_cost['Total consumable cost']
        self.PDT1summmary['Maintenance']         = self.maintenance_cost['Maintenance - Maintenance Cost']
        self.PDT1summmary['Red Mud Disposal']    = self.other_cost['Red Mud Disposal Cost 2']
        self.PDT1summmary['Other Costs']         = self.other_cost['Other cost allowance']
        self.PDT1summmary['Operating Cash Cost'] = self.PDT1summmary[['Bauxite','Caustic','Lime','Energy','Labour','Consumables','Maintenance','Red Mud Disposal','Other Costs']].sum(axis=1)
        self.PDT1summmary['Packaging']           = self.other_cost['Packaging']
        self.PDT1summmary['Cash Cost - FAW']     = self.PDT1summmary['Operating Cash Cost'] + self.PDT1summmary['Packaging']
     
    def calc_pdt_summary2(self):
        self.pdt_1_costSum['Bauxite']               = [ (self.PDT1summmary['Bauxite'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Caustic']               = [ (self.PDT1summmary['Caustic'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Lime']                  = [ (self.PDT1summmary['Lime'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Energy']                = [ (self.PDT1summmary['Energy'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Labour']                = [ (self.PDT1summmary['Labour'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Consumables']           = [ (self.PDT1summmary['Consumables'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Maintenance']           = [ (self.PDT1summmary['Maintenance'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Red Mud Disposal']      = [ (self.PDT1summmary['Red Mud Disposal'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Other Costs']           = [ (self.PDT1summmary['Other Costs'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Operaing cash costs']   = self.pdt_1_costSum[['Bauxite','Caustic','Lime','Energy','Labour','Consumables','Maintenance','Red Mud Disposal','Other Costs']].sum(axis=1)
        self.pdt_1_costSum['Packaging']             = [ (self.PDT1summmary['Packaging'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Cash Cost - FAW 1']     = [ (self.PDT1summmary['Cash Cost - FAW'][x] if self.cap_production.loc[x,'Production - sub module']>0 else 0) for x in self.pdt_cost_1.index ]
        self.pdt_1_costSum['Cash Cost - FAW 2']     = self.pdt_1_costSum['Cash Cost - FAW 1']/self.macro_economic['Exchange Rate']
        
        
        self.pdt1_headline['Capacity -kt'] = self.cap_production['Capacity -sub module']
        self.pdt1_headline['Production -kt'] = self.cap_production['Production - sub module']
        self.pdt1_headline['Utilisation'] =self.cap_production['Utilization - sub module']
        self.pdt1_headline['Cash Cost - FAW 1'] = self.pdt_1_costSum['Cash Cost - FAW 1']
        self.pdt1_headline['Cash Cost - FAW 2'] = self.pdt_1_costSum['Cash Cost - FAW 2']
        
        l=[]
        for x in self.pdt_global['Ownership']:
            if x =='SOE':
                l.append(1)
            elif x == 'Private':
                l.append(2)
            else:
                l.append('')
        self.pdt1_headline['SOE=1,Private=2'] = l
         
        
        
    #pdt fuctions
    def func_1_bauxites(self, ref_col): #ref no
        d = {self.refPDT1['Ref. No.'][x]:self.refPDT1[ref_col][x] for x in self.refPDT1.index}
        return [(d[self.pdt_cost_1['Ref. No.'][x]]) for x in self.pdt_cost_1.index]

    def func_2_lk2(self, ref_col): #province
        d = {self.lookup2['Province Prices & Rates - Current'][x]:self.lookup2[ref_col][x] for x in self.lookup2.index}
        return [(d[self.macro_economic['Province'][x]]) for x in self.pdt_cost_1.index]
    def func_3_iBX(self, ref_col): #ref
        res = []
        d = {self.ImpBxSummary['Ref. No.'][x]:self.ImpBxSummary[ref_col][x] for x in self.ImpBxSummary.index}
        for row in self.pdt_cost_1.index:
            try:
                res.append( d[self.pdt_cost_1.loc[row,'Ref. No.']] )
            except:
                res.append(0)
                #print('index : ',row)
        return res #[(d[self.pdt_cost_1['Ref. No.'][x]]) for x in self.pdt_cost_1.index[:-7]]
    
    def func_4_lk1(self,ref_col):
        res = []
        d = {self.lookup1['Digestion Technology'][x]:self.lookup1[ref_col][x] for x in self.lookup1.index}
        for row in self.pdt_cost_1.index:
            try:
                res.append( d[self.pdt_global.loc[row,'Refinery Digestion Technology']] )
            except:
                res.append(0)
        return res #[ d[self.pdt_global.loc[row,'Refinery Digestion Technology']] for row in self.pdt_cost_1.index]


#pdt_cost_2
class PDT_Cost_Current2():
    def __init__(self, bxTonnes, pdt1):
        self.ref_base_cap2 = pd.read_excel(refCapProduction_filename, sheet_name='Base Capacity 2')
        self.ref_base_prod2 = pd.read_excel(refCapProduction_filename, sheet_name='Base Production 2')
        self.pdt_cost_2 = pdt1.pdt_cost_2
        self.refPDT1 = pdt1.refPDT1
        self.lookup1 = pdt1.lookup1
        self.lookup2 = pdt1.lookup2
        self.lookup3 = pdt1.lookup3
        self.lookup4 = pdt1.lookup4
        self.ImpBxSummary = pdt1.ImpBxSummary
        self.cap_production2 = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.pdt_global = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.bauxite1 = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.bauxite2 = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.bauxite3 = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.bauxite_sourcing_mix = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.bauxite_final_xtics = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.caustic_price = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.pdt_header = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.energy_for_steam = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.energy_calcining_alumina = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.lime = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.limestone = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.labour = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.consumables = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.usg_bauxite = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.usg_caustic = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.usg_energy = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.usg_lime = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.usg_limestone = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.usg_labour = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.usg_consumables = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.bauxite_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.caustic_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.energy_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.lime_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.labour_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.consumables_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.maintenance_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.other_cost = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        self.PDT2summmary = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        #self. = pd.DataFrame(columns=[], index=self.pdt_cost_2.index)
        
        
    def calc_all(self):
        self.calc_pdt_headers()
        self.calc_capacityProduction()
        self.calc_global()
        self.calc_bauxite1()
        self.calc_bauxite2()
        self.calc_bauxite3()
        self.calc_bauxite_sourcing_mix()
        self.calc_bauxite_final_xtics()
        self.calc_caustic_price()
        self.calc_energy_raising()
        self.calc_energy_calcing_alumina()
        self.calc_lime_limestone()
        self.calc_labour()
        self.calc_flocculent()
        self.calc_usagebauxite()
        self.calc_usagecaustic()
        self.calc_usageenergy()
        self.calc_usagelimelimestone()
        self.calc_usagelabour()
        self.calc_usageconsumables()
        self.calc_costsbauxite()
        self.calc_costscaustic()
        self.calc_costsenergy()
        self.calc_costslime()
        self.calc_costslabour()
        self.calc_costsconsumables()
        self.calc_costsmaintenance()
        self.calc_costsother()
        self.calc_pdtsummary()
        
    def calc_pdt_headers(self):
        self.pdt_header['Ref. No'] = self.ref_base_cap2['Ref. No.']
        self.pdt_header['country'] = self.ref_base_cap2['Province']
        self.pdt_header['Exchange Rate'] = 9
        
    def calc_capacityProduction(self):
        year = '2020'
        self.cap_production2['Count in current capacity totals'] = self.pdt_cost_2['Capacity - count totals']
        self.cap_production2['Capacity - for maintenance and labour'] =  self.pdt_cost_2['Capacity - cap maintenance calcs']
        
        for row in self.pdt_cost_2.index:
            self.cap_production2.at[row,'Capacity -sub module'] = float(self.ref_base_cap2.loc[self.ref_base_cap2['Ref. No.']==self.pdt_cost_2.loc[row,'Ref. No.'], year])*1000
            self.cap_production2.at[row,'Production - sub module'] =float(self.ref_base_prod2.loc[self.ref_base_cap2['Ref. No.']==self.pdt_cost_2.loc[row,'Ref. No.'], year]) *1000
        l=[]
        for row in self.pdt_cost_2.index:
            if self.cap_production2.loc[row,'Capacity -sub module']>0:
                l.append( self.cap_production2.loc[row,'Production - sub module']/self.cap_production2.loc[row,'Capacity -sub module'])
            else:
                l.append(0)
        self.cap_production2['Utilization - sub module']=  l
        
    def calc_global(self):
        self.pdt_global['Ownership'] = self.pdt_cost_2['Ownership']
        self.pdt_global['Alumina Produced'] = self.pdt_cost_2['Alumina Produced']
        self.pdt_global['Digestion Technology code'] = self.func_1_bauxites('Digestion Technology Code')
        d= {self.lookup1['Key'][x]:self.lookup1['Digestion Technology'][x]  for x in self.lookup1.index}
        l = []
        for _ in self.pdt_global['Digestion Technology code']:
            try:
                l.append(d[_])
            except:
                l.append(0)
        self.pdt_global['Refinery Digestion Technology'] = l
        
    def calc_bauxite1(self):
        self.bauxite1['Alumina Grade(Reacting)'] = self.func_1_bauxites('Bauxite1 - Aa Grade (Reacting)')
        self.bauxite1['A/S ratio (Reacting/Reacting)'] =self.func_1_bauxites('Bauxite1 - A/S ratio (Reacting)')
        self.bauxite1['Moisture'] = self.func_1_bauxites('Bauxite1 - Moisture')
        self.bauxite1['Silica Grade(Reacting)'] = [ (self.bauxite1['Alumina Grade(Reacting)'][x]/self.bauxite1['A/S ratio (Reacting/Reacting)'][x] if self.bauxite1['A/S ratio (Reacting/Reacting)'][x] else 0) for x in self.pdt_cost_2.index]
        
        self.bauxite1['Mining Dressing (FAW)'] =self.pdt_cost_2['Bauxite 1 - mining Dressing (FAW)']
        self.bauxite1['Final Freight to refinery']=self.pdt_cost_2['Bauxite 1 - final freight to ref']
        
        self.bauxite1['Total Delivered 1'] =self.bauxite1['Mining Dressing (FAW)']+self.bauxite1['Final Freight to refinery']
        self.bauxite1['Total Delivered 2'] =self.bauxite1['Total Delivered 1']/(1-self.bauxite1['Moisture'])
    
    def calc_bauxite2(self):
        self.bauxite2['Alumina Grade(Reacting)'] =self.func_1_bauxites('Bauxite 2 - Aa Grade (Reacting)')
        self.bauxite2['A/S ratio (Reacting/Reacting)'] =self.func_1_bauxites('Bauxite 2 - A/S ratio (Reacting)')
        self.bauxite2['Moisture'] =self.func_1_bauxites('Bauxite 2 - Moisture')
        self.bauxite2['Silica Grade(Reacting)'] = [ (self.bauxite2['Alumina Grade(Reacting)'][x]/self.bauxite2['A/S ratio (Reacting/Reacting)'][x] if self.bauxite2['A/S ratio (Reacting/Reacting)'][x] else 0) for x in self.pdt_cost_2.index]
        
        self.bauxite2['Mining Dressing (FAW)'] =self.pdt_cost_2['Bauxite 2 - mining Dressing (FAW)']
        self.bauxite2['Final Freight to refinery'] =self.pdt_cost_2['Bauxite 2 - final freight to ref']
        
        self.bauxite2['Total Delivered 1'] = self.bauxite2['Mining Dressing (FAW)']+self.bauxite2['Final Freight to refinery']
        self.bauxite2['Total Delivered 2'] = self.bauxite2['Total Delivered 1']/(1-self.bauxite2['Moisture'])
    
    def calc_bauxite3(self):
        self.bauxite3['Alumina Grade(Reacting)'] = self.func_3_iBX('Alumina Grade(Reacting')
        self.bauxite3['A/S ratio (Reacting/Reacting)'] =self.func_3_iBX('A/S ratio(Reacting/Reacting)')
        self.bauxite3['Moisture'] = self.func_3_iBX('Moisture')
        self.bauxite3['Silica Grade(Reacting)'] =[ (self.bauxite3['Alumina Grade(Reacting)'][x]/self.bauxite3['A/S ratio (Reacting/Reacting)'][x] if self.bauxite3['A/S ratio (Reacting/Reacting)'][x] else 0) for x in self.pdt_cost_2.index]
        
        self.bauxite3['Purchase - CFR'] = self.func_3_iBX('Avg CFR Price')
        self.bauxite3['Total Transport Fee'] = self.func_1_bauxites('Bauxite 3 - Transport fee')
        
        self.bauxite3['Total Delivered 1'] = self.bauxite3['Purchase - CFR']*1.39*(1-self.bauxite3['Moisture'])+self.bauxite3['Total Transport Fee']
        self.bauxite3['Total Delivered 2'] = self.bauxite3['Total Delivered 1']/(1-self.bauxite3['Moisture'])
    
    def calc_bauxite_sourcing_mix(self):
        self.bauxite_sourcing_mix['Domestic Self Supply'] = self.func_1_bauxites('Bauxite Sourcing - Domestic Self Supply')
        self.bauxite_sourcing_mix['Domestic Purchased'] = self.func_1_bauxites('Bauxite Sourcing - Domestic Purchased')
        self.bauxite_sourcing_mix['Import Purchased'] = 1-(self.bauxite_sourcing_mix['Domestic Self Supply']+self.bauxite_sourcing_mix['Domestic Purchased'])
        self.bauxite_sourcing_mix['Total'] = self.bauxite_sourcing_mix['Domestic Self Supply']+self.bauxite_sourcing_mix['Domestic Purchased'] +self.bauxite_sourcing_mix['Import Purchased']
    
    def calc_bauxite_final_xtics(self):
        val1 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite1['Alumina Grade(Reacting)'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite2['Alumina Grade(Reacting)'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite3['Alumina Grade(Reacting)']
        val2 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite1['Moisture'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite2['Moisture'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite3['Moisture']
        val3 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite1['Silica Grade(Reacting)'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite2['Silica Grade(Reacting)'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite3['Silica Grade(Reacting)']
        
        self.bauxite_final_xtics['Alumina Grade (Reacting)'] = val1
        self.bauxite_final_xtics['Moisture'] =val2
        self.bauxite_final_xtics['Silica Grade (Reacting)']=val3
        self.bauxite_final_xtics['A/S ratio (Reacting / Reacting)'] = [ (self.bauxite_final_xtics['Alumina Grade (Reacting)'][x]/self.bauxite_final_xtics['Silica Grade (Reacting)'][x] if self.bauxite_final_xtics['Silica Grade (Reacting)'][x] > 0 else 0) for x in self.pdt_cost_2.index]
        
        val4 = self.bauxite_sourcing_mix['Domestic Self Supply']*self.bauxite1['Total Delivered 2'] + self.bauxite_sourcing_mix['Domestic Purchased']*self.bauxite2['Total Delivered 2'] + self.bauxite_sourcing_mix['Import Purchased']*self.bauxite3['Total Delivered 2']
        self.bauxite_final_xtics['Total Delivered'] = val4
        
    def calc_caustic_price(self):
        self.caustic_price['Caustic Soda List Price'] = self.func_2_lk2('Caustic Soda List Price')
        self.caustic_price['List Price Custom mul'] =self.pdt_cost_2['Caustic - Purchased - List Price Custom mul']
        self.caustic_price['Customised list price'] = self.caustic_price['Caustic Soda List Price']*self.caustic_price['List Price Custom mul']
        self.caustic_price['Final freight to refinery'] = 0
        self.caustic_price['Final Price - purchased caustic'] = self.caustic_price['Customised list price']+self.caustic_price['Final freight to refinery']
        self.caustic_price['Final Price'] = self.caustic_price['Final Price - purchased caustic']
    
    def calc_energy_raising(self):
        self.energy_for_steam['% from Lignitous Coal'] = self.pdt_cost_2['Energy - Sourcing Lig Coal']
        self.energy_for_steam['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] = 1-self.energy_for_steam['% from Lignitous Coal']
      
    
        self.energy_for_steam['Lignitous Coal List Price FAW']                      = self.func_2_lk2('Lignitous Coal List Price FAW')
        self.energy_for_steam['Customization Multiplier 1'] = self.pdt_cost_2['Energy - Lignitous Coal Custom mul']
        self.energy_for_steam['Final Price FAW']                                    = self.energy_for_steam['Lignitous Coal List Price FAW'] * self.energy_for_steam['Customization Multiplier 1']
        
        self.energy_for_steam['Final Freight'] = self.pdt_cost_2['Energy - Lignitous Coal final freight']
        self.energy_for_steam['Delivered price'] = self.energy_for_steam['Final Freight'] + self.energy_for_steam['Final Price FAW'] 
        
        self.energy_for_steam['Calorific value conversion factor'] = 0.004184
        self.energy_for_steam['Lig. Coal Calorific Value']                          = self.func_2_lk2('Lig. Coal Calorific Value')
        self.energy_for_steam['Lig. Coal Calorific Value2']                         = self.energy_for_steam['Calorific value conversion factor'] * self.energy_for_steam['Lig. Coal Calorific Value']
        self.energy_for_steam['Final Price'] =  [(self.energy_for_steam['Delivered price'][_]/self.energy_for_steam['Lig. Coal Calorific Value2'][_]) for _ in self.energy_for_steam.index]
    
    
        self.energy_for_steam['Gas Price Delivered']                                = self.func_2_lk2('Gas Price Delivered')
        self.energy_for_steam['Customisation multiplier']              = self.pdt_cost_2['Energy - natural gas custom mul']
        self.energy_for_steam['Natural Gas Final Price']                                        = self.energy_for_steam['Gas Price Delivered'] * self.energy_for_steam['Customisation multiplier']
        
        self.energy_for_steam['Gas Calorific Value']                                = self.func_2_lk2('Gas Calorific Value')
        self.energy_for_steam['Final Price2'] = self.energy_for_steam['Natural Gas Final Price'] /self.energy_for_steam['Gas Calorific Value'] 
        self.energy_for_steam['Steam raising fuel - final price'] = (self.energy_for_steam['% from Lignitous Coal']*self.energy_for_steam['Final Price']) + (self.energy_for_steam['% from Natural Gas / Coal Seam Gas / Coke Oven Gas']*self.energy_for_steam['Final Price2'])
    
    
    def calc_energy_calcing_alumina(self):
        self.energy_calcining_alumina['% from Anthracite Coal'] = self.pdt_cost_2['Energy Calcing - sourcing anth']
        self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] = 1 - self.energy_calcining_alumina['% from Anthracite Coal'] 
               
        self.energy_calcining_alumina['Anthracite List Price'] =  self.func_2_lk2('Anthracite List Price')
        self.energy_calcining_alumina['Customization Multiplier 1'] = self.pdt_cost_2['Energy Calcing - anth cos mul']
        self.energy_calcining_alumina['Final Price FAW'] = self.energy_calcining_alumina['Anthracite List Price']*self.energy_calcining_alumina['Customization Multiplier 1']
       
        self.energy_calcining_alumina['Final Freight'] =  self.pdt_cost_2['Energy Calcing - anth final frieght']
        self.energy_calcining_alumina['Delivered price'] =self.energy_calcining_alumina['Final Price FAW']+self.energy_calcining_alumina['Final Freight']
                
        self.energy_calcining_alumina['Calorific value conversion factor'] = 0.004184
        self.energy_calcining_alumina["A'cite Calorific Value"] =  self.func_2_lk2("A'cite Calorific Value")
        self.energy_calcining_alumina["A'cite Calorific Value 2"] =  self.energy_calcining_alumina['Calorific value conversion factor'] * self.energy_calcining_alumina["A'cite Calorific Value"] 
        self.energy_calcining_alumina['Final Price'] = self.energy_calcining_alumina['Delivered price']/self.energy_calcining_alumina["A'cite Calorific Value 2"]
        
        self.energy_calcining_alumina['Gas Price Delivered'] = self.func_2_lk2('Gas Price Delivered')
        self.energy_calcining_alumina['Natural Gas Customization Multiplier'] =self.pdt_cost_2['Energy Calcing - Natural gas custom mul']
        self.energy_calcining_alumina['Natural Gas Final Price'] = self.energy_calcining_alumina['Gas Price Delivered']  *  self.energy_calcining_alumina['Natural Gas Customization Multiplier'] 
        
        self.energy_calcining_alumina['Gas Calorific Value'] = self.func_2_lk2('Gas Calorific Value')
        self.energy_calcining_alumina['Natural Gas Final Price 2'] = self.energy_calcining_alumina['Natural Gas Final Price']/self.energy_calcining_alumina['Gas Calorific Value']
        self.energy_calcining_alumina['Alumina calcining energy - final price'] = (self.energy_calcining_alumina['% from Anthracite Coal'] * self.energy_calcining_alumina['Final Price'] )+(self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] * self.energy_calcining_alumina['Natural Gas Final Price 2'])
       
        self.energy_calcining_alumina['Grid Electricity List Price'] =  self.func_2_lk2('Grid Electricity List Price')
        self.energy_calcining_alumina['Electricity Customisation multiplier'] = self.pdt_cost_2['Energy Calcing - Electricity custom mul']
        self.energy_calcining_alumina['Electricity Final Price'] = self.energy_calcining_alumina['Grid Electricity List Price'] * self.energy_calcining_alumina['Electricity Customisation multiplier']
    
    def calc_lime_limestone(self):
        self.lime['Lime List Price'] =  self.func_2_lk2('Lime List Price')
        self.lime['Customisation multiplier 1'] = self.pdt_cost_2['Lime - custom mul']
        self.lime['Customised price'] = self.lime['Lime List Price'] *self.lime['Customisation multiplier 1']
        
        self.lime['Final Freight']              =  self.pdt_cost_2['Lime - Final freight']
        self.lime['Final Price']                = self.lime['Customised price'] + self.lime['Final Freight']
        
        self.limestone['Limestone List Price']  = self.func_2_lk2('Limestone List Price')
        self.limestone['Customisation multiplier 1'] = self.pdt_cost_2['Limestone - custom mul']
        self.limestone['Customised price']      =self.limestone['Limestone List Price'] * self.limestone['Customisation multiplier 1']
        
        self.limestone['Final Freight']             = self.pdt_cost_2['Limestone - Final freight']
        self.limestone['Final Price']               = self.limestone['Customised price'] + self.limestone['Final Freight']
        
    def calc_labour(self):
        self.labour['Labor General List Price']         = self.func_2_lk2('Labor General List Price')
        self.labour['Customisation multiplier']         = self.pdt_cost_2['Labour - custom mul']
        self.labour['Customised Price']                 = self.labour['Labor General List Price']  * self.labour['Customisation multiplier']
        self.labour['hours worked per year']            = self.pdt_cost_2['Labour - hours worked']
        self.labour['Labor Multiplier to All up Costs for Private Co.'] = self.pdt_cost_2['Labour - all up costs Private']
        self.labour['Labor Multiplier to All up Costs for SOE']         = self.pdt_cost_2['Labour - all up costs SOE']
        self.labour['Final multiplier to all up costs'] =  self.pdt_cost_2['Labour - final multiplier']
        self.labour['Final Price - all up costs']       = self.labour['Customised Price'] * 12/self.labour['hours worked per year'] * self.labour['Final multiplier to all up costs']
  
    def calc_flocculent(self):
        self.consumables['Flocculent List Price'] = self.func_2_lk2('Flocculent List Price')
        self.consumables['Customisation multiplier 1'] = self.pdt_cost_2['Flocculent - custom mul']
        self.consumables['Customised Price'] = self.consumables['Flocculent List Price']*self.consumables['Customisation multiplier 1']
        
        self.consumables['Final Freight'] = self.pdt_cost_2['Flocculent - final freight']
        
        self.consumables['Final Price'] =self.consumables['Customised Price'] +self.consumables['Final Freight']

    def calc_usagebauxite(self):
        self.usg_bauxite['Handling losses'] = self.pdt_cost_2['Usage - Bauxite -Handling loss']
        self.usg_bauxite['Alumina qaulity'] = self.func_4_lk1('Alumina quality')
        self.usg_bauxite['DSP AA:SiO2']     = self.func_4_lk1('DSP AA:SiO2')
        self.usg_bauxite['Extrcation efficiency'] =self.func_4_lk1('Extraction Efficiency')
        result = []
        res = 0
        for row in self.pdt_cost_2.index:
            if (self.bauxite_final_xtics.loc[row,'Alumina Grade (Reacting)']-self.usg_bauxite.loc[row,'DSP AA:SiO2']*self.bauxite_final_xtics.loc[row,'Silica Grade (Reacting)']) <=0 or (1-self.usg_bauxite.loc[row,'Handling losses'])<=0 or(1-self.usg_bauxite.loc[row,'Extrcation efficiency']) <=0:
                res = 0
                result.append(res)
            else:
                res = 1/(self.bauxite_final_xtics.loc[row,'Alumina Grade (Reacting)']-self.usg_bauxite.loc[row,'DSP AA:SiO2']*self.bauxite_final_xtics.loc[row,'Silica Grade (Reacting)'])
                res = res/(1-self.usg_bauxite.loc[row,'Handling losses'])/self.usg_bauxite.loc[row,'Extrcation efficiency']*self.usg_bauxite.loc[row,'Alumina qaulity']
                result.append(res)
        self.usg_bauxite['Bauxite Use']     = result
        self.usg_bauxite['Customise multiplier'] =self.func_1_bauxites('Cus Mul - BAR')
        self.usg_bauxite['Final bauxite'] = self.usg_bauxite['Bauxite Use']*self.usg_bauxite['Customise multiplier']
    
    def calc_usagecaustic(self):
        self.usg_caustic['DSP Na:Si'] = self.func_4_lk1('DSP Na:Si')
        self.usg_caustic['Caustic Wash loss'] = self.func_4_lk1('Caustic wash loss')
        value = self.usg_caustic['DSP Na:Si']* self.bauxite_final_xtics['Silica Grade (Reacting)']*self.usg_bauxite['Final bauxite'] + self.usg_caustic['Caustic Wash loss']
        self.usg_caustic['Caustic use'] = value
        self.usg_caustic['Customise multiplier'] = self.func_1_bauxites('Cus Mul - Caustic')
        self.usg_caustic['Final caustic use rate'] = self.usg_caustic['Caustic use']*self.usg_caustic['Customise multiplier']
        self.usg_caustic['Final Sodium Carbonate use rate'] = self.pdt_cost_2['Usage - Sodium Carbonate use rate']
   
    def calc_usageenergy(self):
        self.usg_energy['Required Steam raising energy'] =self.func_4_lk1('Required Steam raising energy')
        self.usg_energy['Customisation multiplier 1'] = self.pdt_cost_2['Usage - Energy -custom mul 1']
        self.usg_energy['Steam energy'] = self.usg_energy['Required Steam raising energy']*self.usg_energy['Customisation multiplier 1']
        
        self.usg_energy['If Anthracite'] =6.5
        self.usg_energy['If Natural gas/Coal Steam'] = 3.2
        val = self.energy_calcining_alumina['% from Anthracite Coal']*self.usg_energy['If Anthracite'] + self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas']*self.usg_energy['If Natural gas/Coal Steam']
        self.usg_energy['Final before cusmising'] = val
        self.usg_energy['Customisation multiplier 2'] = self.pdt_cost_2['Usage - Energy -custom mul 2']
        self.usg_energy['Calcining energy'] =self.usg_energy['Final before cusmising']*self.usg_energy['Customisation multiplier 2']
        
        self.usg_energy['Required Electrical energy'] = self.func_4_lk1('Required Electrical energy')
        self.usg_energy['Customisation multiplier 3'] = self.pdt_cost_2['Usage - Energy -custom mul 3']
        self.usg_energy['Electrical energy - final'] = self.usg_energy['Required Electrical energy']*self.usg_energy['Customisation multiplier 3']
      
    def calc_usagelimelimestone(self):
        self.usg_lime['Lime rate'] = self.func_4_lk1('Lime rate')
        self.usg_lime['Custom multiplier'] = self.pdt_cost_2['Usage - Lime Custom mul']
        self.usg_lime['Final Lime rate'] = self.usg_lime['Lime rate']*self.usg_lime['Custom multiplier']
        
        self.usg_limestone['Limestone 1 rate'] = self.func_4_lk1('Limestone rate')
        self.usg_limestone['Limestone 2 rate'] = self.usg_limestone['Limestone 1 rate'] *self.bauxite_final_xtics['Silica Grade (Reacting)']
        self.usg_limestone['Custom multiplier'] =self.pdt_cost_2['Usage - Limestone Custom mul']
        self.usg_limestone['Customed Limestone rate'] =self.usg_limestone['Limestone 2 rate']*self.usg_limestone['Custom multiplier']
        self.usg_limestone['Final Limestone rate'] =self.usg_limestone['Customed Limestone rate']*self.usg_bauxite['Final bauxite']
   
    def calc_usagelabour(self):
        val1 = [(10*pow(self.cap_production2['Capacity - for maintenance and labour'][x],-0.2) if (float(self.cap_production2['Capacity - for maintenance and labour'][x]))>1 else 0) for x in self.pdt_cost_2.index]
        val2 = [(1.5 if self.pdt_global['Digestion Technology code'][x]==2 or self.pdt_global['Digestion Technology code'][x]==3  else 1) for x in self.pdt_cost_2.index]
        self.usg_labour['Workforce Productivity'] = [val1[_]*val2[_] for _ in self.pdt_cost_2.index]
        self.usg_labour['Customise'] = self.pdt_cost_2['Usage -Labour Customise']
        self.usg_labour['Final Workforce Productivity'] =self.usg_labour['Workforce Productivity'] * (1 + self.usg_labour['Customise'] )
    
    def calc_usageconsumables(self):
        self.usg_consumables['Red Mud Make - scaling factor'] = [max(1, self.usg_bauxite['Final bauxite'][x]+ self.usg_lime['Final Lime rate'][x] + self.usg_limestone['Final Limestone rate'][x]-1) for x in self.pdt_cost_2.index]
        #flocculent
        self.usg_consumables['Usage rate'] = self.pdt_cost_2['Usage - Flocculent - usage rate']
        self.usg_consumables['customise']  = self.pdt_cost_2['Usage - Flocculent - customise']
        self.usg_consumables['Customised usage rate'] =  [(self.usg_consumables['Usage rate'][x] * (1+self.usg_consumables['customise'][x] )) for x in self.pdt_cost_2.index ]
        self.usg_consumables['Usage'] = self.usg_consumables['Customised usage rate'] * self.usg_consumables['Red Mud Make - scaling factor']/1000
    
    ##########
    def calc_costsbauxite(self):
        self.bauxite_cost['bauxite cost'] = self.bauxite_final_xtics['Total Delivered'] * self.usg_bauxite['Final bauxite']
        
    def calc_costscaustic(self):
        self.caustic_cost['Caustic cost'] = self.usg_caustic['Final caustic use rate'] * self.caustic_price['Final Price']
        self.caustic_cost['Sodium Carbonate cost'] = 0
        self.caustic_cost['Total caustic cosr'] = self.caustic_cost['Caustic cost'] + self.caustic_cost['Sodium Carbonate cost']

    def calc_costsenergy(self):
        self.energy_cost['Lignitious coal'] = self.usg_energy['Steam energy'] *  self.energy_for_steam['Final Price']
        self.energy_cost['Anthracite/Gas']  = self.usg_energy['Calcining energy'] * self.energy_calcining_alumina['Alumina calcining energy - final price']
        self.energy_cost['Electricity']     = self.usg_energy['Electrical energy - final'] * self.energy_calcining_alumina['Electricity Final Price']
        self.energy_cost['Total energy cost']   = self.energy_cost['Lignitious coal']  + self.energy_cost['Anthracite/Gas'] +  self.energy_cost['Electricity']
        
    def calc_costslime(self):
        self.lime_cost['Lime']              = self.usg_lime['Final Lime rate'] * self.lime['Final Price'] 
        self.lime_cost['Limestone']         = self.usg_limestone['Final Limestone rate'] * self.limestone['Final Price']
        self.lime_cost['Total lime cost']   = self.lime_cost['Lime'] + self.lime_cost['Limestone']

    def calc_costslabour(self):
        self.labour_cost['Labour'] = self.labour['Final Price - all up costs'] * self.usg_labour['Final Workforce Productivity']
        
    def calc_costsconsumables(self):
        self.consumables_cost['Flocculent']         = self.usg_consumables['Usage'] * self.consumables['Final Price']
        val1 = [(250*pow(self.cap_production2['Production - sub module'][x],-0.33) if (float(self.cap_production2['Production - sub module'][x]))>1 else 0) for x in self.pdt_cost_2.index]
        val2 = [(1.5 if self.pdt_global['Digestion Technology code'][x]==2 or self.pdt_global['Digestion Technology code'][x]==3  else 1) for x in self.pdt_cost_2.index]
        self.consumables_cost['Allowance for other Consumables']  = [val1[_]*val2[_] for _ in self.pdt_cost_2.index]
        self.consumables_cost['Total consumable cost'] = self.consumables_cost['Flocculent'] +  self.consumables_cost['Allowance for other Consumables']
        
    def calc_costsmaintenance(self):
        self.maintenance_cost['C&M Formulae Capital'] = self.pdt_cost_2['C&M Formulae Capital']
        self.maintenance_cost['Maintenance - Portion of Capex'] = self.pdt_cost_2['Maintenance Portion of Capex']
        self.maintenance_cost['Maintenance - Maintenance Cost'] =self.maintenance_cost['C&M Formulae Capital'] * self.maintenance_cost['Maintenance - Portion of Capex']
        
    def calc_costsother(self):
        self.other_cost['Other cost allowance'] =  self.pdt_cost_2['Other costs - allowance']
        
        val = self.usg_bauxite['Final bauxite']+self.usg_caustic['Final caustic use rate']+self.usg_caustic['Final Sodium Carbonate use rate']+self.usg_lime['Final Lime rate']+self.usg_limestone['Final Limestone rate']
        self.other_cost['Red mud quantity'] = val -1
        
        self.other_cost['Red Mud Disposal Cost 1'] = self.func_2_lk2('Red Mud Disposal Cost')
        self.other_cost['Red Mud Disposal Cost 2'] = [max(0, self.other_cost['Red mud quantity'][x]*self.other_cost['Red Mud Disposal Cost 1'][x] ) for x in self.pdt_cost_2.index]
        
        self.other_cost['Packaging'] = self.pdt_cost_2['Other costs - Packaging']
        
    def calc_pdtsummary(self):
        self.PDT2summmary['Bauxite']             = self.bauxite_cost['bauxite cost'] 
        self.PDT2summmary['Caustic']             = self.caustic_cost['Total caustic cosr']
        self.PDT2summmary['Lime']                = self.lime_cost['Total lime cost']
        self.PDT2summmary['Energy']              = self.energy_cost['Total energy cost']
        self.PDT2summmary['Labour']              = self.labour_cost['Labour']
        self.PDT2summmary['Consumables']         = self.consumables_cost['Total consumable cost']
        self.PDT2summmary['Maintenance']         = self.maintenance_cost['Maintenance - Maintenance Cost']
        self.PDT2summmary['Red Mud Disposal']    = self.other_cost['Red Mud Disposal Cost 2']
        self.PDT2summmary['Other Costs']         = self.other_cost['Other cost allowance']
        self.PDT2summmary['Operating Cash Cost'] = self.PDT2summmary[['Bauxite','Caustic','Lime','Energy','Labour','Consumables','Maintenance','Red Mud Disposal','Other Costs']].sum(axis=1)
        self.PDT2summmary['Packaging']           = self.other_cost['Packaging']
        self.PDT2summmary['Cash Cost - FAW']     = self.PDT2summmary['Operating Cash Cost'] + self.PDT2summmary['Packaging']
     
    #########
    
     
    #pdt fuctions
    #self.func_2_lk2
    def func_1_bauxites(self, ref_col): #ref no
        d = {self.refPDT1['Ref. No.'][x]:self.refPDT1[ref_col][x] for x in self.refPDT1.index}
        return [(d[self.pdt_cost_2['Ref. No.'][x]]) for x in self.pdt_cost_2.index]
    
    def func_2_lk2(self, ref_col): #province
        d = {self.lookup2['Province Prices & Rates - Current'][x]:self.lookup2[ref_col][x] for x in self.lookup2.index}
        res = []
        for row in self.pdt_cost_2.index:
            try:
                res.append(d[self.pdt_header.loc[row,'country']])
            except:
                res.append(0)
        return res# [(d[self.pdt_header['country'][x]]) for x in self.pdt_cost_2.index]
    
    def func_3_iBX(self, ref_col): #ref
        res = []
        d = {self.ImpBxSummary['Ref. No.'][x]:self.ImpBxSummary[ref_col][x] for x in self.ImpBxSummary.index}
        for row in self.pdt_cost_2.index:
            try:
                res.append( d[self.pdt_cost_2.loc[row,'Ref. No.']] )
            except:
                res.append(0)
                #print('index : ',row)
        return res
    def func_4_lk1(self,ref_col):
        res = []
        d = {self.lookup1['Digestion Technology'][x]:self.lookup1[ref_col][x] for x in self.lookup1.index}
        for row in self.pdt_cost_2.index:
            try:
                res.append( d[self.pdt_global.loc[row,'Refinery Digestion Technology']] )
            except:
                res.append(0)
        return res



#pdt_cost_2





#leagu master
class League_Master():
    def __init__(self, pdt):
        self.leaguetab = pd.DataFrame(columns=[], index=pdt.pdt_cost_1.index)
        self.provincial = pd.DataFrame(columns=[])
        self.regional = pd.DataFrame(columns=[])
        self.leagueProvince = pd.DataFrame(columns=[])
        self.pdt_input = pdt.pdt_cost_1
        self.pdt_econ = pdt.macro_economic
        self.pdt_headline = pdt.pdt1_headline
        self.pdt_sum = pdt.pdt_1_costSum
        self.pdt_fac = pdt.pdt_cost_1.loc[0, 'Exchange Rate']
        
        
    def calc_all(self):
        self.cal_lm_1()
        self.cal_lm_2()
        self.cal_lm_3()
        self.cal_lm_4()
    
    def cal_lm_1(self):
        self.leaguetab['Ref'] = self.pdt_input['Ref. No.']
        self.leaguetab['Name'] = self.pdt_econ['Sub Name - technology variant']
        self.leaguetab['Province'] =self.pdt_econ['Province']
        self.leaguetab['Region'] = self.pdt_input['Prov - Category']
        self.leaguetab['Owner'] = self.pdt_econ['Owner']
        self.leaguetab['Cost - Bx'] = self.pdt_sum['Bauxite']
        self.leaguetab['Cost - Caustic'] =self.pdt_sum['Caustic']
        self.leaguetab['Cost - Energy'] =self.pdt_sum['Energy']
        self.leaguetab['Cost - Opex'] = self.pdt_sum['Cash Cost - FAW 1']
        self.leaguetab['Production KT'] =self.pdt_headline['Production -kt']
        
        
        self.leaguetab['SOE=1, Private=2'] = self.pdt_headline['SOE=1,Private=2']
        rank = sorted(self.leaguetab['Cost - Opex'][:-4])
        
        value = []
        for row in self.leaguetab.index:
            cnt = self.leaguetab.loc[:row,'Cost - Opex'].to_list()
            self.leaguetab.loc[row, 'Rank'] = cnt.count(self.leaguetab.loc[row,'Cost - Opex']) + rank.index(self.leaguetab.loc[row,'Cost - Opex'])
            if self.leaguetab.loc[row, 'Owner'] == 'Weiqiao':
                value.append(1)
            elif self.leaguetab.loc[row, 'Owner'] == 'Xinfa':
                value.append(2)
            elif self.leaguetab.loc[row, 'Owner'] == 'Chalco':
                value.append(3)
            else:
                value.append(4)
        self.leaguetab['Groups'] = value
        self.leaguetab['Cost $/t'] = [self.leaguetab.loc[x,'Cost - Opex']/self.pdt_fac for x in self.leaguetab.index]
    def cal_lm_2(self):
        self.provincial['Provincial'] = ['Chongqing','Guangxi','Guizhou','Henan','Inner Mongolia', 'Hubei', 'Hunan','Shandong','Shanxi','Yunnan']
        for row in self.provincial.index:
            self.provincial.at[row,'Production kT'] = self.leaguetab.loc[self.leaguetab['Province']==self.provincial.loc[row,'Provincial'], 'Production KT'].sum()/1
        for row in self.provincial.index:
            self.provincial.at[row,'Cost - Opex'] = float(self.leaguetab.loc[self.leaguetab['Province']==self.provincial.loc[row,'Provincial'], ['Cost - Opex','Production KT']].product(axis=1).sum())/self.pdt_fac/self.provincial.loc[row,'Production kT'] if self.provincial.loc[row,'Production kT']> 0 else 0
            self.provincial.at[row,'Cost - Energy'] = self.leaguetab.loc[self.leaguetab['Province']==self.provincial.loc[row,'Provincial'], ['Cost - Energy','Production KT']].product(axis=1).sum()/self.pdt_fac/self.provincial.loc[row,'Production kT'] if self.provincial.loc[row,'Production kT']> 0 else 0
            self.provincial.at[row,'Cost - Caustic'] = self.leaguetab.loc[self.leaguetab['Province']==self.provincial.loc[row,'Provincial'], ['Cost - Caustic','Production KT']].product(axis=1).sum()/self.pdt_fac/self.provincial.loc[row,'Production kT'] if self.provincial.loc[row,'Production kT']> 0 else 0
            self.provincial.at[row,'Cost - Bx'] = self.leaguetab.loc[self.leaguetab['Province']==self.provincial.loc[row,'Provincial'], ['Cost - Bx','Production KT']].product(axis=1).sum()/self.pdt_fac/self.provincial.loc[row,'Production kT'] if self.provincial.loc[row,'Production kT']> 0 else 0
            self.provincial.at[row,'Cost RMB/t'] =self.provincial.loc[row,'Cost - Opex']*self.pdt_fac
            
    def cal_lm_3(self):
        self.regional['Regional'] = [ 'Shandong','Shanxi', 'Henan', 'Guangxi','Guizhou', 'Other']
        for row in self.regional.index:
            self.regional.at[row,'Production kT'] = self.leaguetab.loc[self.leaguetab['Region']==self.regional.loc[row,'Regional'], 'Production KT'].sum()/1
        for row in self.regional.index:
            self.regional.at[row,'Cost - Opex'] = self.leaguetab.loc[self.leaguetab['Region']==self.regional.loc[row,'Regional'], ['Cost - Opex','Production KT']].product(axis=1).sum()/self.pdt_fac/self.regional.loc[row,'Production kT'] if self.regional.loc[row,'Production kT']> 0 else 0
            self.regional.at[row,'Cost - Energy'] = self.leaguetab.loc[self.leaguetab['Region']==self.regional.loc[row,'Regional'], ['Cost - Energy','Production KT']].product(axis=1).sum()/self.pdt_fac/self.regional.loc[row,'Production kT'] if self.regional.loc[row,'Production kT']> 0 else 0
            self.regional.at[row,'Cost - Caustic'] = self.leaguetab.loc[self.leaguetab['Region']==self.regional.loc[row,'Regional'], ['Cost - Caustic','Production KT']].product(axis=1).sum()/self.pdt_fac/self.regional.loc[row,'Production kT'] if self.regional.loc[row,'Production kT']> 0 else 0
            self.regional.at[row,'Cost - Bx'] = self.leaguetab.loc[self.leaguetab['Region']==self.regional.loc[row,'Regional'], ['Cost - Bx','Production KT']].product(axis=1).sum()/self.pdt_fac/self.regional.loc[row,'Production kT'] if self.regional.loc[row,'Production kT']> 0 else 0
            self.regional.at[row,'Cost RMB/t'] =self.regional.loc[row,'Cost - Opex']*self.pdt_fac
    def cal_lm_4(self):
        self.leagueProvince['Province'] =self.provincial['Provincial']
        self.leagueProvince['Cost RMB/t'] =self.provincial['Cost RMB/t']
        self.leagueProvince['Cost - Bx'] =self.provincial['Cost - Bx']
        self.leagueProvince['Cost - Caustic'] =self.provincial['Cost - Caustic']
        self.leagueProvince['Cost - Energy'] =self.provincial['Cost - Energy']
        self.leagueProvince['Cost - Opex'] =self.provincial['Cost - Opex']
        self.leagueProvince['Production mT'] =self.provincial['Production kT']/1000
        
        rank = sorted(self.leagueProvince['Cost - Opex'])
        for row in self.leagueProvince.index:
            cnt = self.leagueProvince.loc[:row,'Cost - Opex'].to_list()
            self.leagueProvince.loc[row, 'Rank'] = cnt.count(self.leagueProvince.loc[row,'Cost - Opex']) + rank.index(self.leagueProvince.loc[row,'Cost - Opex'])
        
#league master














#AA model engine
impBxTonnes = Import_Bx_Tonnes()
impBxTonnes.calc_all()

pdt_cost = PDT_Cost_Current(impBxTonnes)
pdt_cost.calc_all()

pdt_cost2 = PDT_Cost_Current2(impBxTonnes, pdt_cost)
pdt_cost2.calc_all()

lmaster = League_Master(pdt_cost)
lmaster.calc_all()



writer1 = pd.ExcelWriter('outputs//Import Bx Tonnes & CFR prices.xlsx')
impBxTonnes.table2.to_excel(writer1, sheet_name='Table 2 Tonnages only', encoding='utf-8', index=False)
impBxTonnes.table3.to_excel(writer1, sheet_name='Table 3 Prices only', encoding='utf-8', index=False)
impBxTonnes.table4.to_excel(writer1, sheet_name='Table 4 Tonnages Blank', encoding='utf-8', index=False)
impBxTonnes.table5.to_excel(writer1, sheet_name='Table 5 Prices Blank', encoding='utf-8', index=False)
writer1.save()


writer2 = pd.ExcelWriter('outputs//PDT Cost Current tab 1.xlsx')
pdt_cost.cap_production.to_excel(writer2, sheet_name='Capacity & Production',encoding='utf-8', index=False)
pdt_cost.pdt_global.to_excel(writer2, sheet_name='Global',encoding='utf-8', index=False)
pdt_cost.bauxite_1.to_excel(writer2, sheet_name='Bauxite 1',encoding='utf-8', index=False)
pdt_cost.bauxite_2.to_excel(writer2, sheet_name='Bauxite 2',encoding='utf-8', index=False)
pdt_cost.bauxite_3.to_excel(writer2, sheet_name='Bauxite 3',encoding='utf-8', index=False)
pdt_cost.bauxite_sourcing_mix.to_excel(writer2, sheet_name='Bauxite Sourcing mix',encoding='utf-8', index=False)
pdt_cost.bauxite_final_xtics.to_excel(writer2, sheet_name='Bauxite Final Xtics',encoding='utf-8', index=False)
pdt_cost.caustic_purchase.to_excel(writer2, sheet_name='Caustic purchase',encoding='utf-8', index=False)
pdt_cost.caustic_selfsupplied.to_excel(writer2, sheet_name='Caustic Self supply',encoding='utf-8', index=False)
pdt_cost.energy_for_steam.to_excel(writer2, sheet_name='Energy for steam raising',encoding='utf-8', index=False)
pdt_cost.energy_calcining_alumina.to_excel(writer2, sheet_name='Energy for calcinig alum',encoding='utf-8', index=False)
pdt_cost.lime.to_excel(writer2, sheet_name='Lime Prices',encoding='utf-8', index=False)
pdt_cost.limestone.to_excel(writer2, sheet_name='Limestone Prices',encoding='utf-8', index=False)
pdt_cost.labour.to_excel(writer2, sheet_name='Labour Prices',encoding='utf-8', index=False)
pdt_cost.consumables.to_excel(writer2, sheet_name='Comsumables Prices',encoding='utf-8', index=False)
pdt_cost.usg_bauxite.to_excel(writer2, sheet_name='Usages - Bauxite',encoding='utf-8', index=False)
pdt_cost.usg_caustic.to_excel(writer2, sheet_name='Usages -Caustic',encoding='utf-8', index=False)
pdt_cost.usg_energy.to_excel(writer2, sheet_name='Usages - Energy',encoding='utf-8', index=False)
pdt_cost.usg_lime.to_excel(writer2, sheet_name='Usages -Lime',encoding='utf-8', index=False)
pdt_cost.usg_limestone.to_excel(writer2, sheet_name='Usages -Limestone',encoding='utf-8', index=False)
pdt_cost.usg_labour.to_excel(writer2, sheet_name='Usages -labour',encoding='utf-8', index=False)
pdt_cost.usg_consumables.to_excel(writer2, sheet_name='Usages -consum',encoding='utf-8', index=False)
pdt_cost.bauxite_cost.to_excel(writer2, sheet_name='Bauxite_cost ',encoding='utf-8', index=False)
pdt_cost.caustic_cost.to_excel(writer2, sheet_name='Caustic',encoding='utf-8', index=False)
pdt_cost.energy_cost.to_excel(writer2, sheet_name='Energy cost',encoding='utf-8', index=False)
pdt_cost.lime_cost.to_excel(writer2, sheet_name='Lime cost',encoding='utf-8', index=False)
pdt_cost.labour_cost.to_excel(writer2, sheet_name='Labour cost',encoding='utf-8', index=False)
pdt_cost.consumables_cost.to_excel(writer2, sheet_name='Consumbles cost',encoding='utf-8', index=False)
pdt_cost.maintenance_cost.to_excel(writer2, sheet_name='Maintenance Cost',encoding='utf-8', index=False)
pdt_cost.other_cost.to_excel(writer2, sheet_name='Other costs',encoding='utf-8', index=False)
pdt_cost.PDT1summmary.to_excel(writer2, sheet_name='PDT summary',encoding='utf-8', index=False)
pdt_cost.pdt1_headline.to_excel(writer2, sheet_name='Headline',encoding='utf-8', index=False)
pdt_cost.pdt_1_costSum.to_excel(writer2, sheet_name='PDT Cost Summary',encoding='utf-8', index=False)
#pdt_cost..to_excel(writer2, sheet_name=' ',encoding='utf-8', index=False)
writer2.save()

writer5 = pd.ExcelWriter('outputs//PDT Cost Current tab 2.xlsx')
pdt_cost2.cap_production2.to_excel(writer5, sheet_name='Capcity Production',encoding='utf-8', index=False)
pdt_cost2.pdt_global.to_excel(writer5, sheet_name='Global',encoding='utf-8', index=False)
pdt_cost2.bauxite1.to_excel(writer5, sheet_name='Bauxite 1',encoding='utf-8', index=False)
pdt_cost2.bauxite2.to_excel(writer5, sheet_name='Bauxite 2',encoding='utf-8', index=False)
pdt_cost2.bauxite3.to_excel(writer5, sheet_name='Bauxite 3',encoding='utf-8', index=False)
pdt_cost2.bauxite_sourcing_mix.to_excel(writer5, sheet_name='Bauxite Sourcing Mix',encoding='utf-8', index=False)
pdt_cost2.bauxite_final_xtics.to_excel(writer5, sheet_name='Bauxite Final Xtics',encoding='utf-8', index=False)
pdt_cost2.caustic_price.to_excel(writer5, sheet_name='Caustic Price',encoding='utf-8', index=False)
pdt_cost2.energy_for_steam.to_excel(writer5, sheet_name='Energy Stm Raisng',encoding='utf-8', index=False)
pdt_cost2.energy_calcining_alumina.to_excel(writer5, sheet_name='Energy Calcinig',encoding='utf-8', index=False)
pdt_cost2.lime.to_excel(writer5, sheet_name='Lime Prices',encoding='utf-8', index=False)
pdt_cost2.limestone.to_excel(writer5, sheet_name='Limestone Prcs',encoding='utf-8', index=False)
pdt_cost2.labour.to_excel(writer5, sheet_name='labour Prices',encoding='utf-8', index=False)
pdt_cost2.consumables.to_excel(writer5, sheet_name='Consumables Prcs',encoding='utf-8', index=False)
pdt_cost2.usg_bauxite.to_excel(writer5, sheet_name='Bauxite USage',encoding='utf-8', index=False)
pdt_cost2.usg_caustic.to_excel(writer5, sheet_name='Caustic Usage',encoding='utf-8', index=False)
pdt_cost2.usg_energy.to_excel(writer5, sheet_name='Energy Usage',encoding='utf-8', index=False)
pdt_cost2.usg_lime.to_excel(writer5, sheet_name='Lime Usage',encoding='utf-8', index=False)
pdt_cost2.usg_limestone.to_excel(writer5, sheet_name='Limestone Usage',encoding='utf-8', index=False)
pdt_cost2.usg_labour.to_excel(writer5, sheet_name='Labour Usage',encoding='utf-8', index=False)
pdt_cost2.usg_consumables.to_excel(writer5, sheet_name='Consumables',encoding='utf-8', index=False)
pdt_cost2.bauxite_cost.to_excel(writer5, sheet_name='bauxite_cost',encoding='utf-8', index=False)
pdt_cost2.caustic_cost.to_excel(writer5, sheet_name='caustic_cost',encoding='utf-8', index=False)
pdt_cost2.energy_cost.to_excel(writer5, sheet_name='energy_cost',encoding='utf-8', index=False)
pdt_cost2.lime_cost.to_excel(writer5, sheet_name='lime_cost',encoding='utf-8', index=False)
pdt_cost2.labour_cost.to_excel(writer5, sheet_name='labour_cost',encoding='utf-8', index=False)
pdt_cost2.consumables_cost.to_excel(writer5, sheet_name='consumables_cost',encoding='utf-8', index=False)
pdt_cost2.other_cost.to_excel(writer5, sheet_name='other_cost',encoding='utf-8', index=False)
pdt_cost2.PDT2summmary.to_excel(writer5, sheet_name='PDT2summmary',encoding='utf-8', index=False)
#pdt_cost2..to_excel(writer5, sheet_name='',encoding='utf-8', index=False)
#pdt_cost2..to_excel(writer5, sheet_name='',encoding='utf-8', index=False)
#pdt_cost2..to_excel(writer5, sheet_name='',encoding='utf-8', index=False)
#pdt_cost2..to_excel(writer5, sheet_name='',encoding='utf-8', index=False)
#pdt_cost2..to_excel(writer5, sheet_name='',encoding='utf-8', index=False)
#pdt_cost2..to_excel(writer5, sheet_name='',encoding='utf-8', index=False)
writer5.save()

writer4 =  pd.ExcelWriter('outputs//Refineries Capacity&Production Output.xlsx')
pdt_cost.ref_base_cap_regions.to_excel(writer4, sheet_name='Base Capacity Splits',encoding='utf-8', index=False)
pdt_cost.ref_base_prod_regions.to_excel(writer4, sheet_name='Base Production Splits',encoding='utf-8', index=False)
writer4.save()


writer3 = pd.ExcelWriter("outputs/league master output.xlsx")
lmaster.leaguetab.to_excel(writer3, sheet_name="League Master", encoding='utf-8', index=False)
lmaster.provincial.to_excel(writer3, sheet_name="LeageM Provincial", encoding='utf-8', index=False)
lmaster.regional.to_excel(writer3, sheet_name="LeagueM Regional", encoding='utf-8', index=False)
lmaster.leagueProvince.to_excel(writer3, sheet_name="League Province", encoding='utf-8', index=False)
writer3.save()

# start here

# ms = alumina
# sm = db, dfdb
# mm = multilevel

maflat_time = time.perf_counter()

snapshot_output_data = pd.DataFrame(columns=ma_conv.out_col)

db_list = [
    snapshot_output_data,
    ma_conv.single_year_mult_out(impBxTonnes.table2, "Table 2 Tonnages only"),
    ma_conv.single_year_mult_out(impBxTonnes.table3, "Table 3 Prices only"),
    ma_conv.single_year_mult_out(impBxTonnes.table4, "Table 4 Tonnages Blank"),
    ma_conv.single_year_mult_out(impBxTonnes.table5, "Table 5 Prices Blank"),
    
    ma_conv.single_year_mult_out(pdt_cost.cap_production, "Capacity & Production"),
    ma_conv.single_year_mult_out(pdt_cost.pdt_global, "Global"),
    ma_conv.single_year_mult_out(pdt_cost.bauxite_1, "Bauxite 1"),
    ma_conv.single_year_mult_out(pdt_cost.bauxite_2, "Bauxite 2"),
    ma_conv.single_year_mult_out(pdt_cost.bauxite_3, "Bauxite 3"),
    ma_conv.single_year_mult_out(pdt_cost.bauxite_sourcing_mix, "Bauxite Sourcing mix"),
    ma_conv.single_year_mult_out(pdt_cost.bauxite_final_xtics, "Bauxite Final Xtics"),
    ma_conv.single_year_mult_out(pdt_cost.caustic_purchase, "Caustic purchase"),
    ma_conv.single_year_mult_out(pdt_cost.caustic_selfsupplied, "Caustic Self supply"),
    ma_conv.single_year_mult_out(pdt_cost.energy_for_steam, "Energy for steam raising"),
    ma_conv.single_year_mult_out(pdt_cost.energy_calcining_alumina, "Energy for calcinig alum"),
    ma_conv.single_year_mult_out(pdt_cost.lime, "Lime Prices"),
    ma_conv.single_year_mult_out(pdt_cost.limestone, "Limestone Prices"),
    ma_conv.single_year_mult_out(pdt_cost.labour, "Labour Prices"),
    ma_conv.single_year_mult_out(pdt_cost.consumables, "Comsumables Prices"),
    ma_conv.single_year_mult_out(pdt_cost.usg_bauxite, "Usages - Bauxite"),
    ma_conv.single_year_mult_out(pdt_cost.usg_caustic, "Usages -Caustic"),
    ma_conv.single_year_mult_out(pdt_cost.usg_energy, "Usages - Energy"),
    ma_conv.single_year_mult_out(pdt_cost.usg_lime, "Usages -Lime"),
    ma_conv.single_year_mult_out(pdt_cost.usg_limestone, "Usages -Limestone"),
    ma_conv.single_year_mult_out(pdt_cost.usg_labour, "Usages -labour"),
    ma_conv.single_year_mult_out(pdt_cost.usg_consumables, "Usages -consum"),
    ma_conv.single_year_mult_out(pdt_cost.bauxite_cost, "Bauxite_cost"),
    ma_conv.single_year_mult_out(pdt_cost.caustic_cost, "Caustic"),
    ma_conv.single_year_mult_out(pdt_cost.energy_cost, "Energy cost"),
    ma_conv.single_year_mult_out(pdt_cost.lime_cost, "Lime cost"),
    ma_conv.single_year_mult_out(pdt_cost.labour_cost, "Labour cost"),
    ma_conv.single_year_mult_out(pdt_cost.consumables_cost, "Consumbles cost"),
    ma_conv.single_year_mult_out(pdt_cost.maintenance_cost, "Maintenance Cost"),
    ma_conv.single_year_mult_out(pdt_cost.other_cost, "Other costs"),
    ma_conv.single_year_mult_out(pdt_cost.PDT1summmary, "PDT summary"),
    ma_conv.single_year_mult_out(pdt_cost.pdt1_headline, "Headline"),
    ma_conv.single_year_mult_out(pdt_cost.pdt_1_costSum, "PDT Cost Summary"),
    
    ma_conv.single_year_mult_out(pdt_cost2.cap_production2, "Capcity Production"),
    ma_conv.single_year_mult_out(pdt_cost2.pdt_global, "Global"),
    ma_conv.single_year_mult_out(pdt_cost2.bauxite1, "Bauxite 1"),
    ma_conv.single_year_mult_out(pdt_cost2.bauxite2, "Bauxite 2"),
    ma_conv.single_year_mult_out(pdt_cost2.bauxite3, "Bauxite 3"),
    ma_conv.single_year_mult_out(pdt_cost2.bauxite_sourcing_mix, "Bauxite Sourcing Mix"),
    ma_conv.single_year_mult_out(pdt_cost2.bauxite_final_xtics, "Bauxite Final Xtics"),
    ma_conv.single_year_mult_out(pdt_cost2.caustic_price, "Caustic Price"),
    ma_conv.single_year_mult_out(pdt_cost2.energy_for_steam, "Energy Stm Raisng"),
    ma_conv.single_year_mult_out(pdt_cost2.energy_calcining_alumina, "Energy Calcinig"),
    ma_conv.single_year_mult_out(pdt_cost2.lime, "Lime Prices"),
    ma_conv.single_year_mult_out(pdt_cost2.limestone, "Limestone Prcs"),
    ma_conv.single_year_mult_out(pdt_cost2.labour, "labour Prices"),
    ma_conv.single_year_mult_out(pdt_cost2.consumables, "Consumables Prcs"),
    ma_conv.single_year_mult_out(pdt_cost2.usg_bauxite, "Bauxite USage"),
    ma_conv.single_year_mult_out(pdt_cost2.usg_caustic, "Caustic Usage"),
    ma_conv.single_year_mult_out(pdt_cost2.usg_energy, "Energy Usage"),
    ma_conv.single_year_mult_out(pdt_cost2.usg_lime, "Lime Usage"),
    ma_conv.single_year_mult_out(pdt_cost2.usg_limestone, "Limestone Usage"),
    ma_conv.single_year_mult_out(pdt_cost2.usg_labour, "Labour Usage"),
    ma_conv.single_year_mult_out(pdt_cost2.usg_consumables, "Consumables"),
    ma_conv.single_year_mult_out(pdt_cost2.bauxite_cost, "bauxite_cost"),
    ma_conv.single_year_mult_out(pdt_cost2.caustic_cost, "caustic_cost"),
    ma_conv.single_year_mult_out(pdt_cost2.energy_cost, "energy_cost"),
    ma_conv.single_year_mult_out(pdt_cost2.lime_cost, "lime_cost"),
    ma_conv.single_year_mult_out(pdt_cost2.labour_cost, "labour_cost"),
    ma_conv.single_year_mult_out(pdt_cost2.consumables_cost, "consumables_cost"),
    ma_conv.single_year_mult_out(pdt_cost2.other_cost, "other_cost"),
    ma_conv.single_year_mult_out(pdt_cost2.PDT2summmary, "PDT2summmary"),
    
    ma_conv.mult_year_single_output(pdt_cost.ref_base_cap_regions, "Base Capacity Splits"),
    ma_conv.mult_year_single_output(pdt_cost.ref_base_prod_regions, "Base Production Splits"),

    ma_conv.single_year_mult_out(lmaster.leaguetab, "League Master"),
    ma_conv.single_year_mult_out(lmaster.provincial, "LeageM Provincial"),
    ma_conv.single_year_mult_out(lmaster.regional, "LeagueM Regional"),
    ma_conv.single_year_mult_out(lmaster.leagueProvince, "League Province"),
    
    ]

snapshot_output_data = pd.concat(db_list, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, ma_conv.out_col]
snapshot_output_data.to_csv("_snapshot_output_data.csv", index=False)

print("Time taken to convet to flat db: {0} ".format(time.perf_counter() - maflat_time))


uploadtodb.upload(snapshot_output_data)
