# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb

al_flat = Flatdbconverter("Quarterly Chart Pack")

#workbooks
data_input_annual_wbk = 'Inputs\Data input annual.xlsx' 
bx_imports_wbk = 'Inputs\Bx Imports.xlsx'
other_inputs_bx_wbk = 'Inputs\Other inputs for bx pro .xlsx'
ref_aa_prod_wbk = 'Inputs\Refineries & Aa Production.xlsx'
data_input_monthly_wbk = 'Inputs\Data input monthly.xlsx'

data_monthly_wbk = 'Inputs\Data Monthly Inputs.xlsx'

mines_bx_prd_wbk = 'Inputs\Mines & Bx Production inputs.xlsx'
bauxite_sources_wbk = 'Inputs\Bauxite sources.xlsx'

global_bx_wbk = 'Inputs\Global Bx Quarterly inputs.xlsx'
aa_production_bx_wbk = 'Inputs\AA prodn from Import Bx-Mthly.xlsx'
cm_global_wbk = 'Inputs\CM Global Bx Production inputs.xlsx'

data_chartss_wbk = 'Inputs\Data Charts inputs.xlsx'

class ALengine():
    def __init__ (self):
        self.dataInputAnnual= pd.read_excel(data_input_annual_wbk, sheet_name='Sheet1')
        self.bxImports_rawData = pd.read_excel(bx_imports_wbk, sheet_name='Raw trade data')
        self.bxImports_tab1 = pd.read_excel(bx_imports_wbk, sheet_name='Table 1')
        self.bxImports_tab2 = pd.read_excel(bx_imports_wbk, sheet_name='Table 2')
        self.bxImports_tab3 = pd.read_excel(bx_imports_wbk, sheet_name='Table 3')
        self.bxImports_tab4 = pd.read_excel(bx_imports_wbk, sheet_name='Table 4')
        self.bxImports_tab5 = pd.read_excel(bx_imports_wbk, sheet_name='Table 5')
        self.bxImports_tab6 = pd.read_excel(bx_imports_wbk, sheet_name='Table 6')
        self.bxImports_tab7 = pd.read_excel(bx_imports_wbk, sheet_name='Table 7')
        self.Changjiang_LME_tab1 = pd.read_excel('Inputs\Changjiang & LME Daily inputs.xlsx', sheet_name='Table 1')
        self.Changjiang_LME_tab2 = pd.read_excel('Inputs\Changjiang & LME Daily inputs.xlsx', sheet_name='Table 2')
        self.platts_vs_cmaax = pd.read_excel('Inputs\Platts Vs CMAAX inputs.xlsx',sheet_name='Sheet1')
        
        
        self.otherInput_bx_tab1 = pd.read_excel(other_inputs_bx_wbk, sheet_name='Table 1')
        self.otherInput_bx_tab2 = pd.read_excel(other_inputs_bx_wbk, sheet_name='Table 2')
        self.ref_AA_productions = pd.read_excel(ref_aa_prod_wbk, sheet_name='Sheet1')
        
        self.data_inp_montly_tab1 = pd.read_excel(data_input_monthly_wbk, sheet_name='Alumina Monthly Supply')
        self.data_inp_montly_tab2 = pd.read_excel(data_input_monthly_wbk, sheet_name='Aluminuim Monthly Supply')
        self.data_inp_montly_tab3 = pd.read_excel(data_input_monthly_wbk, sheet_name='Alumina Price Trend')
        self.data_inp_montly_bax1 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports')
        self.data_inp_montly_bax2 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 2')
        self.data_inp_montly_bax3 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 3')
        self.data_inp_montly_bax4 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 4')
        self.data_inp_montly_bax5 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 5')
        self.data_inp_montly_baxStyle = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Style')
        self.data_inp_montly_exchange_rate =pd.read_excel(data_input_monthly_wbk, sheet_name='Exchange Rate(R;U)')
        self.data_inp_montly_inpBAR = pd.read_excel(data_input_monthly_wbk, sheet_name='Input BAR')
        self.dataInputMontly_BAR = pd.read_excel(data_input_monthly_wbk, sheet_name='BAR')
        
        self.dataInputMontly_CmPortBx = pd.read_excel(data_input_monthly_wbk, sheet_name='CM Port Bx Inventry')
        self.dataInputMontly_global_alumina = pd.read_excel(data_input_monthly_wbk, sheet_name='Global Alumina Prd')
        
        self.AAproductions = pd.read_excel(aa_production_bx_wbk, sheet_name='Sheet1')
        
        self.minesBxPrdocution1 = pd.read_excel(mines_bx_prd_wbk, sheet_name='Sheet1')
        self.minesBxPrdocution2 = pd.read_excel(mines_bx_prd_wbk, sheet_name='Sheet2')
        self.minesBxPrdocution3 = pd.read_excel(mines_bx_prd_wbk, sheet_name='Sheet3')
        self.minesBxPrdocution_final = pd.DataFrame(columns=[], index=self.minesBxPrdocution1.index)
        self.BauxiteSources = pd.read_excel(bauxite_sources_wbk, sheet_name='Sheet1')
        self.BauxiteSourcesSummary = pd.read_excel(bauxite_sources_wbk, sheet_name='Summary')
        
        self.data_monthly_inp_comspt = pd.read_excel(data_monthly_wbk, sheet_name='Sheet1')
        
        self.GlobalBx = pd.read_excel(global_bx_wbk, sheet_name='Sheet1')
        self.cm_global_prd = pd.read_excel(cm_global_wbk, sheet_name='Sheet1')
        
        self.special_charts_1 = pd.DataFrame(columns=[])
        self.special_charts_2 = pd.DataFrame(columns=[])
        
        self.data_charts_11 = pd.read_excel(data_chartss_wbk, sheet_name='Chart11')
        self.data_charts_12 = pd.read_excel(data_chartss_wbk, sheet_name='Chart12')
        
        self.data_input_annual = pd.DataFrame(columns=[], index=self.dataInputAnnual.index)
        self.bx_imports_table1 = pd.DataFrame(columns=self.bxImports_tab1.columns, index=self.bxImports_tab1.index)
        self.bxImports_tab8 = self.bxImports_tab7.copy()
        self.bxImports_tab9 = pd.DataFrame(columns=[])
        self.refAAproductions =pd.DataFrame(columns=[], index=self.ref_AA_productions.index)
        self.changjiangLME = pd.DataFrame(columns=[], index=self.Changjiang_LME_tab2)
        self.plattsVsCmaax = pd.DataFrame(columns=[], index=self.platts_vs_cmaax.index)
        self.bxImports_tab3_copy = pd.DataFrame(columns=[], index=self.bxImports_tab3.index)
        self.BAR_tab = pd.DataFrame(columns=[])
        
        self.dataInputMontly_alumina_montly_supply = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_aluminium_montly_supply = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_bauxite_imports1 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_bauxite_imports2 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_bauxite_imports3 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_bauxite_imports4 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_bauxite_imports5 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_input_bar = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        
        self.dataInputMontly_inland1= pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataInputMontly_inland2 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        
        self.dataMonthly_alumina_monthly = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataMonthly_aluminium_supply = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataMonthly_bauxite_imps1 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataMonthly_bauxite_imps2 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataMonthly_bauxite_imps3 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataMonthly_consumpt = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        
        self.cmGlobalProd_main = pd.DataFrame(columns=[], index=self.cm_global_prd.index)
        self.specialCharts_out1 = pd.DataFrame(columns=[], index=range(17))
        self.specialCharts_out2 = pd.DataFrame(columns=[], index=range(17))
        
        self.dataCharts_1 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_1a= pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_2 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_3 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_4 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_5 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_6 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_7 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_8 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_9 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_10 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_11 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_12 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts_13 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts__10 = pd.DataFrame(columns=[], index=self.data_input_annual.index)
        self.dataCharts__11 = pd.DataFrame(columns=[], index=self.data_input_annual.index)
        
    def calc_all(self):
        self.calc_data_input_annual()
        self.calc_bx_imports_1()
        self.calc_bx_imports_2()
        self.calc_bx_imports_3()
        self.calc_bx_imports_4()
        self.calc_bx_imports_5()
        self.calc_bx_imports_6()
        self.calc_bx_imports_7()
        self.calc_bx_imports_8()
        self.calc_bx_imports_9()
        self.calc_ref_aa_productions()
        self.calc_Changjiang_LME_Daily()
        self.calc_platts_vs_cmaax()
        
        
        self.calc_data_input_monthly_1()
        self.calc_data_input_monthly_2()
        self.calc_data_input_monthly_3()
        self.calc_data_input_monthly_4()
        self.calc_data_input_monthly_5()
        self.calc_data_input_monthly_6()
        self.calc_data_input_monthly_7()
        self.calc_mines_bx_production_1()
        self.calc_bauxite_sources_1()
        self.calc_bauxite_sources_2()
        self.calc_bauxite_sources_3()
        self.calc_mines_bx_production_2()
        self.calc_global_bx_quarterly_1()
        
        self.calc_cm_global_prd_1()
        
        self.calc_aa_engines()
        self.calc_data_input_monthly_8()
        self.calc_data_monthly_1()
        self.calc_data_monthly_2()
        self.calc_data_monthly_3()
        self.calc_data_monthly_4()
        
        self.calc_data_charts_1()
        self.calc_special_charts()
        
    def calc_data_input_annual(self):
        self.data_input_annual['Year'] = self.dataInputAnnual['Year']
        self.data_input_annual['Alumina Capacity'] = self.dataInputAnnual['Alumina Capacity']
        self.data_input_annual['Alumina Production'] = self.dataInputAnnual['Alumina Production']
        self.data_input_annual['Alumina Utilisation'] =self.data_input_annual['Alumina Production']/self.data_input_annual['Alumina Capacity']
        self.data_input_annual['Aluminium Capacity'] = self.dataInputAnnual['Aluminium Capacity']
        self.data_input_annual['Aluminium Production'] = self.dataInputAnnual['Aluminium Production']
        self.data_input_annual['Aluminium Utilisation'] = self.data_input_annual['Aluminium Production'] /self.data_input_annual['Aluminium Capacity']
    
    def calc_bx_imports_1(self):
        for col in self.bxImports_tab1.columns[3:]:
            month= int(col[:3])
            year = int(col[-4:])
            for row in self.bxImports_tab1.index:
                value = 0
                if pd.isna(self.bxImports_tab1.loc[row,'Regency']):
                    value = self.bxImports_rawData.loc[(self.bxImports_rawData['Country of Origin']==self.bxImports_tab1.loc[row,'Country']) & (self.bxImports_rawData['Year']==year) & (self.bxImports_rawData['Month']==month), 'Tonnage'].astype(float).sum()
                else:
                    value = self.bxImports_rawData.loc[(self.bxImports_rawData['Country of Origin']==self.bxImports_tab1.loc[row,'Country']) & (self.bxImports_rawData['Regency']==self.bxImports_tab1.loc[row,'Regency']) &(self.bxImports_rawData['Year']==year)& (self.bxImports_rawData['Month']==month), 'Tonnage'].astype(float).sum()
                self.bxImports_tab1.loc[row, col] = value
        self.bxImports_tab1.loc[4,' 9 - 2018'] = 0
        
    #SMB split tonnes
    def calc_bx_imports_2(self):
        self.bxImports_tab2.loc[2,' 1 - 2016'] =4/12/11
        self.bxImports_tab2.loc[1,' 1 - 2016'] =self.bxImports_tab1.loc[11,' 1 - 2016'] /1000000  - self.bxImports_tab2.loc[2,' 1 - 2016']
        self.bxImports_tab2.loc[1,' 12 - 2016'] =9.1/12
        
        cols = self.bxImports_tab2.columns[20:30].to_list()
        cols2 = self.bxImports_tab2.columns[19:29].to_list()
        for i in range(len(cols)):
            self.bxImports_tab2.loc[0,cols[i]] =self.bxImports_tab2.loc[0,cols2[i]] +1.25/11
        cols = self.bxImports_tab2.columns[19:30].to_list()
        cols2 = self.bxImports_tab2.columns[18:29].to_list()
        for i in range(len(cols)):
            self.bxImports_tab2.loc[5,cols[i]] =self.bxImports_tab2.loc[5,cols2[i]] +  8/12/11
        cols = self.bxImports_tab2.columns[21:30].to_list()
        cols2 = self.bxImports_tab2.columns[20:29].to_list()
        for i in range(len(cols)):
            self.bxImports_tab2.loc[4,cols[i]] =self.bxImports_tab2.loc[4,cols2[i]] +  5/12/10
        cols = self.bxImports_tab2.columns[7:16].to_list()
        cols2 = self.bxImports_tab2.columns[6:15].to_list()
        for i in range(len(cols)):
            self.bxImports_tab2.loc[1,cols[i]] =self.bxImports_tab2.loc[1,cols2[i]] +  (self.bxImports_tab2.loc[1,' 12 - 2016']-self.bxImports_tab2.loc[1,' 1 - 2016'])/11
            self.bxImports_tab2.loc[2,cols[i]] =self.bxImports_tab2.loc[2,cols2[i]] +  4/12/11
        self.bxImports_tab2.loc[1,' 11 - 2016'] = self.bxImports_tab2.loc[1,' 10 - 2016'] + (self.bxImports_tab2.loc[1,' 12 - 2016']-self.bxImports_tab2.loc[1,' 1 - 2016'])/11
        
    def calc_bx_imports_3(self):
        for col in self.bxImports_tab3.columns[3:]:
            for i in range(11):
                self.bxImports_tab3.loc[i, col] = self.bxImports_tab1.loc[i, col].copy()
            for i in range(11,18):
                self.bxImports_tab3.loc[i, col] = self.bxImports_tab1.loc[11, col]*self.bxImports_tab2.loc[i-11, col]/self.bxImports_tab2[col].sum()
            
            for i in range(18, len(self.bxImports_tab3)):
                self.bxImports_tab3.loc[i, col] = self.bxImports_tab1.loc[i-6, col].copy()
                
    def calc_bx_imports_4(self):
        for col in self.bxImports_tab3.columns[3:]:
            for row in self.bxImports_tab4.index:
                self.bxImports_tab4.loc[row,col] =self.bxImports_tab1.loc[self.bxImports_tab1['Country']==self.bxImports_tab4.loc[row,'Country'], col].sum()
            
    def calc_bx_imports_5(self):
        d = self.bxImports_tab4['Country'].to_list()
        for row in self.bxImports_tab5.index:
            self.bxImports_tab5.loc[row,'Index'] = d.index(self.bxImports_tab5.loc[row,'Country'])+1
        for col in self.bxImports_tab3.columns[3:]:
            for row in self.bxImports_tab5.index:
                value = 0
                l = self.bxImports_tab4[col].to_list()
                if l[self.bxImports_tab5.loc[row,'Index']-1] <=0:
                    value = 0
                else:
                    value = self.bxImports_tab3.loc[row,col]/l[self.bxImports_tab5.loc[row,'Index']-1]
                self.bxImports_tab5.loc[row,col] = value
                
    def calc_bx_imports_6(self):
        9+4
    def calc_bx_imports_7(self):
        for col in self.bxImports_tab3.columns[3:]:
            for row in self.bxImports_tab7.index:
                value =self.bxImports_tab5.loc[self.bxImports_tab5['Country']==self.bxImports_tab7.loc[row,'Country'], col] * self.bxImports_tab6.loc[self.bxImports_tab6['Country']==self.bxImports_tab7.loc[row,'Country'], col]
                self.bxImports_tab7.loc[row,col] = value.sum()
    
    def calc_bx_imports_8(self):
        coll =self.bxImports_tab8.columns[3]
        for row in self.bxImports_tab8.index:
            value = self.bxImports_tab5.loc[self.bxImports_tab5['Country']==self.bxImports_tab8.loc[row,'Country'], coll] * self.bxImports_tab6.loc[self.bxImports_tab6['Country']==self.bxImports_tab8.loc[row,'Country'], coll]
            self.bxImports_tab8.loc[row, coll] = value.sum()
        cols1 = self.bxImports_tab8.columns[4:].to_list()
        cols2 = self.bxImports_tab8.columns[3:].to_list()
        for i in range(len(cols1)):
            for row in self.bxImports_tab8.index:
                self.bxImports_tab8.loc[row, cols1[i]] = self.bxImports_tab7.loc[row, cols1[i]] if self.bxImports_tab7.loc[row, cols1[i]]>0 else self.bxImports_tab8.loc[row, cols2[i]]
                
    def calc_bx_imports_9(self):   
        self.bxImports_tab9['Country'] = ['Australia','Australia','ctz','Australia','Australia']
        self.bxImports_tab9['Unnamed'] = ['LT','HT','','LT','HT']
        
        self.bxImports_tab3_copy['Country'] = self.bxImports_tab6['Country']
        self.bxImports_tab3_copy['Regency'] = self.bxImports_tab6['Regency']
        self.bxImports_tab3_copy['Unnamed'] = self.bxImports_tab6['Unnamed: 2']
        for col in self.bxImports_tab3.columns[3:]:
            self.bxImports_tab3_copy[col] = self.bxImports_tab3[col]
        for col in self.bxImports_tab3.columns[3:]:
            for row in range(2):
                self.bxImports_tab9.at[row, col] = self.bxImports_tab3_copy.loc[(self.bxImports_tab3_copy['Unnamed']==self.bxImports_tab9.loc[row,'Unnamed']) &(self.bxImports_tab3_copy['Country']==self.bxImports_tab9.loc[row,'Country'] ), str(col)].sum()
            self.bxImports_tab9.at[2, col] = self.bxImports_tab9.loc[:2,col].sum() - self.bxImports_tab4.loc[0,col]
            self.bxImports_tab9.at[3, col] = self.bxImports_tab9.loc[0,col]/self.bxImports_tab4.loc[0,col]  if(self.bxImports_tab4.loc[0,col])>0 else 0
            self.bxImports_tab9.at[4, col] = self.bxImports_tab9.loc[1,col]/self.bxImports_tab4.loc[0,col]  if(self.bxImports_tab4.loc[0,col])>0 else 0
        
    def calc_ref_aa_productions(self):
        self.refAAproductions['Key'] = self.ref_AA_productions['Key']
        self.refAAproductions['Refinery Name'] = self.ref_AA_productions['Refinery Name']
        self.refAAproductions['Country'] = self.ref_AA_productions['Country']
        d = {self.otherInput_bx_tab1['Country'][x]:self.otherInput_bx_tab1['IAI Wiorld Al Region'][x] for x in self.otherInput_bx_tab1.index}
        l = []
        for country in self.refAAproductions['Country'].to_list():
            try:
                l.append(d[country])
            except:
                l.append(0)
        self.refAAproductions['Region'] = l
        self.refAAproductions['Q1 2017'] = self.ref_AA_productions['Q1 2017']
        self.refAAproductions['Q2 2017'] = self.ref_AA_productions['Q2 2017']
        self.refAAproductions['Q3 2017'] = self.ref_AA_productions['Q3 2017']
        self.refAAproductions['Q4 2017'] = self.ref_AA_productions['Q4 2017']
        self.refAAproductions['2017'] = self.refAAproductions[['Q1 2017','Q2 2017','Q3 2017','Q4 2017']].sum(axis=1)
        
        self.refAAproductions['Q1 2018'] = self.ref_AA_productions['Q1 2018']
        self.refAAproductions['Q2 2018'] = self.ref_AA_productions['Q2 2018']
        self.refAAproductions['Q3 2018'] = self.ref_AA_productions['Q3 2018']
        self.refAAproductions['Q4 2018'] = self.ref_AA_productions['Q4 2018']
        self.refAAproductions['2018'] = self.refAAproductions[['Q1 2018','Q2 2018','Q3 2018','Q4 2018']].sum(axis=1)
       
        self.refAAproductions['Q1 2019'] = self.ref_AA_productions['Q1 2019']
        self.refAAproductions['Q2 2019'] = self.ref_AA_productions['Q2 2019']
        self.refAAproductions['Q3 2019'] = self.ref_AA_productions['Q3 2019']
        self.refAAproductions['Q4 2019'] = self.ref_AA_productions['Q4 2019']
        self.refAAproductions['2019'] = self.refAAproductions[['Q1 2019','Q2 2019','Q3 2019','Q4 2019']].sum(axis=1)
       
        self.refAAproductions['Q1 2020'] = self.ref_AA_productions['Q1 2020']
        self.refAAproductions['Q2 2020'] = self.ref_AA_productions['Q2 2020']
        self.refAAproductions['Q3 2020'] = self.ref_AA_productions['Q3 2020']
        self.refAAproductions['Q4 2020'] = self.ref_AA_productions['Q4 2020']
        self.refAAproductions['2020'] = self.refAAproductions[['Q1 2020','Q2 2020','Q3 2020','Q4 2020']].sum(axis=1)
       
    def calc_Changjiang_LME_Daily(self):
        date_new = self.Changjiang_LME_tab2['Date 1'][0].to_pydatetime()
        self.changjiangLME['Date'] = [( date_new + timedelta(days=i)).strftime("%d/%m/%Y") for i in range(self.changjiangLME.shape[0]) ]
        
        d = {self.Changjiang_LME_tab2['Date 3'][x]:self.Changjiang_LME_tab2['USD:CNY'][x] for x in self.Changjiang_LME_tab2.index}
        l = []
        for date in self.changjiangLME['Date']:
            try:
                l.append(d[datetime.strptime(date,"%d/%m/%Y" )])
            except:
                l.append(0)
        self.changjiangLME['USD:CNY'] = l 
        
        ref_col = self.Changjiang_LME_tab2[['Cash Buyer','Cash Seller & Settlement']].sum(axis = 1)/2
        d2 = {self.Changjiang_LME_tab2['Date 2'][x]:ref_col[x] for x in self.Changjiang_LME_tab2.index}
        l2 = []
        for date in self.changjiangLME['Date']:
            try:
                l2.append(d2[datetime.strptime(date,"%d/%m/%Y" )])
            except:
                l2.append(0)
        self.changjiangLME['LME'] = l2
        self.changjiangLME['LME Cash'] = self.changjiangLME[['USD:CNY','LME']].product(axis=1)

        d3 = {self.Changjiang_LME_tab2['Date 1'][x]:self.Changjiang_LME_tab2['Average'][x] for x in self.Changjiang_LME_tab2.index}
        l3 = []
        for date in self.changjiangLME['Date']:
            try:
                l3.append(d3[datetime.strptime(date,"%d/%m/%Y" )])
            except:
                l3.append(0)
        d4 = {self.Changjiang_LME_tab1['Date'][x]:self.Changjiang_LME_tab1['Factor'][x] for x in range(self.Changjiang_LME_tab1.shape[0])}
        l4 = []
        for date in self.changjiangLME['Date']:
            k = datetime.strptime(date,"%d/%m/%Y" )
            l4.append(  d4.get(k) or d4[ min(d4.keys(), key = lambda key: abs(key-k))]   )
        self.changjiangLME['Changjiang Spot Cargo VAT excl.'] = [l3[_]/l4[_] for _ in range(self.changjiangLME.shape[0])]
        
    def calc_platts_vs_cmaax(self):
        self.plattsVsCmaax['Month'] = [(date.to_pydatetime().strftime('%d/%m/%Y')) for date in self.platts_vs_cmaax['Month']]
        self.plattsVsCmaax['Date'] = [(date.to_pydatetime().strftime('%d/%m/%Y')) for date in self.platts_vs_cmaax['Date']]
        
        #excel_date
        d =  [ [int(vv[-4:]), int(vv[3:5]), int(vv[:2])] for vv in  self.plattsVsCmaax['Date']]
        res = [ self.excel_date( d[ind][0], d[ind][1], d[ind][2]  ) for ind in range(len(d))]
        value = self.platts_vs_cmaax['Australia (FOB WA)'] + self.platts_vs_cmaax['Frieght']
        self.plattsVsCmaax['Date Value'] = res
        self.plattsVsCmaax['VAT'] =[value[_]*self.platts_vs_cmaax.loc[1,'VAT'] if res[_]>43221 else value[_]*self.platts_vs_cmaax.loc[0,'VAT']  for _ in self.platts_vs_cmaax.index]
        for row in range(2):
            self.plattsVsCmaax.loc[row, 'VAT'] = (self.platts_vs_cmaax.loc[row,'Australia (FOB WA)'] + self.platts_vs_cmaax.loc[row,'Frieght'] ) * self.platts_vs_cmaax.loc[0,'VAT']
        for row in range(2009, self.plattsVsCmaax.shape[0]):
            self.plattsVsCmaax.loc[row, 'VAT'] = (self.platts_vs_cmaax.loc[row,'Australia (FOB WA)'] + self.platts_vs_cmaax.loc[row,'Frieght'] ) * 0.13
         
        for row in range(205):
            self.plattsVsCmaax.at[row, 'Port Handling'] = self.platts_vs_cmaax.loc[0,'Port Handling (RMB/t)']/self.platts_vs_cmaax.loc[row, 'USD:RMB']
        for row in range(205,285):
            self.plattsVsCmaax.at[row, 'Port Handling'] = self.platts_vs_cmaax.loc[1,'Port Handling (RMB/t)']/self.platts_vs_cmaax.loc[row, 'USD:RMB']
        for row in range(285,1465):
            self.plattsVsCmaax.at[row, 'Port Handling'] = self.platts_vs_cmaax.loc[2,'Port Handling (RMB/t)']/self.platts_vs_cmaax.loc[row, 'USD:RMB']
        for row in range(1465,1951):
            self.plattsVsCmaax.at[row, 'Port Handling'] = self.platts_vs_cmaax.loc[3,'Port Handling (RMB/t)']/self.platts_vs_cmaax.loc[row, 'USD:RMB']
        for row in range(1951, self.plattsVsCmaax.shape[0]):
            self.plattsVsCmaax.at[row, 'Port Handling'] = self.platts_vs_cmaax.loc[4,'Port Handling (RMB/t)']/self.platts_vs_cmaax.loc[row, 'USD:RMB']
        
        self.plattsVsCmaax['Australia(Real)'] = self.platts_vs_cmaax[['Australia (FOB WA)','Frieght']].astype(float).sum(axis=1)+ self.plattsVsCmaax[['VAT','Port Handling']].astype(float).sum(axis=1)
        self.plattsVsCmaax['Import Premium'] = self.plattsVsCmaax['Australia(Real)'] - self.platts_vs_cmaax['CMAAX']
        
    def calc_data_input_monthly_1(self):
        table = self.dataInputMontly_alumina_montly_supply.copy()
        for col in ['Date','Year', 'Quarter','Australia','India','Vietnam','Other','Total Import']:
            table[col] = self.data_inp_montly_tab1[col]
        for row in range(120, self.data_inp_montly_tab1.shape[0]):
            table.loc[row,'Total Import'] = table.loc[row,['Australia','India','Vietnam','Other']].sum()
        for col in ['Production, Mt Aa','Production - Shandong','Production - Henan','Production - Shanxi','Production - Guizhou','Production - Guangxi','Production - Chongqing','Production - Other']:
            table[col] = self.data_inp_montly_tab1[col]
        table['Total Production'] = table[['Production - Shandong','Production - Henan','Production - Shanxi','Production - Guizhou','Production - Guangxi','Production - Chongqing','Production - Other']].sum(axis=1)
        self.dataInputMontly_alumina_montly_supply = table
        
    def calc_data_input_monthly_2(self):
        table = self.dataInputMontly_aluminium_montly_supply.copy()
        for col in ['Date','Year', 'Quarter']:
            table[col] = self.data_inp_montly_tab1[col]
        for col in self.data_inp_montly_tab2.columns[:26]:
            table[col] = self.data_inp_montly_tab2[col]
        for row in range(120, self.data_inp_montly_tab2.shape[0]):
           table.loc[row,'Total'] = table.loc[row,['Henan','Shandong','Inner Mongolia','Gansu','Qinghai', 'Shanxi','Guizhou', 'Yunnan', 'Sichuan', 'Guangxi', 'Ningxia', 'Shaanxi','Hubei', 'Jiangsu', 'Zhejiang', 'Chongqing', 'Fujian', 'Hunan','Liaoning', 'Xinjiang', 'Heilongjiang', 'Hebei', 'Jiangxi', 'Jilin']].sum()
        self.dataInputMontly_aluminium_montly_supply =table

    def calc_data_input_monthly_3(self):
        table =self.dataInputMontly_bauxite_imports1.copy()
        for col in ['Date','Year', 'Quarter']:
            table[col] = self.data_inp_montly_tab1[col]
        for col in ['Australia', 'India', 'Indonesia', 'Malaysia','Guinea', 'Other']:
            table[col] = self.data_inp_montly_bax1[col]
        table['Total'] = table[['Australia', 'India', 'Indonesia', 'Malaysia','Guinea', 'Other']].sum(axis=1)
        self.dataInputMontly_bauxite_imports1 = table
        
        #two
        table =self.dataInputMontly_bauxite_imports2.copy()
        for col in ['Date','Year', 'Quarter']:
            table[col] = self.data_inp_montly_tab1[col]
        for col in ['Chalco-Shandong', 'Chalco-Zhengzhou','Chalco-Zhongzhou', 'Chiping Xinfa', 'Nanshan', 'Weiqiao', 'Lubei','Jinjiang', 'Xinwang', 'Yunnan Aluminium', 'SPIC', 'Other']:
            table[col] = self.data_inp_montly_bax2[col]
        table['Total'] = table[['Chalco-Shandong', 'Chalco-Zhengzhou','Chalco-Zhongzhou', 'Chiping Xinfa', 'Nanshan', 'Weiqiao', 'Lubei','Jinjiang', 'Xinwang', 'Yunnan Aluminium', 'SPIC', 'Other']].sum(axis=1)
        self.dataInputMontly_bauxite_imports2 = table
        
        #three
        table =self.dataInputMontly_bauxite_imports3.copy()
        for col in ['Date','Year', 'Quarter']:
            table[col] = self.data_inp_montly_tab1[col]
        #for col in 
        self.dataInputMontly_bauxite_imports3 = table
        #come back here okay
        
        #four
        table =self.dataInputMontly_bauxite_imports4.copy()
        for col in ['Date','Year', 'Quarter']:
            table[col] = self.data_inp_montly_tab1[col]
        for col in self.data_inp_montly_bax4.columns[1:]:
            table[col] =self.data_inp_montly_bax4[col]
        table['Total'] = table[['Australia', 'Brazil','Dominican Republic','Fiji','Ghana', 'Guinea', 'Guyana','India','Indonesia','Jamaica', 'Malaysia', 'Montenegro','Sierra Leone', 'Solomon Islands', 'Turkey', 'Vietnam', 'Laos', 'Saudi Arabia', 'Thailand', 'Other']].sum(axis=1)
        self.dataInputMontly_bauxite_imports4 = table
        
        #five
        table =self.dataInputMontly_bauxite_imports5.copy()
        for col in ['Date','Year', 'Quarter']:
            table[col] = self.data_inp_montly_tab1[col]
        for col in self.data_inp_montly_bax5.columns[1:]:
            table[col] = self.data_inp_montly_bax5[col]
        self.dataInputMontly_bauxite_imports5 = table

    def calc_data_input_monthly_4(self):
        for col in ['Date','Year', 'Quarter']:
            self.dataInputMontly_input_bar[col] = self.data_inp_montly_tab1[col]
        for col in self.data_inp_montly_inpBAR.columns:
            self.dataInputMontly_input_bar[col] = self.data_inp_montly_inpBAR[col]
        for country in self.dataInputMontly_input_bar.columns[4:20]:
            for row in range(252, self.dataInputMontly_input_bar.shape[0]):
                self.dataInputMontly_input_bar.loc[row, country] =self.func_1(row,country)
                self.dataInputMontly_input_bar.loc[row, 'Australia LT'] =self.func_2(row,'LT')
                self.dataInputMontly_input_bar.loc[row, 'Australia HT'] =self.func_2(row,'HT')
            
    def calc_data_input_monthly_5(self):
        tab = self.BAR_tab.copy()
        
        cols =self.data_inp_montly_baxStyle.columns[1:]
        for row in range(240,self.dataInputMontly_BAR.shape[0] ):
            tab['L1'] = self.data_inp_montly_baxStyle.loc[row,cols].copy()
            tab['L2'] = self.dataInputMontly_bauxite_imports4.loc[row,cols].copy().fillna(0)
            tab['L3'] = self.dataInputMontly_input_bar.loc[row,cols].copy().fillna(0)
           
            value1 = tab.loc[tab['L1']=='HT',['L2','L3']].product(axis=1).sum() 
            value1 = (value1) + float(self.dataInputMontly_bauxite_imports4.loc[row,'Australia']) * float(self.dataInputMontly_input_bar.loc[row,'Australia']) * float(self.dataInputMontly_input_bar.loc[row,'Australia HT'])
            value2 = tab.loc[tab['L1']=='HT','L2'].sum()
            value2 = (value2) + float(self.dataInputMontly_input_bar.loc[row,'Australia HT']) *float(self.dataInputMontly_bauxite_imports4.loc[row,'Australia']) 
            
            val1 =   tab.loc[tab['L1']=='LT',['L2','L3']].product(axis=1).sum() 
            val1 = (val1) + float(self.dataInputMontly_bauxite_imports4.loc[row,'Australia']) * float(self.dataInputMontly_input_bar.loc[row,'Australia']) * float(self.dataInputMontly_input_bar.loc[row,'Australia LT'])
            val2 =   tab.loc[tab['L1']=='LT','L2'].sum() 
            val2 = (val2) + float(self.dataInputMontly_input_bar.loc[row,'Australia LT']) *float(self.dataInputMontly_bauxite_imports4.loc[row,'Australia'] )
            
            value3 = tab[['L2','L3']].product(axis=1).sum()
            value3 = float(value3)/self.dataInputMontly_bauxite_imports4.loc[row, 'Total']
            self.dataInputMontly_BAR.loc[row,'HT'] = value1/value2
            self.dataInputMontly_BAR.loc[row,'LT'] = val1/val2
            self.dataInputMontly_BAR.loc[row,'Total Combined'] = value3
            
    def calc_data_input_monthly_6(self):
        cols = self.dataInputMontly_CmPortBx.columns[:-2]
        for row in range(230,274):
            self.dataInputMontly_CmPortBx.loc[row, 'CM Port, Million DMT'] =self.dataInputMontly_CmPortBx.loc[row, cols].sum() * 0.9
        for row in range(284, self.dataInputMontly_CmPortBx.shape[0]):
            self.dataInputMontly_CmPortBx.loc[row, 'CM Port, Million DMT'] =self.dataInputMontly_CmPortBx.loc[row, cols].sum() * 0.9
        
    def calc_data_input_monthly_7(self):
        cols = self.dataInputMontly_global_alumina.columns
        self.dataInputMontly_global_alumina['Total (ex China)'] = self.dataInputMontly_global_alumina[cols].sum(axis=1)
        
    def calc_data_input_monthly_8(self):
        for row in range(264, len(self.dataInputMontly_inland1)-1):
            self.dataInputMontly_inland1.at[row, 'Import Bx consumed'] = self.AAproductions.loc[748, str(row-196)]
            self.dataInputMontly_inland1.at[row, 'Henan']= self.AAproductions.loc[749, str(row-196)]
            self.dataInputMontly_inland1.at[row, 'Shanxi'] = self.AAproductions.loc[750, str(row-196)]
            self.dataInputMontly_inland1.at[row, 'Guizhou'] = self.AAproductions.loc[751, str(row-196)]
            self.dataInputMontly_inland1.at[row, 'Guangxi'] = self.AAproductions.loc[752, str(row-196)]
            self.dataInputMontly_inland1.at[row, 'Chongqing'] = self.AAproductions.loc[753, str(row-196)]
            self.dataInputMontly_inland1.at[row, 'Other'] = self.AAproductions.loc[754, str(row-196)]
            
            self.dataInputMontly_inland1.at[row, ''] = ''
            
            self.dataInputMontly_inland1.at[row, 'Alumina - Henan'] = self.AAproductions.loc[750, str(row-196)]/self.dataInputMontly_BAR.loc[row,'HT']
            self.dataInputMontly_inland1.at[row, 'Alumina - Shanxi'] = self.AAproductions.loc[751, str(row-196)]/self.dataInputMontly_BAR.loc[row,'HT']
            self.dataInputMontly_inland1.at[row, 'Alumina - Guizhou'] = self.AAproductions.loc[752, str(row-196)]/self.dataInputMontly_BAR.loc[row,'HT']
            self.dataInputMontly_inland1.at[row, 'Alumina - Guangxi'] = self.AAproductions.loc[753, str(row-196)]/self.dataInputMontly_BAR.loc[row,'HT']
            self.dataInputMontly_inland1.at[row, 'Alumina - Chongqing'] = self.AAproductions.loc[754, str(row-196)]/self.dataInputMontly_BAR.loc[row,'HT']
            self.dataInputMontly_inland1.at[row, 'Alumina - Other'] = self.AAproductions.loc[755, str(row-196)]/self.dataInputMontly_BAR.loc[row,'HT']
    
    #data montly
    def calc_data_monthly_1(self):
        for col in ['Date','Year', 'Quarter']:
            self.dataMonthly_alumina_monthly[col] = self.data_inp_montly_tab1[col]
        self.dataMonthly_alumina_monthly['Production'] =  self.dataInputMontly_alumina_montly_supply['Total Production']
        self.dataMonthly_alumina_monthly['6 Month Moving Average'] = self.func_6months( self.dataMonthly_alumina_monthly['Production'] )
        self.dataMonthly_alumina_monthly['Imports'] = self.dataInputMontly_alumina_montly_supply['Total Import']/1000
        for col in self.dataInputMontly_alumina_montly_supply.columns[8:]:
            self.dataMonthly_alumina_monthly[col] = self.dataInputMontly_alumina_montly_supply[col]
            
    def calc_data_monthly_2(self):
        for col in self.dataInputMontly_aluminium_montly_supply.columns[:12]:
            self.dataMonthly_aluminium_supply[col] = self.dataInputMontly_aluminium_montly_supply[col]
        self.dataMonthly_aluminium_supply['Xinjiang'] = self.dataInputMontly_aluminium_montly_supply['Xinjiang']
        cols = ['Sichuan', 'Guangxi', 'Ningxia', 'Shaanxi', 'Hubei', 'Jiangsu','Zhejiang', 'Chongqing', 'Fujian', 'Hunan', 'Liaoning','Heilongjiang', 'Hebei', 'Jiangxi', 'Jilin']
        self.dataMonthly_aluminium_supply['Other'] = self.dataInputMontly_aluminium_montly_supply[cols].sum(axis=1)
        cols = ['Henan', 'Shandong', 'Inner Mongolia', 'Gansu', 'Qinghai', 'Shanxi','Guizhou', 'Yunnan', 'Xinjiang','Other']
        self.dataMonthly_aluminium_supply['Total'] =self.dataMonthly_aluminium_supply[cols].sum(axis=1)
        
        self.dataMonthly_aluminium_supply['Production'] = self.dataInputMontly_aluminium_montly_supply['Total']/1000000
        self.dataMonthly_aluminium_supply['6 Month Moving Average'] = self.func_6months( self.dataMonthly_aluminium_supply['Production'] )
        self.dataMonthly_aluminium_supply['China'] = self.dataMonthly_alumina_monthly['Production']
        self.dataMonthly_aluminium_supply['Imports'] = self.dataMonthly_alumina_monthly['Imports']
        self.dataMonthly_aluminium_supply['Demand from Aluminium'] = self.dataMonthly_aluminium_supply['Production']*1.94
        self.dataMonthly_aluminium_supply[''] = ''
        
        self.dataMonthly_aluminium_supply['Exports'] = self.data_inp_montly_tab1['Export, kt Aa - Total']/1000
        self.dataMonthly_aluminium_supply['Balance'] = (self.dataMonthly_aluminium_supply['China']+self.dataMonthly_aluminium_supply['Imports']+self.dataMonthly_aluminium_supply['Demand from Aluminium'])-self.dataMonthly_aluminium_supply['Exports']
        
        self.dataMonthly_aluminium_supply[''] = ''
        
        self.dataMonthly_aluminium_supply['% of Demand'] =self.dataMonthly_aluminium_supply['China']/self.dataMonthly_aluminium_supply['Demand from Aluminium']
        self.dataMonthly_aluminium_supply['Self Sufficciency'] = self.dataMonthly_aluminium_supply['% of Demand']
        
        self.dataMonthly_aluminium_supply[''] = ''
        
        self.dataMonthly_aluminium_supply['Output'] =self.dataMonthly_aluminium_supply['Production']
        self.dataMonthly_aluminium_supply['Net Import'] = self.data_inp_montly_tab2['Net Import, Mt Al']
        
    def calc_data_monthly_3(self):
        for col in self.dataInputMontly_bauxite_imports1.columns:
            self.dataMonthly_bauxite_imps1[col] = self.dataInputMontly_bauxite_imports1[col]
        
        for col in self.dataInputMontly_bauxite_imports2.columns:
            self.dataMonthly_bauxite_imps2[col] = self.dataInputMontly_bauxite_imports2[col]
        
        for col in self.dataInputMontly_bauxite_imports3.columns:
            self.dataMonthly_bauxite_imps3[col] = self.dataInputMontly_bauxite_imports3[col]
       
    def calc_data_monthly_4(self):
        for col in ['Date','Year', 'Quarter']:
            self.dataMonthly_consumpt[col] = self.data_inp_montly_tab1[col]
        self.dataMonthly_consumpt['Consump'] = self.data_monthly_inp_comspt['Consumption of Bauxite, Mt']
        self.dataMonthly_consumpt['Imported Bauxite'] = 0
        for row in range(120, 201 ): #234
            self.dataMonthly_consumpt.loc[row, 'Imported Bauxite'] = (self.dataMonthly_alumina_monthly.loc[row,'Production - Shandong']- self.dataMonthly_alumina_monthly.loc[row,'Production - Shandong'] * self.dataMonthly_consumpt.loc[row,'Consump'])*3
        for row in range(201, 234):
            self.dataMonthly_consumpt.loc[row, 'Imported Bauxite'] = ((self.dataMonthly_alumina_monthly.loc[row,'Production - Shandong']- self.dataMonthly_alumina_monthly.loc[row,'Production - Shandong'] * self.dataMonthly_consumpt.loc[row,'Consump'])+0.0383333)*3                 
        self.dataMonthly_consumpt['Domestic Bauxite'] = 1
        self.dataMonthly_consumpt['Bauxite Requirement'] = self.dataMonthly_consumpt['Imported Bauxite']+self.dataMonthly_consumpt['Domestic Bauxite']
        self.dataMonthly_consumpt['% Bauxite Consump'] =self.dataMonthly_consumpt['Domestic Bauxite']/self.dataMonthly_consumpt['Bauxite Requirement']
        #self.data_monthly_inp_comspt
    
    def calc_data_charts_1(self):
        for col in self.dataMonthly_bauxite_imps1.columns:
            self.dataCharts_1[col] = self.dataMonthly_bauxite_imps1[col]
        for col in self.dataMonthly_bauxite_imps3.columns:
            self.dataCharts_1a[col] = self.dataMonthly_bauxite_imps3[col]
        6+7
        self.dataCharts_2['Refineries'] =2
        self.dataCharts_2['Port'] = self.dataInputMontly_CmPortBx['CM Port, Million DMT']
        
        
        self.dataCharts_3['Imported Bauxite'] =self.dataMonthly_consumpt['Imported Bauxite']
        self.dataCharts_3['Domestic Bauxite'] =self.dataMonthly_consumpt['Domestic Bauxite']
        self.dataCharts_3['% Bauxite Consumpt']=self.dataMonthly_consumpt['% Bauxite Consump']
        
        self.dataCharts_4['Weiqiao'] = self.dataMonthly_bauxite_imps2['Weiqiao']
        self.dataCharts_4['Chiping Xinfa']= self.dataMonthly_bauxite_imps2['Chiping Xinfa']
        self.dataCharts_4['Chalco-Shandong']= self.dataMonthly_bauxite_imps2['Chalco-Shandong']
        self.dataCharts_4['Nanshan']= self.dataMonthly_bauxite_imps2['Nanshan']
        self.dataCharts_4['Chalco-Zhengzhou*']= self.dataMonthly_bauxite_imps2['Chalco-Zhengzhou']
        self.dataCharts_4['Other']= self.dataMonthly_bauxite_imps2['Other']
        self.dataCharts_4['Lubei']= self.dataMonthly_bauxite_imps2['Lubei']
        self.dataCharts_4['Xinwang*']= self.dataMonthly_bauxite_imps2['Xinwang']
        self.dataCharts_4['Chalco-Zhongzhou*']= self.dataMonthly_bauxite_imps2['Chalco-Zhongzhou']
        self.dataCharts_4['Jinjiang*']= self.dataMonthly_bauxite_imps2['Jinjiang']
        self.dataCharts_4['Yunnan Aluminium*']= self.dataMonthly_bauxite_imps2['Yunnan Aluminium']
        self.dataCharts_4['SPIC*']= self.dataMonthly_bauxite_imps2['SPIC']
        
        for col in  self.dataMonthly_alumina_monthly.columns[7:]:
            self.dataCharts_5[col] = self.dataMonthly_alumina_monthly[col]
        self.dataCharts_5[''] = ''
        self.dataCharts_5['Chalco Spot'] = self.data_inp_montly_tab3['Chalco Spot']
        self.dataCharts_5['CMAAX'] = self.data_inp_montly_tab3['CMAAX (VAT incl.)']
        #self.data_inp_montly_tab3
        
        self.dataCharts_6[''] = ''
        self.dataCharts_6['Australia'] = self.dataInputMontly_alumina_montly_supply['Australia']
        self.dataCharts_6['India'] = self.dataInputMontly_alumina_montly_supply['India']
        self.dataCharts_6['Vietnam'] = self.dataInputMontly_alumina_montly_supply['Vietnam']
        self.dataCharts_6['Other'] = self.dataInputMontly_alumina_montly_supply['Other']
        self.dataCharts_6['Australia  Prices'] = 00
        
        for col in self.dataMonthly_aluminium_supply.columns[4:15]:
            self.dataCharts_7[col] = self.dataMonthly_aluminium_supply[col]
        self.dataCharts_7[''] =''
        self.dataCharts_7['% of Demand'] = self.dataMonthly_aluminium_supply['Self Sufficciency']
        
        self.dataCharts_8['Net Import'] = self.dataMonthly_aluminium_supply['Net Import']
        self.dataCharts_8[''] =''
        self.dataCharts_8['Changjiang Spot Cargo'] = self.data_inp_montly_tab3['Changjiang Spot Cargo']
        self.dataCharts_8['LME (RMB)'] = 0
        self.dataCharts_8['FX Rate'] = self.data_inp_montly_exchange_rate
        #self.data_inp_montly_tab3
        #self.data_inp_montly_exchange_rate
        
        for row in range(204, len(self.dataCharts_10)):
            self.dataCharts_10.at[row,'HT Bx'] = self.AAproductions.loc[1052,str(row-196)]
            self.dataCharts_10.at[row,'LT Bx'] = self.AAproductions.loc[1053,str(row-196)]
            self.dataCharts_10.at[row,'TOTAL'] = self.AAproductions.loc[1054,str(row-196)]
        
        self.dataCharts__10['Chart Year'] = self.data_input_annual['Year']
        self.dataCharts__10['Alumina Capacity'] =self.data_input_annual['Alumina Capacity']
        self.dataCharts__10['Alumina Production'] =self.data_input_annual['Alumina Production']
        self.dataCharts__10['Utilisation'] =self.data_input_annual['Alumina Utilisation']
        self.dataCharts__10['Estimated Capacity']=0
        self.dataCharts__10['Effective Capacity'] = 0
        self.dataCharts__10['Active'] = self.dataCharts__10['Alumina Capacity']-self.dataCharts__10['Estimated Capacity']
        
        
        self.dataCharts__11['Chart Year'] = self.data_input_annual['Year']
        self.dataCharts__11['Aluminium Capacity'] =self.data_input_annual['Aluminium Capacity']
        self.dataCharts__11['Aluminium Production'] =self.data_input_annual['Aluminium Production']
        self.dataCharts__11['Aluminium Utilisation'] =self.data_input_annual['Aluminium Utilisation']
        self.dataCharts__11['Estimated Capacity']=0
        self.dataCharts__11['Effective Capacity'] = 0
        self.dataCharts__11['Active'] = self.dataCharts__11['Aluminium Capacity']-self.dataCharts__11['Estimated Capacity']
        
        #self.data_charts_11
    
    
    
    
    
    
    def calc_mines_bx_production_1(self):
        self.minesBxPrdocution1['LT Av. Al2O3'] =self.minesBxPrdocution1['Total Al2O3']- self.minesBxPrdocution1['Monohydrate '] - self.minesBxPrdocution1['LT Reactive SiO2']
        self.minesBxPrdocution1['Quartz'] = self.minesBxPrdocution1['Total SiO2'] - self.minesBxPrdocution1['LT Reactive SiO2']
        self.minesBxPrdocution1['LT'] =  [(1/self.minesBxPrdocution1.loc[row,'LT Av. Al2O3']/self.otherInput_bx_tab2.loc[0,'Value']) if self.minesBxPrdocution1.loc[row,'LT Av. Al2O3']>0 else 0 for row in self.minesBxPrdocution1.index]
        self.minesBxPrdocution1['HT-B'] = [(1/(self.minesBxPrdocution1.loc[row,'Total Al2O3'] -self.minesBxPrdocution1.loc[row,'LT Reactive SiO2'] -self.otherInput_bx_tab2.loc[4,'Value']*self.minesBxPrdocution1.loc[row,'Quartz'] )/self.otherInput_bx_tab2.loc[0,'Value']) for row in self.minesBxPrdocution1.index]
        self.minesBxPrdocution1['HT-D'] = [(1/(self.minesBxPrdocution1.loc[row,'Total Al2O3']-self.otherInput_bx_tab2.loc[8,'Value']*self.minesBxPrdocution1.loc[row,'Total SiO2'])/self.otherInput_bx_tab2.loc[0,'Value'])  for row in self.minesBxPrdocution1.index]
        value =[]
        for row in self.minesBxPrdocution1.index:
            if self.minesBxPrdocution1.loc[row, 'Style'] == 'LT':
                value.append(self.minesBxPrdocution1.loc[row,'LT'])
            elif self.minesBxPrdocution1.loc[row, 'Style'] == 'HT-B':
                value.append(self.minesBxPrdocution1.loc[row,'HT-B'])
            elif self.minesBxPrdocution1.loc[row, 'Style'] == 'HT-D':
                value.append(self.minesBxPrdocution1.loc[row,'HT-D'])
            else:
                value.append('')
        self.minesBxPrdocution1['BAR'] = value
        
    #mines after bauxite imports
    def calc_mines_bx_production_2(self):
        for row in range(1, self.minesBxPrdocution2.shape[0]):
            self.minesBxPrdocution2.loc[row,8] = self.minesBxPrdocution2.loc[row,[9,10,11,12]].sum()
            self.minesBxPrdocution2.loc[row,13] = self.minesBxPrdocution2.loc[row,[14,15,16,17]].sum()
            self.minesBxPrdocution2.loc[row,18] = self.minesBxPrdocution2.loc[row,[19,20,21,22]].sum()
            self.minesBxPrdocution2.loc[row,23] = self.minesBxPrdocution2.loc[row,[24,25,26,27]].sum()
        
        self.minesBxPrdocution_final[1] = self.minesBxPrdocution2[1]
        self.minesBxPrdocution_final[2] = self.minesBxPrdocution2[2]
        self.minesBxPrdocution_final[3] = self.minesBxPrdocution2[3]
        for row in range(1, self.minesBxPrdocution2.shape[0]):
            for col in range(4,8):
                self.minesBxPrdocution_final.at[0,col] = self.minesBxPrdocution2.loc[0,col+5]
                self.minesBxPrdocution_final.at[row,col] = self.minesBxPrdocution2.loc[row,col+5] if float(self.minesBxPrdocution2.loc[row,col+5]) != 0 else self.minesBxPrdocution3.loc[row,col+5] + self.BauxiteSourcesSummary.loc[row,col+5]
            self.minesBxPrdocution_final.at[0,8] = self.minesBxPrdocution2.loc[0,8]
            self.minesBxPrdocution_final.at[row,8] = self.minesBxPrdocution_final.loc[row,[4,5,6,7]].sum()
            for col in range(9,13):
                self.minesBxPrdocution_final.at[0,col] = self.minesBxPrdocution2.loc[0,col+5]
                self.minesBxPrdocution_final.at[row,col] = self.minesBxPrdocution2.loc[row,col+5] if float(self.minesBxPrdocution2.loc[row,col+5]) != 0 else self.minesBxPrdocution3.loc[row,col+5] + self.BauxiteSourcesSummary.loc[row,col+5]
            self.minesBxPrdocution_final.at[0,13] = self.minesBxPrdocution2.loc[0,13]
            self.minesBxPrdocution_final.at[row,13] = self.minesBxPrdocution_final.loc[row,[9,10,11,12]].sum()
            for col in range(14,18):
                self.minesBxPrdocution_final.at[0,col] = self.minesBxPrdocution2.loc[0,col+5]
                self.minesBxPrdocution_final.at[row,col] = self.minesBxPrdocution2.loc[row,col+5] if float(self.minesBxPrdocution2.loc[row,col+5]) != 0 else self.minesBxPrdocution3.loc[row,col+5] + self.BauxiteSourcesSummary.loc[row,col+5]
            self.minesBxPrdocution_final.at[0,18] = self.minesBxPrdocution2.loc[0,18]
            self.minesBxPrdocution_final.at[row,18] = self.minesBxPrdocution_final.loc[row,[14,15,16,17]].sum()
            for col in range(19,23):
                self.minesBxPrdocution_final.at[0,col] = self.minesBxPrdocution2.loc[0,col+5]
                self.minesBxPrdocution_final.at[row,col] = self.minesBxPrdocution2.loc[row,col+5] if float(self.minesBxPrdocution2.loc[row,col+5]) != 0 else self.minesBxPrdocution3.loc[row,col+5] + self.BauxiteSourcesSummary.loc[row,col+5]
            self.minesBxPrdocution_final.at[0,23] = self.minesBxPrdocution2.loc[0,23]
            self.minesBxPrdocution_final.at[row,23] = self.minesBxPrdocution_final.loc[row,[19,20,21,22]].sum()
            

        
    
        
    def calc_bauxite_sources_1(self):
        d1 = {self.minesBxPrdocution1['Key'][_]:self.minesBxPrdocution1['Name'][_] for _ in self.minesBxPrdocution1.index}
        d2 = {self.minesBxPrdocution1['Key'][_]:self.minesBxPrdocution1['BAR'][_] for _ in self.minesBxPrdocution1.index}
        for row in range(1, 1400, 14):
            for col in range(8, len(self.BauxiteSources.columns)+1):
                col2 = 0
                try:
                    col2 = str(int(self.BauxiteSources.loc[0,col]))
                except:
                    col2 = self.BauxiteSources.loc[0,col]
                try:
                    self.BauxiteSources.loc[row,col]= float(self.refAAproductions.loc[self.refAAproductions['Key']== self.BauxiteSources.loc[row,3] , col2])
                except:
                    self.BauxiteSources.loc[row,col]= 0
            try:
                self.BauxiteSources.loc[row+2,6] = d1[self.BauxiteSources.loc[row+2,5]]
                self.BauxiteSources.loc[row+2,7] = d2[self.BauxiteSources.loc[row+2,5]]
                
                self.BauxiteSources.loc[row+3,6] = d1[self.BauxiteSources.loc[row+3,5]]
                self.BauxiteSources.loc[row+3,7] = d2[self.BauxiteSources.loc[row+3,5]]
                
                self.BauxiteSources.loc[row+4,6] = d1[self.BauxiteSources.loc[row+4,5]]
                self.BauxiteSources.loc[row+4,7] = d2[self.BauxiteSources.loc[row+4,5]]
        
                self.BauxiteSources.loc[row+5,6] = d1[self.BauxiteSources.loc[row+5,5]]
                self.BauxiteSources.loc[row+5,7] = d2[self.BauxiteSources.loc[row+5,5]]
                
                self.BauxiteSources.loc[row+6,6] = d1[self.BauxiteSources.loc[row+6,5]]
                self.BauxiteSources.loc[row+6,7] = d2[self.BauxiteSources.loc[row+6,5]]
            except:
                pass
                
    def calc_bauxite_sources_2(self):
        for col in range(8, len(self.BauxiteSources.columns)+1):
            for row in range(3, 1400,14):
                self.BauxiteSources.loc[row,col] = float(self.BauxiteSources.loc[row+6,col])*self.BauxiteSources.loc[row-2,col]*float(self.BauxiteSources.loc[row,7]) if float(self.BauxiteSources.loc[row+6,col]) > 0 else 0
                self.BauxiteSources.loc[row+1,col] = float(self.BauxiteSources.loc[1+row+6,col])*self.BauxiteSources.loc[row-2,col]*float(self.BauxiteSources.loc[1+row,7]) if float(self.BauxiteSources.loc[1+row+6,col]) > 0 else 0
                self.BauxiteSources.loc[row+2,col] = float(self.BauxiteSources.loc[2+row+6,col])*self.BauxiteSources.loc[row-2,col]*float(self.BauxiteSources.loc[2+row,7]) if float(self.BauxiteSources.loc[2+row+6,col]) > 0 else 0
                self.BauxiteSources.loc[row+3,col] = float(self.BauxiteSources.loc[3+row+6,col])*self.BauxiteSources.loc[row-2,col]*float(self.BauxiteSources.loc[3+row,7]) if float(self.BauxiteSources.loc[3+row+6,col]) > 0 else 0
                self.BauxiteSources.loc[row+4,col] = float(self.BauxiteSources.loc[4+row+6,col])*self.BauxiteSources.loc[row-2,col]*float(self.BauxiteSources.loc[4+row,7]) if float(self.BauxiteSources.loc[4+row+6,col]) > 0 else 0
    
    def calc_bauxite_sources_3(self):
        for row in range(1, self.BauxiteSourcesSummary.shape[0]):
            for col in range(8, len(self.BauxiteSources.columns)+1):
                self.BauxiteSourcesSummary.loc[row,col] = self.BauxiteSources.loc[self.BauxiteSources[5]==self.BauxiteSourcesSummary.loc[row,5], col].astype(float).sum()
               
        
        
    def calc_global_bx_quarterly_1(self):
        for col in list(map(int, range(6, len(self.GlobalBx.columns)+1))):
            self.GlobalBx.loc[11,col] = self.GlobalBx.loc[2:10,col].astype(float).sum()
            self.GlobalBx.loc[54,col] = self.GlobalBx.loc[11,col]
        for col in list(map(int, range(14, len(self.GlobalBx.columns)+1))):
            self.GlobalBx.loc[44,col] = float(self.GlobalBx.loc[31,col])*0.86*3.42/1000+0.1595*float(self.GlobalBx.loc[15,col])/0.12/1000
            self.GlobalBx.loc[45,col] = self.GlobalBx.loc[19,col] 
            self.GlobalBx.loc[46,col] = self.GlobalBx.loc[14:17,col].astype(float).sum()/1000
            self.GlobalBx.loc[47,col] = self.GlobalBx.loc[22:25,col].astype(float).sum()/1000*0.9
            self.GlobalBx.loc[48,col] = float(self.GlobalBx.loc[28,col])*0.9/1000+(float(self.GlobalBx.loc[15,col])/0.12*0.45)/1000
        self.GlobalBx.at[59,6] = 'Q3 2019'
        self.GlobalBx.at[59,7] = 'Q4 2019'
        self.GlobalBx.at[59,8] = 'Q-O-Q'
        self.GlobalBx.at[59,10] = 'Blank'
        self.GlobalBx.at[59,11] = 'Mt'
        
        self.GlobalBx.at[60, 1] = 'Rio'
        self.GlobalBx.at[61, 1] = 'Alcoa'
        self.GlobalBx.at[62, 1] = 'SMB'
        self.GlobalBx.at[63, 1] = 'S32'
        self.GlobalBx.at[64, 1] = 'Hydro'
        self.GlobalBx.at[65, 1] = 'Rusal'
        self.GlobalBx.at[66, 1] = 'Chalco'
        self.GlobalBx.at[67, 1] = 'Other*'
        self.GlobalBx.at[68, 1] = 'Total'
        self.GlobalBx.at[69, 1] = ''
        self.GlobalBx.at[70, 1] = 'Total'
        self.GlobalBx.at[70, 6] = self.GlobalBx.loc[54,16]/1000
        self.GlobalBx.at[70, 7] = self.GlobalBx.loc[54,17]/1000
        
        data = self.GlobalBx.loc[43:52,:]
        for col in [6,7]:
            self.GlobalBx.at[68, col] = self.GlobalBx.loc[70, col]
            self.GlobalBx.at[60, col] = float(data.loc[data[1]==self.GlobalBx.loc[60,1], (int(col)+10)])
            self.GlobalBx.at[61, col] = float(data.loc[data[1]==self.GlobalBx.loc[61,1], (int(col)+10)])
            self.GlobalBx.at[62, col] = float(data.loc[data[1]==self.GlobalBx.loc[62,1], (int(col)+10)])
            self.GlobalBx.at[63, col] = float(data.loc[data[1]==self.GlobalBx.loc[63,1], (int(col)+10)])
            self.GlobalBx.at[64, col] = float(data.loc[data[1]==self.GlobalBx.loc[64,1], (int(col)+10)])
            self.GlobalBx.at[65, col] = float(data.loc[data[1]==self.GlobalBx.loc[65,1], (int(col)+10)])
            self.GlobalBx.at[66, col] = float(data.loc[data[1]==self.GlobalBx.loc[66,1], (int(col)+10)])
            self.GlobalBx.at[67, col] = self.GlobalBx.loc[70, col] - self.GlobalBx.loc[60:66, col].astype(float).sum()
        for row in range(60,69):
            self.GlobalBx.at[row, 8] = self.GlobalBx.loc[row, 7] /self.GlobalBx.loc[row, 6]  - 1
            self.GlobalBx.at[row, 11] = self.GlobalBx.loc[row, 7]
            try:
                self.GlobalBx.at[row, 10] = self.GlobalBx.loc[60:row-1, 7].sum()
            except:
                pass
            self.GlobalBx.at[row, 12] = self.GlobalBx.loc[60:row, 7].sum()/self.GlobalBx.loc[70, 7] 
            self.GlobalBx.at[row, 13] = self.GlobalBx.loc[row, 7] /self.GlobalBx.loc[68, 7]
        self.GlobalBx.at[68, 10] = 0
        self.GlobalBx.at[68, 12] = 1
        for row in range(60,68):
            self.GlobalBx.at[row, 16] = self.GlobalBx.loc[row, 1]
            self.GlobalBx.at[row, 17] = self.GlobalBx.loc[row, 13]
    
    
    
    #CM Global and BX Prodution
    def calc_cm_global_prd_1(self):
        d = {self.minesBxPrdocution1['Name'][_]:self.minesBxPrdocution1['Key'][_]  for _ in self.minesBxPrdocution1.index}
        l=[]
        for row in self.cm_global_prd.index:
            try:
                l.append(d[self.cm_global_prd.loc[row,'Name'] ])
            except:
                l.append(d[self.cm_global_prd.loc[row+1,'Name'] ])
        self.cmGlobalProd_main['Key'] = l
        for cols in self.cm_global_prd.columns:
            self.cmGlobalProd_main[cols] = self.cm_global_prd[cols]
        
        cols = self.minesBxPrdocution_final.loc[0,4:].to_list()
        for v in range(4,len(cols),5):
            cols[v] = str(int(cols[v]))
        for i in range(len(cols)):
            result = [ float(self.minesBxPrdocution_final.loc[self.minesBxPrdocution_final[1]==self.cmGlobalProd_main.loc[r, 'Key'], 4+i])  for r in self.cmGlobalProd_main.index ]
            self.cmGlobalProd_main[cols[i]] = result*self.cm_global_prd['Offtake( %)']
            
        
        #self.cmGlobalProd_main #
        #self.cm_global_prd #input
            
    def calc_aa_productions_cal_1(self,col):
        self.AAproductions.columns = list(map(str, self.AAproductions.columns.to_list()))
            #capacities_total_rows
        for cRows in [_ for _ in range(7,750,31)]:
            self.AAproductions.at[cRows,col] = self.AAproductions.loc[cRows-4:cRows-1,col].astype(float).sum()
        #productions_total_rows
        for pRows in [_ for _ in range(13,750,31)]:  
            self.AAproductions.at[pRows,col] = self.AAproductions.loc[pRows-4:pRows-1,col].astype(float).sum()
        
        for k in range(0,710,31):
            #capacities
            cap = [float(_) for _ in self.AAproductions.loc[3+k:6+k,col]]
            bax = [_ for _ in self.AAproductions.loc[15+k:18+k,col]]
            d_i = [1 - float(_) for _ in self.AAproductions.loc[20+k:23+k,col]]
            val1 =0
            val2 = 0
            try:
                val1 = cap[bax.index('D')]
                val2 = cap[bax.index('D-I')] * d_i[bax.index('D-I')]
            except:
                val2 = 0
            self.AAproductions.at[25+k,col] = val1 + val2
            data = self.AAproductions.loc[15+k:18+k,'7':col].copy().reset_index(drop=True)
            data['colH']=self.AAproductions.loc[3+k:6+k,col].copy().reset_index(drop=True).astype(float)
            val1 = data.loc[(data['7']=='L-B')&(data[col]=='I'), 'colH']
            data2 = self.AAproductions.loc[20+k:23+k,'7':col].copy().reset_index(drop =True)
            data2['colH']=self.AAproductions.loc[3+k:6+k,col].copy().reset_index(drop=True).astype(float)
            data2['8'].astype(float)
            val2 = data2.loc[data2['7']=='H-B-ODT-Sweet',col:'colH'].prod(axis=1)
            val3 = data2.loc[data2['7']=='H-B-Sweet',col:'colH'].prod(axis=1)
            
            val1 = 0 if val1.empty else val1
            val2 = 0 if val2.empty else val2
            val3 = 0 if val3.empty else val3
            self.AAproductions.at[27+k,col] = float(val1) + float(val2) +float(val3)
            self.AAproductions.at[26+k,col] = self.AAproductions.at[7+k,col] - self.AAproductions.at[25+k,col] -self.AAproductions.at[27+k,col]
            #production
            prod = [float(_) for _ in self.AAproductions.loc[9+k:12+k,col]]
            bax = [_ for _ in self.AAproductions.loc[15+k:18+k,col]]
            d_i = [1 - float(_) for _ in self.AAproductions.loc[20+k:23+k,col]]
            val1 =0
            val2 = 0
            try:
                val1 = prod[bax.index('D')]
                val2 = prod[bax.index('D-I')] * d_i[bax.index('D-I')]
            except:
                val2 = 0
            self.AAproductions.at[29+k,col] = val1 + val2
            data = self.AAproductions.loc[15+k:18+k,'7':col].copy().reset_index(drop=True)
            data['colH']=self.AAproductions.loc[9+k:12+k,col].copy().reset_index(drop=True).astype(float)
            val1 = data.loc[(data['7']=='L-B')&(data[col]=='I'), 'colH']
            data2 = self.AAproductions.loc[20+k:23+k,'7':col].copy().reset_index(drop =True)
            data2['colH']=self.AAproductions.loc[9+k:12+k,col].copy().reset_index(drop=True).astype(float)
            data2['8'].astype(float)
            val2 = data2.loc[data2['7']=='H-B-ODT-Sweet',col:'colH'].prod(axis=1)
            val3 = data2.loc[data2['7']=='H-B-Sweet',col:'colH'].prod(axis=1)
            val1 = 0 if val1.empty else val1
            val2 = 0 if val2.empty else val2
            val3 = 0 if val3.empty else val3
            self.AAproductions.at[31+k,col] = float(val1) + float(val2) +float(val3)
            self.AAproductions.at[30+k,col] = self.AAproductions.at[13+k,col] - self.AAproductions.at[29+k,col] -self.AAproductions.at[31+k,col]
        self.AAproductions.loc[755,col] = self.AAproductions.loc[748,col]
        while(int(col)>91):
            for row in range(758,764):
                self.AAproductions.loc[row, col] = float(self.AAproductions.loc[row-9,col])/1
            break
    #summaries
    def calc_aa_productions_cal_2(self,col):
        self.AAproductions.loc[767,'2'] = 'Summary - total AA production - all bauxites'
        
        self.AAproductions.loc[768,'1'] =1
        self.AAproductions.loc[768,'2'] ='Bosai'
        self.AAproductions.loc[768,'3'] ='spy'
        self.AAproductions.loc[768,'4'] ='nanchuan'
        self.AAproductions.loc[768,'5'] ='producion total'
        rw = 769
        for pRow in range(13,750,31):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[pRow-11, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[pRow-11, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[pRow-11, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[pRow-11, '4']
            self.AAproductions.loc[rw,'5'] = self.AAproductions.loc[pRow, '5']
            self.AAproductions.loc[rw,col] = self.AAproductions.loc[pRow, col]
            rw = rw + 1
        self.AAproductions.loc[793,'1'] = 25
        self.AAproductions.loc[793,'2'] ='Inland blending'
        self.AAproductions.loc[793,'3'] = 'varoius'
        self.AAproductions.loc[793,'4'] = 'varoius'
        self.AAproductions.loc[793,'5'] = 'Production - Total'
        self.AAproductions.loc[793,col] = float(self.AAproductions.loc[764,col])/1000/float(self.AAproductions.loc[0,col])*365
        
        self.AAproductions.loc[794,'5'] = 'Total'
        self.AAproductions.loc[794,col] = self.AAproductions.loc[769:793,col].sum()
        self.AAproductions.loc[795,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[769:792,'3':col].copy()
        self.AAproductions.loc[795,col] =  sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[796,'5'] = 'Shandong total -model monthly'
        self.AAproductions.loc[796,col] = self.AAproductions.loc[795,col] /365*float(self.AAproductions.loc[0,col])
        self.AAproductions.loc[797,'5'] = 'Shandong actual monthly'
        values = self.data_inp_montly_tab1.loc[204:,'Production - Shandong'].to_list()
        self.AAproductions.loc[797,col] = values[int(col)-8]
        self.AAproductions.loc[798,col] = (self.AAproductions.loc[797,col] - self.AAproductions.loc[796,col])/float(self.AAproductions.loc[0,col])*365
        self.AAproductions.loc[799,'7'] = 0
        self.AAproductions.loc[799,col] = self.AAproductions.loc[798,col] + self.AAproductions.loc[799,str(int(col)-1)]
        self.AAproductions.loc[800,col] = self.AAproductions.loc[798,col]/365*float(self.AAproductions.loc[0,col])/self.AAproductions.loc[797,col]
        
        self.AAproductions.loc[803,'5'] ='Inner Mongolia - model - monthly annualised'
        sel = self.AAproductions.loc[769:792,'3':col].copy()
        self.AAproductions.loc[803,col] =  sel.loc[sel['3']=='Inner Mongolia',col].sum()
        
        self.AAproductions.loc[807,'5']='Inner Mongolia - model - monthly annualised'
        self.AAproductions.loc[808,'5'] = 'Inner Mongolia - actual - monthly annualised'
        self.AAproductions.loc[808,col] = self.AAproductions.loc[804,col]/float(self.AAproductions.loc[0,col])*365
        
        self.AAproductions.loc[811,'3'] = 'chart Shandong'
        self.AAproductions.loc[811,'5'] = 'Month'
        self.AAproductions.loc[812,'4'] = 'Modelled - annualised'
        self.AAproductions.loc[813,'4'] = 'Reported - annualised'
        self.AAproductions.loc[812,col] = self.AAproductions.loc[796,col]/float(self.AAproductions.loc[0,col])*365
        self.AAproductions.loc[813,col] = self.AAproductions.loc[797,col]/float(self.AAproductions.loc[0,col])*365
    
    def calc_aa_productions_cal_3(self,col):
        self.AAproductions.loc[815,'3'] = 'Summary - AA production using HT imported bauxite'
        rw = 816
        for pRow in range(30,750,31):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[pRow-28, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[pRow-28, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[pRow-28, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[pRow-28, '4']
            self.AAproductions.loc[rw,'5'] = self.AAproductions.loc[pRow, '5']
            self.AAproductions.loc[rw,col] = self.AAproductions.loc[pRow, col]
            rw = rw + 1
        self.AAproductions.loc[840,'1'] = 25
        self.AAproductions.loc[840,'2'] ='Inland blending'
        self.AAproductions.loc[840,'3'] = 'varoius'
        self.AAproductions.loc[840,'4'] = 'varoius'
        self.AAproductions.loc[840,'5'] = 'Production - Total'
        self.AAproductions.loc[840,col] = float(self.AAproductions.loc[764,col])/1000/float(self.AAproductions.loc[0,col])*365
        self.AAproductions.loc[841,'5'] = 'Total'
        self.AAproductions.loc[841,col] = self.AAproductions.loc[816:840,col].astype(float).sum()
        self.AAproductions.loc[842,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[816:839,'3':col].copy()
        self.AAproductions.loc[842,col] =  sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[843,'5'] = 'Shandong total expected monthly'
        self.AAproductions.loc[843,col] = self.AAproductions.loc[842,col] /365*float(self.AAproductions.loc[0,col])
        
        self.AAproductions.loc[846,'3'] = 'Summary - AA production using LT imported bauxite'
        rw = 847
        for pRow in range(31,750,31):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[pRow-29, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[pRow-29, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[pRow-29, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[pRow-29, '4']
            self.AAproductions.loc[rw,'5'] = self.AAproductions.loc[pRow, '5']
            self.AAproductions.loc[rw,col] = self.AAproductions.loc[pRow, col]
            rw = rw + 1
        self.AAproductions.loc[871,'1'] = 25
        self.AAproductions.loc[871,'2'] ='Inland blending'
        self.AAproductions.loc[871,'3'] = 'varoius'
        self.AAproductions.loc[871,'4'] = 'varoius'
        self.AAproductions.loc[871,'5'] = 'Production - Total'
        self.AAproductions.loc[871,col] = float(self.AAproductions.loc[764,col])/1000/float(self.AAproductions.loc[0,col])*365
        self.AAproductions.loc[872,'5'] = 'Total'
        self.AAproductions.loc[872,col] = self.AAproductions.loc[847:871,col].astype(float).sum()
        self.AAproductions.loc[873,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[847:871,'3':col].copy()
        self.AAproductions.loc[873,col] =  sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[874,'5'] = 'Shandong total expected monthly'
        self.AAproductions.loc[874,col] = self.AAproductions.loc[873,col] /365*float(self.AAproductions.loc[0,col])
        
    def calc_aa_productions_cal_4(self,col):
        self.AAproductions.loc[877,'3'] = 'Summary - AA production using HT imported bauxite'
        rw = 878
        for row in range(816,841):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[row, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[row, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[row, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[row, '4']
            self.AAproductions.loc[rw,col] = self.AAproductions.loc[row, col] +  self.AAproductions.loc[row+31, col]
            rw = rw + 1
        self.AAproductions.loc[903,'5'] = 'Total'
        self.AAproductions.loc[903,col] = self.AAproductions.loc[878:902,col].astype(float).sum()
        self.AAproductions.loc[904,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[878:902,'3':col].copy()
        self.AAproductions.loc[904,col] =  sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[905,'5'] = 'Shandong total expected monthly'
        self.AAproductions.loc[905,col] = self.AAproductions.loc[904,col] /365*float(self.AAproductions.loc[0,col])
        
        self.AAproductions.loc[908,'5'] ='Bauxite to Alumina Ratio (BAR)'
        self.AAproductions.loc[910,'5'] = 'HT Bauxite: Alumina ratio (HT BAR)'
        self.AAproductions.loc[911,'5'] ='LT Bauxite: Alumina ratio (LT BAR)'
        
        self.AAproductions.loc[913,'5'] = 'modelled HT buaxite use'    
        self.AAproductions.loc[914,'5'] = 'modelled LT buaxite use'
        while(int(col)<43):
            self.AAproductions.loc[908,col] = 2.90
            self.AAproductions.loc[910,col] = 2.90
            self.AAproductions.loc[911,col] = 2.90
            break
        
        self.AAproductions.loc[913,col] = self.AAproductions.loc[841,col] * self.AAproductions.loc[910,col] / 365 * float(self.AAproductions.loc[0,col])
        self.AAproductions.loc[914,col] = self.AAproductions.loc[872,col] * self.AAproductions.loc[911,col] / 365 * float(self.AAproductions.loc[0,col])
    
        self.AAproductions.loc[916,col] = self.AAproductions.loc[913:915,col].sum()
        self.AAproductions.loc[917,col] = self.AAproductions.loc[903,col] / 365 * float(self.AAproductions.loc[0,col])
        self.AAproductions.loc[918,col] = (self.AAproductions.loc[797,col] + float(self.AAproductions.loc[804,col])) - ( self.AAproductions.loc[796,col] + self.AAproductions.loc[803,col] / 365 * float(self.AAproductions.loc[0,col]))
        self.AAproductions.loc[919,col] = self.AAproductions.loc[918,col] / (self.AAproductions.loc[796,col] + self.AAproductions.loc[803,col] / 365 * float(self.AAproductions.loc[0,col]))+1
        
        self.AAproductions.loc[921,col] = self.AAproductions.loc[916,col] * self.AAproductions.loc[919,col]
        self.AAproductions.loc[922,col] = self.AAproductions.loc[903,col] / 365 * float(self.AAproductions.loc[0,col]) * self.AAproductions.loc[919,col]
        self.AAproductions.loc[923,col] = self.AAproductions.loc[921,col] /  float(self.AAproductions.loc[0,col]) *365
        self.AAproductions.loc[924,col] = self.AAproductions.loc[797,col] + float(self.AAproductions.loc[804,col]) - self.AAproductions.loc[919,col] * (self.AAproductions.loc[796,col] + self.AAproductions.loc[803,col] / 365 * float(self.AAproductions.loc[0,col]))
        
        
    def calc_aa_productions_cal_5(self,col):
        self.AAproductions.loc[926,'3'] = 'Summary - AA production using HT imported bauxite'
        rw = 927
        for pRow in range(7,750,31):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[pRow-5, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[pRow-5, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[pRow-5, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[pRow-5, '4']
            self.AAproductions.loc[rw,'5'] = self.AAproductions.loc[pRow, '5']
            self.AAproductions.loc[rw,col] = self.AAproductions.loc[pRow, col]
            rw = rw + 1
        self.AAproductions.loc[951,'1'] = 25
        self.AAproductions.loc[951,'2'] ='Inland blending'
        self.AAproductions.loc[951,'3'] = 'varoius'
        self.AAproductions.loc[951,'4'] = 'varoius'
        self.AAproductions.loc[951,'5'] = 'Production - Total'
        self.AAproductions.loc[951,col] = float(self.AAproductions.loc[764,col])/1000/float(self.AAproductions.loc[0,col])*365
        self.AAproductions.loc[952,'5'] = 'Total'
        self.AAproductions.loc[952,col] = self.AAproductions.loc[927:951,col].astype(float).sum()
        self.AAproductions.loc[953,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[927:951,'3':col].copy()
        self.AAproductions.loc[953,col] =  sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[954,'5'] = 'Shandong total expected monthly'
        self.AAproductions.loc[954,col] = self.AAproductions.loc[953,col] /365*float(self.AAproductions.loc[0,col])
        
        self.AAproductions.loc[956,'3'] = 'Summary - capacity using HT imported bauxite'
        rw = 957
        for pRow in range(26,750,31):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[pRow-24, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[pRow-24, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[pRow-24, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[pRow-24, '4']
            self.AAproductions.loc[rw,'5'] = self.AAproductions.loc[pRow, '5']
            self.AAproductions.loc[rw,col] = self.AAproductions.loc[pRow, col]
            rw = rw + 1
        self.AAproductions.loc[981,'1'] = 25
        self.AAproductions.loc[981,'2'] ='Inland blending'
        self.AAproductions.loc[981,'3'] = 'varoius'
        self.AAproductions.loc[981,'4'] = 'varoius'
        self.AAproductions.loc[981,'5'] = 'Production - Total'
        self.AAproductions.loc[981,col] = float(self.AAproductions.loc[764,col])/1000/float(self.AAproductions.loc[0,col])*365
        self.AAproductions.loc[982,'5'] = 'Total'
        self.AAproductions.loc[982,col] = self.AAproductions.loc[957:981,col].astype(float).sum()
        self.AAproductions.loc[983,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[957:981,'3':col].copy()
        self.AAproductions.loc[983,col] = sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[984,'5'] = 'Shandong total expected monthly'
        self.AAproductions.loc[984,col] = self.AAproductions.loc[983,col] /365*float(self.AAproductions.loc[0,col])
        
        self.AAproductions.loc[986,'3'] = 'Summary - capacity using LT imported bauxite'
        rw = 987
        for pRow in range(27,750,31):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[pRow-25, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[pRow-25, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[pRow-25, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[pRow-25, '4']
            self.AAproductions.loc[rw,'5'] = self.AAproductions.loc[pRow, '5']
            self.AAproductions.loc[rw,col] = self.AAproductions.loc[pRow, col]
            rw = rw + 1
        self.AAproductions.loc[1011,'1'] = 25
        self.AAproductions.loc[1011,'2'] ='Inland blending'
        self.AAproductions.loc[1011,'3'] = 'varoius'
        self.AAproductions.loc[1011,'4'] = 'varoius'
        self.AAproductions.loc[1011,'5'] = 'Production - Total'
        self.AAproductions.loc[1011,col] = float(self.AAproductions.loc[764,col])/1000/float(self.AAproductions.loc[0,col])*365
        self.AAproductions.loc[1012,'5'] = 'Total'
        self.AAproductions.loc[1012,col] = self.AAproductions.loc[987:1011,col].astype(float).sum()
        self.AAproductions.loc[1013,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[987:1011,'3':col].copy()
        self.AAproductions.loc[1013,col] = sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[1014,'5'] = 'Shandong total expected monthly'
        self.AAproductions.loc[1014,col] = self.AAproductions.loc[1013,col] /365*float(self.AAproductions.loc[0,col])
        
    def calc_aa_productions_cal_6(self,col):
        self.AAproductions.loc[1016,'3'] = 'Summary - total capacity using imported bauxite'
        rw = 1017
        for row in range(987,1012):
            self.AAproductions.loc[rw,'1'] = self.AAproductions.loc[row, '1']
            self.AAproductions.loc[rw,'2'] = self.AAproductions.loc[row, '2'] 
            self.AAproductions.loc[rw,'3'] = self.AAproductions.loc[row, '3']
            self.AAproductions.loc[rw,'4'] = self.AAproductions.loc[row, '4']
            self.AAproductions.loc[rw,col] = float(self.AAproductions.loc[row, col]) + float(self.AAproductions.loc[row-30, col])
            rw = rw + 1
        self.AAproductions.loc[1042,'5'] = 'Total'
        self.AAproductions.loc[1042,col] = self.AAproductions.loc[1017:1041,col].astype(float).sum()
        self.AAproductions.loc[1043,'5'] = 'Shandong total'
        sel = self.AAproductions.loc[1017:1041,'3':col].copy()
        self.AAproductions.loc[1043,col] =  sel.loc[sel['3']=='Shandong',col].sum()
        self.AAproductions.loc[1044,'5'] = 'Shandong total expected monthly'
        self.AAproductions.loc[1044,col] = self.AAproductions.loc[1043,col] /365*float(self.AAproductions.loc[0,col])
    
    def calc_aa_productions_cal_7(self,col):
        self.AAproductions.loc[1046,'1'] = 'Summary - alumina production by process'
        self.AAproductions.loc[1047,'5'] = 'LT Bauxite'
        self.AAproductions.loc[1048,'5'] = 'HT Bauxite'
        self.AAproductions.loc[1049,'5'] = 'LT/HT'
        
        self.AAproductions.loc[1047,col] = self.AAproductions.loc[872,col] * self.AAproductions.loc[919,col]
        self.AAproductions.loc[1048,col] = self.AAproductions.loc[841,col] * self.AAproductions.loc[919,col]
                
        self.AAproductions.loc[1052,'1'] = 'Data to Data Chart Sheet'
        self.AAproductions.loc[1052,'5'] = 'HT Bx'
        self.AAproductions.loc[1053,'5'] = 'LT Bx'
        self.AAproductions.loc[1054,'5'] = 'Total'
        
        self.AAproductions.loc[1052,col] = self.AAproductions.loc[1048,col] / 365*float(self.AAproductions.loc[0,col])
        self.AAproductions.loc[1053,col] = self.AAproductions.loc[1047,col] / 365*float(self.AAproductions.loc[0,col])
        self.AAproductions.loc[1054,col] = self.AAproductions.loc[1052:1053,col].astype(float).sum()
        
        #self.AAproductions engine
    def calc_aa_engines(self):
        for i in range(8, len(self.AAproductions.columns)+1):
            ALengine.calc_aa_productions_cal_1(self, str(i))
            ALengine.calc_aa_productions_cal_2(self, str(i))
            ALengine.calc_aa_productions_cal_3(self, str(i))
            ALengine.calc_aa_productions_cal_4(self, str(i))
            ALengine.calc_aa_productions_cal_5(self, str(i))
            ALengine.calc_aa_productions_cal_6(self, str(i))
            ALengine.calc_aa_productions_cal_7(self, str(i))
            
    
        
    
    #special Charts
    def calc_special_charts(self):
        #0,2,1,4,6
        self.special_charts_1['Country'] = self.data_charts_11.loc[[0,2,1,4,6],'Country'].copy().reset_index(drop=True)
        self.special_charts_1['Capacity'] = self.data_charts_11.loc[[0,2,1,4,6],'Q4 Capacity'].copy().reset_index(drop=True)
        self.special_charts_1['Production*'] = self.data_charts_11.loc[[0,2,1,4,6],'Q4 Operational'].copy().reset_index(drop=True)
        self.special_charts_1['Utilisation'] = self.special_charts_1['Production*']/self.special_charts_1['Capacity']
        
        #1,0,2,5,3,4
        self.special_charts_2['Country'] = self.data_charts_12.loc[[1,0,2,5,3,4],'Country'].copy().reset_index(drop=True)
        self.special_charts_2['Capacity'] = self.data_charts_12.loc[[1,0,2,5,3,4],'Q4 Capacity'].copy().reset_index(drop=True)
        self.special_charts_2['Production*'] = self.data_charts_12.loc[[1,0,2,5,3,4],'Q4 Operational'].copy().reset_index(drop=True)
        self.special_charts_2['Utilisation'] = self.special_charts_2['Production*']/self.special_charts_2['Capacity']
        
        row2 = self.special_charts_1.index.to_list()
        row = list(range(1,15,3))
        for i in range(len(row)):
            self.specialCharts_out1.at[row[i], 'Country'] = self.special_charts_1.loc[row2[i],'Country']
            self.specialCharts_out1.at[row[i], 'Capacity'] = self.special_charts_1.loc[row2[i],'Capacity']
            self.specialCharts_out1.at[row[i]+1, 'Production*'] = self.special_charts_1.loc[row2[i],'Production*']
            self.specialCharts_out1.at[row2[i]+1, 'Country2'] = self.special_charts_1.loc[row2[i],'Country']
            self.specialCharts_out1.at[row2[i]+1, 'Utilisation'] = self.special_charts_1.loc[row2[i],'Utilisation']
            
        row2 = self.special_charts_2.index.to_list()
        row = list(range(1,19,3))
        for i in range(len(row)):
            self.specialCharts_out2.at[row[i], 'Country'] = self.special_charts_2.loc[row2[i],'Country']
            self.specialCharts_out2.at[row[i], 'Capacity'] = self.special_charts_2.loc[row2[i],'Capacity']
            self.specialCharts_out2.at[row[i]+1, 'Production*'] = self.special_charts_2.loc[row2[i],'Production*']
            self.specialCharts_out2.at[row2[i]+1, 'Country2'] = self.special_charts_2.loc[row2[i],'Country']
            self.specialCharts_out2.at[row2[i]+1, 'Utilisation'] = self.special_charts_2.loc[row2[i],'Utilisation']
        
        
        
        #general functions
    def excel_date(self, y, m, d):
        date1 = datetime(y, m, d)
        temp = datetime(1899, 12, 30)    # Note, not 31st Dec but 30th!
        delta = date1 - temp
        return float(delta.days) + (float(delta.seconds) / 86400)
    
    def func_1(self, row, Country):
        row = row - 252
        cols = self.bxImports_tab8.columns[3:].to_list()
        value = self.bxImports_tab8.loc[al.bxImports_tab8['Country']==Country, cols[row]].copy()
        return float(value)
    
    def func_2(self, row, unknown):
        row = row - 252
        cols = self.bxImports_tab9.columns[3:].to_list()
        table = self.bxImports_tab9.loc[3:,:]
        value = table.loc[table['Unnamed']==unknown, cols[row]].copy()
        return float(value)
    
    def func_6months(self, v):
        d = v.copy()
        for i in range(10, len(v)-1):
            d[i] = v[1+i-6:i+1].sum()/6
        return d
        

al = ALengine()
al.calc_all()

writer1 = pd.ExcelWriter('Outputs\Data Input Annual Output.xlsx')
al.data_input_annual.to_excel(writer1,sheet_name='Sheet1', encoding ='utf-8', index=False)
writer1.save()

writer2 = pd.ExcelWriter('Outputs\Bx Imports Output.xlsx')
al.bxImports_tab1.to_excel(writer2,sheet_name='Table 1', encoding='utf-8', index=False)
al.bxImports_tab2.to_excel(writer2,sheet_name='Table 2', encoding='utf-8', index=False)
al.bxImports_tab3.to_excel(writer2,sheet_name='Table 3', encoding='utf-8', index=False)
al.bxImports_tab4.to_excel(writer2,sheet_name='Table 4', encoding='utf-8', index=False)
al.bxImports_tab5.to_excel(writer2,sheet_name='Table 5', encoding='utf-8', index=False)
al.bxImports_tab7.to_excel(writer2,sheet_name='Table 7', encoding='utf-8', index=False)
al.bxImports_tab8.to_excel(writer2,sheet_name='Table 8', encoding='utf-8', index=False)
al.bxImports_tab9.to_excel(writer2,sheet_name='Table 9', encoding='utf-8', index=False)
writer2.save()

writer3 = pd.ExcelWriter('Outputs\Refineries and Aa productions.xlsx')
al.refAAproductions.to_excel(writer3,sheet_name='Table 1', encoding='utf-8', index=False)
writer3.save()

writer4 = pd.ExcelWriter('Outputs\Changjiang & LME Daily.xlsx')
al.changjiangLME.to_excel(writer4,sheet_name='Table 1', encoding='utf-8', index=False)
writer4.save()

writer5 = pd.ExcelWriter('Outputs\Platts Vs Cmaax.xlsx')
al.plattsVsCmaax.to_excel(writer5,sheet_name='Table 1', encoding='utf-8', index=False)
writer5.save()

writer6 = pd.ExcelWriter('Outputs\Data Input Monthlyy.xlsx')
al.dataInputMontly_alumina_montly_supply.to_excel(writer6,sheet_name='Alumina Monthly Supply', encoding='utf-8', index=False)
al.dataInputMontly_aluminium_montly_supply.to_excel(writer6,sheet_name='Aluminium Monthly Supply', encoding='utf-8', index=False)
al.dataInputMontly_bauxite_imports1.to_excel(writer6,sheet_name='Bauxite Imports 1', encoding='utf-8', index=False)
al.dataInputMontly_bauxite_imports2.to_excel(writer6,sheet_name='Bauxite Imports 2', encoding='utf-8', index=False)
al.dataInputMontly_bauxite_imports3.to_excel(writer6,sheet_name='Bauxite Imports 3', encoding='utf-8', index=False)
al.dataInputMontly_bauxite_imports4.to_excel(writer6,sheet_name='Bauxite Imports 4', encoding='utf-8', index=False)
al.dataInputMontly_bauxite_imports5.to_excel(writer6,sheet_name='Bauxite Imports 5', encoding='utf-8', index=False)
al.dataInputMontly_input_bar.to_excel(writer6,sheet_name='Input BAR', encoding='utf-8', index=False)
al.dataInputMontly_BAR.to_excel(writer6,sheet_name='BAR', encoding='utf-8', index=False)
al.dataInputMontly_CmPortBx.to_excel(writer6,sheet_name='CM Port Bx Invt.', encoding='utf-8', index=False)
al.dataInputMontly_global_alumina.to_excel(writer6,sheet_name='Global Alumina', encoding='utf-8', index=False)
al.dataInputMontly_inland1.to_excel(writer6,sheet_name='Inland ref 1', encoding='utf-8', index=False)
al.dataInputMontly_inland2.to_excel(writer6,sheet_name='Inland ref 2', encoding='utf-8', index=False)
writer6.save()

writer7= pd.ExcelWriter('Outputs\Mines and Bx Productions.xlsx')
al.minesBxPrdocution1.to_excel(writer7,sheet_name='Main Table', encoding='utf-8', index=False)
al.minesBxPrdocution_final.to_excel(writer7,sheet_name='final', encoding='utf-8', index=False, header=False)
writer7.save()

writer8= pd.ExcelWriter('Outputs\Bauxite Sources.xlsx')
al.BauxiteSources.to_excel(writer8,sheet_name='Main Table', encoding='utf-8', index=False, header=False)
al.BauxiteSourcesSummary.to_excel(writer8,sheet_name='Summary', encoding='utf-8', index=False, header=False)
writer8.save()

writer9 = pd.ExcelWriter('Outputs\Global Bx Quarterly Data.xlsx')
al.GlobalBx.to_excel(writer9,sheet_name='Main Table', encoding='utf-8', index=False, header=False)
writer9.save()
                       
writer10 = pd.ExcelWriter('Outputs\AA Productions from Imp Bx Monthly.xlsx')
al.AAproductions.to_excel(writer10,sheet_name='Main Table', encoding='utf-8', index=False, header=False)
writer10.save()
                      
writer11 = pd.ExcelWriter('Outputs\Data Monthly.xlsx')
al.dataMonthly_alumina_monthly.to_excel(writer11,sheet_name='Alumina montlhy', encoding='utf-8', index=False)
al.dataMonthly_aluminium_supply.to_excel(writer11,sheet_name='Aluminuim Supply', encoding='utf-8', index=False)
al.dataMonthly_bauxite_imps1.to_excel(writer11,sheet_name='Bauxite Imports 1', encoding='utf-8', index=False)
al.dataMonthly_bauxite_imps2.to_excel(writer11,sheet_name='Bauxite Imports 2', encoding='utf-8', index=False)
al.dataMonthly_bauxite_imps3.to_excel(writer11,sheet_name='Bauxite Imports 3', encoding='utf-8', index=False)
al.dataMonthly_consumpt.to_excel(writer11,sheet_name='Consumptions', encoding='utf-8', index=False)
writer11.save()

writer12 = pd.ExcelWriter('Outputs\CM Global Bx Productions.xlsx')
al.cmGlobalProd_main.to_excel(writer12,sheet_name='Main', encoding='utf-8', index=False)
writer12.save()

writer13 = pd.ExcelWriter('Outputs\Special Charts Outputs.xlsx')
al.specialCharts_out1.to_excel(writer13, sheet_name='Fig 15', encoding='utf-8',index=False)
al.specialCharts_out2.to_excel(writer13, sheet_name='Fig 22', encoding='utf-8',index=False)
writer13.save()

writer14 =pd.ExcelWriter('Outputs\Data Charts Outputs.xlsx')
al.dataCharts_1.to_excel(writer14, sheet_name='Charts 1', encoding='utf-8',index=False)
al.dataCharts_2.to_excel(writer14, sheet_name='Charts 2', encoding='utf-8',index=False)
al.dataCharts_3.to_excel(writer14, sheet_name='Charts 3', encoding='utf-8',index=False)
al.dataCharts_4.to_excel(writer14, sheet_name='Charts 4', encoding='utf-8',index=False)
al.dataCharts_5.to_excel(writer14, sheet_name='Charts 5', encoding='utf-8',index=False)
al.dataCharts_6.to_excel(writer14, sheet_name='Charts 6', encoding='utf-8',index=False)
al.dataCharts_7.to_excel(writer14, sheet_name='Charts 7', encoding='utf-8',index=False)
al.dataCharts_8.to_excel(writer14, sheet_name='Charts 8', encoding='utf-8',index=False)
al.dataCharts_9.to_excel(writer14, sheet_name='Charts 9', encoding='utf-8',index=False)
al.dataCharts_10.to_excel(writer14, sheet_name='Charts 10', encoding='utf-8',index=False)
al.dataCharts_11.to_excel(writer14, sheet_name='Charts 11', encoding='utf-8',index=False)
al.dataCharts_12.to_excel(writer14, sheet_name='Charts 12', encoding='utf-8',index=False)
al.dataCharts_13.to_excel(writer14, sheet_name='Charts 13', encoding='utf-8',index=False)

al.dataCharts__10.to_excel(writer14, sheet_name='Chart_10', encoding='utf-8',index=False)
al.dataCharts__11.to_excel(writer14, sheet_name='Chart_11', encoding='utf-8',index=False)
writer14.save()

dblist = [
    al_flat.single_year_mult_out(al.data_input_annual, "data input annual"),
    al_flat.mult_year_single_output(al.bxImports_tab1, "bx Imports tab1", idx_of_index=[[0,2]], idx_of_values=[[2,]], label="Date"),
    al_flat.mult_year_single_output(al.bxImports_tab2, "bx Imports tab2", idx_of_index=[[3,4]], idx_of_values=[[0,3], [4,]], label="Date"),
    al_flat.mult_year_single_output(al.bxImports_tab3, "bx Imports tab3", idx_of_index=[[0,2]], idx_of_values=[[2,]], label="Date"),
    al_flat.mult_year_single_output(al.bxImports_tab4, "bx Imports tab4", idx_of_index=[[0,1]], idx_of_values=[[1,]], label="Date"),
    al_flat.mult_year_single_output(al.bxImports_tab5, "bx Imports tab5", idx_of_index=[[0,3]], idx_of_values=[[3,]], label="Date"),
    al_flat.mult_year_single_output(al.bxImports_tab7, "bx Imports tab7", idx_of_index=[[0,1]], idx_of_values=[[1,]], label="Date"),
    al_flat.mult_year_single_output(al.bxImports_tab8, "bx Imports tab8", idx_of_index=[[0,1]], idx_of_values=[[1,]], label="Date"),
    al_flat.mult_year_single_output(al.bxImports_tab9, "bx Imports tab9", idx_of_index=[[0,2]], idx_of_values=[[2,]], label="Date"),
    al_flat.mult_year_single_output(al.refAAproductions, "Refineries and Aa productions", idx_of_index=[[0,4]], idx_of_values=[[4,]]),
    al_flat.single_year_mult_out(al.changjiangLME, "Changjiang & LME Daily"),
    al_flat.single_year_mult_out(al.plattsVsCmaax, "Platts Vs Cmaax.xlsx"),
    al_flat.single_year_mult_out(al.dataInputMontly_alumina_montly_supply, "data Input Montly alumina montly supply"),
    al_flat.single_year_mult_out(al.dataInputMontly_aluminium_montly_supply, "data Input Montly aluminium montly supply"),
    al_flat.single_year_mult_out(al.dataInputMontly_bauxite_imports1, "data Input Montly bauxite imports 1"),
    al_flat.single_year_mult_out(al.dataInputMontly_bauxite_imports2, "data Input Montly bauxite imports 2"),
    al_flat.single_year_mult_out(al.dataInputMontly_bauxite_imports3, "data Input Montly bauxite imports 3"),
    al_flat.single_year_mult_out(al.dataInputMontly_bauxite_imports4, "data Input Montly bauxite imports4"),
    al_flat.single_year_mult_out(al.dataInputMontly_bauxite_imports5, "data InputMontly bauxite imports5"),
    al_flat.single_year_mult_out(al.dataInputMontly_input_bar, "data Input Montly input bar"),
    al_flat.single_year_mult_out(al.dataInputMontly_BAR, "Data Input Montly BAR"),
    al_flat.single_year_mult_out(al.dataInputMontly_CmPortBx, "Data Input Montly_CmPortBx"),
    al_flat.single_year_mult_out(al.dataInputMontly_global_alumina, "Data Input Montly global alumina"),
    al_flat.single_year_mult_out(al.dataInputMontly_inland1, "Data Input Montly inland 1"),
    # empty table
    # al_flat.single_year_mult_out(al.dataInputMontly_inland2, "Data Input Montly inland 2"),
    al_flat.single_year_mult_out(al.minesBxPrdocution1, "mines Bx Prdocution"),
    al_flat.mult_year_single_output(al.minesBxPrdocution_final, "minesBxPrdocution_final", idx_of_index=[[0,3]], idx_of_values=[[3,]]),
    al_flat.mult_year_single_output(al.BauxiteSources, "Bauxite Sources", idx_of_index=[[0,5]], idx_of_values=[[5,]]),
    al_flat.mult_year_single_output(al.BauxiteSourcesSummary, "Bauxite Sources Summary", idx_of_index=[[0,2]], idx_of_values=[[2,]]),
    al_flat.mult_year_single_output(al.GlobalBx, "Global Bx Quarterly Data", idx_of_index=[[0,1]], idx_of_values=[[1,]]),
    al_flat.mult_year_single_output(al.AAproductions, "AA productions", idx_of_index=[[0,7]], idx_of_values=[[7,]]),
    al_flat.single_year_mult_out(al.dataMonthly_alumina_monthly, "data Monthly alumina monthly"),
    al_flat.single_year_mult_out(al.dataMonthly_aluminium_supply, "data Monthly aluminium supply"),
    al_flat.single_year_mult_out(al.dataMonthly_bauxite_imps1, "data Monthly bauxite_import 1"),
    al_flat.single_year_mult_out(al.dataMonthly_bauxite_imps2, "data Monthly bauxite_import 2"),
    al_flat.single_year_mult_out(al.dataMonthly_bauxite_imps3, "data Monthly bauxite_import 3"),
    al_flat.single_year_mult_out(al.dataMonthly_consumpt, "data Monthly consumptions"),
    al_flat.mult_year_single_output(al.cmGlobalProd_main, "CM Global Bx Productions", idx_of_index=[[0,10]], idx_of_values=[[10,]]),
    al_flat.single_year_mult_out(al.specialCharts_out1, "special Charts out 1"),
    al_flat.single_year_mult_out(al.specialCharts_out2, "special Charts out 2"),
    al_flat.single_year_mult_out(al.dataCharts_1, "data Charts 1"),
    al_flat.single_year_mult_out(al.dataCharts_2, "data Charts 2"),
    al_flat.single_year_mult_out(al.dataCharts_3, "data Charts 3"),
    al_flat.single_year_mult_out(al.dataCharts_4, "data Charts 4"),
    al_flat.single_year_mult_out(al.dataCharts_5, "data Charts 5"),
    al_flat.single_year_mult_out(al.dataCharts_6, "data Charts 6"),
    al_flat.single_year_mult_out(al.dataCharts_7, "data Charts 7"),
    al_flat.single_year_mult_out(al.dataCharts_8, "data Charts 8"),
    # empty table
    # al_flat.single_year_mult_out(al.dataCharts_9, "data Charts 9"),
    al_flat.single_year_mult_out(al.dataCharts_10, "data Charts 10"),
    # empty tables
    # (al.dataCharts_11, "dataCharts_11"),
    # (al.dataCharts_12, "dataCharts_12"),
    # (al.dataCharts_13, "dataCharts_13"),
    al_flat.single_year_mult_out(al.dataCharts__10, "dataCharts 10 _1"),
    al_flat.single_year_mult_out(al.dataCharts__11, "dataCharts 11 "),
]

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, al_flat.out_col]
snapshot_output_data.to_csv('snapshot_output_data.csv', index=False)
uploadtodb.upload(snapshot_output_data)