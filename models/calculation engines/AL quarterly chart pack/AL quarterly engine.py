# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from calendar import monthrange
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb
from al_qrtly_script import restruct


rest = restruct()

al_flat = Flatdbconverter("Quarterly Chart Pack")

#workbooks
data_input_annual_wbk = './Inputs/Data Input Annual.xlsx' 
bx_imports_wbk = './Inputs/Bx Imports.xlsx'
other_inputs_bx_wbk = './Inputs/Other inputs for bx pro .xlsx'
ref_aa_prod_wbk = './Inputs/Refineries & Aa Production.xlsx'
data_input_monthly_wbk = './Inputs/Data Input Monthly.xlsx'

data_monthly_wbk = './Inputs/Data Monthly.xlsx'

mines_bx_prd_wbk = './Inputs/Mines & Bx Production.xlsx'
bauxite_sources_wbk = './Inputs/Bauxite Sources.xlsx'

global_bx_wbk = './Inputs/Global Bx Quarterly Data.xlsx'
aa_production_bx_wbk = './Inputs/AA prodn from Import Bx-Mthly.xlsx'
cm_global_wbk = './Inputs/CM Global Bx Production inputs.xlsx'

data_chartss_wbk = './Inputs/Data Charts.xlsx'

class ALengine():
    def __init__ (self):
        #updated these, sheet names with some columns

        self.dataInputAnnual= rest["data_input_annually"]
        self.bxImports_rawData = rest["al_quart_chart_raw_trade_data"]
        self.bxImports_tab1 = rest["al_quart_chart_table_1"]
        self.bxImports_tab2 = rest["al_quart_chart_smb_split_tonnes"]
        self.bxImports_tab3 = pd.read_excel(bx_imports_wbk, sheet_name='Re-done tonnes')#table3
        self.bxImports_tab4 = pd.read_excel(bx_imports_wbk, sheet_name='Re-done proportions within Ctry')#table4
        self.bxImports_tab5 = rest["al_quart_chart_proportions"]
        self.bxImports_tab6 = rest["al_quart_chart_tt_for_different_bauxite"]
        self.bxImports_tab7 = rest["al_quart_chart_tt_per_country"]
        self.bxImports_tab8 = pd.read_excel(bx_imports_wbk, sheet_name='Tt per Country no gaps')#tab8
        self.Changjiang_LME_tab1 = rest["changjiang_lme_daily_return_to_contents"]
        self.Changjiang_LME_tab2 = rest["changjiang_lme_daily_inputs"]
        self.Changjiang_LME_tab2[['Date 1', 'Date 2', 'Date 3']] = rest["changjiang_lme_daily_inputs"][['Date 1', 'Date 2', 'Date 3']].astype('datetime64[ns]')
        self.platts_vs_cmaax = rest["platts_vs_cmaax_inputs"]
        self.platts_vs_cmaax["Month"] = rest["platts_vs_cmaax_inputs"]["Month"].astype('datetime64[ns]')
        self.otherInput_bx_tab1 = rest["other_inputs_bxpro"]
        self.otherInput_bx_tab2 = rest["other_inputs_bxpro_1"]
        self.ref_AA_productions = rest["Refineries_Aa_Production_1"]
        self.data_inp_montly_tab1 = rest["alumina_monthly_supply"]
        self.data_inp_montly_tab2 = rest["aluminium_monthly_supply"]
        self.data_inp_montly_tab3 = rest["alumina_price_trend"]
        self.data_inp_montly_bax1 = rest["data_input_monthly_bauxite_imports"]
        self.data_inp_montly_bax2 = rest["data_input_monthly_bauxite_imports_2"]
        self.data_inp_montly_bax3 = rest["data_input_monthly_bauxite_imports_3"]
        self.data_inp_montly_bax4 = rest["data_input_monthly_bauxite_imports_4"]
        self.data_inp_montly_bax5 = rest["data_input_monthly_bauxite_imports_5"]
        self.data_inp_montly_baxStyle = rest["data_input_monthly_bauxite_style"]
        self.data_inp_montly_exchange_rate = rest["data_input_monthly_exchange_rate"]
        self.data_inp_montly_inpBAR = rest["data_input_monthly_input_bar"]
        self.dataInputMontly_BAR = rest["data_input_monthly_bar"]
        self.data_inp_montly_bauxite_usage = rest["data_input_monthly_bauxite_usage"]
        self.data_imported_bauxite = rest["data_input_monthly_aa_imported_bauxite"]
        self.dataInputMontly_CmPortBx = rest["data_input_monthly_cm_port_bx_inventry"]
        self.dataInputMontly_global_alumina = rest["data_input_monthly_global_alumina_prd"]
        self.AAproductions_main         = rest["aa_prodn_from_import_bx_mthly"]
        self.AAproductions_bx_consumed  = rest["aa_prodn_from_import_bx_mthly_import_bx_consumed"]
        self.AAproductions_sheet3       = rest["aa_prodn_from_import_bx_mthly_1"]
        self.minesBxPrdocution1 = rest["mines_bx_production_1"]
        self.minesBxPrdocution2 = rest["mines_bx_production_2"]
        self.minesBxPrdocution3 = rest["mines_bx_production_3"]
        self.BauxiteSources_inputs = rest["al_quart_chart_bauxite_sources"]
        self.data_monthly_inp_comspt = rest["Data_Monthly_Inputs"]
        self.data_monthly_imp_CGA = rest["al_quart_chart_CGA"]
        self.data_monthly_imp_aaImportedBauxite = rest["al_quart_chart_aa_from_imported_baux"]
        self.data_monthly_imp_CMerrorAdjustment = rest["al_quart_chart_CM_Imports_Bx_error_Adjustment"]
        self.data_monthly_imp_ImportedBauxiteStocks = rest["al_quart_chart_imported_bauxite_stocks_major"]
        self.GlobalBx_cmglobal      = rest["Global_Bx_Quarterly_inputs"]
        self.GlobalBx_rioReport     = rest["Rio_Quarterly_Report"]
        self.GlobalBx_russelReport  = rest["Rusal_Quarterly_Report"]
        self.GlobalBx_hydro         = rest["Hydro_Quarterly_Bx_"]
        self.GlobalBx_south32       = rest["South_32_Alumina_Production"]
        self.GlobalBx_hindalco      = rest["Hindalco"]
        self.GlobalBx_vedanta       = rest["Vedanta"]
        self.GlobalBx_nalco         = rest["NALCO"]
        self.GlobalBx_productionMainCompanies = rest["Production_for_Main_Companies"]
        self.data_charts_11 = rest["al_quart_chart_chart11"]
        self.data_charts_12 = rest["al_quart_chart_chart12"]
        self.data_charts_9 = rest["al_quart_chart_chart9"]
        self.data_chartt_11 = rest["al_quart_chart_chartt_11"]
        self.data_chartt_12 = rest["al_quart_chart_chartt_12"]
        self.cm_global_prd   = rest["cm_global_bx_production_inputs"]
        self.cm_global_prd_chineseSum = rest["summary_chinese_province"] 
        self.cm_global_prd_china_provincial = rest["china_provincial_imp"]
        
        # self.dataInputAnnual= pd.read_excel(data_input_annual_wbk, sheet_name='Sheet1')
        # self.bxImports_rawData = pd.read_excel(bx_imports_wbk, sheet_name='Raw trade data')
        # self.bxImports_tab1 = pd.read_excel(bx_imports_wbk, sheet_name='Table 1')
        # self.bxImports_tab2 = pd.read_excel(bx_imports_wbk, sheet_name='SMB split tonnes')#table2
        # self.bxImports_tab3 = pd.read_excel(bx_imports_wbk, sheet_name='Re-done tonnes')#table3
        # self.bxImports_tab4 = pd.read_excel(bx_imports_wbk, sheet_name='Re-done proportions within Ctry')#table4
        # self.bxImports_tab5 = pd.read_excel(bx_imports_wbk, sheet_name='Proportions')#table5
        # self.bxImports_tab6 = pd.read_excel(bx_imports_wbk, sheet_name='Tt for different bauxite') #tab6
        # self.bxImports_tab7 = pd.read_excel(bx_imports_wbk, sheet_name='Tt per Country')#tab7
        # self.bxImports_tab8 = pd.read_excel(bx_imports_wbk, sheet_name='Tt per Country no gaps')#tab8
        
        # self.Changjiang_LME_tab1 = pd.read_excel('./Inputs/Changjiang & LME Daily inputs.xlsx', sheet_name='Table 1') 
        # self.Changjiang_LME_tab2 = pd.read_excel('./Inputs/Changjiang & LME Daily inputs.xlsx', sheet_name='Table 2')
        
        '''
        # self.Changjiang_LME_tab1 = pd.read_excel('./Inputs/Changjiang & LME Daily.xlsx', sheet_name='Return to Contents')
        # self.Changjiang_LME_tab2 = pd.read_excel('./Inputs/Changjiang & LME Daily.xlsx', sheet_name='Main Table')
        '''
        # self.platts_vs_cmaax = pd.read_excel('./Inputs/Platts Vs CMAAX.xlsx', sheet_name='Main Sheet')
        
        # self.otherInput_bx_tab1 = pd.read_excel(other_inputs_bx_wbk, sheet_name='Table 1')
        # self.otherInput_bx_tab2 = pd.read_excel(other_inputs_bx_wbk, sheet_name='Table 2')
        # self.ref_AA_productions = pd.read_excel(ref_aa_prod_wbk, sheet_name='Sheet1')
        
        # self.data_inp_montly_tab1 = pd.read_excel(data_input_monthly_wbk, sheet_name='Alumina Monthly Supply')
        # self.data_inp_montly_tab2 = pd.read_excel(data_input_monthly_wbk, sheet_name='Aluminuim Monthly Supply')
        # self.data_inp_montly_tab3 = pd.read_excel(data_input_monthly_wbk, sheet_name='Alumina Price Trend')
        # self.data_inp_montly_bax1 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports')
        # self.data_inp_montly_bax2 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 2')
        # self.data_inp_montly_bax3 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 3')
        # self.data_inp_montly_bax4 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 4')
        # self.data_inp_montly_bax5 = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Imports 5')
        # self.data_inp_montly_baxStyle = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Style')
        # self.data_inp_montly_exchange_rate =pd.read_excel(data_input_monthly_wbk, sheet_name='Exchange Rate(R;U)')
        # self.data_inp_montly_inpBAR = pd.read_excel(data_input_monthly_wbk, sheet_name='Input BAR')
        # self.dataInputMontly_BAR = pd.read_excel(data_input_monthly_wbk, sheet_name='BAR')
        # self.data_inp_montly_bauxite_usage = pd.read_excel(data_input_monthly_wbk, sheet_name='Bauxite Usage')
        # self.data_imported_bauxite = pd.read_excel(data_input_monthly_wbk, sheet_name="Aa Imported Bauxite")
        # self.dataInputMontly_CmPortBx = pd.read_excel(data_input_monthly_wbk, sheet_name='CM Port Bx Inventry')
        # self.dataInputMontly_global_alumina = pd.read_excel(data_input_monthly_wbk, sheet_name='Global Alumina Prd')
        
        ### updated these
        # self.AAproductions = pd.read_excel(aa_production_bx_wbk, sheet_name='Sheet1')
        # self.AAproductions_main         = pd.read_excel(aa_production_bx_wbk, sheet_name='Main Sheet')
        # self.AAproductions_bx_consumed  = pd.read_excel(aa_production_bx_wbk, sheet_name='Import Bx Consumed')
        # self.AAproductions_sheet3       = pd.read_excel(aa_production_bx_wbk, sheet_name='Sheet3')
        self.AAproductions_out1 = pd.DataFrame(columns=self.AAproductions_main.columns)
        self.AAproductions_out2 = pd.DataFrame(columns=self.AAproductions_main.columns)
        self.AAproductions_inland_ref = pd.DataFrame(columns=self.AAproductions_bx_consumed.columns)
        self.AAproductions_summary_prodTotal = pd.DataFrame(columns=self.AAproductions_main.columns)
        self.AAproductions_inner_mongolia_chart_shandong = pd.DataFrame(columns=[])
        
        self.AAproductions_summary_AAprodHT = pd.DataFrame(columns=self.AAproductions_main.columns)
        self.AAproductions_summary_AAprodLT = pd.DataFrame(columns=self.AAproductions_main.columns)
        self.AAproductions_summary_totalCapImpBx = pd.DataFrame(columns=[])
        self.AAproductions_summary_modelled_991_1003 = pd.DataFrame(columns=[])
        
        self.AAproductions_summary_capTotal = pd.DataFrame(columns=[])
        self.AAproductions_summary_AAcapHT  = pd.DataFrame(columns=[])
        self.AAproductions_summary_AAcapLT  = pd.DataFrame(columns=[])
        self.AAproductions_summary_totalCapImpBx_cap = pd.DataFrame(columns=[])
        self.AAproductions_summary_alumina_production = pd.DataFrame(columns=[])
        self.AAproductions_summary_data_to_chart_sheet = pd.DataFrame(columns=[])
        '''
        # self.AAproductions_main         = pd.read_excel(aa_production_bx_wbk, sheet_name='Main Sheet')
        # self.AAproductions_bx_consumed  = pd.read_excel(aa_production_bx_wbk, sheet_name='Import Bx Consumed')
        # self.AAproductions_sheet3       = pd.read_excel(aa_production_bx_wbk, sheet_name='Sheet3')
        self.AAproductions_out1 = pd.DataFrame(columns=[])
        '''
        #columns name, removed only index column names
        # self.minesBxPrdocution1 = pd.read_excel(mines_bx_prd_wbk, sheet_name='Sheet1')
        # self.minesBxPrdocution2 = pd.read_excel(mines_bx_prd_wbk, sheet_name='Sheet2')
        # self.minesBxPrdocution3 = pd.read_excel(mines_bx_prd_wbk, sheet_name='Sheet3')
        self.minesBxPrdocution_final = pd.DataFrame(columns=[], index=self.minesBxPrdocution1.index)
        
        #updated 
        # self.BauxiteSources_inputs = pd.read_excel(bauxite_sources_wbk, sheet_name='Main Sheet')
        cols = ['No','Refinery Name',"Supplying Mines",'BAR']+self.BauxiteSources_inputs.columns[3:].to_list()
        self.BauxiteSources = pd.DataFrame(columns=cols)
        self.BauxiteSourcesSummary = pd.DataFrame(columns=[])
        
        # self.cm_global_prd              = pd.read_excel(cm_global_wbk, sheet_name='Sheet1')
        # self.cm_global_prd_chineseSum   = pd.read_excel(cm_global_wbk, sheet_name='Summary Chinese Province')
        # self.cm_global_prd_china_provincial = pd.read_excel(cm_global_wbk, sheet_name='China Provincial Imp')

        
        
        # self.data_monthly_inp_comspt = pd.read_excel(data_monthly_wbk, sheet_name='Sheet1')
        # self.data_monthly_imp_CGA = pd.read_excel(data_monthly_wbk, sheet_name="CGA")
        #added 3 more input files to data monthly
        # self.data_monthly_imp_aaImportedBauxite = pd.read_excel(data_monthly_wbk, sheet_name="Aa from Imported Bauxite")
        # self.data_monthly_imp_CMerrorAdjustment = pd.read_excel(data_monthly_wbk, sheet_name="CM Import Bx-Error Adjustment")
        # self.data_monthly_imp_ImportedBauxiteStocks = pd.read_excel(data_monthly_wbk, sheet_name="Imported Bauxite Stocks-Major P")
        
        #updated inputs for global bx
        self.GlobalBx_cmglobal      = pd.read_excel(global_bx_wbk, sheet_name='CM Global Bx Production')
        # self.GlobalBx_rioReport     = pd.read_excel(global_bx_wbk, sheet_name='Rio Quarterly Report')
        # self.GlobalBx_russelReport  = pd.read_excel(global_bx_wbk, sheet_name='Rusal Quarterly Report')
        # self.GlobalBx_hydro         = pd.read_excel(global_bx_wbk, sheet_name='Hydro Quarterly Bx ')
        # self.GlobalBx_south32       = pd.read_excel(global_bx_wbk, sheet_name='South 32 Alumina Production')
        # self.GlobalBx_hindalco      = pd.read_excel(global_bx_wbk, sheet_name='Hindalco')
        # self.GlobalBx_vedanta       = pd.read_excel(global_bx_wbk, sheet_name='Vedanta')
        # self.GlobalBx_nalco         = pd.read_excel(global_bx_wbk, sheet_name='NALCO')
        # self.GlobalBx_productionMainCompanies = pd.read_excel(global_bx_wbk, sheet_name='Production for Main Companies')
        self.GlobalBx_out_tab1 = pd.DataFrame(columns=[])
        self.GlobalBx_out_tab2 = pd.DataFrame(columns=[])
        self.GlobalBx_out_tab3 = pd.DataFrame(columns=[])
        
        self.special_charts_1 = pd.DataFrame(columns=[])
        self.special_charts_2 = pd.DataFrame(columns=[])
        
        # self.data_charts_11 = pd.read_excel(data_chartss_wbk, sheet_name='Chart11')
        # self.data_charts_12 = pd.read_excel(data_chartss_wbk, sheet_name='Chart12')
        # self.data_charts_9 = pd.read_excel(data_chartss_wbk, sheet_name='Chart9')
        # self.data_chartt_11 = pd.read_excel(data_chartss_wbk, sheet_name='chartt 11')
        # self.data_chartt_12 = pd.read_excel(data_chartss_wbk, sheet_name='chartt 12')  
        
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
        self.dataInputMontly_alumina_price_trend = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
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
        self.Alumina_Province = pd.DataFrame()
        self.Bauxite_Usage_Sheet = pd.DataFrame()
        self.Alumina_Province = pd.DataFrame()
        self.Bauxite_Usage_Sheet = pd.DataFrame()
        self.Imported_Bauxite_Sheet = pd.DataFrame()
        self.Global_Alumina_Production_Sheet = pd.DataFrame()
        self.Merchant_Alumina_Prod_sheet = pd.DataFrame()
        self.CGA_Sheet = pd.DataFrame()
        self.Bauxite_Usage_t_Sheet = pd.DataFrame()
        self.Bauxite_Demand_Sheet = pd.DataFrame()
        self.Domestic_Bauxite_Demand_Sheet = pd.DataFrame()
        self.Domestic_Bauxite_Summary_Sheet = pd.DataFrame()
        self.Import_Bauxite_Summary_Sheet = pd.DataFrame()


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
        self.dataCharts_11 = pd.DataFrame(columns=['Operating','Long Term Idled','idle','Stack for Chart Making'] )#, index=self.data_inp_montly_tab1.index)
        self.dataCharts_12 = pd.DataFrame(columns=['Operating','Long Term Idled','idle','Stack for Chart Making'] )#, index=self.data_inp_montly_tab1.index)
        self.dataCharts_13 = pd.DataFrame(columns=[], index=self.data_inp_montly_tab1.index)
        self.dataCharts__10 = pd.DataFrame(columns=[], index=self.data_input_annual.index)
        self.dataCharts__11 = pd.DataFrame(columns=[], index=self.data_input_annual.index)
        
    #engine run all
    #engine run all
    def calc_all(self):
        self.calc_data_input_annual()
        self.calc_bx_imports_1()
        self.calc_bx_imports_2()
        self.calc_bx_imports_3()
        self.calc_bx_imports_4()
        self.calc_bx_imports_5()
        self.calc_bx_imports_7()
        self.calc_bx_imports_8()
        self.calc_bx_imports_9()
        self.calc_ref_aa_productions()
        self.calc_Changjiang_LME_Daily()
        self.calc_platts_vs_cmaax()
        
        
        
        self.calc_data_input_monthly_1()
        self.calc_data_input_monthly_2()
        self.calc_data_input_monthly_2_price_trends()
        self.calc_data_input_monthly_3()
        self.calc_data_input_monthly_4()
        self.calc_data_input_monthly_5()
        self.calc_data_input_monthly_6()
        self.calc_data_input_monthly_7()
        
        #here
        self.calc_AA_production_1()
        self.calc_AA_production_2()
        self.calc_AA_production_3()
        self.calc_AA_production_4()
        self.calc_AA_production_5()
        self.calc_AA_production_6()
        self.calc_AA_production_7()
        self.calc_AA_production_8()
        self.calc_AA_production_9()
        self.calc_AA_production_10()
        self.calc_AA_production_11()
        self.calc_AA_production_12()
        self.calc_AA_production_13()
        self.calc_AA_production_14()
        
        #here
        
        self.calc_data_monthly_1()
        self.calc_data_monthly_2()
        self.calc_data_monthly_3()
        self.calc_data_monthly_4()
        
        
        self.calc_data_charts_1()
        self.calc_special_charts()
        
        self.calc_mines_bx_production_1()
        self.calc_bauxite_sources_1()
        self.calc_mines_bx_production_2()
        self.calc_mines_bx_production_3()
        
        self.calc_cm_global_prd()
        
        self.calc_global_bx_quarterly_1()
        
        '''
        self.calc_AA_production_1()
        self.calc_AA_production_2()
        self.calc_AA_production_3()
        self.calc_AA_production_4()
        self.calc_AA_production_5()
        self.calc_AA_production_6()
        self.calc_AA_production_7()
        self.calc_AA_production_8()
        self.calc_AA_production_9()
        self.calc_AA_production_10()
        self.calc_AA_production_11()
        self.calc_AA_production_12()
        self.calc_AA_production_13()
        self.calc_AA_production_14()
        '''
        self.calc_data_charts_2_charts10()
        
        self.calc_data_input_monthly_8()
        self.Alumina_By_Province_Mt_Aa()
        self.CGA()
        self.Global_Alumina_Production()
        self.Bauxite_Usage()
        self.Bauxite_Usage_t_t()
        self.Aa_Imported_Bauxite()
        self.Merchant_Alumina_Prod()
        self.Bauxite_Demand()
        self.Domestic_Bauxite_Demand()
        self.Domestic_Bauxite_Summary()
        self.Import_Bauxite_Summary()
        
        
    
    def calc_data_input_annual(self):
        self.data_input_annual['Year'] = self.dataInputAnnual['Year']
        self.data_input_annual['Alumina Capacity'] = self.dataInputAnnual['Alumina Capacity']
        self.data_input_annual['Alumina Production'] = self.dataInputAnnual['Alumina Production']
        self.data_input_annual['Alumina Utilisation'] =self.data_input_annual['Alumina Production']/self.data_input_annual['Alumina Capacity']
        self.data_input_annual['Aluminium Capacity'] = self.dataInputAnnual['Aluminium Capacity']
        self.data_input_annual['Aluminium Production'] = self.dataInputAnnual['Aluminium Production']
        self.data_input_annual['Aluminium Utilisation'] = self.data_input_annual['Aluminium Production'] /self.data_input_annual['Aluminium Capacity']
    
    
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
        print(self.platts_vs_cmaax['Australia (FOB WA)'] , self.platts_vs_cmaax['Frieght'])
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
        
   #bx imports
    def calc_bx_imports_1(self):
        for col in self.bxImports_tab2.columns[1:]:
            print(col)
            month= int(col[:3])
            year = int(col[-4:])
            for row in self.bxImports_tab1.index:
                value = 0
                if pd.isna(self.bxImports_tab1.loc[row,'Regency']):
                    value = self.bxImports_rawData.loc[(self.bxImports_rawData['Country of Origin']==self.bxImports_tab1.loc[row,'Country']) & (self.bxImports_rawData['Year']==year) & (self.bxImports_rawData['Month']==month), 'Tonnage'].astype(float).sum()
                else:
                    value = self.bxImports_rawData.loc[(self.bxImports_rawData['Country of Origin']==self.bxImports_tab1.loc[row,'Country']) & (self.bxImports_rawData['Regency']==self.bxImports_tab1.loc[row,'Regency']) &(self.bxImports_rawData['Year']==year)& (self.bxImports_rawData['Month']==month), 'Tonnage'].astype(float).sum()
                self.bxImports_tab1.at[row, col] = value
        self.bxImports_tab1.at[4,' 9 - 2018'] = 0
        
        
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
     
    #re dones tonnes
    def calc_bx_imports_3(self):
        for col in self.bxImports_tab2.columns[1:]:
            for i in range(11):
                self.bxImports_tab3.at[i, col] = self.bxImports_tab1.loc[i, col].copy()
            for i in range(11,18):
                self.bxImports_tab3.at[i, col] = self.bxImports_tab1.loc[11, col]*self.bxImports_tab2.loc[i-11, col]/float(self.bxImports_tab2[col].sum())
            
            for i in range(18, len(self.bxImports_tab3)):
                self.bxImports_tab3.at[i, col] = self.bxImports_tab1.loc[i-6, col].copy()
    
    #redonne proportions
    def calc_bx_imports_4(self):
        for col in self.bxImports_tab2.columns[1:]:
            for row in self.bxImports_tab4.index:
                self.bxImports_tab4.at[row,col] =self.bxImports_tab1.loc[self.bxImports_tab1['Country']==self.bxImports_tab4.loc[row,'Country'], col].sum()
       
    #proportions
    def calc_bx_imports_5(self):
        d = self.bxImports_tab4['Country'].to_list()
        for row in self.bxImports_tab5.index:
            self.bxImports_tab5.at[row,'Index'] = d.index(self.bxImports_tab5.loc[row,'Country'])+1
        for col in self.bxImports_tab2.columns[1:]:
            for row in self.bxImports_tab5.index:
                value = 0
                l = self.bxImports_tab4[col].to_list()
                if l[int(self.bxImports_tab5.loc[row,'Index'])-1] <=0:
                    value = 0
                else:
                    value = self.bxImports_tab3.loc[row,col]/l[int(self.bxImports_tab5.loc[row,'Index'])-1]
                self.bxImports_tab5.at[row,col] = value
                
    #t/t per country
    def calc_bx_imports_7(self):
        for col in self.bxImports_tab2.columns[1:]:
            for row in self.bxImports_tab7.index:
                value =self.bxImports_tab5.loc[self.bxImports_tab5['Country']==self.bxImports_tab7.loc[row,'Country'], col] * self.bxImports_tab6.loc[self.bxImports_tab6['Country']==self.bxImports_tab7.loc[row,'Country'], col]
                self.bxImports_tab7.at[row,col] = value.sum()
        
    #t/t per country no gaps
    def calc_bx_imports_8(self):
        coll =self.bxImports_tab2.columns[1]
        for row in self.bxImports_tab8.index:
            value = self.bxImports_tab5.loc[self.bxImports_tab5['Country']==self.bxImports_tab8.loc[row,'Country'], coll] * self.bxImports_tab6.loc[self.bxImports_tab6['Country']==self.bxImports_tab8.loc[row,'Country'], coll]
            self.bxImports_tab8.at[row, coll] = value.sum()
        cols1 = self.bxImports_tab2.columns[2:].to_list()
        cols2 = self.bxImports_tab2.columns[1:].to_list()
        for i in range(len(cols1)):
            for row in self.bxImports_tab8.index:
                self.bxImports_tab8.at[row, cols1[i]] = self.bxImports_tab7.loc[row, cols1[i]] if self.bxImports_tab7.loc[row, cols1[i]]>0 else self.bxImports_tab8.loc[row, cols2[i]]
                #print(self.bxImports_tab7.loc[row, cols1[i]] if self.bxImports_tab7.loc[row, cols1[i]]>0 else self.bxImports_tab8.loc[row, cols2[i]])
    
    #Australia LT/HT split
    def calc_bx_imports_9(self):   
        self.bxImports_tab9['Country'] = ['Australia','Australia','ctz','Australia','Australia']
        self.bxImports_tab9['Technology Code'] = ['LT','HT','','LT','HT']
        
        self.bxImports_tab3_copy['Country'] = self.bxImports_tab6['Country']
        self.bxImports_tab3_copy['Regency'] = self.bxImports_tab6['Regency']
        self.bxImports_tab3_copy['Technology Code'] = self.bxImports_tab6['Technology Code']
        for col in self.bxImports_tab2.columns[1:]:
            self.bxImports_tab3_copy[col] = self.bxImports_tab3[col]
        for col in self.bxImports_tab3.columns[3:]:
            for row in range(2):
                self.bxImports_tab9.at[row, col] = self.bxImports_tab3_copy.loc[(self.bxImports_tab3_copy['Technology Code']==self.bxImports_tab9.loc[row,'Technology Code']) &(self.bxImports_tab3_copy['Country']==self.bxImports_tab9.loc[row,'Country'] ), str(col)].sum()
            self.bxImports_tab9.at[2, col] = self.bxImports_tab9.loc[:2,col].sum() - self.bxImports_tab4.loc[0,col]
            self.bxImports_tab9.at[3, col] = self.bxImports_tab9.loc[0,col]/self.bxImports_tab4.loc[0,col]  if(self.bxImports_tab4.loc[0,col])>0 else 0
            self.bxImports_tab9.at[4, col] = self.bxImports_tab9.loc[1,col]/self.bxImports_tab4.loc[0,col]  if(self.bxImports_tab4.loc[0,col])>0 else 0
        
    
    
    #data input monthly
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
        for col in self.data_inp_montly_tab2.columns[:-3]:
            table[col] = self.data_inp_montly_tab2[col]
        for row in range(120, self.data_inp_montly_tab2.shape[0]):
           table.loc[row,'Total'] = table.loc[row,['Henan','Shandong','Inner Mongolia','Gansu','Qinghai', 'Shanxi','Guizhou', 'Yunnan', 'Sichuan', 'Guangxi', 'Ningxia', 'Shaanxi','Hubei', 'Jiangsu', 'Zhejiang', 'Chongqing', 'Fujian', 'Hunan','Liaoning', 'Xinjiang', 'Heilongjiang', 'Hebei', 'Jiangxi', 'Jilin']].sum()
        self.dataInputMontly_aluminium_montly_supply =table

    #price trends
    def calc_data_input_monthly_2_price_trends(self):
        table = self.dataInputMontly_alumina_price_trend.copy()
        for col in ['Date','Year','Quarter', 'Rmb/t Aa exc VAT', 'Chalco Spot', 'Non-Chalco Spot', 'CMAAX (VAT incl.)']:
            table[col] = self.data_inp_montly_tab3[col]
    
        as_vals = self.data_inp_montly_tab3['Aluminina Imports Price - Australia'].copy()
        data = self.platts_vs_cmaax[['Month','Australia (FOB WA)']].copy()
        data['year'] = [i.year for i in data['Month']]
        data['Month'] = [i.month for i in data['Month']]
        for row in range(278, len(as_vals)):
            month = self.data_inp_montly_tab3.loc[row,'Date'].month
            year  = self.data_inp_montly_tab3.loc[row,'Date'].year
            as_vals[row] = data.loc[(data['year']==year)&(data['Month']==month), 'Australia (FOB WA)'].mean()
        table['Imported CIF'] = [( float('nan') if as_vals[i]==0 else (as_vals[i]* (1+self.data_inp_montly_tab3.loc[i,'Import Duties'])*self.data_inp_montly_exchange_rate.loc[i,'Exchange Rate']))  for i in range(len(table)) ]
        table['Import Duties'] = self.data_inp_montly_tab3['Import Duties']
        table['VAT Rate'] = self.data_inp_montly_tab3['VAT Rate']
        table["Imported CIF (TAX incl.)"] =[( float(table.loc[i,'Imported CIF'])*(1+float(table.loc[i,'Import Duties']))*(1+float(table.loc[i,'VAT Rate']))) for i in range(len(table))]
        
        table['US$/t Chalco Spot'] = table['Chalco Spot'].astype(float)/ self.data_inp_montly_exchange_rate['Exchange Rate']
        table['US$/t Non-Chalco Spot'] = table['Non-Chalco Spot'].astype(float)/ self.data_inp_montly_exchange_rate['Exchange Rate']
        table['US$/t Imported CIF'] = table['Imported CIF'].astype(float)/ self.data_inp_montly_exchange_rate['Exchange Rate']
        
        table['Alumina Imports Price - Austalia '] = as_vals
        
        for col in  ['Changjiang Spot Cargo','LME (USD)', 'LME (RMB)']:
            table[col] = self.data_inp_montly_tab3[col]
        table["VAT of (LME Price + Taxes)"] = [(float(table.loc[i,'LME (RMB)'])*float(table.loc[i,'VAT Rate'])*(1+(float(table.loc[i,'Import Duties']))))for i in range(len(table))]
        
        self.dataInputMontly_alumina_price_trend = table
        #self.data_inp_montly_tab3
    
    


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
        for col in ['Indonesia', 'Australia', 'India', 'Malaysia', 'Guinea']:
            table[col] = self.data_inp_montly_bax3[col]
        for col in ['Indonesia', 'Australia', 'India', 'Malaysia', 'Guinea']:
            for row in range(144, len(self.data_inp_montly_bax3)):
                table.at[row, col] = (self.data_inp_montly_bax5.loc[row, col]) if float(self.data_inp_montly_bax5.loc[row, col])>0 else float('nan')
        self.dataInputMontly_bauxite_imports3 = table
        #come back here okay
        
        #four
        table =self.dataInputMontly_bauxite_imports4.copy()
        for col in ['Date','Year', 'Quarter']:
            table[col] = self.data_inp_montly_tab1[col]
        for col in self.data_inp_montly_bax4.columns[4:]:
            table[col] =self.data_inp_montly_bax4[col]
        for col in self.data_inp_montly_bax4.columns[4:-4]:
            for row in range(288, len(table)):
                bb = table['Date'][row]
                date_col = ' '+ str(bb.month)+' - '+str(bb.year)
                #print(self.bxImports_tab4.loc[self.bxImports_tab4['Country']==col, date_col].copy())
                table.loc[row, col] = float(self.bxImports_tab4.loc[self.bxImports_tab4['Country']==col, date_col].copy())
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
        
        cols =self.data_inp_montly_baxStyle.columns[4:]
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
        cols = self.dataInputMontly_CmPortBx.columns[3:-2]
        for row in range(230,274):
            self.dataInputMontly_CmPortBx.loc[row, 'CM Port, Million DMT'] =self.dataInputMontly_CmPortBx.loc[row, cols].sum() * 0.9
        for row in range(284, self.dataInputMontly_CmPortBx.shape[0]):
            self.dataInputMontly_CmPortBx.loc[row, 'CM Port, Million DMT'] =self.dataInputMontly_CmPortBx.loc[row, cols].sum() * 0.9
        
    def calc_data_input_monthly_7(self):
        cols = self.dataInputMontly_global_alumina.columns[3:]
        self.dataInputMontly_global_alumina['Total (ex China)'] = self.dataInputMontly_global_alumina[cols].sum(axis=1)
    
    #please run affter aa productions
    def calc_data_input_monthly_8(self):
        for row in range(264, len(self.dataInputMontly_inland1)-1):
            self.dataInputMontly_inland1.at[row, 'Import Bx consumed'] = self.AAproductions_bx_consumed.loc[0, self.AAproductions_bx_consumed.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Henan']       = self.AAproductions_bx_consumed.loc[1, self.AAproductions_bx_consumed.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Shanxi']      = self.AAproductions_bx_consumed.loc[2, self.AAproductions_bx_consumed.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Guizhou']     = self.AAproductions_bx_consumed.loc[3, self.AAproductions_bx_consumed.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Guangxi']     = self.AAproductions_bx_consumed.loc[4, self.AAproductions_bx_consumed.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Chongqing']   = self.AAproductions_bx_consumed.loc[5, self.AAproductions_bx_consumed.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Other']       = self.AAproductions_bx_consumed.loc[6, self.AAproductions_bx_consumed.columns[row-203]]
            
            self.dataInputMontly_inland1.at[row, ''] = ''
            
            self.dataInputMontly_inland1.at[row, 'Alumina - Henan']     = self.AAproductions_inland_ref.loc[0, self.AAproductions_inland_ref.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Alumina - Shanxi']    = self.AAproductions_inland_ref.loc[1, self.AAproductions_inland_ref.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Alumina - Guizhou']   = self.AAproductions_inland_ref.loc[2, self.AAproductions_inland_ref.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Alumina - Guangxi']   = self.AAproductions_inland_ref.loc[3, self.AAproductions_inland_ref.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Alumina - Chongqing'] = self.AAproductions_inland_ref.loc[4, self.AAproductions_inland_ref.columns[row-203]]
            self.dataInputMontly_inland1.at[row, 'Alumina - Other']     = self.AAproductions_inland_ref.loc[5, self.AAproductions_inland_ref.columns[row-203]]

    
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
        self.dataMonthly_consumpt['Imported Bauxite'] = self.data_monthly_inp_comspt['Imported Bauxite - revised']
        self.dataMonthly_consumpt['Domestic Bauxite'] = self.data_monthly_inp_comspt['Domestic Bauxite - revised']
        self.dataMonthly_consumpt['Bauxite Requirement'] = self.data_monthly_inp_comspt['Bauxite Requirement - revised']
        self.dataMonthly_consumpt['% Bauxite Consump'] = self.data_monthly_inp_comspt['% Bauxite Consumption Domestic - revised']
        v = [i.to_pydatetime()  for i in self.dataMonthly_consumpt['Date']]
        k = v.index(datetime(2018,1,1,0,0)) #276
        for i in range(k, self.dataMonthly_consumpt.shape[0]):
            self.dataMonthly_consumpt.at[i,'Imported Bauxite'] = self.AAproductions_summary_modelled_991_1003.loc[6,v[i]]
            self.dataMonthly_consumpt.at[i,'Domestic Bauxite'] = ( self.dataMonthly_alumina_monthly.loc[276,'Total Production'] - self.AAproductions_summary_modelled_991_1003.loc[7,v[i]])
            self.dataMonthly_consumpt.at[i,'Bauxite Requirement'] = self.dataMonthly_consumpt.loc[i,'Imported Bauxite'] + self.dataMonthly_consumpt.loc[i,'Domestic Bauxite']
            self.dataMonthly_consumpt.at[i,'% Bauxite Consump'] = self.dataMonthly_consumpt.loc[i,'Domestic Bauxite'] /self.dataMonthly_consumpt.loc[i,'Bauxite Requirement']
        #self.AAproductions_summary_modelled_991_1003
        '''
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
        '''
        
    #data charts
    def calc_data_charts_1(self):
        for col in self.dataMonthly_bauxite_imps1.columns:
            self.dataCharts_1[col] = self.dataMonthly_bauxite_imps1[col]
        for col in self.dataMonthly_bauxite_imps3.columns:
            self.dataCharts_1a[col] = self.dataMonthly_bauxite_imps3[col]
        #6+7
        self.dataCharts_2['Date'] = self.dataCharts_1['Date']
        self.dataCharts_2['Year'] = self.dataCharts_1['Year']
        self.dataCharts_2['Quarter'] = self.dataCharts_1['Quarter']
        self.dataCharts_2['Refineries'] =2
        self.dataCharts_2['Port'] = self.dataInputMontly_CmPortBx['CM Port, Million DMT']
        
        self.dataCharts_3['Date'] = self.dataCharts_1['Date']
        self.dataCharts_3['Year'] = self.dataCharts_1['Year']
        self.dataCharts_3['Quarter'] = self.dataCharts_1['Quarter']
        self.dataCharts_3['Imported Bauxite'] =self.dataMonthly_consumpt['Imported Bauxite']
        self.dataCharts_3['Domestic Bauxite'] =self.dataMonthly_consumpt['Domestic Bauxite']
        self.dataCharts_3['% Bauxite Consumpt']=self.dataMonthly_consumpt['% Bauxite Consump']
        
        self.dataCharts_4['Date'] = self.dataCharts_1['Date']
        self.dataCharts_4['Year'] = self.dataCharts_1['Year']
        self.dataCharts_4['Quarter'] = self.dataCharts_1['Quarter']
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
        
        self.dataCharts_6['Date'] = self.dataCharts_1['Date']
        self.dataCharts_6['Year'] = self.dataCharts_1['Year']
        self.dataCharts_6['Quarter'] = self.dataCharts_1['Quarter']
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
        
        self.dataCharts_8['Date'] = self.dataCharts_1['Date']
        self.dataCharts_8['Year'] = self.dataCharts_1['Year']
        self.dataCharts_8['Quarter'] = self.dataCharts_1['Quarter']
        self.dataCharts_8['Net Import'] = self.dataMonthly_aluminium_supply['Net Import']
        self.dataCharts_8[''] =''
        self.dataCharts_8['Changjiang Spot Cargo'] = self.data_inp_montly_tab3['Changjiang Spot Cargo']
        self.dataCharts_8['LME (RMB)'] = 0
        self.dataCharts_8['FX Rate'] = 0
        #self.data_inp_montly_tab3
        #self.data_inp_montly_exchange_rate
        
        
        self.dataCharts_9['Date'] = self.dataCharts_1['Date']
        self.dataCharts_9['Year'] = self.dataCharts_1['Year']
        self.dataCharts_9['Quarter'] = self.dataCharts_1['Quarter']
        for col in self.data_charts_9.columns[3:5]:
            self.dataCharts_9[col] = self.data_charts_9[col]
        self.dataCharts_9['Australia BAR'] = 0
        
        self.dataCharts_9['Brazil tonnes'] = self.data_charts_9['Brazil tonnes']
        self.dataCharts_9['Brazil $/t'] = self.data_charts_9['Brazil $/t']
        self.dataCharts_9['Brazil BAR'] = 0
        
        self.dataCharts_9['Dominic Rep. tonnes'] = self.data_charts_9['Dominic Rep. Tonnes']
        self.dataCharts_9['Dominic Rep. $/t'] = self.data_charts_9['Dominic Rep. $/t']
        self.dataCharts_9['Dominic Rep. BAR'] = 0
   
        self.dataCharts_9['Fiji tonnes'] = self.data_charts_9['Fiji tonnes']
        self.dataCharts_9['Fiji $/t'] = self.data_charts_9['Fiji $/t']
        self.dataCharts_9['Fiji BAR'] = 0
        
        self.dataCharts_9['Ghana tonnes'] = self.data_charts_9['Ghana tonnes']
        self.dataCharts_9['Ghana $/t'] = self.data_charts_9['Ghana $/t']
        self.dataCharts_9['Ghana BAR'] = 0
        
        self.dataCharts_9['Ghana tonnes'] = self.data_charts_9['Ghana tonnes']
        self.dataCharts_9['Ghana $/t'] = self.data_charts_9['Ghana $/t']
        self.dataCharts_9['Ghana BAR'] = 0
        
        self.dataCharts_9['Guinea tonnes'] = self.data_charts_9['Guinea tonnes']
        self.dataCharts_9['Guinea $/t'] = self.data_charts_9['Guinea $/t']
        self.dataCharts_9['Guinea BAR'] = 0
        
        self.dataCharts_9['Guyana tonnes'] = self.data_charts_9['Guyana tonnes']
        self.dataCharts_9['Guyana $/t'] = self.data_charts_9['Guyana $/t']
        self.dataCharts_9['Guyana BAR'] = 0
        
        self.dataCharts_9['India tonnes'] = self.data_charts_9['India tonnes']
        self.dataCharts_9['India $/t'] = self.data_charts_9['India $/t']
        self.dataCharts_9['India BAR'] = 0
        
        self.dataCharts_9['Indonesia tonnes'] = self.data_charts_9['Indonesia tonnes']
        self.dataCharts_9['Indonesia $/t'] = self.data_charts_9['Indonesia $/t']
        self.dataCharts_9['Indonesia BAR'] = 0
        
        self.dataCharts_9['Jamaica tonnes'] = self.data_charts_9['Jamaica tonnes']
        self.dataCharts_9['Jamaica $/t'] = self.data_charts_9['Jamaica $/t']
        self.dataCharts_9['Jamaica BAR'] = 0
        
        self.dataCharts_9['Laos tonnes'] = self.data_charts_9['Laos tonnes']
        self.dataCharts_9['Laos $/t'] = self.data_charts_9['Laos $/t']
        self.dataCharts_9['Laos BAR'] = 0
        
        self.dataCharts_9['Saudi Arabia tonnes'] = self.data_charts_9['Saudi Arabia tonnes']
        self.dataCharts_9['Saudi Arabia $/t'] = self.data_charts_9['Saudi Arabia $/t']
        self.dataCharts_9['Saudi Arabia BAR'] = 0
        
        self.dataCharts_9['Malaysia tonnes'] = self.data_charts_9['Malaysia tonnes']
        self.dataCharts_9['Malaysia $/t'] = self.data_charts_9['Malaysia $/t']
        self.dataCharts_9['Malaysia BAR'] = 0
        
        countries = ['Australia', 'Brazil', 'Fiji', 'Ghana', 'Guinea', 'Guyana', 'India', 'Indonesia', 'Jamaica', 'Saudi Arabia','Malaysia']
        for country in countries:
            self.dataCharts_9.loc[216:, country+' '+'tonnes'] = self.dataInputMontly_bauxite_imports4.loc[216:, country]
            self.dataCharts_9.loc[216:, country+' '+'$/t'] = self.dataInputMontly_bauxite_imports5.loc[216:, country]
            self.dataCharts_9.loc[216:, country+' ' + 'BAR'] = self.dataInputMontly_input_bar.loc[216:, country]
        country= 'Dominic Rep.'
        self.dataCharts_9.loc[216:, country+' '+'tonnes'] = self.dataInputMontly_bauxite_imports4.loc[216:,'Dominican Republic']
        self.dataCharts_9.loc[216:, country+' '+'$/t'] = self.dataInputMontly_bauxite_imports5.loc[216:, 'Dominican Republic']
        self.dataCharts_9.loc[216:, country+' ' + 'BAR'] = self.dataInputMontly_input_bar.loc[216:, 'Dominican Republic']
        
        xx = ['Montenegro', 'Thailand','Turkey', 'Vietnam', 'Solomon Islands']
        for country in xx:
            self.dataCharts_9[ country+' '+'tonnes'] = self.dataInputMontly_bauxite_imports4[country].copy()
            self.dataCharts_9[ country+' '+'$/t'] = self.dataInputMontly_bauxite_imports5[country].copy()
            self.dataCharts_9[ country+' ' + 'BAR'] = self.dataInputMontly_input_bar[country].copy()
        
        self.dataCharts_9['HT BAR'] = self.dataInputMontly_BAR['HT'].copy()
        self.dataCharts_9['LT BAR'] = self.dataInputMontly_BAR['LT'].copy()
        self.dataCharts_9['combined BAR'] = self.dataInputMontly_BAR['Total Combined'].copy()
        
        self.data_chartt_11['Q4 idled'] = self.data_chartt_11['Q4 Capacity'] - self.data_chartt_11['Q4 Operational']
        self.data_chartt_11['Q3 idled'] = self.data_chartt_11['Q3 Capacity'] - self.data_chartt_11['Q3 Operational']
        row = 0
        for country in  ['Shandong','Henan','Shanxi','Guizhou', 'Guangxi']:
            self.dataCharts_11.at[row,'Operating'] = float(self.data_chartt_11.loc[self.data_chartt_11['Country']==country ,'Q4 Operational'])
            self.dataCharts_11.at[row+1,'Long Term Idled']=float(self.data_chartt_11.loc[self.data_chartt_11['Country']==country , 'Long Term Idled'])
            self.dataCharts_11.at[row+1, 'idle'] = float(self.data_chartt_11.loc[self.data_chartt_11['Country']==country ,'Q4 idled'])
            row = row+2
        self.dataCharts_11.at[10,'Operating'] = float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='Chongqing' ,'Q4 Operational']) +float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='IM' ,'Q4 Operational'])+float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='Other' ,'Q4 Operational'])
        self.dataCharts_11.at[11,'Long Term Idled']=float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='Chongqing' , 'Long Term Idled'])+float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='IM' ,'Long Term Idled'])+float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='Other' ,'Long Term Idled'])
        self.dataCharts_11.at[11, 'idle'] = float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='Chongqing' ,'Q4 idled'])+float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='IM' ,'Q4 idled'])+float(self.data_chartt_11.loc[self.data_chartt_11['Country']=='Other' ,'Q4 idled'])
        
        self.dataCharts_11.at[12, 'Operating'] = self.dataCharts_11['Operating'].sum()
        self.dataCharts_11.at[12, 'Long Term Idled'] = self.dataCharts_11['Long Term Idled'].sum()
        self.dataCharts_11.at[12, 'idle'] = self.dataCharts_11['idle'].sum()
        self.dataCharts_11.at[0,'Stack for Chart Making' ] = 0
        for i in range(1, 13):
            self.dataCharts_11.at[i,'Stack for Chart Making' ] = self.dataCharts_11.loc[i-1,:].sum()
            
            

        self.data_chartt_12['Q4 idled'] = self.data_chartt_12['Q4 Capacity'] - self.data_chartt_12['Q4 Operational']
        self.data_chartt_12['Q3 idled'] = self.data_chartt_12['Q3 Capacity'] - self.data_chartt_12['Q3 Operational']
        row = 0
        for country in  ['Xinjiang', 'Shandong','Henan','Qinghai','Gansu', 'Inner Mongolia', 'Yunnan','Other' ]:
            self.dataCharts_12.at[row,'Operating'] = float(self.data_chartt_12.loc[self.data_chartt_12['Country']==country ,'Q4 Operational'])
            self.dataCharts_12.at[row+1,'Long Term Idled']=float(self.data_chartt_12.loc[self.data_chartt_12['Country']==country , 'Long Term Idled'])
            self.dataCharts_12.at[row+1, 'idle'] = float(self.data_chartt_12.loc[self.data_chartt_12['Country']==country ,'Q4 idled'])
            row = row+2
        self.dataCharts_12.at[16, 'Operating'] = self.dataCharts_12['Operating'].sum()
        self.dataCharts_12.at[16, 'Long Term Idled'] = self.dataCharts_12['Long Term Idled'].sum()
        self.dataCharts_12.at[16, 'idle'] = self.dataCharts_12['idle'].sum()
        self.dataCharts_12.at[0,'Stack for Chart Making' ] = 0
        for i in range(1, 17):
            self.dataCharts_12.at[i,'Stack for Chart Making' ] = self.dataCharts_12.loc[i-1,:].sum()
            
        
        self.dataCharts_13['Date'] = self.dataCharts_1['Date']
        self.dataCharts_13['Year'] = self.dataCharts_1['Year']
        self.dataCharts_13['Quarter'] = self.dataCharts_1['Quarter']
        self.dataCharts_13['Imports'] = self.dataInputMontly_alumina_montly_supply['Total Import']
        self.dataCharts_13['Exports'] = [0-x for x in  self.data_inp_montly_tab1['Export, kt Aa - Total']]
        
        
        self.dataCharts__10['Chart Year'] = self.data_input_annual['Year']
        self.dataCharts__10['Alumina Capacity'] =self.data_input_annual['Alumina Capacity']
        self.dataCharts__10['Alumina Production'] =self.data_input_annual['Alumina Production']
        self.dataCharts__10['Utilisation'] =self.data_input_annual['Alumina Utilisation']
        self.dataCharts__10['Estimated Capacity']=0
        self.dataCharts__10['Effective Capacity'] = 0
        self.dataCharts__10['Active'] = self.dataCharts__10['Alumina Capacity']-self.dataCharts__10['Estimated Capacity']
        
        self.dataCharts__11['Date'] = self.dataCharts_1['Date']
        self.dataCharts__11['Year'] = self.dataCharts_1['Year']
        self.dataCharts__11['Quarter'] = self.dataCharts_1['Quarter']
        self.dataCharts__11['Chart Year'] = self.data_input_annual['Year']
        self.dataCharts__11['Aluminium Capacity'] =self.data_input_annual['Aluminium Capacity']
        self.dataCharts__11['Aluminium Production'] =self.data_input_annual['Aluminium Production']
        self.dataCharts__11['Aluminium Utilisation'] =self.data_input_annual['Aluminium Utilisation']
        self.dataCharts__11['Estimated Capacity']=0
        self.dataCharts__11['Effective Capacity'] = 0
        self.dataCharts__11['Active'] = self.dataCharts__11['Aluminium Capacity']-self.dataCharts__11['Estimated Capacity']
        
        #self.data_charts_11
    #data charts
    def calc_data_charts_2_charts10(self):
        self.dataCharts_10['Date'] = self.dataCharts_1['Date']
        self.dataCharts_10['Year'] = self.dataCharts_1['Year']
        self.dataCharts_10['Quarter'] = self.dataCharts_1['Quarter']
        v = [i.to_pydatetime()  for i in self.dataCharts_10['Date']]
        k = v.index(datetime(2012,1,1,0,0)) #204
        for i in range(k, self.dataCharts_10.shape[0]):
            self.dataCharts_10.at[i, 'HT Bx'] = self.AAproductions_summary_data_to_chart_sheet.loc[1, v[i]]
            self.dataCharts_10.at[i, 'LT Bx'] = self.AAproductions_summary_data_to_chart_sheet.loc[0, v[i]]
            self.dataCharts_10.at[i, 'TOTAL'] = self.AAproductions_summary_data_to_chart_sheet.loc[2, v[i]]
        #self.AAproductions_summary_data_to_chart_sheet
        '''
        for row in range(204, len(self.dataCharts_10)):
            self.dataCharts_10.at[row,'HT Bx'] = self.AAproductions.loc[1052,str(row-196)]
            self.dataCharts_10.at[row,'LT Bx'] = self.AAproductions.loc[1053,str(row-196)]
            self.dataCharts_10.at[row,'TOTAL'] = self.AAproductions.loc[1054,str(row-196)]
        '''
        
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
        
    def calc_mines_bx_production_2(self):
        self.minesBxPrdocution2[2017]=self.minesBxPrdocution2[['Q1 2017', 'Q2 2017','Q3 2017','Q4 2017']].sum(axis=1)
        self.minesBxPrdocution2[2018]=self.minesBxPrdocution2[['Q1 2018', 'Q2 2018','Q3 2018','Q4 2018']].sum(axis=1)
        self.minesBxPrdocution2[2019]=self.minesBxPrdocution2[['Q1 2019', 'Q2 2019','Q3 2019','Q4 2019']].sum(axis=1)
        self.minesBxPrdocution2[2020]=self.minesBxPrdocution2[['Q1 2020', 'Q2 2020','Q3 2020','Q4 2020']].sum(axis=1)
        
    def calc_mines_bx_production_3(self):
        self.minesBxPrdocution_final['Key'] = self.minesBxPrdocution1['Key']
        self.minesBxPrdocution_final['Name'] = self.minesBxPrdocution1['Name']
        self.minesBxPrdocution_final['Country'] = self.minesBxPrdocution1['Country']
        for col in self.minesBxPrdocution2.columns[3:]:
            for i in self.minesBxPrdocution_final.index:
                self.minesBxPrdocution_final.at[i,col] = (float(self.minesBxPrdocution3.loc[i,col])+ float(self.BauxiteSourcesSummary.loc[i,col])) if pd.isna(self.minesBxPrdocution2.loc[i,col]) else  self.minesBxPrdocution2.loc[i,col]
        self.minesBxPrdocution_final[2017]=self.minesBxPrdocution_final[['Q1 2017', 'Q2 2017','Q3 2017','Q4 2017']].sum(axis=1)
        self.minesBxPrdocution_final[2018]=self.minesBxPrdocution_final[['Q1 2018', 'Q2 2018','Q3 2018','Q4 2018']].sum(axis=1)
        self.minesBxPrdocution_final[2019]=self.minesBxPrdocution_final[['Q1 2019', 'Q2 2019','Q3 2019','Q4 2019']].sum(axis=1)
        self.minesBxPrdocution_final[2020]=self.minesBxPrdocution_final[['Q1 2020', 'Q2 2020','Q3 2020','Q4 2020']].sum(axis=1)
        
        #1 = self.minesBxPrdocution2
        #2 = self.minesBxPrdocution3
        #3 = self.BauxiteSourcesSummary
    
        
    def calc_bauxite_sources_1(self):
        SUPname = {self.minesBxPrdocution1['Key'][_]:self.minesBxPrdocution1['Name'][_] for _ in self.minesBxPrdocution1.index}
        BAR     = {self.minesBxPrdocution1['Key'][_]:self.minesBxPrdocution1['BAR'][_] for _ in self.minesBxPrdocution1.index}
        REFname = {self.refAAproductions['Key'][_]:self.refAAproductions['Refinery Name'][_] for _ in self.refAAproductions.index}
        nos = list(dict.fromkeys(al.BauxiteSources_inputs['No.'] ))
        nos  = [x for x in (dict.fromkeys(self.BauxiteSources_inputs['No.'] )) if str(x) != 'nan']
        ref_keys = [x for x in (dict.fromkeys(self.BauxiteSources_inputs['Refinery Key'] )) if str(x) != 'nan']
        #['No','Refinery Name',"Supplying Mines",'BAR']
              
        j=0
        for row in range(len(self.BauxiteSources_inputs)):
            j = j+1 if (row>1 and row%5==0) else j
            self.BauxiteSources.at[row, 'No'] = nos[j]
            # print(row)
            try:
                self.BauxiteSources.at[row, 'Refinery Name'] = REFname[ref_keys[j]]
                self.BauxiteSources.at[row, "Supplying Mines"] = SUPname[self.BauxiteSources_inputs.loc[row,'Supplying Mines Keys']]
                self.BauxiteSources.at[row, "BAR"] = BAR[self.BauxiteSources_inputs.loc[row,'Supplying Mines Keys']]
            except:
                pass
            for col in self.BauxiteSources_inputs.columns[3:].to_list():
                try:
                    prod_value = float(self.refAAproductions.loc[self.refAAproductions['Key']== ref_keys[j]  , str(col)])
                except:
                    prod_value = 0
                result = (float(self.BauxiteSources_inputs.loc[row,col])*prod_value*self.BauxiteSources.at[row, "BAR"])   if  (float(self.BauxiteSources_inputs.loc[row,col])>0)  else 0
                self.BauxiteSources.at[row, col] = 0 if pd.isna(result) else result

        #summary
        self.BauxiteSourcesSummary['Key'] = self.minesBxPrdocution1['Key']
        self.BauxiteSourcesSummary['Name'] = [SUPname[i] for i in self.BauxiteSourcesSummary['Key']]
        for col in self.BauxiteSources_inputs.columns[3:].to_list():
            self.BauxiteSourcesSummary[col] = [(self.BauxiteSources.loc[ self.BauxiteSources['Supplying Mines']== name, col].sum()) for name in self.BauxiteSourcesSummary['Name'] ]
            
            
    #CM Global and BX Prodution
    def calc_cm_global_prd(self):
        d = {self.minesBxPrdocution1['Name'][_]:self.minesBxPrdocution1['Key'][_]  for _ in self.minesBxPrdocution1.index}
        self.cm_global_prd['Ownership'] = self.cm_global_prd['Ownership'].fillna(' ')
        for i in self.cm_global_prd.index:
            value = 0
            try:
                value = d[self.cm_global_prd.loc[i,'Name']]
            except:
                value = d[self.cm_global_prd.loc[i+1,'Name']]
            self.cm_global_prd.at[i,'Key'] = value
            for col in [2017, 2018,2019,2020, 'Q1 2017','Q2 2017','Q3 2017','Q4 2017',
                         'Q1 2018','Q2 2018','Q3 2018','Q4 2018',
                         'Q1 2019','Q2 2019','Q3 2019','Q4 2019',
                         'Q1 2020','Q2 2020','Q3 2020','Q4 2020']:
                self.cm_global_prd.at[i,col] = float(self.minesBxPrdocution_final.loc[ self.minesBxPrdocution_final['Key']==self.cm_global_prd.loc[i,'Key'], col ]) *float(self.cm_global_prd.loc[i,'Offtake( %)'])
        self.cm_global_prd[2017]=self.cm_global_prd[['Q1 2017', 'Q2 2017','Q3 2017','Q4 2017']].sum(axis=1)
        self.cm_global_prd[2018]=self.cm_global_prd[['Q1 2018', 'Q2 2018','Q3 2018','Q4 2018']].sum(axis=1)
        self.cm_global_prd[2019]=self.cm_global_prd[['Q1 2019', 'Q2 2019','Q3 2019','Q4 2019']].sum(axis=1)
        self.cm_global_prd[2020]=self.cm_global_prd[['Q1 2020', 'Q2 2020','Q3 2020','Q4 2020']].sum(axis=1)
            
        #self.cm_global_prd_chineseSum
        #self.cm_global_prd_china_provincial
        
        #summary prod by region
        self.cm_global_prd_sum1 = pd.DataFrame(columns=[])
        self.cm_global_prd_sum1['Region'] = [ 'Oceania', 'East & Central Europe', 'South America', 'Africa & Asia (ex China)', 'West Europe', 'North America']
        for col in self.cm_global_prd_chineseSum.columns[1:]:
            for row in range(self.cm_global_prd_sum1.shape[0]-1):
                self.cm_global_prd_sum1.at[row,col] = self.cm_global_prd.loc[(self.cm_global_prd['Region']==self.cm_global_prd_sum1.loc[row,'Region'] )&(self.cm_global_prd['Ownership']==' '), col].sum()
            self.cm_global_prd_sum1.at[6,col ] = self.cm_global_prd_sum1[col].sum()
        self.cm_global_prd_sum1.at[6,'Region' ]='SUM'
        
        #summary prod by chinese province
        self.cm_global_prd_chineseSum[2017] = self.cm_global_prd_chineseSum[['Q1 2017', 'Q2 2017','Q3 2017','Q4 2017']].sum(axis=1)
        self.cm_global_prd_chineseSum[2018] = self.cm_global_prd_chineseSum[['Q1 2018', 'Q2 2018','Q3 2018','Q4 2018']].sum(axis=1)
        self.cm_global_prd_chineseSum[2019] = self.cm_global_prd_chineseSum[['Q1 2019', 'Q2 2019','Q3 2019','Q4 2019']].sum(axis=1)
        self.cm_global_prd_chineseSum[2020] = self.cm_global_prd_chineseSum[['Q1 2020', 'Q2 2020','Q3 2020','Q4 2020']].sum(axis=1)
        self.cm_global_prd_chineseSum.at[8,'Province'] = 'SUM'
        for col in self.cm_global_prd_chineseSum.columns[1:]:
            self.cm_global_prd_chineseSum.at[8,col] = self.cm_global_prd_chineseSum[col].sum()
            
        
        #China Provincial imp Bauxite
        self.cm_global_prd_china_provincial.at[8, 'Province'] = 'SUM'
        for col in self.cm_global_prd_china_provincial.columns[1:]:
            self.cm_global_prd_china_provincial.at[8,col] = self.cm_global_prd_china_provincial[col].sum()
        
        #summart Prod by coutry
        self.cm_global_prd_sum2 = pd.DataFrame(columns=[])
        self.cm_global_prd_sum2['Country'] = [ 'China', 'Australia', 'Guinea', 'Brazil', "India", 'Russia', 'Jamaica', 'Malaysia', 'Other' ]
        self.cm_global_prd_sum2.at[9,'Country'] = 'SUM'
        for col in self.cm_global_prd_china_provincial.columns[1:]:
            self.cm_global_prd_sum2.at[0, col] = self.cm_global_prd_chineseSum.loc[8,col]
            for row in range(1, self.cm_global_prd_sum2.shape[0]-1):
                self.cm_global_prd_sum2.at[row, col] = self.cm_global_prd.loc[(self.cm_global_prd['Country']==self.cm_global_prd_sum2.loc[row,'Country'] )&(self.cm_global_prd['Ownership']==' '), col].sum()
            self.cm_global_prd_sum2.at[8,col] = self.cm_global_prd[col].sum() - self.cm_global_prd_sum2.loc[1:8,col].sum()
            self.cm_global_prd_sum2.at[9,col] = self.cm_global_prd_sum2[col].sum()
            
            
            
    #global bx quarterly
        '''
        # self.GlobalBx_cmglobal      = pd.read_excel(global_bx_wbk, sheet_name='CM Global Bx Production')
        # self.GlobalBx_rioReport     = pd.read_excel(global_bx_wbk, sheet_name='Rio Quarterly Report')
        self.GlobalBx_russelReport  = pd.read_excel(global_bx_wbk, sheet_name='Rusal Quarterly Report')
        self.GlobalBx_hydro         = pd.read_excel(global_bx_wbk, sheet_name='Hydro Quarterly Bx ')
        self.GlobalBx_south32       = pd.read_excel(global_bx_wbk, sheet_name='South 32 Alumina Production')
        self.GlobalBx_hindalco      = pd.read_excel(global_bx_wbk, sheet_name='Hindalco')
        self.GlobalBx_vedanta       = pd.read_excel(global_bx_wbk, sheet_name='Vedanta')
        self.GlobalBx_nalco         = pd.read_excel(global_bx_wbk, sheet_name='NALCO')
        self.GlobalBx_productionMainCompanies = pd.read_excel(global_bx_wbk, sheet_name='Production for Main Companies')
        '''
    def calc_global_bx_quarterly_1(self):
        q_year = []
        for i in self.GlobalBx_cmglobal.index:
            q_year.append(self.GlobalBx_cmglobal.loc[i,'Quarter']+' '+ str(int(self.GlobalBx_cmglobal.loc[i, 'Year'])))
            
        data=self.GlobalBx_out_tab1.copy()
        for col in ['Year', 'Quarter']:
            data[col] = self.GlobalBx_cmglobal[col]
        data['Q_Year'] = q_year
        for col in ['China','Australia','Guinea','Brazil','India','Russia', 'Jamaica', 'Malaysia', 'Other']:
            data[col] = self.GlobalBx_cmglobal[col]
        for row in range(12, len(self.GlobalBx_cmglobal)):
            data.loc[row, 'China'] = 9999
            for col in ['Australia','Guinea','Brazil','India','Russia', 'Jamaica', 'Malaysia', 'Other']:
                data.loc[row,col] = float(self.cm_global_prd_sum2.loc[self.cm_global_prd_sum2['Country']==col, data.loc[row, 'Q_Year']])
        data['Total'] = data[['China','Australia','Guinea','Brazil','India','Russia', 'Jamaica', 'Malaysia', 'Other']].sum(axis=1)
        self.GlobalBx_cmglobal = data
        
        
        data=self.GlobalBx_out_tab1.copy()
        for col in ['Year', 'Quarter']:
            data[col] = self.GlobalBx_productionMainCompanies[col]
        data['Q_Year'] = q_year
        for col in ['S32', 'Alcoa', 'Rio', 'Rusal', 'Hydro', 'Chalco','Noranda', 'Votorantim', 'SMB']:
            data[col] = self.GlobalBx_productionMainCompanies[col]
        for row in range(12, self.GlobalBx_productionMainCompanies.shape[0]):
            data.loc[row, 'S32'] = float(self.GlobalBx_south32.loc[row,'Worsley'])*0.86*3.42/1000+0.1595*float(self.GlobalBx_rioReport.loc[row, 'MRN (12%)'])/0.12/1000
            data.loc[row, 'Alcoa'] = self.GlobalBx_rioReport.loc[row, 'Alcoa Quarterly Report']
            data.loc[row, 'Rio']   = float(self.GlobalBx_rioReport.loc[row, ['Gove (100%)', 'MRN (12%)', 'CBG (45%)','Weipa (100%)']].sum()) /1000
            data.loc[row, 'Rusal'] = float(self.GlobalBx_russelReport.loc[row,['Guinea', 'Russia', 'Jamaica', 'Guyana']].sum())/1000*0.9
            data.loc[row, 'Hydro'] = float(self.GlobalBx_hydro.loc[row, 'Paragominas'])*0.9/1000+(float(self.GlobalBx_rioReport.loc[row, 'MRN (12%)'])/0.12*0.45)/1000
        self.GlobalBx_productionMainCompanies = data
        
        self.GlobalBx_out_tab1['Country'] = ['Rio','Alcoa','SMB','S32','Hydro', 'Rusal', 'Chalco']
        for i in self.GlobalBx_out_tab1.index:
            col_name = self.GlobalBx_out_tab1.loc[i, 'Country']
            self.GlobalBx_out_tab1.at[i, 'Q3 2019'] = float(self.GlobalBx_productionMainCompanies.loc[self.GlobalBx_productionMainCompanies['Q_Year']=='Q3 2019', col_name])
            self.GlobalBx_out_tab1.at[i, 'Q4 2019'] = float(self.GlobalBx_productionMainCompanies.loc[self.GlobalBx_productionMainCompanies['Q_Year']=='Q4 2019', col_name])
        v1 = float(self.GlobalBx_cmglobal.loc[self.GlobalBx_cmglobal['Q_Year']=='Q3 2019', 'Total'])/1000
        v2 = float(self.GlobalBx_cmglobal.loc[self.GlobalBx_cmglobal['Q_Year']=='Q4 2019', 'Total'])/1000
        self.GlobalBx_out_tab1.at[7, 'Country'] = 'Other*'
        self.GlobalBx_out_tab1.at[7, 'Q3 2019'] = v1 - self.GlobalBx_out_tab1['Q3 2019'].sum()
        self.GlobalBx_out_tab1.at[7, 'Q4 2019'] = v2 - self.GlobalBx_out_tab1['Q4 2019'].sum()
        self.GlobalBx_out_tab1.at[8, 'Country'] = 'Total'
        self.GlobalBx_out_tab1.at[8, 'Q3 2019'] = v1
        self.GlobalBx_out_tab1.at[8, 'Q4 2019'] = v2
        for i in self.GlobalBx_out_tab1.index:
            self.GlobalBx_out_tab1.at[i, 'Q-O-Q']  = float(self.GlobalBx_out_tab1.loc[i, 'Q4 2019'])/float(self.GlobalBx_out_tab1.loc[i, 'Q3 2019'])-1
        
        
        self.GlobalBx_out_tab2['Country']= ['Rio','Alcoa','SMB','S32','Hydro', 'Rusal', 'Chalco', 'Other*', 'Total']
        for i in self.GlobalBx_out_tab2.index:
            self.GlobalBx_out_tab2.at[0, 'Blank'] = 0
            self.GlobalBx_out_tab2.at[i, 'Mt'] = self.GlobalBx_out_tab1.loc[i, 'Q4 2019']
            if i !=0:
                self.GlobalBx_out_tab2.at[i, 'Blank'] = self.GlobalBx_out_tab1.loc[:i-1, 'Q4 2019'].sum()
            self.GlobalBx_out_tab2.at[i, 'no name 1'] = self.GlobalBx_out_tab1.loc[:i, 'Q4 2019'].sum()/v2
            self.GlobalBx_out_tab2.at[i, 'no name 2'] = self.GlobalBx_out_tab1.loc[i, 'Q4 2019']/v2
        self.GlobalBx_out_tab2.at[8, 'Blank'] = 0
        self.GlobalBx_out_tab2.at[8, 'no name 1'] = 1
        
        self.GlobalBx_out_tab3['Country'] = self.GlobalBx_out_tab2['Country']
        self.GlobalBx_out_tab3['no name 1'] = self.GlobalBx_out_tab2['no name 1']
        
    
    
    
    #AA prodn
    def calc_AA_production_1(self):
        #######
        limit = int(max(self.AAproductions_main['Refinery No.'].to_list()))
        limit = (limit-1)*10+1
        for row in range(0, limit, 10):
            for col in self.AAproductions_main.columns:
                row2 = (row/10)*16
                self.AAproductions_out1.at[row, col] = self.AAproductions_main.loc[row2,col]
                self.AAproductions_out1.at[row+1, col] = self.AAproductions_main.loc[row2+1,col]
                self.AAproductions_out1.at[row+2, col] = self.AAproductions_main.loc[row2+2,col]
                self.AAproductions_out1.at[row+3, col] = self.AAproductions_main.loc[row2+3,col]
            self.AAproductions_out1.at[row+4, 'Input'] = 'Capacity - Total'
            for col in self.AAproductions_main.columns[7:]:
                self.AAproductions_out1.at[row+4, col] = self.AAproductions_out1.loc[row:row+4, col].sum()
            #productions
            for col in self.AAproductions_main.columns:
                self.AAproductions_out1.at[row+5, col] = self.AAproductions_main.loc[row2+4,col]
                self.AAproductions_out1.at[row+6, col] = self.AAproductions_main.loc[row2+5,col]
                self.AAproductions_out1.at[row+7, col] = self.AAproductions_main.loc[row2+6,col]
                self.AAproductions_out1.at[row+8, col] = self.AAproductions_main.loc[row2+7,col]
            self.AAproductions_out1.at[row+9, 'Input'] = 'Production - Total'
            for col in self.AAproductions_main.columns[7:]:
                self.AAproductions_out1.at[row+9, col] = self.AAproductions_out1.loc[row+5:row+9, col].sum()
            
            for col in ['Refinery No.', 'Refinery', 'Province','Location']:
                self.AAproductions_out1.at[row:row+10, col] = self.AAproductions_main.loc[row2, col]
            
        ######################################
        limit = int(max(self.AAproductions_main['Refinery No.'].to_list()))
        limit = limit * 6
        for row in range(0, limit, 6):
            row2 = (row/6) * 16
            self.AAproductions_out2.at[row, 'Input'] = 'Capacity domestic Bx'
            self.AAproductions_out2.at[row+1, 'Input'] = 'Capacity HT Import Bx'
            self.AAproductions_out2.at[row+2, 'Input'] = 'Capacity LT Import Bx'
            
            self.AAproductions_out2.at[row+3, 'Input'] = 'AA prodn from domestic Bx'
            self.AAproductions_out2.at[row+4, 'Input'] = 'AA prodn from HT Import Bx'
            self.AAproductions_out2.at[row+5, 'Input'] = 'AA prodn from LT Import Bx'
            for col in self.AAproductions_main.columns[7:]:
                
                #
                tab1 = pd.DataFrame(columns=[])
                tab1['cap'] = self.AAproductions_main.loc[row2:row2+3, col].copy().to_list()
                tab1['pro'] = self.AAproductions_main.loc[row2+4:row2+7, col].copy().to_list()
                tab1['bax'] = self.AAproductions_main.loc[row2+8:row2+11, col].copy().to_list()
                tab1['%%%'] = self.AAproductions_main.loc[row2+12:row2+15, col].copy().to_list()
                tab1['%%_2'] = [(1 - float(i)) for i in tab1['%%%']]
                
                tab1['tech'] = self.AAproductions_main.loc[row2+8:row2+11, 'Technology'].copy().to_list()
                tab1['%_tech'] = self.AAproductions_main.loc[row2+12:row2+15, 'Technology'].copy().to_list()
                tab1['%_tech_2'] = [i[-5:] for i in tab1['%_tech']]
                
                c1 = tab1.loc[tab1['bax']=='D','cap'].sum() + tab1.loc[tab1['bax']=='D-I', ['cap','%%_2']].prod(axis=1).sum()
                c3 = tab1.loc[(tab1['bax']=='I')&(tab1['tech']=='L-B'), 'cap'].sum() + tab1.loc[tab1['%_tech_2']=='Sweet', ['%%%', 'cap']].prod(axis=1).sum()
                c2 = tab1['cap'].sum()-c1-c3
                
                p1 = tab1.loc[tab1['bax']=='D','pro'].sum() + tab1.loc[tab1['bax']=='D-I', ['pro','%%_2']].prod(axis=1).sum()
                p3 = tab1.loc[(tab1['bax']=='I')&(tab1['tech']=='L-B'), 'pro'].sum() + tab1.loc[tab1['%_tech_2']=='Sweet', ['%%%', 'pro']].prod(axis=1).sum()
                p2 = tab1['pro'].sum()-p1-p3
                #
                self.AAproductions_out2.at[row, col] = c1
                self.AAproductions_out2.at[row+1, col] = c2
                self.AAproductions_out2.at[row+2, col] = c3
                
                self.AAproductions_out2.at[row+3, col] = p1
                self.AAproductions_out2.at[row+4, col] = p2
                self.AAproductions_out2.at[row+5, col] = p3
                    
            for col in ['Refinery No.', 'Refinery', 'Province','Location']:
                self.AAproductions_out2.at[row:row+6, col] = self.AAproductions_main.loc[row2, col]
    
    def calc_AA_production_2(self):
        for i in range(34, self.AAproductions_sheet3.shape[1]):
            col = datetime(self.AAproductions_sheet3.columns[i].year, self.AAproductions_sheet3.columns[i].month, self.AAproductions_sheet3.columns[i].day, 0, 0)
            self.AAproductions_sheet3.loc[1,self.AAproductions_sheet3.columns[i]] = float(self.dataInputMontly_BAR.loc[self.dataInputMontly_BAR['Date'] == col,'Total Combined'])#2.9 #
            self.AAproductions_sheet3.loc[2,self.AAproductions_sheet3.columns[i]] = float(self.dataInputMontly_BAR.loc[self.dataInputMontly_BAR['Date'] == col,'HT'])#2.9
            self.AAproductions_sheet3.loc[3,self.AAproductions_sheet3.columns[i]] = float(self.dataInputMontly_BAR.loc[self.dataInputMontly_BAR['Date'] == col,'LT'])#2.9
            
            
    def calc_AA_production_3(self):
        self.AAproductions_inland_ref.at[0, 'Country'] = self.AAproductions_bx_consumed.loc[1,'Country']
        self.AAproductions_inland_ref.at[1, 'Country'] = self.AAproductions_bx_consumed.loc[2,'Country']
        self.AAproductions_inland_ref.at[2, 'Country'] = self.AAproductions_bx_consumed.loc[3,'Country']
        self.AAproductions_inland_ref.at[3, 'Country'] = self.AAproductions_bx_consumed.loc[4,'Country']
        self.AAproductions_inland_ref.at[4, 'Country'] = self.AAproductions_bx_consumed.loc[5,'Country']
        self.AAproductions_inland_ref.at[5, 'Country'] = self.AAproductions_bx_consumed.loc[6,'Country']
        for col in self.AAproductions_bx_consumed.columns[1:]:
            print(col)
            try:
                self.AAproductions_inland_ref .at[0, col] = float(self.AAproductions_bx_consumed.loc[1,col])/float(self.AAproductions_sheet3.loc[2,col])
                self.AAproductions_inland_ref.at[1, col] = float(self.AAproductions_bx_consumed.loc[2,col])/float(self.AAproductions_sheet3.loc[2,col])
                self.AAproductions_inland_ref.at[2, col] = float(self.AAproductions_bx_consumed.loc[3,col])/float(self.AAproductions_sheet3.loc[2,col])
                self.AAproductions_inland_ref.at[3, col] = float(self.AAproductions_bx_consumed.loc[4,col])/float(self.AAproductions_sheet3.loc[2,col])
                self.AAproductions_inland_ref.at[4, col] = float(self.AAproductions_bx_consumed.loc[5,col])/float(self.AAproductions_sheet3.loc[2,col])
                self.AAproductions_inland_ref.at[5, col] = float(self.AAproductions_bx_consumed.loc[6,col])/float(self.AAproductions_sheet3.loc[2,col])
            except ZeroDivisionError:
                pass
            self.AAproductions_inland_ref.at[6, col] = self.AAproductions_inland_ref.loc[:5, col].sum()
        
    def calc_AA_production_4(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        self.AAproductions_summary_prodTotal = self.AAproductions_out1.loc[self.AAproductions_out1['Input']=='Production - Total'].copy().reset_index(drop=True)
        self.AAproductions_summary_prodTotal.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_prodTotal.at[last, 'Refinery'] = 'Inland blending'
        self.AAproductions_summary_prodTotal.at[last, 'Input'] = 'Production - Total'
        self.AAproductions_summary_prodTotal.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_prodTotal.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_prodTotal.at[last+3, 'Input'] = 'Shandong total - model monthly'
        self.AAproductions_summary_prodTotal.at[last+4, 'Input'] = 'Shandong actual monthly'
        self.AAproductions_summary_prodTotal.at[last+5, 'Input'] = 'error - annualised'
        self.AAproductions_summary_prodTotal.at[last+6, 'Input'] = 'cummulative error annualised'
        self.AAproductions_summary_prodTotal.at[last+7, 'Input'] = 'error as percent of actual'
        for col in self.AAproductions_out1.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_prodTotal.at[last, col] = self.AAproductions_inland_ref.loc[6, col]/1000/365*no_days
            self.AAproductions_summary_prodTotal.at[last+1, col] = self.AAproductions_summary_prodTotal.loc[:, col].sum()
            self.AAproductions_summary_prodTotal.at[last+2, col] = self.AAproductions_summary_prodTotal.loc[self.AAproductions_summary_prodTotal['Province']=='Shandong', col].sum()
            self.AAproductions_summary_prodTotal.at[last+3, col] = self.AAproductions_summary_prodTotal.loc[last+2, col]/365*no_days
            self.AAproductions_summary_prodTotal.at[last+4, col] = self.data_inp_montly_tab1.loc[self.data_inp_montly_tab1['Date']== col, 'Production - Shandong']#1.167  #data monthly gx14
            self.AAproductions_summary_prodTotal.at[last+5, col] = (float(self.AAproductions_summary_prodTotal.loc[last+4, col])- float(self.AAproductions_summary_prodTotal.loc[last+3, col]))/no_days*365
            self.AAproductions_summary_prodTotal.at[last+6, col] = self.AAproductions_summary_prodTotal.loc[last+5, datetime(2012, 1, 1, 0, 0):col].sum()
            self.AAproductions_summary_prodTotal.at[last+7, col] = float(self.AAproductions_summary_prodTotal.loc[last+5, col])/365*no_days/float(self.AAproductions_summary_prodTotal.loc[last+4, col])
        
    
        self.AAproductions_inner_mongolia_chart_shandong.at[0, 'Factor'] = 'Inner Mongolia - model - monthly annualised'
        self.AAproductions_inner_mongolia_chart_shandong.at[1, 'Factor'] = 'Inner Mongolia - actual - monthly'
        self.AAproductions_inner_mongolia_chart_shandong.at[2, 'Factor'] = 'Inner Mongolia - actual, monthly annualised'
        self.AAproductions_inner_mongolia_chart_shandong.at[3, 'Factor'] = 'Modelled - annualised'
        self.AAproductions_inner_mongolia_chart_shandong.at[4, 'Factor'] = 'Reported - annualised'
        for col in self.AAproductions_summary_prodTotal.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            bb = al.AAproductions_summary_prodTotal.loc[:16,:]
            self.AAproductions_inner_mongolia_chart_shandong.at[0, col] = bb.loc[bb['Province']=='Inner Mongolia',col].sum()
            self.AAproductions_inner_mongolia_chart_shandong.at[1, col] = float(self.AAproductions_sheet3.loc[0,col])
            self.AAproductions_inner_mongolia_chart_shandong.at[2, col] = float(self.AAproductions_sheet3.loc[0,col])/no_days *365
            self.AAproductions_inner_mongolia_chart_shandong.at[3, col] = self.AAproductions_summary_prodTotal.at[last+3, col]/no_days*365
            self.AAproductions_inner_mongolia_chart_shandong.at[4, col] = self.AAproductions_summary_prodTotal.at[last+4, col]/no_days*365
        
        
    def calc_AA_production_5(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        self.AAproductions_summary_AAprodHT = self.AAproductions_out2.loc[self.AAproductions_out2['Input']=='AA prodn from HT Import Bx'].copy().reset_index(drop=True)
        self.AAproductions_summary_AAprodHT.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_AAprodHT.at[last, 'Refinery'] = 'Inland blending'
        self.AAproductions_summary_AAprodHT.at[last, 'Input'] = 'AA prodn from HT Import Bx'
        self.AAproductions_summary_AAprodHT.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_AAprodHT.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_AAprodHT.at[last+3, 'Input'] = 'Shandong total - expected monthly'
        for col in self.AAproductions_out2.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_AAprodHT.at[last, col] = self.AAproductions_inland_ref.loc[6, col]/1000/365*no_days
            self.AAproductions_summary_AAprodHT.at[last+1, col] = self.AAproductions_summary_AAprodHT.loc[:, col].sum()
            self.AAproductions_summary_AAprodHT.at[last+2, col] = self.AAproductions_summary_AAprodHT.loc[self.AAproductions_summary_AAprodHT['Province']=='Shandong', col].sum()
            self.AAproductions_summary_AAprodHT.at[last+3, col] = self.AAproductions_summary_AAprodHT.loc[last+2, col]/365*no_days
        #self.AAproductions_summary_AAprodHT
    
    def calc_AA_production_6(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        self.AAproductions_summary_AAprodLT = self.AAproductions_out2.loc[self.AAproductions_out2['Input']=='AA prodn from LT Import Bx'].copy().reset_index(drop=True)
        self.AAproductions_summary_AAprodLT.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_AAprodLT.at[last, 'Refinery'] = 'Inland blending'
        self.AAproductions_summary_AAprodLT.at[last, 'Input'] = 'AA prodn from LT Import Bx'
        self.AAproductions_summary_AAprodLT.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_AAprodLT.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_AAprodLT.at[last+3, 'Input'] = 'Shandong total - expected monthly'
        for col in self.AAproductions_out2.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_AAprodLT.at[last, col] = self.AAproductions_inland_ref.loc[6, col]/1000/365*no_days
            self.AAproductions_summary_AAprodLT.at[last+1, col] = self.AAproductions_summary_AAprodLT.loc[:, col].sum()
            self.AAproductions_summary_AAprodLT.at[last+2, col] = self.AAproductions_summary_AAprodLT.loc[self.AAproductions_summary_AAprodLT['Province']=='Shandong', col].sum()
            self.AAproductions_summary_AAprodLT.at[last+3, col] = self.AAproductions_summary_AAprodLT.loc[last+2, col]/365*no_days
        #self.AAproductions_summary_AAprodLT
    
    
    def calc_AA_production_7(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        reff = self.AAproductions_out2.loc[self.AAproductions_out2['Input']=='AA prodn from LT Import Bx'].copy().reset_index(drop=True)
        for col in ['Refinery No.', 'Refinery', 'Province', 'Location']:
            self.AAproductions_summary_totalCapImpBx[col] = reff[col]
        self.AAproductions_summary_totalCapImpBx.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_totalCapImpBx.at[last, 'Refinery'] = 'Inland blending'
        for col in self.AAproductions_summary_AAprodLT.columns[7:]:
            for row in self.AAproductions_summary_totalCapImpBx.index:
                self.AAproductions_summary_totalCapImpBx.at[row, col] = self.AAproductions_summary_AAprodLT.loc[row,col] + self.AAproductions_summary_AAprodHT.loc[row,col]
        self.AAproductions_summary_totalCapImpBx.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_totalCapImpBx.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_totalCapImpBx.at[last+3, 'Input'] = 'Shandong total - expected monthly'
        for col in self.AAproductions_out2.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_totalCapImpBx.at[last+1, col] = self.AAproductions_summary_totalCapImpBx.loc[:, col].sum()
            self.AAproductions_summary_totalCapImpBx.at[last+2, col] = self.AAproductions_summary_totalCapImpBx.loc[self.AAproductions_summary_totalCapImpBx['Province']=='Shandong', col].sum()
            self.AAproductions_summary_totalCapImpBx.at[last+3, col] = self.AAproductions_summary_totalCapImpBx.loc[last+2, col]/365*no_days
        
    def calc_AA_production_8(self):
        self.AAproductions_summary_modelled_991_1003.at[0, 'Modelled Factors'] = 'modelled HT buaxite use'
        self.AAproductions_summary_modelled_991_1003.at[1, 'Modelled Factors'] = 'modelled LT buaxite use'
        self.AAproductions_summary_modelled_991_1003.at[2, 'Modelled Factors'] = 'Modelled bauxite use (mln t per month) - raw'
        self.AAproductions_summary_modelled_991_1003.at[3, 'Modelled Factors'] = 'Modelled alumina production from imported bx (per month) - raw'
        self.AAproductions_summary_modelled_991_1003.at[4, 'Modelled Factors'] = 'error'
        self.AAproductions_summary_modelled_991_1003.at[5, 'Modelled Factors'] = 'error correct factor up'
        self.AAproductions_summary_modelled_991_1003.at[6, 'Modelled Factors'] = 'Modelled bauxite use (mln t per month) - error corrected'
        self.AAproductions_summary_modelled_991_1003.at[7, 'Modelled Factors'] = 'Modelled alumina production from imported bx (per month) - error corrected'
        self.AAproductions_summary_modelled_991_1003.at[8, 'Modelled Factors'] = 'Modelled bauxite use (mln t per year) - error corrected'
        self.AAproductions_summary_modelled_991_1003.at[9, 'Modelled Factors'] = 'ctz [Shandong + IM actual AA] - [error corrected model]'
        for col in self.AAproductions_sheet3.columns[1:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_modelled_991_1003.at[0, col] =float(self.AAproductions_summary_AAprodHT.loc[self.AAproductions_summary_AAprodHT['Input']=='Total', col])*float(self.AAproductions_sheet3.loc[2,col]) /365*no_days
            self.AAproductions_summary_modelled_991_1003.at[1, col] =float(self.AAproductions_summary_AAprodLT.loc[self.AAproductions_summary_AAprodHT['Input']=='Total', col])*float(self.AAproductions_sheet3.loc[3,col]) /365*no_days
            
            self.AAproductions_summary_modelled_991_1003.at[2, col] =self.AAproductions_summary_modelled_991_1003.loc[0, col] + self.AAproductions_summary_modelled_991_1003.loc[1, col]
            self.AAproductions_summary_modelled_991_1003.at[3, col] =float(self.AAproductions_summary_totalCapImpBx.loc[self.AAproductions_summary_totalCapImpBx['Input']=='Total', col]) /365*no_days
            v1 = (float(self.AAproductions_summary_prodTotal.loc[self.AAproductions_summary_prodTotal['Input']=='Shandong actual monthly', col])+float(self.AAproductions_sheet3.loc[0,col]) )
            v2 = (float(self.AAproductions_summary_prodTotal.loc[self.AAproductions_summary_prodTotal['Input']=='Shandong total - model monthly', col])+float(self.AAproductions_inner_mongolia_chart_shandong.loc[0,col])/365*no_days)     
            self.AAproductions_summary_modelled_991_1003.at[4, col] =v1 - v2
            self.AAproductions_summary_modelled_991_1003.at[5, col] =self.AAproductions_summary_modelled_991_1003.loc[4, col]/(  float(self.AAproductions_summary_prodTotal.loc[self.AAproductions_summary_prodTotal['Input']=='Shandong total - model monthly', col])+float(self.AAproductions_inner_mongolia_chart_shandong.loc[0,col]) /365*no_days)+1
            
            self.AAproductions_summary_modelled_991_1003.at[6, col] =self.AAproductions_summary_modelled_991_1003.loc[2, col] * self.AAproductions_summary_modelled_991_1003.loc[5, col]
            self.AAproductions_summary_modelled_991_1003.at[7, col] =float(self.AAproductions_summary_totalCapImpBx.loc[self.AAproductions_summary_totalCapImpBx['Input']=='Total', col])/365*no_days*self.AAproductions_summary_modelled_991_1003.loc[5, col]
            self.AAproductions_summary_modelled_991_1003.at[8, col] =self.AAproductions_summary_modelled_991_1003.loc[6, col]/no_days*365
            v1 = (float(self.AAproductions_summary_prodTotal.loc[self.AAproductions_summary_prodTotal['Input']=='Shandong total - model monthly', col])+float(self.AAproductions_inner_mongolia_chart_shandong.loc[0,col])/365*no_days)
            self.AAproductions_summary_modelled_991_1003.at[9, col] = float(self.AAproductions_summary_prodTotal.loc[self.AAproductions_summary_prodTotal['Input']=='Shandong actual monthly', col])+float(self.AAproductions_sheet3.loc[0,col]) -self.AAproductions_summary_modelled_991_1003.loc[5, col] * v1
            
    def calc_AA_production_9(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        self.AAproductions_summary_capTotal = self.AAproductions_out1.loc[self.AAproductions_out1['Input']=='Capacity - Total'].copy().reset_index(drop=True)
        self.AAproductions_summary_capTotal.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_capTotal.at[last, 'Refinery'] = 'Inland blending'
        self.AAproductions_summary_capTotal.at[last, 'Input'] = 'Capacity - Total'
        self.AAproductions_summary_capTotal.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_capTotal.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_capTotal.at[last+3, 'Input'] = 'Shandong total - expected monthly'
        for col in self.AAproductions_out1.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_capTotal.at[last, col] = self.AAproductions_inland_ref.loc[6, col]/1000/365*no_days
            self.AAproductions_summary_capTotal.at[last+1, col] = self.AAproductions_summary_capTotal.loc[:, col].sum()
            self.AAproductions_summary_capTotal.at[last+2, col] = self.AAproductions_summary_capTotal.loc[self.AAproductions_summary_capTotal['Province']=='Shandong', col].sum()
            self.AAproductions_summary_capTotal.at[last+3, col] = self.AAproductions_summary_capTotal.loc[last+2, col]/365*no_days
            
    def calc_AA_production_10(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        self.AAproductions_summary_AAcapHT = self.AAproductions_out2.loc[self.AAproductions_out2['Input']=='Capacity HT Import Bx'].copy().reset_index(drop=True)
        self.AAproductions_summary_AAcapHT.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_AAcapHT.at[last, 'Refinery'] = 'Inland blending'
        self.AAproductions_summary_AAcapHT.at[last, 'Input'] = 'Capacity HT Import Bx'
        self.AAproductions_summary_AAcapHT.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_AAcapHT.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_AAcapHT.at[last+3, 'Input'] = 'Shandong total - expected monthly'
        for col in self.AAproductions_out2.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_AAcapHT.at[last, col] = self.AAproductions_inland_ref.loc[6, col]/1000/365*no_days
            self.AAproductions_summary_AAcapHT.at[last+1, col] = self.AAproductions_summary_AAcapHT.loc[:, col].sum()
            self.AAproductions_summary_AAcapHT.at[last+2, col] = self.AAproductions_summary_AAcapHT.loc[self.AAproductions_summary_AAcapHT['Province']=='Shandong', col].sum()
            self.AAproductions_summary_AAcapHT.at[last+3, col] = self.AAproductions_summary_AAcapHT.loc[last+2, col]/365*no_days
        self.AAproductions_summary_AAcapHT
    
    def calc_AA_production_11(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        self.AAproductions_summary_AAcapLT = self.AAproductions_out2.loc[self.AAproductions_out2['Input']=='Capacity LT Import Bx'].copy().reset_index(drop=True)
        self.AAproductions_summary_AAcapLT.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_AAcapLT.at[last, 'Refinery'] = 'Inland blending'
        self.AAproductions_summary_AAcapLT.at[last, 'Input'] = 'Capacity LT Import Bx'
        self.AAproductions_summary_AAcapLT.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_AAcapLT.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_AAcapLT.at[last+3, 'Input'] = 'Shandong total - expected monthly'
        for col in self.AAproductions_out2.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_AAcapLT.at[last, col] = self.AAproductions_inland_ref.loc[6, col]/1000/365*no_days
            self.AAproductions_summary_AAcapLT.at[last+1, col] = self.AAproductions_summary_AAcapLT.loc[:, col].sum()
            self.AAproductions_summary_AAcapLT.at[last+2, col] = self.AAproductions_summary_AAcapLT.loc[self.AAproductions_summary_AAcapLT['Province']=='Shandong', col].sum()
            self.AAproductions_summary_AAcapLT.at[last+3, col] = self.AAproductions_summary_AAcapLT.loc[last+2, col]/365*no_days
       
    def calc_AA_production_12(self):
        last = int(max(al.AAproductions_main['Refinery No.'].to_list()))
        reff = self.AAproductions_out2.loc[self.AAproductions_out2['Input']=='Capacity LT Import Bx'].copy().reset_index(drop=True)
        for col in ['Refinery No.', 'Refinery', 'Province', 'Location']:
            self.AAproductions_summary_totalCapImpBx_cap[col] = reff[col]
        self.AAproductions_summary_totalCapImpBx_cap.at[last, 'Refinery No.'] = last+1
        self.AAproductions_summary_totalCapImpBx_cap.at[last, 'Refinery'] = 'Inland blending'
        for col in self.AAproductions_summary_AAcapLT.columns[7:]:
            for row in self.AAproductions_summary_totalCapImpBx_cap.index:
                self.AAproductions_summary_totalCapImpBx_cap.at[row, col] = self.AAproductions_summary_AAcapLT.loc[row,col] + self.AAproductions_summary_AAcapHT.loc[row,col]
        self.AAproductions_summary_totalCapImpBx_cap.at[last+1, 'Input'] = 'Total'
        self.AAproductions_summary_totalCapImpBx_cap.at[last+2, 'Input'] = 'Shandong total'
        self.AAproductions_summary_totalCapImpBx_cap.at[last+3, 'Input'] = 'Shandong total - expected monthly'
        for col in self.AAproductions_out2.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_totalCapImpBx_cap.at[last+1, col] = self.AAproductions_summary_totalCapImpBx_cap.loc[:, col].sum()
            self.AAproductions_summary_totalCapImpBx_cap.at[last+2, col] = self.AAproductions_summary_totalCapImpBx_cap.loc[self.AAproductions_summary_totalCapImpBx_cap['Province']=='Shandong', col].sum()
            self.AAproductions_summary_totalCapImpBx_cap.at[last+3, col] = self.AAproductions_summary_totalCapImpBx_cap.loc[last+2, col]/365*no_days
        self.AAproductions_summary_totalCapImpBx_cap
        
        
        
        
    def calc_AA_production_13(self):
        self.AAproductions_summary_alumina_production.at[0, 'Factor'] = 'LT Bauxite'
        self.AAproductions_summary_alumina_production.at[1, 'Factor'] = 'HT bauxite'
        self.AAproductions_summary_alumina_production.at[2, 'Factor'] = 'LT/HT'
        self.AAproductions_summary_alumina_production.at[3, 'Factor'] = 'Non Merchant production (annualised MTPY alumina)'
        for col in self.AAproductions_main.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_alumina_production.at[0, col] = float(self.AAproductions_summary_AAprodLT.loc[self.AAproductions_summary_AAprodLT['Input'] == 'Total', col])* self.AAproductions_summary_modelled_991_1003.loc[5, col]
            self.AAproductions_summary_alumina_production.at[1, col] = float(self.AAproductions_summary_AAprodHT.loc[self.AAproductions_summary_AAprodHT['Input'] == 'Total', col])* self.AAproductions_summary_modelled_991_1003.loc[5, col]
            self.AAproductions_summary_alumina_production.at[2, col] = self.AAproductions_summary_alumina_production.loc[0, col]/self.AAproductions_summary_alumina_production.loc[1, col]
            self.AAproductions_summary_alumina_production.at[3, col] = (self.AAproductions_summary_AAprodLT[col][:3].sum() +no_days) *self.AAproductions_sheet3.loc[3, col]
        
    def calc_AA_production_14(self):
        self.AAproductions_summary_data_to_chart_sheet.at[0, 'Factor'] = 'LT Bx'
        self.AAproductions_summary_data_to_chart_sheet.at[1, 'Factor'] = 'HT Bx'
        self.AAproductions_summary_data_to_chart_sheet.at[2, 'Factor'] = 'Total'
        for col in self.AAproductions_main.columns[7:]:
            no_days = monthrange(col.year, col.month)[1]
            self.AAproductions_summary_data_to_chart_sheet.at[0, col] = self.AAproductions_summary_alumina_production.loc[0, col]/365*no_days
            self.AAproductions_summary_data_to_chart_sheet.at[1, col] = self.AAproductions_summary_alumina_production.loc[1, col] /365*no_days
            self.AAproductions_summary_data_to_chart_sheet.at[2, col] = self.AAproductions_summary_data_to_chart_sheet.loc[0, col]  + self.AAproductions_summary_data_to_chart_sheet.loc[1, col] 
        
    
    
   #AA prodn
    
    
    
            
        
            
        #labaran codes
    def Get_Date_Year_Quarter(self):
        alumina = self.data_inp_montly_tab1.copy()
        alumina["Date"] = alumina["Date"].dt.strftime("%m-%d-%y")
        AL_Qrtly = pd.DataFrame()
        AL_Qrtly["Date"] = np.nan
        AL_Qrtly['Year'] = np.nan
        AL_Qrtly["Quarter"] = np.nan
        GenerealIndex= []
        for i in range(len(alumina['Date'])):
             if(i >=120):
                 GenerealIndex.append(i)
        AL_Qrtly['index'] = GenerealIndex
        a =0
        for i in GenerealIndex:
             AL_Qrtly['Date'][a]= alumina["Date"][i]
             AL_Qrtly["Year"][a]= alumina["Year"][i]
             AL_Qrtly["Quarter"][a]=alumina["Quarter"][i]
             a = a+1
        # AL_Qrtly["Year"]= AL_Qrtly["Year"].astype(int)
        return AL_Qrtly

        #  model start here

    def Alumina_By_Province_Mt_Aa(self):
             alumina = self.data_inp_montly_tab1.copy()
             Alumina_Province =self.Get_Date_Year_Quarter()
             Alumina_Province["Alumina_Province_Shandong"] = 0
             Alumina_Province["Alumina_Province_Henan"] = 0
             Alumina_Province["Alumina_Province_Shanxi"] =0
             Alumina_Province["Alumina_Province_Guizhou"]=0
             Alumina_Province["Alumina_Province_Guangxi"]=0
             Alumina_Province["Alumina_Province_Chongqing"]=0
             Alumina_Province["Alumina_Province_Other"]=0
             Alumina_Province["Alumina_Province_Total"] =0
        # //gAlumina Province MT AA
             currentIndex=alumina[alumina['Date']=='01-01-16'].index.values
             meIndex= Alumina_Province[Alumina_Province["Date"]=='01-01-16'].index.values
             for  i in range( (len(Alumina_Province["Date"]) -int(meIndex) )):
                 Alumina_Province["Alumina_Province_Shandong"][meIndex] =float( alumina["Production - Shandong"][currentIndex])
                 Alumina_Province["Alumina_Province_Henan"][meIndex] = float(alumina["Production - Henan"][currentIndex])
                 Alumina_Province["Alumina_Province_Shanxi"][meIndex] = float(alumina["Production - Shanxi"][currentIndex])
                 Alumina_Province["Alumina_Province_Guizhou"][meIndex] = float(alumina["Production - Guizhou"][currentIndex])
                 Alumina_Province["Alumina_Province_Guangxi"][meIndex] = float(alumina["Production - Guangxi"][currentIndex])
                 Alumina_Province["Alumina_Province_Chongqing"][meIndex] = float(alumina["Production - Chongqing"][currentIndex])
                 Alumina_Province["Alumina_Province_Other"][meIndex] = float(alumina["Production - Other"][currentIndex])
                 Alumina_Province["Alumina_Province_Total"][meIndex] = float(Alumina_Province["Alumina_Province_Shandong"][meIndex] ) + float(Alumina_Province["Alumina_Province_Henan"][meIndex]) + float(Alumina_Province["Alumina_Province_Shanxi"][meIndex]) + float(Alumina_Province["Alumina_Province_Guizhou"][meIndex] ) +float(Alumina_Province["Alumina_Province_Guangxi"][meIndex] )+ float(Alumina_Province["Alumina_Province_Chongqing"][meIndex]) + float(Alumina_Province["Alumina_Province_Other"][meIndex])
                 meIndex = meIndex+1
                 currentIndex = currentIndex+1
             Alumina_Province.drop("index", axis=1 , inplace=True)
             self.Alumina_Province = Alumina_Province.copy()
             return Alumina_Province
        
    def Bauxite_Usage(self):
        bauxit = self.data_inp_montly_bauxite_usage.copy()
        Bauxite_Usage = self.Get_Date_Year_Quarter()
        me = self.data_inp_montly_tab1.copy()
        # Bauxite_Usage = Get_Date_Year()
        Bauxite_Usage["Bauxite_Usage_Henan"] = 0
        Bauxite_Usage["Bauxite_Usage_Shanxi"] =0
        Bauxite_Usage["Bauxite_Usage_Guizhou"]=0
        Bauxite_Usage["Bauxite_Usage_Guangxi"]=0
        Bauxite_Usage["Bauxite_Usage_Chongqing"]=0
        Bauxite_Usage["Bauxite_Usage_Other"]=0
        currentIndex= me[me['Date']=='01-01-17'].index.values
        meIndex= Bauxite_Usage[Bauxite_Usage["Date"]=='01-01-17'].index.values
        # currentIndex = int(one[0])
        # meIndex = int(two[0])
        for  i in range( (len(Bauxite_Usage["Date"]) -int(meIndex) )):
             Bauxite_Usage["Bauxite_Usage_Henan"][meIndex] = float(bauxit["Henan"][currentIndex])
             Bauxite_Usage["Bauxite_Usage_Shanxi"][meIndex] = float(bauxit["Shanxi"][currentIndex])
             Bauxite_Usage["Bauxite_Usage_Guizhou"][meIndex] = float(bauxit["Guizhou"][currentIndex])
             Bauxite_Usage["Bauxite_Usage_Guangxi"][meIndex] = float(bauxit["Guangxi"][currentIndex])
             Bauxite_Usage["Bauxite_Usage_Chongqing"][meIndex] = float(bauxit["Chongqing"][currentIndex])
             Bauxite_Usage["Bauxite_Usage_Other"][meIndex] = float(bauxit["Other"][currentIndex])
             meIndex = meIndex+1
             currentIndex = currentIndex+1
        Bauxite_Usage.drop("index", axis=1 , inplace=True)
        self.Bauxite_Usage_Sheet  = Bauxite_Usage.copy()

    def Aa_Imported_Bauxite(self):
        Imported_Bauxite = self.Get_Date_Year_Quarter()
        me = self.data_inp_montly_tab1.copy()
        data = self.data_imported_bauxite.copy()

        Imported_Bauxite['Henan'] =0
        Imported_Bauxite['Shanxi'] =0
        Imported_Bauxite["Guizhou"] =0
        Imported_Bauxite["Guangxi"] =0
        Imported_Bauxite["Chongqing"] =0
        Imported_Bauxite["IM"] =0
        Imported_Bauxite["Other"] =0
        currentIndex= data[data['Date']=='01-01-16'].index.values
        meIndex = Imported_Bauxite[Imported_Bauxite["Date"]=='01-01-16'].index.values
        lastIndex = data[data['Date']=='12-01-2018'].index.values
        while currentIndex != lastIndex:
                 Imported_Bauxite["Henan"][meIndex] = float(data["Henan"][currentIndex])
                 Imported_Bauxite["Shanxi"][meIndex] = float(data["Shanxi"][currentIndex])
                 Imported_Bauxite["Guizhou"][meIndex] = float(data["Guizhou"][currentIndex])
                 Imported_Bauxite["Guangxi"][meIndex] = float(data["Guangxi"][currentIndex])
                 Imported_Bauxite["Chongqing"][meIndex] = float(data["Chongqing"][currentIndex])
                 Imported_Bauxite["Other"][meIndex] = float(data["Other"][currentIndex])
                 Imported_Bauxite["IM"][meIndex] = float(data["IM"][currentIndex])
                 currentIndex=currentIndex+1
                 meIndex = meIndex+1
        currentIndex= data[data['Date']=='01-01-19'].index.values
        meIndex = Imported_Bauxite[Imported_Bauxite["Date"]=='01-01-19'].index.values
        lastIndex= data[data['Date']==data["Date"].iloc[-1]].index.values #to change BX 
        self.AAproductions_bx_consumed # for other 
        a= 85
        while currentIndex != lastIndex:
            Imported_Bauxite["Henan"][meIndex] =0 #calculation depends on AA production not coded 
            Imported_Bauxite["Shanxi"][meIndex]=0  #calculation depends on AA production not coded
            Imported_Bauxite["Guizhou"][meIndex]=0  #calculation depends on AA production not coded
            Imported_Bauxite["Guangxi"][meIndex] =0  #calculation depends on AA production not coded
            Imported_Bauxite["Chongqing"][meIndex] = 0   #calculation depends on AA production not coded
            Imported_Bauxite["IM"][meIndex] = 0   #calculation depends on AA production not coded
            Imported_Bauxite["Other"][meIndex] =(self.AAproductions_bx_consumed[self.AAproductions_bx_consumed.columns[85]][6]/self.AAproductions_bx_consumed[self.AAproductions_bx_consumed.columns[85]][6])/1000
            a = a+1
            currentIndex=currentIndex+1
            meIndex = meIndex+1
        Imported_Bauxite.drop("index", axis=1 , inplace=True)
        self.Imported_Bauxite_Sheet = Imported_Bauxite.copy()
        return Imported_Bauxite
        # print(self.Imported_Bauxite_Sheet)
    def Global_Alumina_Production(self):
         Global_Alumina = self.Get_Date_Year_Quarter()
         me = self.data_inp_montly_tab1.copy()
         data = self.dataInputMontly_global_alumina.copy()
         alumina = self.Alumina_By_Province_Mt_Aa()
         cga = self.CGA()
         Global_Alumina["Africa & Asia (ex China)"]=0
         Global_Alumina["North America"]=0
         Global_Alumina["South America"]=0
         Global_Alumina["West Europe"]=0
         Global_Alumina["East & Central Europe"]=0
         Global_Alumina["Oceania"]=0
         Global_Alumina["ROW Est_Un-reported"]=0
         Global_Alumina["China"]=0
         Global_Alumina["Total"] =0
         a = Global_Alumina[Global_Alumina["Date"]=='10-01-2018'].index.values 
         currentIndex= me[me['Date']=='01-01-16'].index.values
         meIndex = Global_Alumina[Global_Alumina["Date"]=='01-01-16'].index.values
         aluminaIndex= alumina[alumina['Date']=='01-01-16'].index.values
         cgaIndex= cga[cga['Date']=='01-01-16'].index.values
         for  i in range( (len(Global_Alumina["Date"]) -int(meIndex) )):
                
                 Global_Alumina["Africa & Asia (ex China)"][meIndex] = float(data["Africa & Asia (ex China)"][currentIndex])
                 Global_Alumina["North America"][meIndex] = float(data["North America"][currentIndex])
                 Global_Alumina["South America"][meIndex] = float(data["South America"][currentIndex])
                 Global_Alumina["West Europe"][meIndex] = float(data["West Europe"][currentIndex])
                 Global_Alumina["East & Central Europe"][meIndex] = float(data["East & Central Europe"][currentIndex])
                 Global_Alumina["Oceania"][meIndex] = float(data["Oceania"][currentIndex])
                 Global_Alumina["ROW Est_Un-reported"][meIndex] = float(data["ROW Est_Un-reported"][currentIndex])
                 Global_Alumina["China"][meIndex] = (alumina["Alumina_Province_Total"][aluminaIndex]+cga["CGA - total"][cgaIndex])*1000
                 Global_Alumina["Total"][meIndex]= float(Global_Alumina["Africa & Asia (ex China)"][meIndex]) +float(Global_Alumina["North America"][meIndex]) + float(Global_Alumina["South America"][meIndex]) + float(Global_Alumina["West Europe"][meIndex]) + float(Global_Alumina["East & Central Europe"][meIndex]) + float(Global_Alumina["Oceania"][meIndex]) + float(Global_Alumina["ROW Est_Un-reported"][meIndex]) + float(Global_Alumina["China"][meIndex])
                 meIndex = meIndex+1
                 currentIndex = currentIndex+1
                 cgaIndex = cgaIndex+1
                 aluminaIndex = aluminaIndex+1



        
         currentIndex= me[me['Date']=='01-01-18'].index.values
         meIndex = Global_Alumina[Global_Alumina["Date"]=='01-01-18'].index.values
         lastIndex = Global_Alumina[Global_Alumina["Date"]=='10-01-2018'].index.values 
         aluminaIndex= alumina[alumina['Date']=='01-01-18'].index.values
         cgaIndex= cga[cga['Date']=='01-01-18'].index.values
         for  i in range( (len(Global_Alumina["Date"]) -int(meIndex) )):
                 Global_Alumina["China"][meIndex] = alumina["Alumina_Province_Total"][aluminaIndex]*1000
                 Global_Alumina["Total"][meIndex]= float(Global_Alumina["Africa & Asia (ex China)"][meIndex]) +float(Global_Alumina["North America"][meIndex]) + float(Global_Alumina["South America"][meIndex]) + float(Global_Alumina["West Europe"][meIndex]) + float(Global_Alumina["East & Central Europe"][meIndex]) + float(Global_Alumina["Oceania"][meIndex]) + float(Global_Alumina["ROW Est_Un-reported"][meIndex]) + float(Global_Alumina["China"][meIndex])
                 meIndex = meIndex+1
                 currentIndex = currentIndex+1
                 cgaIndex = cgaIndex+1
                 aluminaIndex = aluminaIndex+1
         Global_Alumina.drop("index", axis=1 , inplace=True)
         self.Global_Alumina_Production_Sheet = Global_Alumina.copy()
    
    def Merchant_Alumina_Prod(self):
        Merchant_Alumina = self.Aa_Imported_Bauxite()
        Merchant_Alumina["Shandong"] = self.Alumina_Province["Alumina_Province_Shandong"] - 0.7/12
        for i in Merchant_Alumina["Shandong"]:
            if(i<0):
                index =  Merchant_Alumina[Merchant_Alumina["Shandong"]== i].index.values
                Merchant_Alumina["Shandong"][index] =0
        
        currentIndex= self.Alumina_Province[self.Alumina_Province['Date']=='03-01-19'].index.values
        meIndex = Merchant_Alumina[Merchant_Alumina["Date"]=='03-01-19' ].index.values

        for i in range( len(Merchant_Alumina["Shandong"]) - int(meIndex)):
            Merchant_Alumina["Shandong"][meIndex] = self.Alumina_Province["Alumina_Province_Shandong"][currentIndex]
            meIndex = meIndex+1
            currentIndex = currentIndex+1
        self.Merchant_Alumina_Prod_sheet = Merchant_Alumina.copy()
        return Merchant_Alumina
    
    def CGA(self):
        CGA = self.Get_Date_Year_Quarter()
        data = self.data_monthly_imp_CGA.copy()
        CGA["CGA - total"]= 0
        CGA["CGA - Import"]=0
        CGA['CGA-Domestic'] =0
        CGA.drop("index", axis=1 , inplace=True)
        # get CGA["total"] values 
        currentIndex= data[data['Date']=='01-01-16'].index.values
        meIndex = CGA[CGA["Date"]=='01-01-16'].index.values
        for  i in range( (len(CGA["Date"]) -int(meIndex) )):
                 CGA["CGA - total"][meIndex] = float(data["CGA - total"][currentIndex])
                 currentIndex=currentIndex+1
                 meIndex = meIndex+1
        currentIndex= data[data['Date']=='01-01-20'].index.values
        meIndex = CGA[CGA["Date"]=='01-01-20'].index.values
        lastIndex= data[data['Date']==data["Date"].iloc[-1]].index.values
        while currentIndex != lastIndex:
                 CGA['CGA - Import'][meIndex] = float(data["CGA - Import"][currentIndex])
                 CGA['CGA-Domestic'][meIndex] = CGA['CGA - total'][meIndex] - CGA["CGA - Import"][meIndex]
                 currentIndex=currentIndex+1
                 meIndex = meIndex+1
        

       
        self.CGA_Sheet = CGA.copy()
        return CGA

    def Bauxite_Usage_t_t(self):
        me = self.data_inp_montly_tab1.copy()
        bar = self.dataInputMontly_BAR.copy()
        Bauxite_Usage_t = self.Get_Date_Year_Quarter()
        Bauxite_Usage_t["Weighted Average by Import Data"] =0
        # currentIndex=  me[ me['Date']=='01-01-16'].index.values
        meIndex = Bauxite_Usage_t[Bauxite_Usage_t["Date"]=='01-01-16'].index.values[0]
        # meIndex = 182
        currentIndex= 252
        lastIndex= me[me['Date']==me["Date"].iloc[-1]].index.values
         
        while currentIndex != lastIndex:
                 a = bar["Total Combined"][currentIndex]
                 Bauxite_Usage_t["Weighted Average by Import Data"][meIndex] = float(a)
                 if(meIndex == 182):
                         Bauxite_Usage_t["Weighted Average by Import Data"][meIndex] = float(bar["Total Combined"][currentIndex])

                 meIndex = meIndex+1
                 currentIndex = currentIndex+1
             

       
        self.Bauxite_Usage_t_Sheet = Bauxite_Usage_t.copy()
        self.Bauxite_Usage_t_Sheet.drop("index", axis=1 , inplace=True)
        return Bauxite_Usage_t


    def Bauxite_Demand(self):
        BX = self.Merchant_Alumina_Prod()
        a= self.Bauxite_Usage_t_t()
        col = BX.columns[3:]
        i =0 ;
        lastIndex= BX[BX['Date']==BX["Date"].iloc[-1]].index.values


        while i <lastIndex:
            BX["Henan"][i] = BX["Henan"][i]*a["Weighted Average by Import Data"][i]
            BX["Shanxi"][i] = BX["Shanxi"][i]*a["Weighted Average by Import Data"][i]
            BX["Guizhou"][i]= BX["Guizhou"][i]*a["Weighted Average by Import Data"][i]
            BX["Chongqing"][i] = BX["Chongqing"][i]*a["Weighted Average by Import Data"][i]
            BX["IM"][i] = BX["IM"][i]*a["Weighted Average by Import Data"][i]
            BX["Other"][i]= BX["Other"][i]*a["Weighted Average by Import Data"][i]
            BX["Shandong"][i] =BX["Shandong"][i]*a["Weighted Average by Import Data"][i]
            i = i+1
        self.Bauxite_Demand_Sheet = BX.copy()
    

    def Domestic_Bauxite_Demand(self):
        DBD = self.Get_Date_Year_Quarter()
        Alumina_Province = self.Alumina_By_Province_Mt_Aa()
        Imported_Bauxite = self.Aa_Imported_Bauxite()
        CGa = self.CGA()
        DBD['Henan'] =0
        DBD['Shanxi'] =0
        DBD["Guizhou"] =0
        DBD["Guangxi"] =0
        DBD["Chongqing"] =0
        DBD["Other"] =0
        DBD["CGA"] =0
        DBD["Total"]=0
        Alumina_Index= Alumina_Province[Alumina_Province['Date']=='01-01-17'].index.values
        import_Index = Imported_Bauxite[Imported_Bauxite["Date"]=='01-01-17'].index.values
        CGA_Index = CGa[CGa["Date"]=='01-01-17'].index.values
        meIndex = DBD[DBD["Date"]=='01-01-17'].index.values
        lastIndex= Alumina_Province[Alumina_Province['Date']==Alumina_Province["Date"].iloc[-1]].index.values
        while Alumina_Index != lastIndex:
             DBD['Henan'][meIndex] = (Alumina_Province["Alumina_Province_Henan"][Alumina_Index]-Imported_Bauxite["Henan"][import_Index])*2.37 #to change 2.37 to formular when coded in bauxite Usage domestic bars
             DBD['Shanxi'][meIndex] =Alumina_Province["Alumina_Province_Shanxi"][Alumina_Index] * 2.37
             DBD["Guizhou"][meIndex] =Alumina_Province["Alumina_Province_Guizhou"][Alumina_Index] * 2.37
             DBD["Guangxi"][meIndex]=Alumina_Province["Alumina_Province_Guangxi"][Alumina_Index] * 2.37
             DBD["Chongqing"][meIndex] =(Alumina_Province["Alumina_Province_Chongqing"][Alumina_Index]-Imported_Bauxite["Chongqing"][import_Index])*2.37
             DBD["Other"][meIndex] =(Alumina_Province["Alumina_Province_Other"][Alumina_Index]- Imported_Bauxite["Other"][import_Index]) *2.37
             DBD["CGA"][meIndex] = CGa["CGA - total"][CGA_Index]*2.37
             DBD["Total"][meIndex]= float(DBD['Henan'][meIndex]) + float(DBD['Shanxi'][meIndex]) + float(DBD["Guizhou"][meIndex]) + float(DBD["Guangxi"][meIndex]) + float(DBD["Chongqing"][meIndex]) + float(DBD["Other"][meIndex]) + float(DBD["CGA"][meIndex] )
             meIndex = meIndex+1
             Alumina_Index = Alumina_Index+1
             import_Index = import_Index+1
             CGA_Index = CGA_Index+1
        
        DBD.drop("index", axis=1 , inplace=True)
        DBD.fillna(value=0, inplace=True)
        self.Domestic_Bauxite_Demand_Sheet = DBD.copy()
        return DBD
    
    def Domestic_Bauxite_Summary(self):
        dbd = self.Domestic_Bauxite_Demand()
        cga =self.CGA()
        DBS =self.Get_Date_Year_Quarter()
        DBS["Production for SGA - Till 2019"]=0
        DBS["Production for CGA - Till 2019"]=0
        DBS["Production for Refractory"]=0
        # 
        DBS["Bauxite Production in China"]=0
        DBS["Production for SGA - From Jan2020"]=0
        DBS["Production for CGA - From Jan2020"]=0
        DBS["Production for Refractory 1"]=0
        DBS["Bauxite Production in China 1"] =0
        # print(self.data_inp_montly_bauxite_usage["Refractory Baxuite Mln t"])
        meIndex = DBS[DBS["Date"]=='01-01-17'].index.values
        currentIndex= dbd[dbd['Date']=='01-01-17'].index.values
        lastIndex= dbd[dbd['Date']==dbd["Date"].iloc[-1]].index.values
        refin = self.data_inp_montly_bauxite_usage["Refractory Baxuite Mln t"].copy()
        # a =self.data_inp_montly_bauxite_usage[self.data_inp_montly_bauxite_usage['Date']=='01-01-17'].index.values
        a = 264
        for  i in range( (len(DBS["Date"]) -int(meIndex) )):
             DBS["Production for SGA - Till 2019"][meIndex]=float(dbd['Henan'][currentIndex])+ float(dbd['Shanxi'][currentIndex]) + float(dbd["Guizhou"][currentIndex]) + float(dbd["Guangxi"][currentIndex]) + float(dbd["Chongqing"][currentIndex]) + float(dbd["Other"][currentIndex])
             DBS["Production for CGA - Till 2019"][meIndex]= dbd["CGA"][currentIndex]
             DBS["Production for Refractory"][meIndex]= refin[a]
             DBS["Bauxite Production in China"][meIndex]= float( DBS["Production for SGA - Till 2019"][meIndex]) + float( DBS["Production for CGA - Till 2019"][meIndex]) + float( DBS["Production for Refractory"][meIndex])
             DBS["Production for SGA - From Jan2020"][meIndex]=0
             DBS["Production for CGA - From Jan2020"][meIndex]=0
             DBS["Production for Refractory 1"][meIndex]=0
             DBS["Bauxite Production in China 1"][meIndex] =DBS["Bauxite Production in China"][meIndex]
             if(meIndex>=179):
                 DBS["Production for CGA - From Jan2020"][meIndex]=cga['CGA-Domestic'][currentIndex]*2.3945
                 DBS["Production for SGA - From Jan2020"][meIndex]= dbd["Total"][currentIndex]-DBS["Production for CGA - From Jan2020"][meIndex]
                 DBS["Production for Refractory 1"][meIndex]= refin[a]
                 DBS["Bauxite Production in China 1"][meIndex] =float(DBS["Production for SGA - From Jan2020"][meIndex]) + float(DBS["Production for CGA - From Jan2020"][meIndex]) + float(DBS["Production for Refractory 1"][meIndex])
             currentIndex=currentIndex+1
             meIndex = meIndex+1
             lastIndex=lastIndex+1
             a = a+1

        DBS.drop("index", axis=1 , inplace=True)
        self.Domestic_Bauxite_Summary_Sheet = DBS.copy()
        return DBS

    def Import_Bauxite_Summary(self):
        IBS = self.Get_Date_Year_Quarter()
        con = self.dataMonthly_consumpt.copy()
        dbs = self.Domestic_Bauxite_Summary()
        IBS["Imported for SGA - From Jan2020"]=0
        IBS["Imported for CGA - From Jan2020"]=0
        IBS["Bauxite for SGA"]=0
        IBS["Bauxite for CGA"]=0
        currentIndex = con[con["Date"]=='01-01-2020'].index.values
        meIndex = IBS[IBS["Date"]=='01-01-2020'].index.values
        lastIndex= dbs[dbs['Date']==dbs["Date"].iloc[-1]].index.values
        mIndes = dbs[dbs["Date"]=='01-01-2020'].index.values
        while meIndex != lastIndex:
             IBS["Imported for SGA - From Jan2020"][meIndex]=con["Consump"][currentIndex]-0.53
             IBS["Imported for CGA - From Jan2020"][meIndex]=0.53
             IBS["Bauxite for SGA"][meIndex]= dbs["Production for SGA - From Jan2020"][mIndes]+IBS["Imported for SGA - From Jan2020"][meIndex]
             IBS["Bauxite for CGA"][meIndex]=dbs["Production for CGA - From Jan2020"][mIndes] + IBS["Imported for CGA - From Jan2020"][meIndex]
             meIndex = meIndex+1
             lastIndex=lastIndex+1
             currentIndex = currentIndex+1
             mIndes= mIndes+1




        IBS.drop("index", axis=1, inplace=True)
        self.Import_Bauxite_Summary_Sheet= IBS.copy()
        #labaran codes
        
        
        
    #general functions
    def excel_date(self, y, m, d):
        date1 = datetime(y, m, d)
        temp = datetime(1899, 12, 30)    # Note, not 31st Dec but 30th!
        delta = date1 - temp
        return float(delta.days) + (float(delta.seconds) / 86400)
    
    def func_1(self, row, Country):
        row = row - 252
        cols = self.bxImports_tab8.columns[1:].to_list()
        value = self.bxImports_tab8.loc[al.bxImports_tab8['Country']==Country, cols[row]].copy()
        return float(value)
    
    def func_2(self, row, unknown):
        row = row - 252
        cols = self.bxImports_tab9.columns[2:].to_list()
        table = self.bxImports_tab9.loc[3:,:]
        value = table.loc[table['Technology Code']==unknown, cols[row]].copy()
        return float(value)
    
    def func_6months(self, v):
        d = v.copy()
        for i in range(10, len(v)-1):
            d[i] = v[1+i-6:i+1].sum()/6
        return d
        
        
al = ALengine()
al.calc_all()

writer1 = pd.ExcelWriter('Outputs//Data Input Annual Output.xlsx')
al.data_input_annual.to_excel(writer1,sheet_name='Sheet1', encoding ='utf-8', index=False)
writer1.save()

writer2 = pd.ExcelWriter('Outputs//Bx Imports Output.xlsx')
al.bxImports_tab1.to_excel(writer2,sheet_name='Table 1', encoding='utf-8', index=False)
al.bxImports_tab2.to_excel(writer2,sheet_name='SMB split tonnes', encoding='utf-8', index=False)
al.bxImports_tab3.to_excel(writer2,sheet_name='Re-done tonnes', encoding='utf-8', index=False)
al.bxImports_tab4.to_excel(writer2,sheet_name='Re-done proportions within Ctry', encoding='utf-8', index=False)
al.bxImports_tab5.to_excel(writer2,sheet_name='Proportions', encoding='utf-8', index=False)
al.bxImports_tab6.to_excel(writer2,sheet_name='Tt for different bauxite', encoding='utf-8', index=False)
al.bxImports_tab7.to_excel(writer2,sheet_name='Tt per Country', encoding='utf-8', index=False)
al.bxImports_tab8.to_excel(writer2,sheet_name='Tt per Country no gaps', encoding='utf-8', index=False)
al.bxImports_tab9.to_excel(writer2,sheet_name="Australia LT-HT split", encoding='utf-8', index=False)
writer2.save()

writer3 = pd.ExcelWriter('Outputs//Refineries and Aa productions.xlsx')
al.refAAproductions.to_excel(writer3,sheet_name='Table 1', encoding='utf-8', index=False)
writer3.save()

writer4 = pd.ExcelWriter('Outputs//Changjiang & LME Daily.xlsx')
al.changjiangLME.to_excel(writer4,sheet_name='Table 1', encoding='utf-8', index=False)
writer4.save()

writer5 = pd.ExcelWriter('Outputs//Platts Vs Cmaax.xlsx')
al.plattsVsCmaax.to_excel(writer5,sheet_name='Table 1', encoding='utf-8', index=False)
writer5.save()        

writer6 = pd.ExcelWriter('Outputs//Data Input Monthlyy.xlsx')
al.dataInputMontly_alumina_montly_supply.to_excel(writer6,sheet_name='Alumina Monthly Supply', encoding='utf-8', index=False)
al.dataInputMontly_aluminium_montly_supply.to_excel(writer6,sheet_name='Aluminium Monthly Supply', encoding='utf-8', index=False)
al.dataInputMontly_alumina_price_trend.to_excel(writer6, sheet_name='Alumina Price Trends', encoding='utf-8', index=False )
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

writer7= pd.ExcelWriter('Outputs//Mines and Bx Productions.xlsx')
al.minesBxPrdocution1.to_excel(writer7,sheet_name='Main Table', encoding='utf-8', index=False)
al.minesBxPrdocution2.to_excel(writer7,sheet_name='Table 2', encoding='utf-8', index=False)
al.minesBxPrdocution_final.to_excel(writer7,sheet_name='final', encoding='utf-8', index=False)
writer7.save()


writer8= pd.ExcelWriter('Outputs//Bauxite Sources.xlsx')
al.BauxiteSources.to_excel(writer8,sheet_name='Main Table', encoding='utf-8', index=False)
al.BauxiteSourcesSummary.to_excel(writer8,sheet_name='Summary', encoding='utf-8', index=False)
writer8.save()



writer9 = pd.ExcelWriter('Outputs//Global Bx Quarterly Data.xlsx')
al.GlobalBx_cmglobal.to_excel(writer9,sheet_name='CM Global Bx Production', encoding='utf-8', index=False)
al.GlobalBx_productionMainCompanies.to_excel(writer9,sheet_name='Production for Main Companies', encoding='utf-8', index=False)
al.GlobalBx_out_tab1.to_excel(writer9,sheet_name='Output tab 1', encoding='utf-8', index=False)
al.GlobalBx_out_tab2.to_excel(writer9,sheet_name='Output tab 2', encoding='utf-8', index=False)
al.GlobalBx_out_tab3.to_excel(writer9,sheet_name='Output tab 3', encoding='utf-8', index=False)
writer9.save()

writer10 = pd.ExcelWriter('Outputs//AA Productions from Imp Bx Monthly.xlsx')
al.AAproductions_sheet3.to_excel(writer10,sheet_name='BAR Factors', encoding='utf-8', index=False)
al.AAproductions_out1.to_excel(writer10,sheet_name='Cap and Prod Totals', encoding='utf-8', index=False)
al.AAproductions_out2.to_excel(writer10,sheet_name='AA Prod and Cap Bx', encoding='utf-8', index=False)
al.AAproductions_inland_ref.to_excel(writer10,sheet_name='Inland Ref as Alumina', encoding='utf-8', index=False)
al.AAproductions_summary_prodTotal.to_excel(writer10,sheet_name='Summary total AA production', encoding='utf-8', index=False)
al.AAproductions_inner_mongolia_chart_shandong.to_excel(writer10,sheet_name='Inner Mongolia Chart', encoding='utf-8', index=False)
al.AAproductions_summary_AAprodHT.to_excel(writer10,sheet_name='Summary AA production HT', encoding='utf-8', index=False)
al.AAproductions_summary_AAprodLT.to_excel(writer10,sheet_name='Summary AA production LT', encoding='utf-8', index=False)
al.AAproductions_summary_totalCapImpBx.to_excel(writer10,sheet_name='Summary Total AA prods HTLT', encoding='utf-8', index=False)
al.AAproductions_summary_modelled_991_1003.to_excel(writer10,sheet_name='Summary Modelled Values', encoding='utf-8', index=False)
al.AAproductions_summary_capTotal.to_excel(writer10,sheet_name='Summary Total Capacities', encoding='utf-8', index=False)
al.AAproductions_summary_AAcapHT.to_excel(writer10,sheet_name='Summary Total Capacity HT', encoding='utf-8', index=False)
al.AAproductions_summary_AAcapLT.to_excel(writer10,sheet_name='Summary Total Capacity LT', encoding='utf-8', index=False)
al.AAproductions_summary_totalCapImpBx_cap.to_excel(writer10,sheet_name='Summary Total Caps HT_LT', encoding='utf-8', index=False)
al.AAproductions_summary_alumina_production.to_excel(writer10,sheet_name='Summary Alumina Productions', encoding='utf-8', index=False)
al.AAproductions_summary_data_to_chart_sheet.to_excel(writer10,sheet_name='Summary Data Chart Sheet', encoding='utf-8', index=False)
writer10.save()

writer11 = pd.ExcelWriter('Outputs/Data Monthly.xlsx')
al.dataMonthly_alumina_monthly.to_excel(writer11,sheet_name='Alumina montlhy', encoding='utf-8', index=False)
al.dataMonthly_aluminium_supply.to_excel(writer11,sheet_name='Aluminuim Supply', encoding='utf-8', index=False)
al.dataMonthly_bauxite_imps1.to_excel(writer11,sheet_name='Bauxite Imports 1', encoding='utf-8', index=False)
al.dataMonthly_bauxite_imps2.to_excel(writer11,sheet_name='Bauxite Imports 2', encoding='utf-8', index=False)
al.dataMonthly_bauxite_imps3.to_excel(writer11,sheet_name='Bauxite Imports 3', encoding='utf-8', index=False)
al.dataMonthly_consumpt.to_excel(writer11,sheet_name='Consumptions', encoding='utf-8', index=False)
# New 
al.Alumina_Province.to_excel(writer11, sheet_name="Alumina By Province", encoding='utf-8', index=False)
al.Bauxite_Usage_Sheet.to_excel(writer11, sheet_name="Bauxite Usage", encoding="utf-8", index=False)
al.Imported_Bauxite_Sheet.to_excel(writer11 , sheet_name="Imported Bauxite ", encoding='utf-8', index=False)
al.Global_Alumina_Production_Sheet.to_excel(writer11, sheet_name="Global Alumina Production" , encoding='utf-8', index=False)
al.Merchant_Alumina_Prod_sheet.to_excel(writer11, sheet_name="Merchant Alumina Production"  , encoding='utf-8', index=False)
al.CGA_Sheet.to_excel(writer11, sheet_name="CGA",  encoding='utf-8', index=False)
al.Bauxite_Usage_t_Sheet.to_excel(writer11, sheet_name="Bauxite Usage(t_t)" ,encoding='utf-8', index=False)
al.Bauxite_Demand_Sheet.to_excel(writer11 , sheet_name="Bauxite Demand(Mt)" ,encoding='utf-8', index=False)
al.Domestic_Bauxite_Demand_Sheet.to_excel(writer11, sheet_name="Domestic Bauxite Demand", encoding="utf-8", index=False)
al.Domestic_Bauxite_Summary_Sheet.to_excel(writer11, sheet_name="Domestic Bauxite Summary" , encoding="utf-8", index=False)
al.Import_Bauxite_Summary_Sheet.to_excel(writer11, sheet_name="Imported Bauxite Summary", encoding="utf-8", index=False)
writer11.save()


writer12 = pd.ExcelWriter('Outputs//CM Global Bx Productions.xlsx')
al.cm_global_prd.to_excel(writer12,sheet_name='Main', encoding='utf-8', index=False)
al.cm_global_prd_sum1.to_excel(writer12, sheet_name='Summary Production by Region', index=False)
al.cm_global_prd_chineseSum.to_excel(writer12,sheet_name='Summary Chinese Province', encoding='utf-8', index=False)
al.cm_global_prd_china_provincial.to_excel(writer12,sheet_name='China Provincial Imp', encoding='utf-8', index=False)
al.cm_global_prd_sum2.to_excel(writer12, sheet_name='Summary Production Country', index=False)
writer12.save()


writer13 = pd.ExcelWriter('Outputs//Special Charts Outputs.xlsx')
al.specialCharts_out1.to_excel(writer13, sheet_name='Fig 15', encoding='utf-8',index=False)
al.specialCharts_out2.to_excel(writer13, sheet_name='Fig 22', encoding='utf-8',index=False)
writer13.save()

writer14 =pd.ExcelWriter('Outputs//Data Charts Outputs.xlsx')
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
        al_flat.mult_year_single_output(al.data_input_annual, "bx imports data input annual", [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab1, "bx imports bx_Imports_tab1", [(0,2)], [(2,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab2,'bx imports SMB split tonnes', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab3,'bx imports Re-done tonnes',[(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab4,'bx imports Re-done proportions within Ctry',[(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab5,'bx imports Proportions',[(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab6,'bx imports Tt for different bauxite',[(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab7,'bx imports Tt per Country',[(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab8,'bx imports Tt per Country no gaps',[(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.bxImports_tab9,"bx imports Australia LT-HT split",[(0,2)], [(2,)], label='variable'),
        al_flat.mult_year_single_output(al.refAAproductions, "bx imports refineries aa production", [(0,4)], [(4,)], label='variable'),
    
        al_flat.mult_year_single_output(al.changjiangLME, 'Changjiang & LME Daily', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.plattsVsCmaax, 'Platts Vs Cmaax', [(0,3)], [(3,)], label='variable'),

        al_flat.mult_year_single_output(al.dataInputMontly_alumina_montly_supply, 'data input monthly Alumina Monthly Supply', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_aluminium_montly_supply, 'data input monthly Aluminium Monthly Supply', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_alumina_price_trend, 'data input monthly Alumina Price Trends', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_bauxite_imports1, 'data input monthly Bauxite Imports 1', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_bauxite_imports2, 'data input monthly Bauxite Imports 2', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_bauxite_imports3, 'data input monthly Bauxite Imports 3', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_bauxite_imports4, 'data input monthly Bauxite Imports 4', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_bauxite_imports5, 'data input monthly Bauxite Imports 5', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_input_bar, 'data input monthly Input BAR', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_BAR, 'data input monthly BAR', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_CmPortBx, 'data input monthly CM Port Bx Invt.', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_global_alumina, 'data input monthly Global Alumina', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataInputMontly_inland1, 'data input monthly Inland ref 1', [(0,3)], [(3,)], label='variable'),
        # al_flat.mult_year_single_output(al.dataInputMontly_inland2, 'data input monthly Inland ref 2', [(0,3)], [(3,)], label='variable'),

        al_flat.mult_year_single_output(al.minesBxPrdocution1, 'mines bx production Main Table', [(0,4)], [(4,)], label='variable'),
        al_flat.mult_year_single_output(al.minesBxPrdocution2, 'mines bx production Table 2', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.minesBxPrdocution_final, 'mines bx production final', [(0,3)], [(3,)], label='variable'),

        al_flat.mult_year_single_output(al.BauxiteSources, 'Bauxite Sources', [(0,4)], [(4,)], label='variable'),
        al_flat.mult_year_single_output(al.BauxiteSourcesSummary, 'Bauxite Sources Summary', [(0,2)], [(2,)], label='variable'),
        
        al_flat.mult_year_single_output(al.GlobalBx_cmglobal, 'Global Bx Quarterly Data CM Global Bx Production', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.GlobalBx_productionMainCompanies, 'Global Bx Quarterly Data Production for Main Companies', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.GlobalBx_out_tab1, 'Global Bx Quarterly Data Output tab 1', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.GlobalBx_out_tab2, 'Global Bx Quarterly Data Output tab 2', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.GlobalBx_out_tab3, 'Global Bx Quarterly Data Output tab 3', [(0,1)], [(1,)], label='variable'),

        al_flat.mult_year_single_output(al.AAproductions_sheet3, 'AA Productions from Imp Bx Monthly BAR Factors', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_out1, 'AA Productions from Imp Bx Monthly Cap and Prod Totals', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_out2, 'AA Productions from Imp Bx Monthly AA Prod and Cap Bx', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_inland_ref, 'AA Productions from Imp Bx Monthly Inland Ref as Alumina', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_prodTotal, 'AA Productions from Imp Bx Monthly Summary total AA production', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_inner_mongolia_chart_shandong, 'AA Productions from Imp Bx Monthly Inner Mongolia Chart', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_AAprodHT, 'AA Productions from Imp Bx Monthly Summary AA production HT', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_AAprodLT, 'AA Productions from Imp Bx Monthly Summary AA production LT', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_totalCapImpBx, 'AA Productions from Imp Bx Monthly Summary Total AA prods HTLT', [(0,4)], [(4,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_modelled_991_1003, 'AA Productions from Imp Bx Monthly Summary Modelled Values', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_capTotal, 'AA Productions from Imp Bx Monthly Summary Total Capacities', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_AAcapHT, 'AA Productions from Imp Bx Monthly Summary Total Capacity HT', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_AAcapLT, 'AA Productions from Imp Bx Monthly Summary Total Capacity LT', [(0,7)], [(7,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_totalCapImpBx_cap, 'AA Productions from Imp Bx Monthly Summary Total Caps HT_LT', [(0,4)], [(4,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_alumina_production, 'AA Productions from Imp Bx Monthly Summary Alumina Productions', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.AAproductions_summary_data_to_chart_sheet, 'AA Productions from Imp Bx Monthly Summary Data Chart Sheet', [(0,1)], [(1,)], label='variable'),

        al_flat.mult_year_single_output(al.dataMonthly_alumina_monthly, 'data monthly Alumina montlhy', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataMonthly_aluminium_supply, 'data monthly Aluminuim Supply', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataMonthly_bauxite_imps1, 'data monthly Bauxite Imports 1', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataMonthly_bauxite_imps2, 'data monthly Bauxite Imports 2', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataMonthly_bauxite_imps3, 'data monthly Bauxite Imports 3', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataMonthly_consumpt, 'data monthly Consumptions', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Alumina_Province, "data monthly Alumina By Province", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Bauxite_Usage_Sheet, "data monthly Bauxite Usage", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Imported_Bauxite_Sheet , "data monthly Imported Bauxite ",[(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Global_Alumina_Production_Sheet, "data monthly Global Alumina Production", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Merchant_Alumina_Prod_sheet, "data monthly Merchant Alumina Production", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.CGA_Sheet, "data monthly CGA", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Bauxite_Usage_t_Sheet, "data monthly Bauxite Usage(t_t)", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Bauxite_Demand_Sheet , "data monthly Bauxite Demand(Mt)", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Domestic_Bauxite_Demand_Sheet, "data monthly Domestic Bauxite Demand", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Domestic_Bauxite_Summary_Sheet, "data monthly Domestic Bauxite Summary", [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.Import_Bauxite_Summary_Sheet, "data monthly Imported Bauxite Summary", [(0,3)], [(3,)], label='variable'),

        al_flat.mult_year_single_output(al.cm_global_prd, 'CM Global Bx Productions Main', [(0,10)], [(10,)], label='variable'),
        al_flat.mult_year_single_output(al.cm_global_prd_sum1, 'CM Global Bx Productions Summary Production by Region', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.cm_global_prd_chineseSum, 'CM Global Bx Productions Summary Chinese Province', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.cm_global_prd_china_provincial, 'CM Global Bx Productions China Provincial Imp', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.cm_global_prd_sum2, 'CM Global Bx Productions Summary Production Country', [(0,1)], [(1,)], label='variable'),
        
        al_flat.mult_year_single_output(al.specialCharts_out1, 'special charts Fig 15', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.specialCharts_out2, 'special charts Fig 22', [(0,1)], [(1,)], label='variable'),

        al_flat.mult_year_single_output(al.dataCharts_1, 'data charts output Charts 1', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataCharts_2, 'data charts output Charts 2', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataCharts_3, 'data charts output Charts 3', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataCharts_4, 'data charts output Charts 4', [(0,3)], [(3,)], label='variable'),
        al_flat.single_year_mult_out(al.dataCharts_5, 'data charts output Charts 5'), # change later
        al_flat.mult_year_single_output(al.dataCharts_6, 'data charts output Charts 6', [(0,3)], [(3,)], label='variable'),
        al_flat.single_year_mult_out(al.dataCharts_7, 'data charts output Charts 7'), # data charts output 
        al_flat.mult_year_single_output(al.dataCharts_8, 'data charts output Charts 8', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataCharts_9, 'data charts output Charts 9', [(0,3)], [(3,)], label='variable'),
        # al_flat.mult_year_single_output(al.dataCharts_10, 'data charts output Charts 10', , label='variable'),
        al_flat.single_year_mult_out(al.dataCharts_11, 'data charts output Charts 11'), # data charts output 
        al_flat.single_year_mult_out(al.dataCharts_12, 'data charts output Charts 12'), # data charts output 
        al_flat.mult_year_single_output(al.dataCharts_13, 'data charts output Charts 13', [(0,3)], [(3,)], label='variable'),
        al_flat.mult_year_single_output(al.dataCharts__10, 'data charts output Chart_10', [(0,1)], [(1,)], label='variable'),
        al_flat.mult_year_single_output(al.dataCharts__11, 'data charts output Chart_11', [(0,3)], [(3,)], label='variable')  
]


snapshot = pd.concat(dblist, ignore_index=True)
snapshot.to_csv("snapshot_output_data.csv", index=False)
uploadtodb.upload(snapshot)
