# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 09:03:11 2020

"""
import numpy as np
import pandas as pd
import time
import inspect
import datetime
import csv
from flatdbconverter import Flatdbconverter

a_flat = Flatdbconverter("Aa Price Forecast (CHN)")

default_year = 2020#pd.read_excel('Inputs\\AaPF.xlsx', sheet_name='Year').loc[0,'Year'] #main input year, default is 2020
#timer
class TimerException(Exception):
    pass


class Timer():
    def __init__(self, t_output_filename=None, txt=False, log_result=True):
        if t_output_filename == None:
            raise TimerException("Output filename is required")
        else: 
            if txt:
                t_output_filename = '{0}_{1}.txt'.format(t_output_filename, self._get_curr_date())
            else:
                t_output_filename = '{0}_{1}.csv'.format(t_output_filename, self._get_curr_date())

        self.txt = txt
        self.out_file = t_output_filename
        self._start_time = None
        self._start_asctime = None
        self._function = None
        self._line_no = None
        self._total_time = 0
        self._object_name = None
        self._args_list = None
        self._arg_values = None
        self.log_result = log_result
        self._store = []
        with open('{0}'.format(self.out_file), 'w', encoding="utf-8") as file:
            if not txt:
                fields = ['Module Name', 'Method Name', 'Start time', 'End time', 'Time elapsed', 'Line no', 'Arguments', 'Function Call count', 'Method Outputs']
                csvwriter = csv.writer(file)
                csvwriter.writerow(fields)


    def _get_curr_date(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")

    def start(self):
        """Start new Timer, takes text arg for method description"""
        if self._start_time != None:
            raise TimerException("Timer is running , use .stop() to stop it")
            
        stack = inspect.stack()
        self._line_no = stack[1][2]
        self._function =  stack[1][3]
        self._args_list = inspect.getargvalues(stack[1][0])[0]
        class_name = str(inspect.getargvalues(stack[1][0])[3]['self'])
        i = class_name.find('.')
        j = class_name.find('at')
        self._object_name = class_name[i+1:j]
        args_values = inspect.getargvalues(stack[1][0])[3]
        del args_values['self']
        self._arg_values =  list(args_values.items())
        self._start_time = time.perf_counter()
        self._start_asctime = self._get_curr_date()
        self._store.append(self._function)

    def stop(self,*results, end=False, reset=False):
        # print(results)
        if isinstance(results, (list, tuple)):
            results = [(' ~ ').join(map(str, a)) for a in results]
        """ stop timer and logs elapsed time """
        if self._start_time == None:
            raise TimerException("Timer is not running, use .start() to start it")
        
        elapsed_time = time.perf_counter() - self._start_time
        self._total_time += elapsed_time
        if self.txt:
            self.txt_time_logger(elapsed_time, self._get_curr_date(), results)
        else:
            self.csv_time_logger(elapsed_time, self._get_curr_date(), results)
        self._start_time = None
        if reset:
            self.end_time()
            self._total_time = 0
        if end:
            self.end_time()

    def txt_time_logger(self, elapsed_time, end_time, results):
        count = self._store.count(self._function)
        text = '\n\nModule name:\t{0}\nMethod Name:\t{1}\nStart time:\t{2}\nEnd Time:\t{3}\nTime elapsed:\t{4} secs\nLine no:\t{5}\nArguments:\t{6}\nFunction call count:\t{7}\nMETHOD OUTPUTS'
        log_format = text.format(self._object_name, self._function, self._start_asctime, end_time , elapsed_time, self._line_no, self._arg_values, count )
        
        with open('{0}'.format(self.out_file), 'a', encoding="utf-8") as file:
            file.write(log_format)
            if self.log_result:
                for r in results:
                    file.write('\n{0}'.format(r))

    def csv_time_logger(self, elapsed_time, end_time, results):
        count = self._store.count(self._function)
        row = [self._object_name, self._function, self._start_asctime, end_time , elapsed_time, self._line_no, self._arg_values, count]
        with open('{0}'.format(self.out_file), 'a') as file:
            csvwriter = csv.writer(file)
            if self.log_result:
                for r in results:
                    row.append(r)
            csvwriter.writerow(row)


    def end_time(self):
        if self._start_time != None:
            raise TimerException("Timer is running , use .stop() to stop it")
        text = '\n\n\n Total Time elapsed:\t{0}'.format(self._total_time)
        with open('{0}'.format(self.out_file), 'a') as file:
            file.write(text)

input_filename = 'Inputs\\AaPF.xlsx'
pdt_inp = pd.read_excel (r'Inputs\\AaPF.xlsx',sheet_name='PDT').replace(np.nan, 0) 
RefPDT_inp = pd.read_excel(input_filename, sheet_name='RefPDT').replace(np.nan, 0) 
lookup_input_inp = pd.read_excel(input_filename, sheet_name='lookup').replace(np.nan, 0) 
lookup1_input_inp = pd.read_excel(input_filename, sheet_name='lookup1').replace(np.nan, 0) 
Prices_inp = pd.read_excel(input_filename, sheet_name='CbixPrices').replace(np.nan, 0) 
Ref_base_cap_inp = pd.read_excel('Inputs\\Refineries Capacity and Production.xlsx', sheet_name='Base Capacity').replace(np.nan, 0) 
Ref_base_cap_regions_inp = pd.read_excel('Inputs\\Refineries Capacity and Production.xlsx', sheet_name='Base Capacity by regions').replace(np.nan, 0) 
Ref_base_prod_inp = pd.read_excel('Inputs\\Refineries Capacity and Production.xlsx', sheet_name='Base Production').replace(np.nan, 0) 
Ref_base_prod_regions_inp = pd.read_excel('Inputs\\Refineries Capacity and Production.xlsx', sheet_name='Base Production by regions').replace(np.nan, 0) 
Ref_aa_prod_inp = pd.read_excel('Inputs\\Refineries Capacity and Production.xlsx', sheet_name=' AA production_dom_bauxite').replace(np.nan, 0) 
refPDT1_inp = pd.read_csv('Inputs\Refineries PDT inputs 1.csv').replace(np.nan, 0) 
refPDT2_inp = pd.read_csv('Inputs\Refineries PDT inputs 2.csv').replace(np.nan, 0)        
input_inp = pd.read_excel(input_filename, sheet_name="league master").replace(np.nan, 0) 

#timer
timer = Timer("pdt_cost_current", txt=True, log_result=False)
class PDT_Cost_Current():
    def __init__(self,year):
        timer.start()
        
        self.year = year #main input year, default is 2020
        print('1.. ', self.year)
        self.PDT = pdt_inp
        self.RefPDT = RefPDT_inp
        self.lookup_input = lookup_input_inp
        self.lookup1_input = lookup1_input_inp
        self.Prices = Prices_inp
        self.pdt_max_row = len(self.PDT['Ref'])
        
        self.Ref_base_cap = Ref_base_cap_inp
        self.Ref_base_cap_regions = Ref_base_cap_regions_inp
        self.Ref_base_prod = Ref_base_prod_inp
        self.Ref_base_prod_regions = Ref_base_prod_regions_inp
        self.Ref_aa_prod = Ref_aa_prod_inp
        self.refPDT1 = refPDT1_inp 
        self.refPDT2 = refPDT2_inp 
        
        self.input = input_inp
        
        self.Ref_total_available_cap = pd.DataFrame(columns=self.Ref_aa_prod.columns, index=self.Ref_aa_prod.index) 
        self.Ref_available_cap = pd.DataFrame(columns=self.Ref_aa_prod.columns, index=self.Ref_aa_prod.index) 
        self.Ref_supply_stream = pd.DataFrame(columns=self.Ref_aa_prod.columns, index=self.Ref_aa_prod.index) 
        
        self.Prices.columns = [str(a) for a in self.Prices.columns]
        self.final_cost_summary = pd.DataFrame(columns=[])
        self.Global  = pd.DataFrame(columns=[])
        self.bauxite_domestic_supply = pd.DataFrame(columns=[])
        self.bauxite_domestic_purchased = pd.DataFrame(columns=[])
        self.bauxite_import_purchased = pd.DataFrame(columns=[])
        self.bauxite_sourcing_mix = pd.DataFrame(columns=[])
        self.bauxite_final_characteristics = pd.DataFrame(columns=[])
        self.caustic_self_supplied = pd.DataFrame(columns=[])
        self.caustic_purchased = pd.DataFrame(columns=[])
        self.capacity_and_production = pd.DataFrame(columns=[])
        self.sodium_carbonate_price = pd.DataFrame(columns=[])
        self.sodium_carbonate_purchased = pd.DataFrame(columns=[])
        self.sodium_carbonate_self_supplied = pd.DataFrame(columns=[])
        self.flocculent = pd.DataFrame(columns=[])
        self.bauxite = pd.DataFrame(columns=[])
        self.caustic = pd.DataFrame(columns=[])
        
        #main sheet inputs
        self.mainSheet_demand = pd.read_excel('Inputs\Main Sheet Inputs.xlsx', sheet_name='Demand')
        self.mainSheet_key_inputs = pd.read_excel('Inputs\Main Sheet Inputs.xlsx', sheet_name='Key Inputs')
        self.mainSheet_key_check = pd.DataFrame(columns=self.mainSheet_demand.columns, index= self.mainSheet_demand.index)
       
        
        
        #dexters code
        self.energy_for_steam = pd.DataFrame(columns=[], index=self.PDT.index)
        self.energy_calcining_alumina = pd.DataFrame(columns=[], index=self.PDT.index)
        self.lime = pd.DataFrame(columns=[], index=self.PDT.index)
        self.limestone = pd.DataFrame(columns=[], index=self.PDT.index)
        self.labour = pd.DataFrame(columns=[], index=self.PDT.index)
        self.energy_usageR = pd.DataFrame(columns=[], index=self.PDT.index)
        self.lime_usageR = pd.DataFrame(columns=[], index=self.PDT.index)
        self.limestone_usageR = pd.DataFrame(columns=[], index=self.PDT.index)
        self.labour_usageR = pd.DataFrame(columns=[], index=self.PDT.index)
        self.consumables_usageR = pd.DataFrame(columns=[], index=self.PDT.index)
        self.bauxite_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.caustic_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.energy_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.lime_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.labour_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.consumables_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.maintenance_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.other_cost = pd.DataFrame(columns=[], index=self.PDT.index)
        self.PDTsummmary = pd.DataFrame(columns=[], index=self.PDT.index)
        self.summary =self.PDTsummmary#pd.read_excel('summary_mock.xlsx', sheet_name='Sheet1') # need to be changed
        timer.stop()
        
        
    #Prices tab calculations
    def price_tab_calc(self):
        for col in self.Prices.columns[2:]:
            for row in list(range(5,13)):
                self.Prices.loc[row,col] = self.Prices.loc[row,'2019']/self.Prices.loc[4,'2019'] *self.Prices.loc[4,col]
            for row in list(range(17,25)):
                self.Prices.loc[row,col] = self.Prices.loc[row,'2019']/self.Prices.loc[16,'2019'] *self.Prices.loc[16,col]
        # print(self.Prices.loc[4:12, ['Unnamed: 0', str(self.year)]].values)
        # print(self.Prices.loc[16:24, ['Unnamed: 0', str(self.year)]].values)
    
    #Refineries capacity and production tab
    def refineries_cap_production_calc_1(self):
        for col in self.Ref_base_cap_regions.columns[9:]:
            for row in self.Ref_base_cap_regions.index:
                val1 = self.Ref_base_cap.loc[(self.Ref_base_cap['Prov - Category']==self.Ref_base_cap_regions.loc[row,'Prov - Category'])&(self.Ref_base_cap['Bauxite Now']==self.Ref_base_cap_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                val2 = self.Ref_base_cap.loc[(self.Ref_base_cap['Technology']==self.Ref_base_cap_regions.loc[row,'Technology'])&(self.Ref_base_cap['Bauxite Now']==self.Ref_base_cap_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                self.Ref_base_cap_regions.loc[row,col] =  val1+val2
                
        for col in self.Ref_base_prod_regions.columns[9:]:
            for row in self.Ref_base_prod_regions.index:
                val1 = self.Ref_base_prod.loc[(self.Ref_base_prod['Prov - Category']==self.Ref_base_prod_regions.loc[row,'Prov - Category'])&(self.Ref_base_prod['Bauxite Now']==self.Ref_base_prod_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                val2 = self.Ref_base_prod.loc[(self.Ref_base_prod['Technology']==self.Ref_base_prod_regions.loc[row,'Technology'])&(self.Ref_base_prod['Bauxite Now']==self.Ref_base_prod_regions.loc[row,'Bauxite Now']), col].astype(float).sum()
                self.Ref_base_prod_regions.loc[row,col] =  val1+val2
    
    #remember to update to main sheet reference with year as 2020
    def refineries_cap_production_calc_2(self):
        self.Ref_total_available_cap['Ref. No.'] = self.Ref_base_cap['Ref. No.']
        self.Ref_total_available_cap['Refinery'] = self.Ref_base_cap['Refinery']
        cols = self.Ref_aa_prod.columns[9:].to_list()
        for row in self.Ref_total_available_cap.index:
            for col in cols:
                self.Ref_total_available_cap.loc[row,col] = self.Ref_base_cap.loc[row,col] if self.Ref_base_prod.loc[row,cols].sum() > 0 else 0
    
    #Available Capacity for imported bauxite
    def refineries_cap_production_calc_3(self):
        for col in self.Ref_base_cap.columns[:9]:
            self.Ref_available_cap[col] = self.Ref_base_cap[col]
        for col in self.Ref_aa_prod.columns[9:]:
            for row in self.Ref_total_available_cap.index:
                self.Ref_available_cap.loc[row, col] = self.Ref_total_available_cap.loc[row,col] -self.Ref_aa_prod.loc[row, col]
        
    #Supply Stream
    def refineries_cap_production_calc_4(self):
        for col in self.Ref_base_cap.columns[:9]:
            self.Ref_supply_stream[col] = self.Ref_base_cap[col]
        
        ref1 = self.mainSheet_key_inputs.loc[1,'Value'] # 0.2 #mainsheet B956
        ref2 = self.mainSheet_key_inputs.loc[2,'Value'] # 0.1 #mainsheet B957
        ref3 = self.mainSheet_key_inputs.loc[3,'Value'] # 0.8 #mainsheet B958
        for col in self.Ref_supply_stream.columns[9:]:
            for row in self.Ref_supply_stream.index:
                value = 0
                if self.Ref_total_available_cap.loc[row,col] >0:
                    if self.Ref_available_cap.loc[row,col]/self.Ref_total_available_cap.loc[row,col] < ref1 or self.Ref_available_cap.loc[row,col]<= ref2:
                        value = 0
                    else:
                        if self.Ref_supply_stream.loc[row, 'Bauxite Now'] == 'Domestic':
                            value = max(self.Ref_total_available_cap.loc[row,col] *ref3 - self.Ref_aa_prod.loc[row,col], 0)
                        else:
                            value = self.Ref_available_cap.loc[row,col]
                else:
                    value = 0
                self.Ref_supply_stream.loc[row, col] = value
        self.Ref_supply_stream.loc[10, 2020] = 1.0   #hardcoded value 1
            
        
    


    
    
        #halim's code
    def calc_all_1(self):
        self.refineries_cap_production_calc_2() # repeated here
        self.capacity_sub_module()
        self.production_sub_module()
        self.utilisation_sub_module()
        #self.exchange_rate()
        #self.cost_summary_helper('Bauxite')
        #self.cost_summary_helper('Caustic')
        #self.cost_summary_helper('Lime')
        #self.cost_summary_helper('Energy')
        #self.cost_summary_helper('Labour')
        #self.cost_summary_helper('Consumables')
        #self.cost_summary_helper('Maintenance')
        #self.cost_summary_helper('Red Mud Disposal')
        #self.cost_summary_helper('Other Costs')
        #self.cost_summary_helper('Cash Cost - FAW')
        #self.cost_operation_cash_cost()
        #self.cost_summary_packaging()
        #self.final_cost_FAW_summary_1()
        #self.seo_private()
        self.global_vlookups('Ownership')
        self.global_vlookups('Alumina Produced')
        self.global_vlookups('Digestion Technology Code')
        self.refinery_digestion_technology()
        self.bauxite_1_domestic_vlookups('Bauxite 1 Aa Grade (Reacting)', 'Bauxite 1 Alumina Grade (Reacting)')
        self.bauxite_1_domestic_vlookups('Bauxite 1 A/S ratio (Reacting)', 'Bauxite 1 A/S ratio (Reacting / Reacting)')
        self.bauxite_1_domestic_vlookups('Bauxite 1 Moisture', 'Bauxite 1 Moisture')
        self.bauxite_1_domestic_vlookups('Bauxite 1 Mining-Dressing (FAW) Cost', 'Bauxite 1 Mining-Dressing (FAW)')
        self.bauxite_1_domestic_vlookups('Bauxite 1 Transport distance', 'Bauxite 1 Transport distance')
        self.bauxite_2_domestic_vlookups('Bauxite 2 Aa Grade (Reacting)', 'Bauxite 2 Alumina Grade (Reacting)')
        self.bauxite_2_domestic_vlookups('Bauxite 2 A/S ratio (Reacting)', 'Bauxite 2 A/S ratio (Reacting / Reacting)')
        self.bauxite_2_domestic_vlookups('Bauxite 2 Moisture', 'Bauxite 2 Moisture')
        self.bauxite_2_domestic_vlookups('Bauxite 2 Transport distance', 'Bauxite 2 Transport distance')
        self.bauxite_3_transport_distance()
        # self.bauxite_3_domestic_vlookups('Transport fee', 'Bauxite 3 Total Transport Fee')
        self.bauxite_3_domestic_vlookups('Transport distance', 'Bauxite 3 Transport distance')
        self.bauxite_1_domestic_hlookup('Freight rate (trucking)', 'Bauxite 1 Freight rate (trucking)' )
        self.bauxite_2_domestic_hlookup('Freight rate (trucking)', 'Bauxite 2 Freight rate (trucking)')
        self.bauxite_2_domestic_hlookup('Price - Domestic Bx FAW', 'Bauxite 2 Price - Domestic Bx FAW')
        self.bauxite_3_domestic_hlookup('Freight rate (trucking)', 'Bauxite 3 Freight rate (trucking)')
        self.silica_grade_reacting()
        self.purchase_cfr()
        self.silica_grade_reacting_purchased()
        self.bauxite_1_total_freight()
        self.bauxite_2_total_freight()
        self.bauxite_3_total_freight()
        self.bauxite1_final_freight_to_refinery()
        self.bauxite2_final_freight_to_refinery()
        self.bauxite3_final_freight_to_refinery()
        self.total_delevered()
        self.total_delevered_purchased()
        self.total_delevered_1()
        self.total_delevered_purchased_1()
        self.total_delevered_import_purchased()
        self.total_delevered_import_purchased_1()
        self.bauxite_sourcing_mix_vlookup('Domestic Self Supply', 'Bauxite Sourcing Mix Domestic Self Supply')
        self.bauxite_sourcing_mix_vlookup('Domestic Purchased', 'Bauxite Sourcing Mix Domestic Purchased')
        self.bauxite_sourcing_mix_import_purchased()
        self.bauxite_final_characteristics_calc()
        self.caustic_purchased_list_price('Caustic Soda List Price')
        self.caustic_purchased_customised_list_price()
        self.caustic_purchased_total_freight()
        self.caustic_purchased_final_freight()
        self.caustic_purchased_final_freight1()
        self.final_price_purchased_caustic()
        self.caustic_self_supplied_hlookup_helper('Grid Electricity List Price')
        self.caustic_self_supplied_hlookup_helper('Self-Supplied Electricity List Price')
        self.caustic_self_supplied_hlookup_helper('Chlorine Price')
        self.grid_electricity_final_price()
        self.self_supplied_electricity_final_price()
        self.electricity_cost_for_one_ecu()
        self.total_manufacturing_cost()
        self.cost_to_be_apportioned_to_NaOH()
        self.cost_per_tonne_of_pure_NaOH()    
        self.caustic_self_supplied_total_freight()
        self.caustic_final_freight_to_refinery()
        self.caustic_final_freight_to_refinery1()
        self.final_price_self_supplied_caustic()
        self.caustic_final_price()
        
        self.soduim_carbonate_hlookup_helper('Sodium Carbonate List Price', 'Sodium Carbonate List Price')
        self.sodium_carbonate_self_supply_price()
        self.sodium_carbonate_purchased_total_freight()
        self.sodium_carbonate_purchased_final_freight()
        self.sodium_carbonate_purchased_final_price()
        self.sodium_carbonate_self_supplied_total_freight()
        self.sodium_carbonate_self_supplied_final_freight()
        self.sodium_carbonate_self_supplied_final_price()
        self.sodium_carbonate_final_price()
        
        self.flocculent_lookup_helper('Flocculent List Price', 'Flocculent List Price')
        self.flocculent_lookup_helper('Freight rate (trucking)', 'Flocculent Freight rate (trucking)')
        self.flocculent_customised_price()
        self.flocculent_total_freight()
        self.final_freight()
        self.final_price()
        self.bauxite_hlookup_helper('Alumina quality')
        self.bauxite_hlookup_helper('DSP AA:SiO2')
        self.bauxite_hlookup_helper('Extraction Efficiency')
        self.bauxite_use()
        self.customise_multiplier()
        self.final_bauxite_use_rate()
        self.caustic_lookup_helper('DSP Na:Si')
        self.caustic_lookup_helper('Caustic wash loss')
        self.caustic_use()
        self.caustic_customise_multiplier()
        self.final_caustic_use_rate()
    
    def calc_all_1_a(self):
        self.exchange_rate()
        self.cost_summary_helper('Bauxite')
        self.cost_summary_helper('Caustic')
        self.cost_summary_helper('Lime')
        self.cost_summary_helper('Energy')
        self.cost_summary_helper('Labour')
        self.cost_summary_helper('Consumables')
        self.cost_summary_helper('Maintenance')
        self.cost_summary_helper('Red Mud Disposal')
        self.cost_summary_helper('Other Costs')
        self.cost_summary_helper('Cash Cost - FAW')
        self.cost_operation_cash_cost()
        self.cost_summary_packaging()
        self.final_cost_FAW_summary_1()
        self.seo_private()
        

    #ref PDT codes start
    def ref_cal_1(self):
        road_rate = float(self.refPDT2.loc[self.refPDT2['Factor']=='Road Rate','Value'])
        rail_rate = float(self.refPDT2.loc[self.refPDT2['Factor']=='Rail Rate','Value'])
        waterway_rate = float(self.refPDT2.loc[self.refPDT2['Factor']=='Waterway Rate','Value'])
        rail_loading = float(self.refPDT2.loc[self.refPDT2['Factor']=='Rail Loading','Value'])
        port_handling = float(self.refPDT2.loc[self.refPDT2['Factor']=='Port Handling','Value'])
        
        for row in range(self.refPDT1.shape[0]):
            if float(self.refPDT2.loc[row,'Option 1 Road(km)'])> 0:
                self.refPDT2.loc[row,'Option 1 Freight'] = float(self.refPDT2.loc[row,'Option 1 Road(km)']) * road_rate + float(self.refPDT2.loc[row,'Option 1 Waterway(km)']) * waterway_rate
            elif float(self.refPDT2.loc[row,'Option 1 Waterway(km)'])> 0:
                self.refPDT2.loc[row,'Option 1 Freight'] = float(self.refPDT2.loc[row,'Option 1 Waterway(km)']) * waterway_rate
            else:
                self.refPDT2.loc[row,'Option 1 Freight'] = np.nan
                
            if float(self.refPDT2.loc[row,'Option 2 Railway(km)'])> 0:
                self.refPDT2.loc[row,'Option 2 Freight'] = rail_loading + float(self.refPDT2.loc[row,'Option 2 Railway(km)']) * rail_rate + float(self.refPDT2.loc[row,'Option 2 Road(km)']) * road_rate
            elif float(self.refPDT2.loc[row,'Option 2 Road(km)'])> 0:
                self.refPDT2.loc[row,'Option 2 Freight'] = float(self.refPDT2.loc[row,'Option 2 Road(km)']) * road_rate
            else:
                self.refPDT2.loc[row,'Option 2 Freight'] = np.nan

        samp = self.refPDT2.loc[:, 'Option'].replace('', 0)
        smap = self.refPDT2.loc[:, 'Option'].replace(np.nan, 0)
        # print(samp)
        fc_res = [self.refPDT2.loc[i,'Option 1 Freight'] if samp[i] == 1 else self.refPDT2.loc[i,'Option 2 Freight'] for i in np.arange(len(samp))]
        self.refPDT2.loc[:,'Freight calculated'] = fc_res
        f_store = self.refPDT2.loc[:,'Freight'].values
        # print(fc_res)
        ff_res = [f_store[i] if f_store[i] > 0 else fc_res[i] for i in np.arange(len(fc_res))]
        ph = self.refPDT2.loc[:,'Port Handling'].tolist()
        tt_res = [ff_res[i] if ph[i] == 1 else ff_res[i] + 20 for i in np.arange(len(ph))]
        # tt_res = np.nan_to_num(tt_res)
        self.refPDT2.loc[:,'Freight'] = ff_res
        self.refPDT2.loc[:,'Total Transport'] = tt_res

    #ref PDT codes end



    def seo_private(self):
        timer.start()
        glob = self.Global['Ownership'].to_list()
        result = []
        for c in glob:
            if c == 'SOE':
                result.append(1)
            elif c == 'Private':
                result.append(2)
            else:
                result.append('')
        self.final_cost_summary['SOE=1, Private=2']  = result
        timer.stop(result)

    def cost_summary_helper(self, col):
        timer.start()
        summary = self.summary[col].to_list()
        prod = self.capacity_and_production['Production - sub module']
        result = [summary[i] if round(prod[i], 2) > 0  else 0 for i in np.arange(len(prod))]
        self.final_cost_summary[col] = result
        timer.stop(result)

    def cost_operation_cash_cost(self):
        timer.start()
        result = self.final_cost_summary.loc[:, 'Bauxite':'Other Costs'].sum(axis=1)
        self.final_cost_summary['Operating Cash Cost'] = result.to_list()
        timer.stop(result)

    def cost_summary_packaging(self):
        timer.start()
        prod = self.capacity_and_production['Production - sub module'].to_list()
        occ = self.final_cost_summary['Operating Cash Cost'].to_list()
        pack = self.PDT['Packaging'].to_list()
        result = [pack[i] if prod[i] > 0 and occ[i] > 0 else 0 for i in np.arange(len(prod))]
        self.final_cost_summary['Final Packaging'] = result
        timer.stop(result)

    def exchange_rate(self):
        timer.start()
        #Dexter: updated year to match year in main year input, note original commented
        #result = [self.Prices['2020'].to_list()[1] for a in np.arange(94)]
        print('2.. ', self.year)
        result = [self.Prices[str(self.year)].to_list()[1] for a in np.arange(self.pdt_max_row)]
        # print("exchange rate", result)
        self.final_cost_summary['Exchange Rate - RMB/USD'] = result
        timer.stop(result)

    def final_cost_FAW_summary_1(self):
        timer.start()
        ccfaw = np.array(self.final_cost_summary['Cash Cost - FAW'])
        er = np.array(self.final_cost_summary['Exchange Rate - RMB/USD'])
        result = ccfaw/er
        self.final_cost_summary['Final Cash Cost - FAW1'] = result
        timer.stop(result)

    #from here    

    def capacity_sub_module(self):
        timer.start()
        #year updated here too
        #refinery_refno = self.Ref_base_cap.loc[:self.pdt_max_row, ['Ref. No.', 2020]]
        print('3.. ', self.year, 'this year')
        refinery_refno = self.Ref_base_cap.loc[:self.pdt_max_row, ['Ref. No.', self.year]]
        pdt_refno = self.PDT.loc[:self.pdt_max_row, 'Ref']
        #result = vlookup(pdt_refno, refinery_refno, 'Ref', 'Ref. No.', 2020)
        result = vlookup(pdt_refno, refinery_refno, 'Ref', 'Ref. No.', self.year)
        # print("cap sub module", result)
        result = [a*1000 for a in result]
        self.capacity_and_production['Capacity - sub module'] = result
        timer.stop(result)
        
    def production_sub_module(self):
        timer.start()
        print('3.. ', self.year, 'this year')
        refinery_refno = self.Ref_total_available_cap.loc[:self.pdt_max_row, ['Ref. No.', self.year]]
        pdt_refno = self.PDT.loc[:self.pdt_max_row, 'Ref']
        result = vlookup(pdt_refno, refinery_refno, 'Ref', 'Ref. No.', self.year)
        # print("prod sub module", result)
        result = [a*1000 for a in result]
        self.capacity_and_production['Production - sub module'] = result 
        timer.stop(result)

    def utilisation_sub_module(self):
        timer.start()
        c = self.capacity_and_production['Capacity - sub module'].to_list()
        p = self.capacity_and_production['Production - sub module'].to_list()
        result = [p[i]/c[i] if c[i] > 0 else 0 for i in np.arange(len(c))]
        self.capacity_and_production['Utilisation - sub module'] = result
        timer.stop(result)
        
    def global_vlookups(self, col):
        timer.start()
        pi = self.PDT.copy()
        ref_pdt = self.RefPDT.copy()
        a = pi.loc[: , "Ref"]
        b = ref_pdt.loc[:, ['Ref. No.', col]]
        result = vlookup(a, b, 'Ref', 'Ref. No.', col)
        self.Global[col] = result
        timer.stop(result)
        
    def refinery_digestion_technology(self): # later
        timer.start()
        pi = self.Global.copy()
        lic = self.lookup1_input.copy()
        a = pi.loc[: , "Digestion Technology Code"].to_list()
        b = lic.loc[:, 'Digestion Technology'].to_list()
        c = lic.loc[:, 'Digestion Technology Code'].to_list()
        result = []
        for i , n in enumerate(a):
            index = c.index(a[i])
            result.append(b[index])
        self.Global['Refinery Digestion Technology'] = result
        timer.stop(result)
        
    def bauxite_1_domestic_vlookups(self, col, result_col):
        timer.start()
        pi = self.PDT.copy()
        ref_pdt = self.RefPDT.copy()
        a = pi.loc[: , "Ref"]
        b = ref_pdt.loc[:, ['Ref. No.', col]]
        result = vlookup(a, b, 'Ref', 'Ref. No.', col)
        self.bauxite_domestic_supply[result_col] = np.nan_to_num(result)
        timer.stop(result)
        
    def bauxite_2_domestic_vlookups(self, col, result_col):
        timer.start()
        pi = self.PDT.copy()
        ref_pdt = self.RefPDT.copy()
        a = pi.loc[: , "Ref"]
        b = ref_pdt.loc[:, ['Ref. No.', col]]
        result = vlookup(a, b, 'Ref', 'Ref. No.', col)
        self.bauxite_domestic_purchased[result_col] = np.nan_to_num(result)
        timer.stop(result)
        
    def bauxite_3_domestic_vlookups(self, col, result_col):
        timer.start()
        pi = self.PDT.copy()
        ref_pdt = self.RefPDT.copy()
        a = pi.loc[: , "Ref"]
        b = ref_pdt.loc[:, ['Ref. No.', col]]
        result = vlookup(a, b, 'Ref', 'Ref. No.', col)
        self.bauxite_import_purchased[result_col] = np.nan_to_num(result)
        timer.stop(result)
        
    def bauxite_1_domestic_hlookup(self,name, result_col):
        timer.start()
        pic = self.PDT.copy()
        lic = self.lookup_input.copy()
        province = pic.Province.to_list()
        floc = np.nan_to_num(hlookup(province, lic, 'Province Prices & Rates - Current',name))
        self.bauxite_domestic_supply[result_col] = floc
        timer.stop(floc)
        
    def bauxite_2_domestic_hlookup(self, name, result_col):
        timer.start()
        pic = self.PDT.copy()
        lic = self.lookup_input.copy()
        province = pic.Province.to_list()
        floc = np.nan_to_num(hlookup(province, lic, 'Province Prices & Rates - Current',name ))
        self.bauxite_domestic_purchased[result_col] = floc
        timer.stop(floc)
        
    def bauxite_3_domestic_hlookup(self, name, result_col):
        timer.start()
        pic = self.PDT.copy()
        lic = self.lookup_input.copy()
        province = pic.Province.to_list()
        floc = np.nan_to_num(hlookup(province, lic, 'Province Prices & Rates - Current',name ))
        self.bauxite_import_purchased[result_col] = floc
        timer.stop(floc)
        
    def silica_grade_reacting(self):
        timer.start()
        a = self.bauxite_domestic_supply['Bauxite 1 A/S ratio (Reacting / Reacting)']
        b = self.bauxite_domestic_supply['Bauxite 1 Alumina Grade (Reacting)']
        result = []
        for i in np.arange(len(a)):
            if a[i] > 0:
                result.append(b[i]/a[i])
            else:
                result.append(0)
        self.bauxite_domestic_supply['Bauxite 1 Silica Grade (Reacting)'] = result
        timer.stop(result)
        
    def silica_grade_reacting_purchased(self):
        timer.start()
        a = self.bauxite_domestic_purchased['Bauxite 2 A/S ratio (Reacting / Reacting)']
        b = self.bauxite_domestic_purchased['Bauxite 2 Alumina Grade (Reacting)']
        result = []
        for i in np.arange(len(a)):
            if a[i] > 0:
                result.append(b[i]/a[i])
            else:
                result.append(0)
        self.bauxite_domestic_purchased['Bauxite 2 Silica Grade (Reacting)'] = result
        timer.stop(result)
        
    def bauxite_1_total_freight(self):
        td  = self.bauxite_domestic_supply['Bauxite 1 Transport distance'].to_list()
        fr = self.bauxite_domestic_supply['Bauxite 1 Freight rate (trucking)'].to_list()
        luf = self.PDT['Bauxite 1 Loading + Unloading Fee'].to_list()[:self.pdt_max_row]
        self.bauxite_domestic_supply['Bauxite 1 Total Freight'] = np.add(np.multiply(td, fr), luf)
        
    def bauxite_2_total_freight(self):
        td  = self.bauxite_domestic_purchased['Bauxite 2 Transport distance'].to_list()
        fr = self.bauxite_domestic_purchased['Bauxite 2 Freight rate (trucking)'].to_list()
        luf = self.PDT['Bauxite 2 Loading + Unloading Fee'].to_list()[:self.pdt_max_row]
        self.bauxite_domestic_purchased['Bauxite 2 Total Freight - Before Customisation'] = np.add(np.multiply(td, fr), luf)

    def bauxite_3_transport_distance(self):
        res = self.refPDT2.loc[:,'Total Transport'][:self.pdt_max_row].replace('', 0, regex=True)
        res = res.astype(float)
        self.bauxite_import_purchased['Bauxite 3 Total Transport Fee'] = res
        
    def bauxite_3_total_freight(self):
        td  = np.array(self.bauxite_import_purchased['Bauxite 3 Transport distance'].to_list())
        fr = np.array(self.bauxite_import_purchased['Bauxite 3 Freight rate (trucking)'].to_list())
        luf = np.array(self.PDT['Bauxite 3 Loading + Unloading Fee'].to_list()[:self.pdt_max_row])
        # print(td, fr, luf)
        self.bauxite_import_purchased['Bauxite 3 Total Freight - Before Customisation'] = np.add(np.multiply(td, fr), luf)
        
    def purchase_cfr(self):
        #year updated here too
        #year = self.Prices['2020'].to_list()[0]
        year = self.Prices[str(self.year)].to_list()[0]
        result = np.full(self.pdt_max_row, year)
        # print("purchase cfr", result)
        self.bauxite_import_purchased['Purchase - CFR'] = result
        print('4.. ', self.year)
        
    def bauxite1_final_freight_to_refinery(self):
        td = self.bauxite_domestic_supply['Bauxite 1 Transport distance'].to_list()
        tf = self.bauxite_domestic_supply['Bauxite 1 Total Freight'].to_list()
        cm = self.PDT['Bauxite 1 Customisation multiplier'].to_list()
        result = []
        for i in np.arange(len(td)):
            if td[i] <= 0 :
                result.append(0)
            else :
                result.append(tf[i]*cm[i])
        self.bauxite_domestic_supply['Bauxite 1 Final Freight to refinery'] = result 
        
    def bauxite2_final_freight_to_refinery(self):
        td = self.bauxite_domestic_purchased['Bauxite 2 Transport distance'].to_list()
        tf = self.bauxite_domestic_purchased['Bauxite 2 Total Freight - Before Customisation'].to_list()
        cm = self.PDT['Bauxite 2 Customisation multiplier'].to_list()
        result = []
        for i in np.arange(len(td)):
            if td[i] <= 0 :
                result.append(0)
            else :
                result.append(tf[i]*cm[i])
        self.bauxite_domestic_purchased['Bauxite 2 Final Freight to refinery'] = result
        
    def bauxite3_final_freight_to_refinery(self):
        td = self.bauxite_import_purchased['Bauxite 3 Transport distance'].to_list()
        tf = self.bauxite_import_purchased['Bauxite 3 Total Freight - Before Customisation'].to_list()
        cm = self.PDT['Bauxite 3 Customisation multiplier'].to_list()
        result = []
        for i in np.arange(len(td)):
            if td[i] <= 0 :
                result.append(0)
            else :
                result.append(tf[i]*cm[i])
        self.bauxite_import_purchased['Bauxite 3 Final Freight to refinery'] = result 
    
    def total_delevered(self):
        md = self.bauxite_domestic_supply['Bauxite 1 Mining-Dressing (FAW)'].to_list()
        fftr =  self.bauxite_domestic_supply['Bauxite 1 Final Freight to refinery'].to_list()
        other =  self.PDT['Bauxite 1 Other'].to_list()[:self.pdt_max_row]
        self.bauxite_domestic_supply['Bauxite 1 Total Delivered'] = np.add(md, np.add(fftr, other))
        
    def total_delevered_purchased(self):
        md = self.bauxite_domestic_purchased['Bauxite 2 Price - Domestic Bx FAW'].to_list()
        fftr =  self.bauxite_domestic_purchased['Bauxite 2 Final Freight to refinery'].to_list()
        other =  self.PDT['Bauxite 2 Other'].to_list()[:self.pdt_max_row]
        self.bauxite_domestic_purchased['Bauxite 2 Total Delivered'] = np.add(md, np.add(fftr, other))
        
    def total_delevered_1(self):
        td = self.bauxite_domestic_supply['Bauxite 1 Total Delivered'].to_list()
        m =  self.bauxite_domestic_supply['Bauxite 1 Moisture'].to_list()
        self.bauxite_domestic_supply['Bauxite 1 Total Delivered 1'] = np.divide(td, np.subtract(np.ones(self.pdt_max_row), m))
        
    def total_delevered_purchased_1(self):
        td = self.bauxite_domestic_purchased['Bauxite 2 Total Delivered'].to_list()
        m =  self.bauxite_domestic_purchased['Bauxite 2 Moisture'].to_list()
        self.bauxite_domestic_purchased['Bauxite 2 Total Delivered 1'] = np.divide(td, np.subtract(np.ones(self.pdt_max_row), m))
        
    def total_delevered_import_purchased(self):
        cfr = np.array(self.bauxite_import_purchased['Purchase - CFR'].to_list())
        print('5.. ', self.year)
        er = self.Prices.loc[:, str(self.year)].to_list()[1]
        er = np.full(self.pdt_max_row, er)
        moisture = np.array(self.PDT['Bauxite 3 Moisture'].to_list()[:self.pdt_max_row])
        ttf = np.array(self.bauxite_import_purchased['Bauxite 3 Total Transport Fee'].to_list())
        other = np.array(self.PDT['Bauxite 3 Other'].to_list()[:self.pdt_max_row])
        
        # print(er, cfr, moisture, ttf)
        result = cfr*er*(1.0 - moisture)+ttf+other
        self.bauxite_import_purchased['Bauxite 3 Total Delivered'] = result
        
    def total_delevered_import_purchased_1(self):
        td = self.bauxite_import_purchased['Bauxite 3 Total Delivered'].to_list()
        m =  self.PDT['Bauxite 3 Moisture'].to_list()[:self.pdt_max_row]
        self.bauxite_import_purchased['Bauxite 3 Total Delivered 1'] = np.divide(td, np.subtract(np.ones(self.pdt_max_row), m))
        
    def bauxite_sourcing_mix_vlookup(self, col, result_col):
        pi = self.PDT.copy()
        ref_pdt = self.RefPDT.copy()
        a = pi.loc[: , "Ref"]
        b = ref_pdt.loc[:, ['Ref. No.', col]]
        result = vlookup(a, b, 'Ref', 'Ref. No.', col)
        self.bauxite_sourcing_mix[result_col] = result
        
    def bauxite_sourcing_mix_import_purchased(self):
        dss = self.bauxite_sourcing_mix['Bauxite Sourcing Mix Domestic Self Supply'].to_list()
        dp = self.bauxite_sourcing_mix['Bauxite Sourcing Mix Domestic Purchased'].to_list()
        prev = np.add(dss, dp)
        ip = np.subtract(np.ones(self.pdt_max_row), prev)
        self.bauxite_sourcing_mix['Bauxite Sourcing Mix Import Purchased'] = ip
        self.bauxite_sourcing_mix['Bauxite Sourcing Mix Total'] = np.add(prev, ip)
        
    def bauxite_final_characteristics_calc(self):
        dss = self.bauxite_sourcing_mix['Bauxite Sourcing Mix Domestic Self Supply'].to_list()
        dp = self.bauxite_sourcing_mix['Bauxite Sourcing Mix Domestic Purchased'].to_list()
        ip = self.bauxite_sourcing_mix['Bauxite Sourcing Mix Import Purchased'].to_list()
        b_1_ag = self.bauxite_domestic_supply['Bauxite 1 Alumina Grade (Reacting)'].to_list()
        b_2_ag = self.bauxite_domestic_purchased['Bauxite 2 Alumina Grade (Reacting)'].to_list()
        b_3_ag = self.PDT['Bauxite 3 Alumina Grade (Reacting)'].to_list()[:self.pdt_max_row]
        b_1_m = self.bauxite_domestic_supply['Bauxite 1 Moisture'].to_list()
        b_2_m = self.bauxite_domestic_purchased['Bauxite 2 Moisture'].to_list()
        b_3_m = self.PDT['Bauxite 3 Moisture'].to_list()[:self.pdt_max_row]
        b_1_sg = self.bauxite_domestic_supply['Bauxite 1 Silica Grade (Reacting)'].to_list()
        b_2_sg = self.bauxite_domestic_purchased['Bauxite 2 Silica Grade (Reacting)'].to_list()
        b_3_sg = self.PDT['Bauxite 3 Silica Grade (Reacting)'].to_list()[:self.pdt_max_row]
        b_1_td = self.bauxite_domestic_supply['Bauxite 1 Total Delivered 1'].to_list()[:self.pdt_max_row]
        b_2_td = self.bauxite_domestic_purchased['Bauxite 2 Total Delivered 1'].to_list()[:self.pdt_max_row]
        b_3_td = self.bauxite_import_purchased['Bauxite 3 Total Delivered 1'].to_list()[:self.pdt_max_row]
        b_char_adder = lambda a, b, c: np.add(np.add(np.multiply(dss, a), np.multiply(dp, b)), np.multiply(ip, c))
        first = b_char_adder(b_1_ag, b_2_ag, b_3_ag)
        third = b_char_adder(b_1_sg, b_2_sg, b_3_sg)
        self.bauxite_final_characteristics['Bauxite Final Alumina Grade (Reacting)'] = first
        self.bauxite_final_characteristics['Bauxite Final A/S ratio (Reacting / Reacting)'] = np.divide(first, third)
        self.bauxite_final_characteristics['Bauxite Final Moisture'] = b_char_adder(b_1_m, b_2_m, b_3_m)
        self.bauxite_final_characteristics['Bauxite Final Silica Grade (Reacting)'] = third
        self.bauxite_final_characteristics['Bauxite Final Total Delivered'] = b_char_adder(b_1_td, b_2_td, b_3_td)
        
    def caustic_purchased_list_price(self, name):
        pic = self.PDT.copy()
        lic = self.Prices.loc[4:12, ['Unnamed: 0', str(self.year)]]
        province = pic.Province.to_list()
        result = hlookup(province, lic, 'Unnamed: 0', str(self.year))
        # result = pic.merge(lic, left_on="Province", right_on="Unnamed: 0", how="left").loc[:, [str(self.year)]].values
        self.caustic_purchased[name] = result
        
    def caustic_purchased_customised_list_price(self):
        cslp = self.caustic_purchased['Caustic Soda List Price'].to_list()
        pic = self.PDT['List Price Customisation multiplier'].to_list()[:self.pdt_max_row]
        self.caustic_purchased['Customised List Price'] = np.multiply(cslp, pic)
        
    def caustic_purchased_total_freight(self):
        td = self.PDT['Caustic Purchased Transport distance'].to_list()[:self.pdt_max_row]
        fr = self.PDT['Caustic Purchased Freight rate (trucking)'].to_list()[:self.pdt_max_row]
        tf = self.PDT['Caustic Purchased [Loading + Unloading] Total Fee'].to_list()[:self.pdt_max_row]
        self.caustic_purchased['Caustic Purchased Total Freight - Before Customisation'] = np.add(np.multiply(td, fr), tf)
        
    def caustic_purchased_final_freight(self):
        tf = self.caustic_purchased['Caustic Purchased Total Freight - Before Customisation'].to_list()
        sm = self.PDT['Caustic Purchased Customisaton multiplier'].to_list()[:self.pdt_max_row]
        self.caustic_purchased['Caustic Purchased Final Freight to refinery'] = np.multiply(tf, sm)
        
    def caustic_purchased_final_freight1(self):
        td = self.PDT['Caustic Purchased Transport distance'].to_list()[:self.pdt_max_row]
        cs = self.PDT['Caustic Strength as transported'].to_list()[:self.pdt_max_row]
        ffr = self.caustic_purchased['Caustic Purchased Final Freight to refinery'].to_list()
        result = [0 if td[i] <= 0 else ffr[i]/cs[i] for i in np.arange(len(td))]
        self.caustic_purchased['Caustic Purchased Final Freight to refinery1'] = result 
        
    def final_price_purchased_caustic(self):
        clp = self.caustic_purchased['Customised List Price'].to_list()
        fftr = self.caustic_purchased['Caustic Purchased Final Freight to refinery1'].to_list()
        self.caustic_purchased['Final Price - purchased caustic'] = np.add(clp, fftr)
        
    def caustic_self_supplied_hlookup_helper(self, name):
        pic = self.PDT.copy()
        lic = self.lookup_input.copy()
        province = pic.Province.to_list()
        result = hlookup(province, lic, 'Province Prices & Rates - Current',name )
        self.caustic_self_supplied[name] = result
        
    def grid_electricity_final_price(self):
        gelp = self.caustic_self_supplied['Grid Electricity List Price'].to_list()
        cm = self.PDT['Caustic Self Supplied Customisation multiplier'].to_list()[:self.pdt_max_row]
        self.caustic_self_supplied['Grid Electricity Final Price'] = np.multiply(gelp, cm)
    
    def self_supplied_electricity_final_price(self):
        ssefp = self.caustic_self_supplied['Self-Supplied Electricity List Price'].to_list()
        cm =  self.PDT['Caustic Self Supplied Customisation multiplier'].to_list()[:self.pdt_max_row]
        self.caustic_self_supplied['Self-Supplied Electricity Final Price'] = np.multiply(ssefp, cm)
        
    def electricity_cost_for_one_ecu(self):
        gefp = np.array(self.caustic_self_supplied['Grid Electricity Final Price'].to_list())
        tef_1_ecu = np.array(self.PDT['Total Electricity for 1 ECU for NaOH + Cl2 + H2 prodn'].to_list()[:self.pdt_max_row])
        efcp = np.array(self.PDT['% of Electricity for Caustic Production from Grid'].to_list()[:self.pdt_max_row])
        ssefp = np.array(self.caustic_self_supplied['Self-Supplied Electricity List Price'].to_list())
        self.caustic_self_supplied['Electricity cost for one ECU'] = (gefp * tef_1_ecu * efcp) + (ssefp * tef_1_ecu * (np.ones(self.pdt_max_row) - efcp))
        
    def total_manufacturing_cost(self):
        tmc = np.array(self.PDT['Caustic Self Supplied Other Costs'].to_list()[:self.pdt_max_row])
        eco_1_ecu = np.array(self.caustic_self_supplied['Electricity cost for one ECU'].to_list())
        self.caustic_self_supplied['Total manufacturing cost'] = tmc + eco_1_ecu
        
    def cost_to_be_apportioned_to_NaOH(self):
        tmc = np.array(self.caustic_self_supplied['Total manufacturing cost'].to_list())
        cp = np.array(self.caustic_self_supplied['Chlorine Price'].to_list())
        self.caustic_self_supplied['Cost to be apportioned to NaOH'] = tmc - cp

    def cost_per_tonne_of_pure_NaOH(self):
        ctba = np.array(self.caustic_self_supplied['Cost to be apportioned to NaOH'].to_list())
        tpnp = np.array(self.PDT['tonnes pure NaOH per ECU'].to_list()[:self.pdt_max_row])
        self.caustic_self_supplied['cost per tonne of pure NaOH'] = ctba / tpnp
        
    def caustic_self_supplied_total_freight (self):
        td = np.array(self.PDT['Caustic Self Supplied Transport distance'].to_list()[:self.pdt_max_row])
        fr = np.array(self.PDT['Caustic Self Supplied Freight rate (trucking)'].to_list()[:self.pdt_max_row])
        lutf = np.array(self.PDT['Caustic Self Supplied [Loading + Unloading] Total Fee'].to_list()[:self.pdt_max_row])
        self.caustic_self_supplied['Caustic Self Supplied Total Freight - Before Customisation'] = (td * fr) + lutf
        
    def caustic_final_freight_to_refinery(self):
        tfbc = np.array(self.caustic_self_supplied['Caustic Self Supplied Total Freight - Before Customisation'].to_list())
        cm = np.array(self.PDT['Caustic Price Customisation multiplier'].to_list()[:self.pdt_max_row])
        self.caustic_self_supplied['Caustic Self Supplied Final Freight to refinery'] = tfbc * cm
        
    def caustic_final_freight_to_refinery1(self):
        td = np.array(self.PDT['Caustic Self Supplied Transport distance'].to_list()[:self.pdt_max_row])
        fftr = np.array(self.caustic_self_supplied['Caustic Self Supplied Final Freight to refinery'].to_list())
        csat = np.array(self.PDT['Caustic Strength as transported'].to_list()[:self.pdt_max_row])
        result = [0 if td[i] <= 0 else fftr[i]/csat[i] for i in np.arange(len(csat))]
        self.caustic_self_supplied['Caustic Self Supplied Final Freight to refinery1'] = result
        
    def final_price_self_supplied_caustic(self):
        ctp_NaOH = np.array(self.caustic_self_supplied['cost per tonne of pure NaOH'].to_list())
        fftr1 = np.array(self.caustic_self_supplied['Caustic Self Supplied Final Freight to refinery1'].to_list())
        self.caustic_self_supplied['Final Price - Self-Supplied caustic'] = ctp_NaOH + fftr1
        
    def caustic_final_price(self):
        csat = np.array(self.PDT['Caustic Domestic Purchased'].to_list()[:self.pdt_max_row])
        fppc = np.array(self.caustic_purchased['Final Price - purchased caustic'].to_list())
        fpssc = np.array(self.caustic_self_supplied['Final Price - Self-Supplied caustic'].to_list())
        ccm = np.array(self.PDT["Caustic Customisation multiplier"].to_list()[:self.pdt_max_row])
        result =  (csat * fppc + (1 - csat) * fpssc) * ccm
        # result[8], result[self.pdt_max_row-1], result[self.pdt_max_row-10], result[self.pdt_max_row-11], result[self.pdt_max_row-17], result[self.pdt_max_row-18], result[self.pdt_max_row-35], result[self.pdt_max_row-36], result[self.pdt_max_row-37]  = 1250, 1550, 1550, 1550, 1550, 1550, 1250, 1250, 1250
        self.caustic_self_supplied['Caustic Final Price'] = result
        
    def flocculent_lookup_helper(self, name, result):
        pic = self.PDT.copy()
        lic = self.lookup_input.copy()
        province = pic.Province.to_list()
        floc = hlookup(province, lic, 'Province Prices & Rates - Current',name )
        self.flocculent[result] = floc
        
    def flocculent_customised_price(self):
        fc = self.flocculent.copy()
        a = fc['Flocculent List Price'].to_list()
        b = self.PDT['Flocculent Customisation multiplier'].to_list()[:self.pdt_max_row]
        fc['Flocculent Customised price'] = np.multiply(a, b)
        self.flocculent = fc
        
    def flocculent_total_freight(self):
        fc = self.flocculent.copy()
        td = self.PDT['Flocculent Transport distance'].to_list()[:self.pdt_max_row]
        tr = fc['Flocculent Freight rate (trucking)'].to_list()
        fee = self.PDT['Flocculent Loading + Unloading Fee'].to_list()[:self.pdt_max_row]
        self.flocculent['Flocculent Total Freight'] = np.add(np.multiply(td,tr), fee)
        
    def final_freight(self):
        timer.start()
        fc = self.flocculent.copy()
        td = self.PDT['Flocculent Transport distance'].to_list()[:self.pdt_max_row]
        tf = fc['Flocculent Total Freight'].to_list()
        cm = self.PDT['Flocculent Customisation multiplier'].to_list()[:self.pdt_max_row]
        result = []
        for i, n in enumerate(td):
            if n <= 0 :
                result.append(0)
            else:
                result.append(tf[i]*cm[i])
        self.flocculent['Flocculent Final Freight'] =  result
        timer.stop(result)
    
    def final_price(self):
        fc = self.flocculent.copy()
        cp = fc['Flocculent Customised price'].to_list()
        ff = fc['Flocculent Final Freight'].to_list()
        self.flocculent['Flocculent Final Price'] = np.add(cp, ff)
        
    def bauxite_hlookup_helper(self, name):
        lc = self.lookup1_input.copy()
        tech = self.Global['Refinery Digestion Technology'].to_list()[:self.pdt_max_row]
        res = hlookup(tech, lc, 'Digestion Technology',name )
        self.bauxite[name] = res
        
    def bauxite_use(self):
        timer.start()
        bc = self.bauxite.copy()
        ag = self.bauxite_final_characteristics['Bauxite Final Alumina Grade (Reacting)'].to_list() 
        dsp = bc['DSP AA:SiO2'].to_list()
        sg = self.bauxite_final_characteristics['Bauxite Final Silica Grade (Reacting)'].to_list()  
        hl = self.PDT['Handling losses'].to_list()[:self.pdt_max_row]
        ee = bc['Extraction Efficiency'].to_list()
        aq = bc['Alumina quality'].to_list()
        result = []
        
        for i in range(len(ag)):
            first_case = ag[i] - (dsp[i]*sg[i])
            second_case = 1-hl[i]
            if first_case <= 0 or second_case  <= 0 or ee[i]  <= 0 :
                result.append(0)
            else:
                result.append((1/first_case/second_case/ee[i])*aq[i])
        self.bauxite['Bauxite Use'] = result
        timer.stop(result)
    
    def customise_multiplier(self):
        timer.start()
        pi = self.PDT.copy()
        ref_pdt = self.RefPDT.copy()
        a = pi.loc[: , "Ref"]
        b = ref_pdt.loc[:, ['Ref. No.', 'BAR']]
        result = vlookup(a, b, 'Ref', 'Ref. No.', 'BAR')
        self.bauxite['Customise multiplier'] = result
        timer.stop(result)
        
    def final_bauxite_use_rate(self):
        a = self.bauxite['Bauxite Use'].to_list()
        b = self.bauxite['Customise multiplier'].to_list()
        self.bauxite['Final bauxite use rate'] = np.multiply(a, b)
    
    def caustic_lookup_helper(self, name):
        lc = self.lookup1_input.copy()
        tech = self.Global['Refinery Digestion Technology'].to_list()
        res = hlookup(tech, lc, 'Digestion Technology',name )
        self.caustic[name] = res
        
    def caustic_use(self):
        bc = self.bauxite.copy()
        cu = self.caustic.copy()
        dsp = cu['DSP Na:Si'].to_list()
        sg = self.bauxite_final_characteristics['Bauxite Final Silica Grade (Reacting)'].to_list()
        fbur = bc['Final bauxite use rate'].to_list()
        cwl = cu['Caustic wash loss'].to_list()
        self.caustic['Caustic use'] = np.add(np.multiply(np.multiply(dsp, sg), fbur), cwl)      
        
    def caustic_customise_multiplier(self):
        pi = self.PDT.copy()
        ref_pdt = self.RefPDT.copy()
        a = pi.loc[: , "Ref"]
        b = ref_pdt.loc[:, ['Ref. No.', 'Caustic']]
        result = vlookup(a, b, 'Ref', 'Ref. No.', 'Caustic')
        self.caustic['Customise multiplier'] = result
        
    def final_caustic_use_rate(self): 
        a = self.caustic['Customise multiplier'].to_list()
        b = self.caustic['Caustic use'].to_list()
        self.caustic['Final caustic use rate'] = np.multiply(a, b)
        
    #sodium[9]
    def soduim_carbonate_hlookup_helper(self, name, result_col):
        timer.start()
        pic = self.PDT.copy()
        lic = self.lookup_input.copy()
        province = pic.Province.to_list()
        result = hlookup(province, lic, 'Province Prices & Rates - Current',name )
        self.sodium_carbonate_price[result_col] = result
        timer.stop(result)

    def sodium_carbonate_self_supply_price(self):
        timer.start()
        sclp = np.array(self.PDT['Customisation multiplier for self supply'].to_list()[:self.pdt_max_row])
        cmfss = np.array(self.sodium_carbonate_price['Sodium Carbonate List Price'].to_list())
        # print(sclp * cmfss)
        freight_rate = self.bauxite_domestic_purchased['Bauxite 2 Freight rate (trucking)'].to_list()
        # set freight rate 
        self.sodium_carbonate_purchased['Sodium Carbonate Purchased Freight rate (trucking)'] = freight_rate
        result = sclp * cmfss
        self.sodium_carbonate_price['Sodium Carbonate Self Supply Price'] = result
        timer.stop(freight_rate, result)

    def sodium_carbonate_purchased_total_freight(self):
        timer.start()
        td = np.array(self.PDT['Purchased delivery Transport distance'].to_list()[:self.pdt_max_row])
        fr = np.array(self.sodium_carbonate_purchased['Sodium Carbonate Purchased Freight rate (trucking)'].to_list())
        luf = np.array(self.PDT['Purchased delivery [Loading + Unloading] Total Fee'].to_list()[:self.pdt_max_row])
        result = td * fr + luf
        self.sodium_carbonate_purchased['Sodium Carbonate Purchased Total Freight - Before Customisation'] = result
        timer.stop(result)

    def sodium_carbonate_purchased_final_freight(self):
        timer.start()
        tf = np.array(self.sodium_carbonate_purchased['Sodium Carbonate Purchased Total Freight - Before Customisation'].to_list())
        cm = np.array(self.PDT['Purchased delivery Customisation multiplier'].to_list()[:self.pdt_max_row])
        td = np.array(self.PDT['Purchased delivery Transport distance'].to_list()[:self.pdt_max_row])
        result = [0 if td[i] <= 0 else cm[i] * tf[i] for i in np.arange(len(td))]
        self.sodium_carbonate_purchased['Sodium Carbonate Purchased Final Freight'] = result
        timer.stop(result)

    def sodium_carbonate_purchased_final_price(self):
        timer.start()
        sclp = np.array(self.sodium_carbonate_price['Sodium Carbonate List Price'].to_list())
        scff = np.array(self.sodium_carbonate_purchased['Sodium Carbonate Purchased Final Freight'].to_list())
        result = sclp + scff
        self.sodium_carbonate_purchased['Sodium Carbonate Purchased Final Price'] = result
        timer.stop(result)

    def sodium_carbonate_self_supplied_total_freight(self):
        timer.start()
        freight_rate = self.bauxite_domestic_purchased['Bauxite 2 Freight rate (trucking)'].to_list()
        # set freight rate
        self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Freight rate (trucking)'] = freight_rate
        td = np.array(self.PDT['Self-Supplied delivery Transport distance'].to_list()[:self.pdt_max_row])
        fr = np.array(self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Freight rate (trucking)'].to_list())
        luf = np.array(self.PDT['Self-Supplied delivery [Loading + Unloading] Total Fee'].to_list()[:self.pdt_max_row])
        result = td * fr + luf
        self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Total Freight - Before Customisation'] = result
        timer.stop(result)

    def sodium_carbonate_self_supplied_final_freight(self):
        timer.start()
        tf = np.array(self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Total Freight - Before Customisation'].to_list())
        cm = np.array(self.PDT['Self-Supplied delivery Customisation multiplier'].to_list()[:self.pdt_max_row])
        td = np.array(self.PDT['Self-Supplied delivery Transport distance'].to_list()[:self.pdt_max_row])
        result = [0 if td[i] <= 0 else cm[i] * tf[i] for i in np.arange(len(td))]
        self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Final Freight'] = result
        timer.stop(result)

    def sodium_carbonate_self_supplied_final_price(self):
        timer.start()
        sclp = np.array(self.sodium_carbonate_price['Sodium Carbonate Self Supply Price'].to_list())
        scff = np.array(self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Final Freight'].to_list())
        result = sclp + scff
        self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Final Price'] = result
        timer.stop(result)
        
    def sodium_carbonate_final_price(self):
        timer.start()
        dss = np.array(self.PDT['Sodium Carbonate Domestic Self Supply'].to_list()[:self.pdt_max_row])
        scssfp = np.array(self.sodium_carbonate_self_supplied['Sodium Carbonate Self-Supplied Final Price'].to_list())
        scpfp = np.array(self.sodium_carbonate_purchased['Sodium Carbonate Purchased Final Price'].to_list())
        result = dss * scssfp + (np.ones(self.pdt_max_row) - dss) * scpfp
        self.sodium_carbonate_price['Sodium Carbonate Final Price'] = result
        timer.stop(result)
    #sodium[9]
        
        
        
        
        
    #dexter codes begin here
    #Energy for steam raising
    def energy_for_steam_calc(self):
        self.energy_for_steam['Province'] = self.PDT['Province']
        self.energy_for_steam['% from Lignitous Coal'] = self.PDT['Energy - Sourcing Mix -% from Anthracite Coal']
        self.energy_for_steam['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] = 1-self.energy_for_steam['% from Lignitous Coal']
        lignit = self.Prices.loc[16:24, ['Unnamed: 0', str(self.year)]].reset_index()
        lignit = lignit.merge(self.lookup_input, right_on='Province Prices & Rates - Current', left_on="Unnamed: 0", how="right").loc[:, ["Province Prices & Rates - Current",str(self.year)]]
        lignit.columns = ["Province Prices & Rates - Current", "Lignitous Coal List Price FAW"]
        d = {lignit['Province Prices & Rates - Current'][_]:lignit['Lignitous Coal List Price FAW'][_] for _ in range(lignit.shape[0])}
        self.energy_for_steam['Lignitous Coal List Price FAW']                      = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 1'] = self.PDT['Energy - Lignitous Coal - Customization Multiplier']
        self.energy_for_steam['Final Price FAW']                                    = self.energy_for_steam['Lignitous Coal List Price FAW'] * self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 1']
        self.energy_for_steam['Transport distance']                                 = self.PDT['Energy - Lignitous Coal - Transport Distance']
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Freight rate (trucking)'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_for_steam['Freight rate (trucking)']                            = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_for_steam['Loading + Unloading Fee']                            = self.PDT['Energy - Lignitous Coal - Loading + Unloading Fee']
        self.energy_for_steam['Total Freight']                                      = self.energy_for_steam['Transport distance'] * self.energy_for_steam['Freight rate (trucking)'] + self.energy_for_steam['Loading + Unloading Fee']
        self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 2']                           = self.PDT['Energy - Lignitous Coal - Customization Multiplier 2']
        self.energy_for_steam['Final Freight'] = [(0 if self.energy_for_steam['Transport distance'][x]<=0 else self.energy_for_steam['Total Freight'][x] * self.energy_for_steam['Energy - Lignitous Coal - Customization Multiplier 2'][x]) for x in self.energy_for_steam.index ]
        self.energy_for_steam['Delivered price'] = self.energy_for_steam['Final Freight'] + self.energy_for_steam['Final Price FAW'] 
        
        self.energy_for_steam['Calorific value conversion factor'] = 0.004184
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Lig. Coal Calorific Value'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_for_steam['Lig. Coal Calorific Value']                          = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_for_steam['Lig. Coal Calorific Value2']                         = self.energy_for_steam['Calorific value conversion factor'] * self.energy_for_steam['Lig. Coal Calorific Value']
        self.energy_for_steam['Final Price']                                        = [(self.energy_for_steam['Delivered price'][_]/self.energy_for_steam['Lig. Coal Calorific Value2'][_]) for _ in self.energy_for_steam.index]
        
        #Natural Gas / Coal Seam Gas / Coke Oven Gas
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Gas Price Delivered'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_for_steam['Gas Price Delivered']                                = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_for_steam['Customisation multiplier']                           = self.PDT['Energy - Natural - Customization Multiplier ']
        self.energy_for_steam['Natural Gas Final Price']                                        = self.energy_for_steam['Gas Price Delivered'] * self.energy_for_steam['Customisation multiplier']
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Gas Calorific Value'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_for_steam['Gas Calorific Value']                                = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_for_steam['Final Price2'] = self.energy_for_steam['Natural Gas Final Price'] /self.energy_for_steam['Gas Calorific Value'] 
        self.energy_for_steam['Steam raising fuel - final price'] = (self.energy_for_steam['% from Lignitous Coal']*self.energy_for_steam['Final Price']) + (self.energy_for_steam['% from Natural Gas / Coal Seam Gas / Coke Oven Gas']*self.energy_for_steam['Final Price2'])
        
    #Energy for steam raising calcing alumina
    def energy_for_calcining_alumina_calc(self):
        antra = self.Prices.loc[16:24, ['Unnamed: 0', str(self.year)]].reset_index()
        antra = antra.merge(self.lookup_input, right_on='Province Prices & Rates - Current', left_on="Unnamed: 0", how="right").loc[:, ["Province Prices & Rates - Current",str(self.year)]]
        antra.columns = ["Province Prices & Rates - Current", "Anthracite List Price"]
        antra["Anthracite List Price"] = antra.loc[:, "Anthracite List Price"] * 1.5
        self.energy_calcining_alumina['% from Anthracite Coal'] = self.PDT['Energy - Sourcing Mix - % from Anthracite Coal']
        self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] = 1 - self.energy_calcining_alumina['% from Anthracite Coal'] 
        d = {antra['Province Prices & Rates - Current'][_]:antra['Anthracite List Price'][_] for _ in range(antra.shape[0])}
        self.energy_calcining_alumina['Anthracite List Price'] =  [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_calcining_alumina['Customization Multiplier 1'] = self.PDT['Energy - Anthracite coal - Customization Multiplier 1']
        self.energy_calcining_alumina['Final Price FAW'] = self.energy_calcining_alumina['Anthracite List Price']*self.energy_calcining_alumina['Customization Multiplier 1']
       
        self.energy_calcining_alumina['Transport distance'] = self.PDT['Energy - Anthracite coal - Transport distance']
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Freight rate (trucking)'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_calcining_alumina['Freight rate (trucking)'] = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_calcining_alumina['Loading + Unloading Fee'] = self.PDT['Energy - Anthracite coal - Loading + Unloading Fee']
        self.energy_calcining_alumina['Total Freight'] = self.energy_calcining_alumina['Transport distance']*self.energy_calcining_alumina['Freight rate (trucking)']+ self.energy_calcining_alumina['Loading + Unloading Fee']
        self.energy_calcining_alumina['Customization Multiplier 2'] = self.PDT['Energy - Anthracite coal - Customization Multiplier 2']
        self.energy_calcining_alumina['Final Freight'] = [(0 if self.energy_calcining_alumina['Transport distance'][x]<=0 else self.energy_calcining_alumina['Total Freight'][x] * self.energy_calcining_alumina['Customization Multiplier 2'][x]) for x in self.energy_for_steam.index ]
        
        self.energy_calcining_alumina['Delivered price'] =self.energy_calcining_alumina['Final Price FAW']+self.energy_calcining_alumina['Final Freight']
                
        self.energy_calcining_alumina['Calorific value conversion factor'] = 0.004184
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input["A'cite Calorific Value"][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_calcining_alumina["A'cite Calorific Value"] =  [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_calcining_alumina["A'cite Calorific Value 2"] =  self.energy_calcining_alumina['Calorific value conversion factor'] * self.energy_calcining_alumina["A'cite Calorific Value"] 
        self.energy_calcining_alumina['Final Price'] = self.energy_calcining_alumina['Delivered price']/self.energy_calcining_alumina["A'cite Calorific Value 2"]
        
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Gas Price Delivered'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_calcining_alumina['Gas Price Delivered'] = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_calcining_alumina['Natural Gas Customization Multiplier'] = self.PDT['Energy - Natural Gas/Coal Customization Multiplier 3']
        self.energy_calcining_alumina['Natural Gas Final Price'] = self.energy_calcining_alumina['Gas Price Delivered']  *  self.energy_calcining_alumina['Natural Gas Customization Multiplier'] 
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Gas Calorific Value'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_calcining_alumina['Gas Calorific Value'] = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_calcining_alumina['Natural Gas Final Price 2'] = self.energy_calcining_alumina['Natural Gas Final Price']/self.energy_calcining_alumina['Gas Calorific Value']
        
        self.energy_calcining_alumina['Alumina calcining energy - final price'] = (self.energy_calcining_alumina['% from Anthracite Coal'] * self.energy_calcining_alumina['Final Price'] )+(self.energy_calcining_alumina['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] * self.energy_calcining_alumina['Natural Gas Final Price 2'])
        
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Grid Electricity List Price'][_] for _ in range(self.lookup_input.shape[0])}
        self.energy_calcining_alumina['Grid Electricity List Price'] =  [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.energy_calcining_alumina['Electricity Customisation multiplier'] = self.PDT['Energy - Electricity - Purchased Customization Multiplier']
        self.energy_calcining_alumina['Electricity Final Price'] = self.energy_calcining_alumina['Grid Electricity List Price'] * self.energy_calcining_alumina['Electricity Customisation multiplier']
    
    #Lime & Limestone - Prices & Sourcing Mix
    def lime_and_limestone_calc_1(self):
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Lime List Price'][_] for _ in range(self.lookup_input.shape[0])}
        self.lime['Lime List Price'] =  [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.lime['Customisation multiplier 1'] = self.PDT['Lime - Customisation multiplier 1']
        self.lime['Customised price'] = self.lime['Lime List Price'] *self.lime['Customisation multiplier 1']
        
        self.lime['Transport distance']         = self.PDT['Lime - Transport distance']
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Freight rate (trucking)'][_] for _ in range(self.lookup_input.shape[0])}
        self.lime['Freight rate(trucking)']     = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.lime['Loading + Unloading Fee']    = self.PDT['Lime - Loading + Unloading Fee']
        self.lime['Total Freight']              = self.lime['Transport distance'] * self.lime['Freight rate(trucking)'] + self.lime['Loading + Unloading Fee']
        self.lime['Customisation multiplier 2'] = self.PDT['Lime - Customisation multiplier 1']
        self.lime['Final Freight']              = [(0 if self.lime['Transport distance'][x]<=0 else self.lime['Total Freight'][x] * self.lime['Customisation multiplier 2'][x]) for x in self.energy_for_steam.index ]
        
        self.lime['Final Price']                = self.lime['Customised price'] + self.lime['Final Freight']
        
    def lime_and_limestone_calc_2(self):
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Limestone List Price'][_] for _ in range(self.lookup_input.shape[0])}
        self.limestone['Limestone List Price']  =  [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.limestone['Customisation multiplier 1'] = self.PDT['Limestone - Customisation multiplier 1']
        self.limestone['Customised price']      =self.limestone['Limestone List Price'] * self.limestone['Customisation multiplier 1']
        
        self.limestone['Transport distance']        = self.PDT['Limestone - Transport distance']
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Freight rate (trucking)'][_] for _ in range(self.lookup_input.shape[0])}
        self.limestone['Freight rate(trucking)']    = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.limestone['Loading + Unloading Fee']   = self.PDT['Limestone - Loading + Unloading Fee']
        self.limestone['Total Freight']             = self.limestone['Transport distance'] * self.limestone['Freight rate(trucking)'] + self.limestone['Loading + Unloading Fee']
        self.limestone['Customisation multiplier 2']= self.PDT['Limestone - Customisation multiplier 2']
        self.limestone['Final Freight']             = [(0 if self.limestone['Transport distance'][x]<=0 else self.limestone['Total Freight'][x] * self.limestone['Customisation multiplier 2'][x]) for x in self.energy_for_steam.index ]
        
        self.limestone['Final Price']               = self.limestone['Customised price'] + self.limestone['Final Freight']
    
    #Labour - Prices & Sourcing Mix
    def labour_prices_calc(self):
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Labor General List Price'][_] for _ in range(self.lookup_input.shape[0])}
        self.labour['Labor General List Price']         = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.labour['Customisation multiplier']         = self.PDT['Labour - Customisation multiplier']
        self.labour['Customised Price']                 = self.labour['Labor General List Price']  * self.labour['Customisation multiplier']
        self.labour['hours worked per year']            = self.PDT['Labour - hours worked per year']
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Labor Multiplier to All up Costs for Private Co.'][_] for _ in range(self.lookup_input.shape[0])}
        self.labour['Labor Multiplier to All up Costs for Private Co.'] = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Labor Multiplier to All up Costs for SOE'][_] for _ in range(self.lookup_input.shape[0])}
        self.labour['Labor Multiplier to All up Costs for SOE']         = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.labour['Final multiplier to all up costs'] =  [(self.labour['Labor Multiplier to All up Costs for Private Co.'][x] if self.Global['Ownership'][x]=='Private' else self.labour['Labor Multiplier to All up Costs for SOE'][x] ) for x in self.PDT.index]
        self.labour['Final Price - all up costs']       = self.labour['Customised Price'] * 12/self.labour['hours worked per year'] * self.labour['Final multiplier to all up costs']
    
    #energy usage rate(R472 to eof)
    def energy_usage_rate_calc(self): #updated
        d = {self.lookup1_input['Digestion Technology'][_]:self.lookup1_input['Required Steam raising energy'][_] for _ in range(self.lookup1_input.shape[0])}
        self.energy_usageR['Required Steam raising energy'] = [d[self.Global['Refinery Digestion Technology'][x]] for x in self.PDT.index] ##d['Import LT']  #corrections here
        self.energy_usageR['Customise multiplier 1'] = self.PDT['Energy Usage rate - Custom Multiplier 1']
        self.energy_usageR['steam energy final'] = self.energy_usageR['Required Steam raising energy'] * self.energy_usageR['Customise multiplier 1'] 
        
        self.energy_usageR['If Anthracite'] = 6.5
        self.energy_usageR['If Natural Gas / Coal Seam Gas / Coke Oven Gas'] = 3.2
        self.energy_usageR['Final before customising, based on split of fuels'] = (self.energy_calcining_alumina['% from Anthracite Coal'] * self.energy_usageR['If Anthracite']) + (self.energy_for_steam['% from Natural Gas / Coal Seam Gas / Coke Oven Gas'] * self.energy_usageR['If Natural Gas / Coal Seam Gas / Coke Oven Gas'])
        self.energy_usageR['Customise multiplier 2'] = self.PDT['Energy Usage rate - Custom Multiplier 2']
        self.energy_usageR['Calcining energy final'] = self.energy_usageR['Final before customising, based on split of fuels'] *self.energy_usageR['Customise multiplier 2']
        
        d = {self.lookup1_input['Digestion Technology'][_]:self.lookup1_input['Required Electrical energy'][_] for _ in range(self.lookup1_input.shape[0])}
        self.energy_usageR['Required Electrical energy'] = [d[self.Global['Refinery Digestion Technology'][x]] for x in self.PDT.index]#d['Import LT'] #correction needed here
        self.energy_usageR['Customise multiplier 3'] = self.PDT['Energy Usage rate - Custom Multiplier 3']
        self.energy_usageR['Electrical energy - final'] = self.energy_usageR['Required Electrical energy'] * self.energy_usageR['Customise multiplier 3']
    
    def lime_limestone_usage_rate_calc(self):
        d = {self.lookup1_input['Digestion Technology'][_]:self.lookup1_input['Lime rate'][_] for _ in range(self.lookup1_input.shape[0])}
        self.lime_usageR['Lime rate'] = [d[self.Global['Refinery Digestion Technology'][x]] for x in self.PDT.index]#d['Import LT'] #correction needed here
        self.lime_usageR['Customise multiplier 1'] = self.PDT['Lime Usage rate - Customisation multiplier']
        self.lime_usageR['Final lime rate'] = self.lime_usageR['Lime rate'] *self.lime_usageR['Customise multiplier 1']
        
        d = {self.lookup1_input['Digestion Technology'][_]:self.lookup1_input['Limestone rate'][_] for _ in range(self.lookup1_input.shape[0])}
        self.limestone_usageR['Limestone rate 1'] = [d[self.Global['Refinery Digestion Technology'][x]] for x in self.PDT.index]#d['Import LT'] #correction needed here
        self.limestone_usageR['Limestone rate 2'] = self.limestone_usageR['Limestone rate 1']*self.bauxite_final_characteristics['Bauxite Final Silica Grade (Reacting)']
        self.limestone_usageR['Customise multiplier 2'] = self.PDT['Limestone usage- customisation multiplier']
        self.limestone_usageR['Customised limestone rate'] =self.limestone_usageR['Limestone rate 2'] * self.limestone_usageR['Customise multiplier 2']
        self.limestone_usageR['Final limestone rate'] = self.limestone_usageR['Customised limestone rate'] * self.bauxite['Final bauxite use rate'] #corrections here after mergier
    
    def labour_usage_rate_calc(self):
        val1 = [(10*pow(self.PDT['Capacity - for maintenance and labour calcs'][x],-0.2) if (float(self.PDT['Capacity - for maintenance and labour calcs'][x]))>1 else 0) for x in self.PDT.index]
        val2 = [(1.5 if(self.Global['Digestion Technology Code'][x]==2 or self.Global['Digestion Technology Code'][x]==3 ) else 1) for x in self.PDT.index]
        self.labour_usageR['Workforce Productivity'] = [val1[_]*val2[_] for _ in self.PDT.index]
        self.labour_usageR['customise'] = self.PDT['Labour usage- customisation multiplier']
        self.labour_usageR['Final Workforce Productivity'] = [(self.labour_usageR['Workforce Productivity'][x] * (1+self.labour_usageR['customise'][x] )) for x in self.PDT.index ]
        
    def comsumables_usage_rate_calc(self):
        self.consumables_usageR['Red Mud Make - scaling factor'] = [max(1, self.bauxite['Final bauxite use rate'][x]+ self.lime_usageR['Final lime rate'][x] + self.limestone_usageR['Final limestone rate'][x]-1) for x in self.PDT.index]
        #flocculent
        self.consumables_usageR['Usage rate'] = self.PDT['Consumables - Flocculent - Usage rate']
        self.consumables_usageR['customise']  = self.PDT['Consumables - Flocculent - customise']
        self.consumables_usageR['Customised usage rate'] =  [(self.consumables_usageR['Usage rate'][x] * (1+self.consumables_usageR['customise'][x] )) for x in self.PDT.index ]
        self.consumables_usageR['Usage'] = self.consumables_usageR['Customised usage rate'] * self.consumables_usageR['Red Mud Make - scaling factor']/1000
        
        
    def costs_bauxite(self):
        self.bauxite_cost['bauxite cost'] = self.bauxite_final_characteristics['Bauxite Final Total Delivered'] * self.bauxite['Final bauxite use rate']
        
    def costs_caustic(self):
        self.caustic_cost['Caustic cost'] = self.caustic['Final caustic use rate'] * self.caustic_self_supplied['Caustic Final Price']
        self.caustic_cost['Sodium Carbonate cost'] = 0
        self.caustic_cost['Total caustic cosr'] = self.caustic_cost['Caustic cost'] + self.caustic_cost['Sodium Carbonate cost']
        
    def costs_energy(self):
        self.energy_cost['Lignitious coal'] = self.energy_usageR['steam energy final'] *  self.energy_for_steam['Final Price'] 
        self.energy_cost['Anthracite/Gas']  = self.energy_usageR['Calcining energy final'] * self.energy_calcining_alumina['Alumina calcining energy - final price']
        self.energy_cost['Electricity']     = self.energy_usageR['Electrical energy - final'] * self.energy_calcining_alumina['Electricity Final Price']
        self.energy_cost['Total energy cost']   = self.energy_cost['Lignitious coal']  + self.energy_cost['Anthracite/Gas'] +  self.energy_cost['Electricity']
        
    def costs_lime(self):
        self.lime_cost['Lime']              = self.lime_usageR['Final lime rate'] * self.lime['Final Price'] 
        self.lime_cost['Limestone']         = self.limestone['Final Price'] * self.limestone_usageR['Final limestone rate']
        self.lime_cost['Total lime cost']   = self.lime_cost['Lime'] + self.lime_cost['Limestone']
        
    def costs_labour(self):
        self.labour_cost['Labour'] = self.labour_usageR['Final Workforce Productivity'] * self.labour['Final Price - all up costs']
        
    def costs_consumables(self):
        self.consumables_cost['Flocculent']         = self.consumables_usageR['Usage'] * self.flocculent['Flocculent Final Price']
        val1 = [(250*pow(self.capacity_and_production['Production - sub module'][x],-0.33) if (float(self.capacity_and_production['Production - sub module'][x]))>1 else 0) for x in self.PDT.index]
        val2 = [(1.5 if(self.Global['Digestion Technology Code'][x]==2 or self.Global['Digestion Technology Code'][x]==3 ) else 1) for x in self.PDT.index]
        self.consumables_cost['Allowance for other Consumables']  = [val1[_]*val2[_] for _ in self.PDT.index]
        self.consumables_cost['Total consumable cost'] = self.consumables_cost['Flocculent'] +  self.consumables_cost['Allowance for other Consumables']
        
    def costs_maintenance(self):
        val1 = [(4500*pow(self.PDT['Capacity - for maintenance and labour calcs'][x],-0.09) if (float(self.PDT['Capacity - for maintenance and labour calcs'][x]))>1 else 0) for x in self.PDT.index]
        val2 = [(1.5 if(self.Global['Digestion Technology Code'][x]==2 or self.Global['Digestion Technology Code'][x]==3 ) else 1) for x in self.PDT.index]
        self.maintenance_cost['C&M Formulae Capital RMB/t'] = [val1[_]*val2[_] for _ in self.PDT.index]
        self.maintenance_cost['C&M Formulae Capital US$/t'] = [(self.maintenance_cost['C&M Formulae Capital RMB/t'][x]/7 if self.maintenance_cost['C&M Formulae Capital RMB/t'][x]>0 else ' ') for x in self.PDT.index]
        self.maintenance_cost['Maintenance - Portion of Capex'] = self.PDT['Maintenance Allowance - Portion of Capex']
        self.maintenance_cost['Maintenance - Maintenance Cost'] =self.maintenance_cost['C&M Formulae Capital RMB/t'] * self.maintenance_cost['Maintenance - Portion of Capex']
        
    def costs_other_costs(self):
        self.other_cost['Other cost allowance'] =  [(65*pow(self.capacity_and_production['Production - sub module'][x],-0.1) if (float(self.capacity_and_production['Production - sub module'][x]))>1 else 0) for x in self.PDT.index]
        
        self.other_cost['Red mud quantity'] =  self.bauxite['Final bauxite use rate']+self.caustic['Final caustic use rate']+self.lime_usageR['Final lime rate']+self.limestone_usageR['Final limestone rate']
        self.other_cost['Red mud quantity'] = self.other_cost['Red mud quantity'] - 1
        d = {self.lookup_input['Province Prices & Rates - Current'][_]:self.lookup_input['Red Mud Disposal Cost'][_] for _ in range(self.lookup_input.shape[0])}
        self.other_cost['Red Mud Disposal Cost 1'] = [ d[self.energy_for_steam['Province'][x]] for x in self.energy_for_steam.index ]
        self.other_cost['Red Mud Disposal Cost 2'] = [max(0, self.other_cost['Red mud quantity'][x]*self.other_cost['Red Mud Disposal Cost 1'][x] ) for x in self.PDT.index]
        
        self.other_cost['Packaging'] = self.PDT['Other Costs - Packaging']
        
    def summary(self):
        self.PDTsummmary['Bauxite']             = self.bauxite_cost['bauxite cost'] 
        self.PDTsummmary['Caustic']             = self.caustic_cost['Total caustic cosr']
        self.PDTsummmary['Lime']                = self.lime_cost['Total lime cost']
        self.PDTsummmary['Energy']              = self.energy_cost['Total energy cost']
        self.PDTsummmary['Labour']              = self.labour_cost['Labour']
        self.PDTsummmary['Consumables']         = self.consumables_cost['Total consumable cost']
        self.PDTsummmary['Maintenance']         = self.maintenance_cost['Maintenance - Maintenance Cost']
        self.PDTsummmary['Red Mud Disposal']    = self.other_cost['Red Mud Disposal Cost 2']
        self.PDTsummmary['Other Costs']         = self.other_cost['Other cost allowance']
        self.PDTsummmary['Operating Cash Cost'] = self.PDTsummmary[['Bauxite','Caustic','Lime','Energy','Labour','Consumables','Maintenance','Red Mud Disposal','Other Costs']].sum(axis=1)
        self.PDTsummmary['Packaging']           = self.other_cost['Packaging']
        self.PDTsummmary['Cash Cost - FAW']     = self.PDTsummmary['Operating Cash Cost'] + self.PDTsummmary['Packaging']
    
    #engine
    def calc_all_2(self):
        PDT_Cost_Current.price_tab_calc(self)
        
    def calc_all_3(self):
        PDT_Cost_Current.energy_for_steam_calc(self)
        PDT_Cost_Current.energy_for_calcining_alumina_calc(self)
        PDT_Cost_Current.lime_and_limestone_calc_1(self)
        PDT_Cost_Current.lime_and_limestone_calc_2(self)
        PDT_Cost_Current.labour_prices_calc(self)
        PDT_Cost_Current.energy_usage_rate_calc(self)
        PDT_Cost_Current.lime_limestone_usage_rate_calc(self)
        PDT_Cost_Current.labour_usage_rate_calc(self)
        PDT_Cost_Current.comsumables_usage_rate_calc(self)
        PDT_Cost_Current.costs_bauxite(self)
        PDT_Cost_Current.costs_caustic(self)
        PDT_Cost_Current.costs_energy(self)
        PDT_Cost_Current.costs_lime(self)
        PDT_Cost_Current.costs_labour(self)
        PDT_Cost_Current.costs_consumables(self)
        PDT_Cost_Current.costs_maintenance(self)
        PDT_Cost_Current.costs_other_costs(self)
        PDT_Cost_Current.summary(self)
    
    def calc_all_4(self):    
        PDT_Cost_Current.ref_cal_1(self)
            
    def calc_all_5(self):
        PDT_Cost_Current.refineries_cap_production_calc_1(self)
        PDT_Cost_Current.refineries_cap_production_calc_2(self)
        PDT_Cost_Current.refineries_cap_production_calc_3(self)
        PDT_Cost_Current.refineries_cap_production_calc_4(self)
    
    def calc_all(self):
        self.calc_all_4()
        self.calc_all_2()
        self.calc_all_1()
        self.calc_all_3()
        self.calc_all_1_a()
        self.calc_all_5()
        
        
    
    
#engine functions
def vlookup(df1, df2, on_df1, on_df2, res_col):
    result = df2.merge(df1, left_on=on_df2, right_on=on_df1)
    return result[res_col].to_list()
                  
def hlookup(df1_list, df2, row, col):
    result = []
    for s in df1_list:
        l = df2.loc[df2[row] == s, col].to_list()
        if len(l) == 0:
            result = [*result , np.nan]
        else:
            result = [*result, *l]
    return result

#league master begins here
timer = Timer("league_master", txt=True)
# start of league master
class League_Master():
#League_Master(pdt_cost.input_filename, pdt_cost.input, pdt_cost.PDT_input)
    def __init__(self, pdt):
        timer.start()
        self.pdt = pdt
        self.input = pdt.input
        self.pdt_input = pdt.PDT
        self.china_alumni = None
        self.top_right = pd.DataFrame(columns=[])
        self.Provincial = pd.DataFrame(columns=[])
        self.Regional = pd.DataFrame(columns=[])
        self.soe_private = None
        self.Province = None
        self.Region = None
        self.kt_total = 0
        self.current = pdt.final_cost_summary['Exchange Rate - RMB/USD'][0]
        self.s_5 = self.input.loc[0, 's_5']
        self.merged_league_master = None
        self.lm_max_row = len(self.input['Ref'])
        self.lm_year = int(self.input['Year'][0])
        timer.stop()

    def calc_all(self):
        self.init_cols()
        self.calc_col_t()
        self.sum_kt()
        self.sum_product_helper('Production kT', 'Cost $/t', 'Cost $/t')
        self.sum_product_helper('Production kT', 'Cost -RMB/t Bx', 'Cost -RMB/t Bx')
        self.sum_product_helper('Production kT', 'Cost -RMB/t Caustic', 'Cost -RMB/t Caustic')
        self.sum_product_helper('Production kT', 'Cost -RMB/t Energy', 'Cost -RMB/t Energy')
        self.sum_product_helper('Production kT', 'Cost -RMB/t Opex', 'Cost -RMB/t Opex')
        self.opex_rank()
        self.classify_in_groups()
        self.bx_percent()
        self.group_china_alumni()
        self.dependency()
        self.seo_group_lookup(2, 'Column 2')
        self.seo_group_lookup(3, 'Column 1')
        self.production_kt_index_match()
        self.index_matchup(1, 'SOE', 'Column 2')
        self.index_matchup(2, 'Private', 'Column 2')
        self.index_matchup(1, 'Weiqiao', 'Column 1')
        self.index_matchup(2, 'Xinfa', 'Column 1')
        self.index_matchup(3, 'Chalco', 'Column 1')
        self.index_matchup(4, 'League master Other', 'Column 1')
        self.sum_if_helper('Province', "Provincial")
        self.sum_if_helper('Region', "Regional")
        self.province_sum()
        self.region_sum()
        self.province_product_if('Cost -RMB/t Bx', 'Provincial Cost -$/t Bx')
        self.province_product_if('Cost -RMB/t Caustic', 'Provincial  Cost -$/t Caustic')
        self.province_product_if('Cost -RMB/t Energy', 'Provincial  Cost -$/t Energy')
        self.province_product_if('Cost -RMB/t Opex', 'Provincial Cost -$/t Opex')
        self.region_product_if('Cost -RMB/t Bx', 'Regional Cost -$/t Bx')
        self.region_product_if('Cost -RMB/t Caustic', 'Regional Cost -$/t Caustic')
        self.region_product_if('Cost -RMB/t Energy', 'Regional Cost -$/t Energy')
        self.region_product_if('Cost -RMB/t Opex', 'Regional Cost -$/t Opex')
        self.province_cost_rmb_t()
        self.region_cost_rmb_t()
        # self.merge_tables()
        # self.group_province()
        # self.group_region()

    def init_cols(self):
        timer.start()
        # print(pdt_cost.final_cost_summary)
        self.Provincial.loc[:10, "Provincial"] = self.input.loc[:10, "Provincial"].to_list()
        self.Regional.loc[:5, "Regional"] = self.input.loc[:5, "Regional"].to_list()
        selected_col = self.pdt_input.loc[:, ["Ref", "Production kT", "Province", "Region", "Owner"]]
        self.input = self.input.merge(selected_col, on="Ref", how="left")
        # self.input.loc[94:100, ["Production kT", "Province", "Region", "Owner"]] = 0
        self.input.loc[:self.lm_max_row-1, "Cost -RMB/t Bx"] = self.pdt.final_cost_summary["Bauxite"] 
        self.input.loc[:self.lm_max_row-1, "Cost -RMB/t Caustic"] = self.pdt.final_cost_summary["Caustic"] 
        self.input.loc[:self.lm_max_row-1, "Cost -RMB/t Energy"] = self.pdt.final_cost_summary["Energy"] 
        self.input.loc[:self.lm_max_row-1, "Cost -RMB/t Opex"] = self.pdt.final_cost_summary["Cash Cost - FAW"] 
        self.input.loc[:self.lm_max_row-1, "SOE=1, Private=2"] = self.pdt.final_cost_summary["SOE=1, Private=2"] 
        # print(self.input)
        timer.stop()

    def calc_col_t(self):
        timer.start()
        lmc = self.input.copy()
        opex_val = self.input.loc[:, 'Cost -RMB/t Opex'].to_list()
        c_val = self.current
        cost = [a/c_val for a in opex_val]
        self.input['Cost $/t'] = cost
        timer.stop(cost)

    def sum_kt(self):
        timer.start()
        kt_col = 'Production kT'
        kt_total = self.input.loc[:,kt_col].sum()
        self.kt_total = kt_total
        self.input.loc[self.lm_max_row, 'Production kT'] = kt_total
        timer.stop([kt_total])

#   param with the last val as _ will be referenced in the main table
    def sum_product_helper(self, col_1, col_2, result_col, not_kt=False ):
        timer.start()
        lmc = self.input.copy()
        if col_1[-1] != '_':
            a = np.nan_to_num(self.input[col_1].to_list()[0:self.lm_max_row])
        else: 
            col_1 = col_1[:-1]
            a = lmc[col_1].to_list()
        if col_2[-1] != '_':
            b = np.nan_to_num(self.input[col_2].to_list()[0:self.lm_max_row])
        else:
            col_2 = col_2[:-1]
            b = lmc[col_2].to_list()
        result = np.dot(a, b) / self.kt_total
        lmc.loc[self.lm_max_row, result_col] = result
        self.input = lmc
        timer.stop([result])

    def classify_in_groups(self):
        timer.start()
        lmc = self.input.copy()
        owner = lmc.loc[:, "Owner"].to_list()[:self.lm_max_row]
        for i in np.arange(len(owner)):
            val = owner[i]
            if val == 'Weiqiao':
                owner[i] = 1
            elif val == 'Xinfa':
                owner[i] = 2
            elif val == 'Chalco':
                owner[i] = 3
            else:
                owner[i] = 4
        lmc.loc[:self.lm_max_row-1,"Groups"] = owner
        self.input = lmc
        timer.stop(owner)

    def opex_rank(self):
        timer.start()
        lmc = self.input.copy()
        opex_col = 'Cost -RMB/t Opex'
        opex_col_val = self.input[opex_col].to_list()[:self.lm_max_row]
        ranks = rank(opex_col_val)
        for i in np.arange(len(opex_col_val)):
            ranks[i] = ranks[i] + [*opex_col_val[:i+1]].count(opex_col_val[i]) - 1
        lmc.loc[:self.lm_max_row-1, "Rank"] = ranks
        self.input = lmc
        timer.stop(ranks)

    def bx_percent(self):
        timer.start()
        lmc = self.input.copy()
        div = lmc.loc[self.lm_max_row, 'Cost -RMB/t Bx'] / lmc.loc[self.lm_max_row, 'Cost -RMB/t Opex']
        result = '{0} %'.format(round(div*self.lm_max_row, 2))
        lmc.loc[self.lm_max_row+1, 'Cost -RMB/t Bx'] = result
        self.input = lmc
        timer.stop([result])
        
    def group_china_alumni(self):
        self.china_alumni = self.input.copy()
    
#     start of right side of the sheet
    
    def dependency(self):
        timer.start()
        arr = np.zeros(self.lm_max_row*3 + 1)
        arr[0] = np.NaN
        arr[1] = 1
        for i in np.arange(2, len(arr)):
            if i < 4:
                arr[i] = arr[i-1]
            else:
                arr[i] = arr[i-3] + 1
        self.top_right.loc[:self.lm_max_row*3 + 1, "Column 3"] = arr
        timer.stop(arr)
        
    def seo_group_lookup(self, index, result_col):
        timer.start()
        ca = self.china_alumni.copy()
        arr = self.top_right.loc[:,'Column 3'].to_list()[1:]
        lookup = [*[ca['Rank'].to_list()[:self.lm_max_row]], *[ca['SOE=1, Private=2'].to_list()[:self.lm_max_row]], *[ca['Groups'].to_list()[:self.lm_max_row]]]
        result = modified_vlookup(self.lm_max_row, arr, lookup, index)[:-1]
        self.top_right.loc[0, result_col] = np.NaN
        self.top_right.loc[1:,result_col] = result
        timer.stop(result)

    def index_helper(self, arr_col, lookup_val_col, lookup_col, div=True, r=0):
        tr = self.top_right.copy()
        ca = self.china_alumni.copy()
        arr = ca[arr_col].to_list()
        lookup_val_arr = tr[lookup_val_col].to_list()
        lookup_val_arr = lookup_val_arr[1:] if div else lookup_val_arr
        lookup_arr = ca[lookup_col].to_list()
        result = []
        for n in lookup_val_arr:
            try:
                ind = lookup_arr.index(n)
                if div:
                    val = (arr[ind]/self.kt_total )* self.s_5
                    if r > 0:
                        val = round(val, 1)
                    else:
                        val = round(val)
                    result.append(val)
                else:
                    result.append(round(arr[ind]))
            except ValueError:
                result.append(0)
        return result
    
    def production_kt_index_match(self):
        timer.start()
        first_col = np.array([ np.NaN , *self.index_helper('Production kT', 'Column 3', 'Rank', r=1)])
        second_col = np.zeros(self.lm_max_row*3 + 1)
        third_col = np.zeros(self.lm_max_row*3 + 1)
        prev = 0
        for i in np.arange(4,len(second_col),3):
            val = prev + first_col[i]
            second_col[i:i+3] = [round(val)]*3
            third_col[i:i+3] = [round(val, 2)]*3
            prev = val
        self.top_right.loc[:,'Column 4'] = first_col
        self.top_right.loc[:,'Column 5'] = second_col
        self.top_right.loc[:,'Column 6'] = third_col
        timer.stop(first_col, second_col, third_col)
        
    def index_matchup(self, curr_num, result_col, lookup):
        timer.start()
        compute = np.array(self.index_helper('Cost $/t', 'Column 3', 'Rank', div=False))
        checkup_arr = np.array(self.top_right.loc[: , lookup].to_list())
        for i in np.arange(0,len(checkup_arr)-1, 3):
            if checkup_arr[i+1] == curr_num:
                compute[i:i+3] = [*[compute[i+1]]*2, 0]
            else:
                compute[i:i+3] = np.zeros(3)
        compute[-1] = 0
        self.top_right[result_col] = compute
        timer.stop(compute)
        # self.league_master = lmc
        
    def sum_if_helper(self,lookup, input_col):
        timer.start()
        lmc = self.input.copy()
        result = []
        df = self.input[:]
        max_col = 9
        if input_col == 'Regional':
            max_col = 4
        values = lmc.loc[:, input_col].to_list()[:max_col+1]
        for v in values:   
            val = df.loc[df[lookup] == v , 'Production kT'].sum()
            result.append(val / 1) 
        if input_col == "Provincial":
            self.Provincial.loc[:max_col, "Provincial Production kT"] = result
        else :
            self.Regional.loc[:max_col, "Regional Production kT"] = result
        # print(result)
        timer.stop(result)
    
    def province_sum(self):
        timer.start()
        pro = self.Provincial.copy()
        total = pro.loc[:9, "Provincial Production kT"].sum()
        pro.loc[10, "Provincial Production kT"] = total
        self.Provincial = pro
        timer.stop([total])

    def region_sum(self):
        timer.start()
        reg = self.Regional.copy()
        total = reg.loc[:4, "Regional Production kT"].sum()
        reg.loc[5, "Regional Production kT"] = total
        self.Regional = reg
        timer.stop([total])

    def province_product_if(self,col_1, result_col):
        timer.start()
        result = []
        df = self.input[:]
        values = df.loc[:9, 'Provincial'].to_list()
        divisor = self.Provincial.loc[:9, 'Provincial Production kT'].to_list()
        for i, v in enumerate(values):   
            a = df.loc[df["Province"] == v , col_1].to_list()
            b = df.loc[df["Province"] == v , 'Production kT'].to_list()
            val = np.dot(a,b)
            if val > 0:
                result.append(round(val/divisor[i]/self.current, 2))
            else: 
                result.append(0)
        self.Provincial.loc[:9, result_col] = result
#         store sumproduct of row below
        a = self.Provincial.loc[:9, result_col]
        b = self.Provincial.loc[:9, 'Provincial Production kT']
        kt = self.Provincial.loc[10, 'Provincial Production kT']
        f_result = np.dot(a,b) / kt
        self.Provincial.loc[10, result_col] = f_result
        timer.stop(result, [f_result])

    def region_product_if(self,col_1, result_col):
        timer.start()
        result = []
        df = self.input[:]
        values = df.loc[:4, 'Regional'].to_list()
        divisor = self.Regional.loc[:9, 'Regional Production kT'].to_list()
        for i, v in enumerate(values):   
            a = df.loc[df["Region"] == v , col_1].to_list()
            b = df.loc[df["Region"] == v , 'Production kT'].to_list()
            val = np.dot(a,b)
            if val > 0:
                result.append(round(val/divisor[i]/self.current, 2))
            else: 
                result.append(0)
        self.Regional.loc[:4, result_col] = result
#         store sumproduct of row below
        a = self.Regional.loc[:4, result_col]
        b = self.Regional.loc[:4, 'Regional Production kT']
        kt = self.Regional.loc[5, 'Regional Production kT']
        f_result = np.dot(a,b) / kt
        self.Regional.loc[5, result_col] = f_result
        # print(self.Regional)
        timer.stop(result, [f_result])
  
    def province_cost_rmb_t(self):
        timer.start()
        a = np.array(self.Provincial.loc[:9, "Provincial Cost -$/t Opex"].to_list())
        b = np.array(self.current)
        result = a * b
        self.Provincial.loc[:9, "Provincial Cost RMB/t"] = result
        a = self.Provincial.loc[:9, "Provincial Cost RMB/t"]
        b = self.Provincial.loc[:9, 'Provincial Production kT']
        kt = self.Provincial.loc[10, 'Provincial Production kT']
        f_result = np.dot(a,b) / kt
        self.Provincial.loc[10, "Provincial Cost RMB/t"] = f_result
        timer.stop(result, [f_result])

    def region_cost_rmb_t(self):
        timer.start()
        a = np.array(self.Regional.loc[:4, "Regional Cost -$/t Opex"].to_list())
        b = np.array(self.current)
        result = a * b
        self.Regional.loc[:4, "Regional Cost RMB/t"] = result
        a = self.Regional.loc[:4, "Regional Cost RMB/t"]
        b = self.Regional.loc[:4, 'Regional Production kT']
        kt = self.Regional.loc[5, 'Regional Production kT']
        f_result = np.dot(a,b) / kt
        self.Regional.loc[5, "Regional Cost RMB/t"] = f_result
        timer.stop(result, [f_result])

    # def merge_tables(self):
    #     timer.start()
    #     self.top_right.loc[:, str(self.lm_year)] = self.lm_year
    #     merged = self.input.merge(self.Provincial, on="Provincial", how="left").merge(self.Regional, on="Regional", how="left")
    #     merged = pd.concat([merged, self.top_right],  sort=False, axis = 1)
    #     col = list(merged.columns)
    #     col.remove("Provincial")
    #     col.remove("Regional")
    #     col[16] = "Provincial"
    #     col[22] = "Regional"
    #     merged = merged.reindex(columns=col)
    #     self.merged_league_master = merged
    #     timer.stop(end=True)
            
         

def rank(A):
    R = [0 for i in range(len(A))]
    T = [(A[i], i) for i in range(len(A))]
    T.sort(key=lambda x: x[0])
    (rank, n, i) = (1, 1, 0)
    while i < len(A):
        j = i
        while j < len(A) - 1 and T[j][0] == T[j + 1][0]:
            j += 1
        n = j - i + 1
        for j in range(n):
            idx = T[i+j][1]
            R[idx] = rank
        rank += n
        i += n
    return R

def isNaN(num):
    return num != num

def merge_col_as_head(arr1, arr2):
    result = []
    for i in range(len(arr1)):
        if isNaN(arr1[i]) and isNaN(arr2[i]) :
            result.append('Unnamed: {0}'.format(i))
        elif isNaN(arr1[i]):
            if isinstance(arr2[i], int) or isinstance(arr2[i], float) :
                result.append('Unnamed: {0}'.format(i))
            else: 
                result.append(arr2[i])
        elif isNaN(arr2[i]):           
             if isinstance(arr1[i], int) or isinstance(arr1[i], float):
                result.append('Unnamed: {0}'.format(i))
             else:
                result.append(arr1[i])
        else: 
            result.append('{0} {1}'.format(arr1[i], arr2[i]))
    return result
    
def modified_vlookup(max_row, arr, lookup, index):
    result = np.zeros(max_row*3 + 1)
    for i in range(0,len(arr),3):
        try:
            ind = lookup[0].index(arr[i])
            result[i] = lookup[index-1][ind]
        except ValueError:
            result[i] = 0
    return result


#main sheet 
class Main_Sheet():
    def __init__(self, pdt_cost, demand,key_inputs, key_check):
        self.mainSheet_demand = demand
        self.mainSheet_key_inputs = key_inputs
        self.mainSheet_key_check = key_check
        self.mainSheet_cost = pd.DataFrame(columns=[])
        self.mainSheet_supplycost = pd.DataFrame(columns=[])
        self.mainSheet_supplystream = pd.DataFrame(columns=[])
        self.mainSheet_rankCosts = pd.DataFrame(columns=[])
        self.mainSheet_producesCostRank = pd.DataFrame(columns=[])
        self.mainSheet_costCostRank = pd.DataFrame(columns=[])
        self.mainSheet_supplyCostRank = pd.DataFrame(columns=[])
        self.mainSheet_accumulatedSupplyCostRank = pd.DataFrame(columns=[])
        self.mainSheet_costAvailable = pd.DataFrame(columns=[])
        self.mainSheet_Demand =[]
        self.mainSheet_CostMarginalTonne = pd.DataFrame(columns=[])
        
        
        
        self.supply_stream = pdt_cost.Ref_supply_stream
        #engine
    def calc_all(self):
        Main_Sheet.main_sheet_calc_1(self)
        Main_Sheet.main_sheet_calc_2(self)
        Main_Sheet.main_sheet_calc_3(self)
        Main_Sheet.main_sheet_calc_4(self)
        Main_Sheet.main_sheet_calc_5(self)
        Main_Sheet.main_sheet_calc_6(self)
        Main_Sheet.main_sheet_calc_7(self)
        Main_Sheet.main_sheet_calc_8(self)
        Main_Sheet.main_sheet_calc_9(self)
        Main_Sheet.main_sheet_calc_10(self)
        Main_Sheet.main_sheet_calc_11(self)
        
    def main_sheet_calc_1(self):
        for col in self.mainSheet_demand.columns[2:]:
            self.mainSheet_demand.loc[2,col] = self.mainSheet_demand.loc[0,col] - self.mainSheet_demand.loc[1,col]
            self.mainSheet_demand.loc[3,col] = pdt_cost.Ref_aa_prod[col].sum()
            self.mainSheet_demand.loc[4,col] = self.mainSheet_demand.loc[0,col] - self.mainSheet_demand.loc[1,col] - self.mainSheet_demand.loc[3,col]
            
            self.mainSheet_key_check.loc[0,col] = pdt_cost.Ref_supply_stream[col].sum()
            self.mainSheet_key_check.loc[1,col] = self.mainSheet_demand.loc[4,col]
            self.mainSheet_key_check.loc[2,col] = self.mainSheet_key_check.loc[1,col] /self.mainSheet_key_check.loc[0,col]
        
        self.mainSheet_key_check.loc[0,'Demand'] = 'Modelled capacity to produce alumina from importd bauxite'
        self.mainSheet_key_check.loc[1,'Demand'] = 'Demand for alumina from importd bauxite'
        self.mainSheet_key_check.loc[2,'Demand'] = 'Utilisation'
        self.mainSheet_key_check.rename(columns={'Demand':'Key Check'}, inplace=True)
    
    def main_sheet_calc_2(self):
        self.mainSheet_cost['Ref No'] = pdt_cost.Ref_base_cap['Ref. No.'][:-1]
        self.mainSheet_cost['Refinery'] = pdt_cost.Ref_base_cap['Refinery'][:-1]
        for year in self.mainSheet_key_check.columns[2:]:
            year = int(year)
            pdt = PDT_Cost_Current(year)
            pdt.calc_all()
            lm = League_Master(pdt)
            lm.calc_all()
            self.mainSheet_cost[year] = lm.china_alumni['Cost $/t'][:pdt.pdt_max_row]
            
            # self.mainSheet_cost[year] = data_table(year)
            
    def main_sheet_calc_3(self):
        data = self.mainSheet_cost.copy().fillna(0)
        for col in self.mainSheet_cost.columns[2:]:
            data.loc[24,col] = 200
            data.loc[56,col] = 200
        self.mainSheet_supplycost = data
        
    def main_sheet_calc_4(self):
        self.mainSheet_supplystream['Ref No'] = self.mainSheet_cost['Ref No']
        self.mainSheet_supplystream['Refinery'] = self.mainSheet_cost['Refinery']
        for col in self.mainSheet_cost.columns[2:]:
            self.mainSheet_supplystream[col] = pdt_cost.Ref_supply_stream[col]
            
    def main_sheet_calc_5(self):
        self.mainSheet_rankCosts['Ref No'] = self.mainSheet_cost['Ref No']
        self.mainSheet_rankCosts['Refinery'] = self.mainSheet_cost['Refinery']
        for col in  self.mainSheet_cost.columns[2:]:
            cnt = 0
            rank  = sorted(self.mainSheet_supplycost[col])
            for row in self.mainSheet_supplycost.index:
                cnt = self.mainSheet_supplycost.loc[:row,col].fillna(0).to_list()
                self.mainSheet_rankCosts.at[row,col] = cnt.count(self.mainSheet_supplycost.loc[row, col])+rank.index(self.mainSheet_supplycost.loc[row,col])
            
    def main_sheet_calc_6(self):
        self.mainSheet_producesCostRank['No'] = list(range(1, self.mainSheet_cost.shape[0] +1))
        for col in  self.mainSheet_cost.columns[2:]:
            d = {self.mainSheet_rankCosts[col][x]:self.mainSheet_rankCosts['Refinery'][x] for x in self.mainSheet_cost.index }
            self.mainSheet_producesCostRank[col] = [d[self.mainSheet_producesCostRank.loc[row,'No']] for row in self.mainSheet_producesCostRank.index]
  
    def main_sheet_calc_7(self):
        self.mainSheet_costCostRank['No'] = list(range(1, self.mainSheet_cost.shape[0]+1))
        for col in  self.mainSheet_cost.columns[2:]:
            d = {self.mainSheet_rankCosts[col][x]:self.mainSheet_supplycost[col][x] for x in self.mainSheet_cost.index }
            self.mainSheet_costCostRank[col] = [d[self.mainSheet_costCostRank.loc[row,'No']] for row in self.mainSheet_costCostRank.index]
    
    def main_sheet_calc_8(self):
        self.mainSheet_supplyCostRank['No'] = list(range(1, self.mainSheet_cost.shape[0]+1 ))
        for col in  self.mainSheet_cost.columns[2:]:
            d = {self.mainSheet_rankCosts[col][x]:self.mainSheet_supplystream[col][x] for x in self.mainSheet_cost.index }
            self.mainSheet_supplyCostRank[col] = [d[self.mainSheet_supplyCostRank.loc[row,'No']] for row in self.mainSheet_supplyCostRank.index]
    
    def main_sheet_calc_9(self):
        self.mainSheet_accumulatedSupplyCostRank['No'] = list(range(1, self.mainSheet_cost.shape[0]+1))
        for col in  self.mainSheet_cost.columns[2:]:
            for row in self.mainSheet_cost.index:
                self.mainSheet_accumulatedSupplyCostRank.at[row,col] = self.mainSheet_supplyCostRank.loc[:row,col].sum()
    
    def main_sheet_calc_10(self):
        self.mainSheet_costAvailable['No'] = list(range(1, self.mainSheet_cost.shape[0]+1))
        for col in  self.mainSheet_cost.columns[2:]:
            value = []
            for row in self.mainSheet_cost.index:
                row2 = 0 if row == 0 else row-1
                if self.mainSheet_accumulatedSupplyCostRank.loc[row,col] <= self.mainSheet_demand.loc[4,col]:
                    value.append(self.mainSheet_costCostRank.loc[row,col])
                elif self.mainSheet_accumulatedSupplyCostRank.loc[row2,col] <=self.mainSheet_demand.loc[4,col] and self.mainSheet_accumulatedSupplyCostRank.loc[row,col] >= self.mainSheet_demand.loc[4,col]:
                    value.append(self.mainSheet_costCostRank.loc[row,col])
                else:
                    value.append(0)
            self.mainSheet_costAvailable[col] = value
    
    def main_sheet_calc_11(self):
        self.mainSheet_CostMarginalTonne.at[0,'Name'] = 'Cost Marginal Tonne'
        for col in  self.mainSheet_cost.columns[2:]:
            self.mainSheet_CostMarginalTonne.at[0,col] = max(self.mainSheet_costAvailable[col])
    
#main sheet funtions
# def data_table(data_table_year):
#     print(' new year = ', data_table_year)
#     pdt_tab = PDT_Cost_Current(data_table_year)
#     pdt_tab.calc_all()
#     league_tab = League_Master(pdt_tab)
#     league_tab.calc_all()
#     print(league_tab.merged_league_master['Cost $/t'].values)
#     return league_tab.merged_league_master['Cost $/t']
    
    
        
        
    
    
        
        

# print(l.flocculent)
  
pdt_cost = PDT_Cost_Current(default_year)
pdt_cost.calc_all()

writer = pd.ExcelWriter('Outputs\\PDT Output.xlsx')
ref_PDT_writer = pd.ExcelWriter('Outputs\\Refineries PDT Output.xlsx')
prices_writer = pd.ExcelWriter('Outputs\\Prices Output.xlsx')
ref_cap_prod_writer = pd.ExcelWriter('Outputs\\Refineries Capacity and Production Output.xlsx')
# a_flat.single_year_mult_out
pdt_cost.Global.to_excel(writer, sheet_name='Global', encoding='utf-8', index=False)
pdt_cost.bauxite_domestic_supply.to_excel(writer, sheet_name='Bauxite Domestic supply', encoding='utf-8', index=False)
pdt_cost.bauxite_domestic_purchased.to_excel(writer, sheet_name='Bauxite Domestic purchased', encoding='utf-8', index=False)
pdt_cost.bauxite_import_purchased.to_excel(writer, sheet_name='Bauxite Import purchased', encoding='utf-8', index=False)
pdt_cost.bauxite_sourcing_mix.to_excel(writer, sheet_name='Bauxite Sourcing Mix', encoding='utf-8', index=False)
pdt_cost.bauxite_final_characteristics.to_excel(writer, sheet_name='Bauxite Final Characteristics', encoding='utf-8', index=False)
pdt_cost.caustic_self_supplied.to_excel(writer, sheet_name='Caustic Self supplied', encoding='utf-8', index=False)
pdt_cost.caustic_purchased.to_excel(writer, sheet_name='Caustic Purchased', encoding='utf-8', index=False)
pdt_cost.flocculent.to_excel(writer, sheet_name='Flocculent', encoding='utf-8', index=False)
pdt_cost.bauxite.to_excel(writer, sheet_name='Bauxite', encoding='utf-8', index=False)
pdt_cost.caustic.to_excel(writer, sheet_name='Caustic', encoding='utf-8', index=False)

pdt_cost.energy_for_steam.to_excel(writer, sheet_name='Energy for Steam Raising', encoding ='utf-8', index=False)
pdt_cost.energy_calcining_alumina.to_excel(writer, sheet_name='Energy for Calcining Alumina', encoding='utf-8', index=False)
pdt_cost.lime.to_excel(writer, sheet_name='Lime - Prices & Sourcing Mix', encoding='utf-8', index=False)
pdt_cost.limestone.to_excel(writer, sheet_name='Limestone-Prices & Sourcing Mix', encoding='utf-8', index=False)
pdt_cost.labour.to_excel(writer, sheet_name='Labour - Prices & Sourcing Mix', encoding='utf-8', index=False)
pdt_cost.energy_usageR.to_excel(writer, sheet_name='Energy Usage Rate', encoding='utf-8', index=False)
pdt_cost.lime_usageR.to_excel(writer, sheet_name='Lime - Usage rate',  encoding='utf-8', index=False)
pdt_cost.limestone_usageR.to_excel(writer, sheet_name='Limestone - Usage rate',  encoding='utf-8', index=False)
pdt_cost.labour_usageR.to_excel(writer, sheet_name='Labour - Usage rate', encoding='utf-8', index=False)
pdt_cost.consumables_usageR.to_excel(writer, sheet_name='Consumables - Usage Rates', encoding='utf-8', index=False)
pdt_cost.bauxite_cost.to_excel(writer, sheet_name='Bauxite costs', encoding='utf-8', index=False)
pdt_cost.caustic_cost.to_excel(writer, sheet_name='Caustic costs', encoding='utf-8', index=False)
pdt_cost.energy_cost.to_excel(writer, sheet_name='Energy costs', encoding='utf-8', index=False)
pdt_cost.lime_cost.to_excel(writer, sheet_name='Lime costs', encoding='utf-8', index=False)
pdt_cost.labour_cost.to_excel(writer, sheet_name='Labour costs', encoding='utf-8', index=False)
pdt_cost.consumables_cost.to_excel(writer, sheet_name='Consumables costs', encoding='utf-8', index=False)
pdt_cost.maintenance_cost.to_excel(writer, sheet_name='Maintenance costs', encoding='utf-8', index=False)
pdt_cost.other_cost.to_excel(writer, sheet_name='Other costs', encoding='utf-8', index=False)
pdt_cost.PDTsummmary.to_excel(writer, sheet_name='PDT Cost Current Summary', encoding='utf-8', index=False)
pdt_cost.final_cost_summary.to_excel(writer, sheet_name='Final Cost Summary', encoding='utf-8', index=False)
writer.save()

pdt_cost.refPDT1.to_excel(ref_PDT_writer, sheet_name='Refineries PDT 1',encoding='utf-8', index=False)
pdt_cost.refPDT2.to_excel(ref_PDT_writer, sheet_name='Refineries PDT 2',encoding='utf-8', index=False)
ref_PDT_writer.save() 

pdt_cost.Prices.to_excel(prices_writer, sheet_name='Prices outputs', encoding='utf-8', index=False)
prices_writer.save()

pdt_cost.Ref_base_cap_regions.to_excel(ref_cap_prod_writer, sheet_name='Base Capacity Splits by Region', encoding='utf-8', index=False)
pdt_cost.Ref_base_prod_regions.to_excel(ref_cap_prod_writer, sheet_name='Base Product Splits by Region', encoding='utf-8', index=False)
pdt_cost.Ref_total_available_cap.to_excel(ref_cap_prod_writer, sheet_name='Total Available Capacity', encoding='utf-8', index=False)
pdt_cost.Ref_available_cap.to_excel(ref_cap_prod_writer, sheet_name='Available Capacity for bauxite', encoding='utf-8', index=False)
pdt_cost.Ref_supply_stream.to_excel(ref_cap_prod_writer, sheet_name='Supply Stream', encoding='utf-8', index=False)
ref_cap_prod_writer.save()

league = League_Master(pdt_cost)
league.calc_all()
# print(league.Provincial)
name = "Outputs\\league_master_output.xlsx"
writer1 = pd.ExcelWriter(name)
league.china_alumni.to_excel(writer1, sheet_name="Provincial cost league table", encoding='utf-8', index=False)
league.Provincial.to_excel(writer1, sheet_name="League master Provincial", encoding='utf-8', index=False)
league.Regional.to_excel(writer1, sheet_name="League master Regional", encoding='utf-8', index=False)
league.top_right.to_excel(writer1, sheet_name="League master right Provincial", encoding='utf-8', index=False)
writer1.save()

main_sheet = Main_Sheet(pdt_cost, pdt_cost.mainSheet_demand, pdt_cost.mainSheet_key_inputs, pdt_cost.mainSheet_key_check)
main_sheet.calc_all()
writer2 = pd.ExcelWriter('Outputs\\Main Sheet Output.xlsx')
main_sheet.mainSheet_demand.to_excel(writer2, sheet_name='Demand', encoding='utf-8', index=False)
main_sheet.mainSheet_key_check.to_excel(writer2, sheet_name='Key Check', encoding='utf-8', index=False)
main_sheet.mainSheet_cost.to_excel(writer2, sheet_name='Cost',encoding='utf-8', index=False )

main_sheet.mainSheet_supplycost.to_excel(writer2, sheet_name='Supply Cost',encoding='utf-8', index=False )
main_sheet.mainSheet_supplystream.to_excel(writer2, sheet_name='Supply Stream',encoding='utf-8', index=False )
main_sheet.mainSheet_rankCosts.to_excel(writer2, sheet_name='Rank of Cost',encoding='utf-8', index=False )
main_sheet.mainSheet_producesCostRank.to_excel(writer2, sheet_name='Producers Cost Rank',encoding='utf-8', index=False )
main_sheet.mainSheet_costCostRank.to_excel(writer2, sheet_name='Cost Cost Rank',encoding='utf-8', index=False )
main_sheet.mainSheet_supplyCostRank.to_excel(writer2, sheet_name='Supply Cost Rank',encoding='utf-8', index=False )
main_sheet.mainSheet_accumulatedSupplyCostRank.to_excel(writer2, sheet_name='Accumulated Cost Rnk',encoding='utf-8', index=False )
main_sheet.mainSheet_costAvailable.to_excel(writer2, sheet_name='Cost Available',encoding='utf-8', index=False )
main_sheet.mainSheet_CostMarginalTonne.to_excel(writer2, sheet_name='Cost Marginal Tonne',encoding='utf-8', index=False )
main_sheet.mainSheet_CostMarginalTonne.to_excel(writer2, sheet_name='AA Price Marginal Tonne',encoding='utf-8', index=False )

writer2.save()

dblist = [
    a_flat.single_year_mult_out(pdt_cost.Global, "pdt cost Global"),
    a_flat.single_year_mult_out(pdt_cost.bauxite_domestic_supply, "pdt cost bauxite domestic supply"),
    a_flat.single_year_mult_out(pdt_cost.bauxite_domestic_purchased, "pdt cost bauxite domestic purchased"),
    a_flat.single_year_mult_out(pdt_cost.bauxite_import_purchased, "pdt cost bauxite import purchased"),
    a_flat.single_year_mult_out(pdt_cost.bauxite_sourcing_mix, "pdt cost bauxite sourcing mix"),
    a_flat.single_year_mult_out(pdt_cost.bauxite_final_characteristics, "pdt cost bauxite final characteristics"),
    a_flat.single_year_mult_out(pdt_cost.caustic_self_supplied, "pdt cost caustic self supplied"),
    a_flat.single_year_mult_out(pdt_cost.caustic_purchased, "pdt cost caustic purchased"),
    a_flat.single_year_mult_out(pdt_cost.flocculent, "pdt cost flocculent"),
    a_flat.single_year_mult_out(pdt_cost.bauxite, "pdt cost bauxite"),
    a_flat.single_year_mult_out(pdt_cost.caustic, "pdt cost caustic"),
    a_flat.single_year_mult_out(pdt_cost.energy_for_steam, "pdt cost energy for steam"),
    a_flat.single_year_mult_out(pdt_cost.energy_calcining_alumina, "pdt cost energy calcining alumina"),
    a_flat.single_year_mult_out(pdt_cost.lime, "pdt cost lime"),
    a_flat.single_year_mult_out(pdt_cost.limestone, "pdt cost limestone"),
    a_flat.single_year_mult_out(pdt_cost.labour, "pdt cost labour"),
    a_flat.single_year_mult_out(pdt_cost.energy_usageR, "pdt cost energy usageR"),
    a_flat.single_year_mult_out(pdt_cost.lime_usageR, "pdt cost lime usageR"),
    a_flat.single_year_mult_out(pdt_cost.limestone_usageR, "pdt cost limestone usageR"),
    a_flat.single_year_mult_out(pdt_cost.labour_usageR, "pdt_cost labour_usageR"),
    a_flat.single_year_mult_out(pdt_cost.consumables_usageR, "pdt cost consumables usageR"),
    a_flat.single_year_mult_out(pdt_cost.bauxite_cost, "pdt cost bauxite cost"),
    a_flat.single_year_mult_out(pdt_cost.caustic_cost, "pdt cost caustic cost"),
    a_flat.single_year_mult_out(pdt_cost.energy_cost, "pdt cost energy cost"),
    a_flat.single_year_mult_out(pdt_cost.lime_cost, "pdt cost lime cost"),
    a_flat.single_year_mult_out(pdt_cost.labour_cost, "pdt_cost labour cost"),
    a_flat.single_year_mult_out(pdt_cost.consumables_cost, "pdt cost consumables cost"),
    a_flat.single_year_mult_out(pdt_cost.maintenance_cost, "pdt cost maintenance cost"),
    a_flat.single_year_mult_out(pdt_cost.other_cost, "pdt cost other cost"),
    a_flat.single_year_mult_out(pdt_cost.PDTsummmary, "pdt cost PDT summmary"),
    a_flat.single_year_mult_out(pdt_cost.final_cost_summary, "pdt cost final cost summary"),
    a_flat.single_year_mult_out(pdt_cost.refPDT1, "pdt cost refPDT1"),
    a_flat.single_year_mult_out(pdt_cost.refPDT2, "pdt cost refPDT2"),
    a_flat.mult_year_single_output(pdt_cost.Prices, "pdt cost Prices"),
    a_flat.mult_year_single_output(pdt_cost.Ref_base_cap_regions, "pdt cost Ref base cap regions"),
    a_flat.mult_year_single_output(pdt_cost.Ref_base_prod_regions, "pdt cost Ref base prod regions"),
    a_flat.mult_year_single_output(pdt_cost.Ref_total_available_cap, "pdt cost Ref total available cap"),
    a_flat.mult_year_single_output(pdt_cost.Ref_available_cap, "pdt cost Ref available cap"),
    a_flat.mult_year_single_output(pdt_cost.Ref_supply_stream, "pdt cost Ref supply stream"),
    a_flat.single_year_mult_out(league.china_alumni, "Provincial cost league table"),
    a_flat.single_year_mult_out(league.Provincial, "League master Provincial"),
    a_flat.single_year_mult_out(league.Regional, "League master Regional"),
    a_flat.single_year_mult_out(league.top_right, "League master right Provincial"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_demand, "main sheet mainSheet demand"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_key_check, "main sheet mainSheet key check"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_cost, "main sheet mainSheet cost"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_supplycost, "main sheet supply cost"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_supplystream, "main sheet supply stream "),
    a_flat.mult_year_single_output(main_sheet.mainSheet_rankCosts, "main sheet rank costs"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_producesCostRank, "main sheet produces cost rank"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_costCostRank, "main sheet cost rank"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_supplyCostRank, "main sheet supply cost rank"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_accumulatedSupplyCostRank, "main sheet accumulated supply cost Rank"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_costAvailable, "main sheet cost available"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_CostMarginalTonne, "main sheet cost marginal tonnes"),
    a_flat.mult_year_single_output(main_sheet.mainSheet_CostMarginalTonne, "main sheet aa price marginal tonnes")
]


snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, a_flat.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)


# pdt = PDT_Cost_Current(2022)
# pdt.calc_all()
# lm = League_Master(pdt)
# lm.calc_all()
# print(lm.merged_league_master["Cost $/t"])