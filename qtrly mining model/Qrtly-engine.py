# -*- coding: utf-8 -*-
"""
Created on Sun Aug  2 15:28:56 2020

@author: JOHN
"""
import pandas as pd
from flatdb.flatdbconverter import Flatdbconverter
import time
from outputdb import uploadtodb

q_flat = Flatdbconverter("Quarterly Chart Pack")

override_store = {}
try:
    snaps = pd.read_csv('snapshot_output_data.csv')
    override_rows = snaps.loc[snaps['override_value'] == 1]
    # print(override_rows.values)
    for v in override_rows.values:
        override_store[f'{v[4]}_{v[5]}'] = v[6]
    print(override_store)
except FileNotFoundError:
    pass

class qrtlymodel():
    def __init__ (self):
        
        self.mainInput = pd.read_csv(r'inputs\\main qrtly input.csv',  encoding='ISO-8859-1')
        
        self.diesel_invat = pd.read_excel('inputs\\Diesel Prices and Rebates.xlsx', sheet_name='Prices Including VAT')
        self.diesel_exvat = pd.read_excel('inputs\\Diesel Prices and Rebates.xlsx', sheet_name='Prices excluding VAT')
        self.diesel_rebates = pd.read_excel('inputs\\Diesel Prices and Rebates.xlsx', sheet_name='Price Rebate')
        
        self.gasoline_invat = pd.read_excel('inputs\\Gasoline Prices and Rebates.xlsx', sheet_name='Prices Including VAT')
        self.gasoline_exvat = pd.read_excel('inputs\\Gasoline Prices and Rebates.xlsx', sheet_name='Prices excluding VAT')
        self.gasoline_rebates = pd.read_excel('inputs\\Gasoline Prices and Rebates.xlsx', sheet_name='Price Rebate')
        
        self.lpg_invat = pd.read_excel('inputs\\LPG Prices and Rebates.xlsx', sheet_name='Prices Including VAT')
        self.lpg_exvat = pd.read_excel('inputs\\LPG Prices and Rebates.xlsx', sheet_name='Prices excluding VAT')
        self.lpg_rebates = pd.read_excel('inputs\\LPG Prices and Rebates.xlsx', sheet_name='Price Rebate')
        
        self.electricity_invat = pd.read_excel('inputs\\Electricity Prices and Rebates.xlsx', sheet_name='Prices Including VAT')
        self.electricity_exvat = pd.read_excel('inputs\\Electricity Prices and Rebates.xlsx', sheet_name='Prices excluding VAT')
        self.electricity_rebates = pd.read_excel('inputs\\Electricity Prices and Rebates.xlsx', sheet_name='Price Rebate')
        
        self.explosives_invat = pd.read_excel('inputs\\Explosives Prices and Rebates.xlsx', sheet_name='Prices Including VAT')
        self.explosives_exvat = pd.read_excel('inputs\\Explosives Prices and Rebates.xlsx', sheet_name='Prices excluding VAT')
        self.explosives_rebates = pd.read_excel('inputs\\Explosives Prices and Rebates.xlsx', sheet_name='Price Rebate')
        
        self.water_invat = pd.read_excel('inputs\\Water Prices and Rebates.xlsx', sheet_name='Prices Including VAT')
        self.water_exvat = pd.read_excel('inputs\\Water Prices and Rebates.xlsx', sheet_name='Prices excluding VAT')
        self.water_rebates = pd.read_excel('inputs\\Water Prices and Rebates.xlsx', sheet_name='Price Rebate')
        
        self.labour_invat = pd.read_excel('inputs\\Labour Prices and Rebates.xlsx', sheet_name='Prices Including VAT')
        self.labour_exvat = pd.read_excel('inputs\\Labour Prices and Rebates.xlsx', sheet_name='Prices excluding VAT')
        self.labour_rebates = pd.read_excel('inputs\\Labour Prices and Rebates.xlsx', sheet_name='Price Rebate')
        
        self.currency = pd.read_csv(r'inputs\\Currency_n_FX.csv',  encoding='ISO-8859-1')
        self.vat = pd.read_csv(r'inputs\\VAT_Rates.csv',  encoding='ISO-8859-1')
        
        self.diesel_price_rebates = pd.DataFrame(columns=[], index=self.diesel_invat.index)
        self.diesel_price_vat = pd.DataFrame(columns=[], index=self.diesel_invat.index)
        self.diesel_price_exVAT = pd.DataFrame(columns=[], index=self.diesel_invat.index)
        
        self.gasoline_price_rebates = pd.DataFrame(columns=[], index=self.gasoline_invat.index)
        self.gasoline_price_vat = pd.DataFrame(columns=[], index=self.gasoline_invat.index)
        self.gasoline_price_exVAT = pd.DataFrame(columns=[], index=self.gasoline_invat.index)
        
        self.lpg_price_rebates = pd.DataFrame(columns=[], index=self.lpg_invat.index)
        self.lpg_price_vat = pd.DataFrame(columns=[], index=self.lpg_invat.index)
        self.lpg_price_exVAT = pd.DataFrame(columns=[], index=self.lpg_invat.index)
        
        self.electricity_price_rebates = pd.DataFrame(columns=[], index=self.electricity_invat.index)
        self.electricity_price_vat = pd.DataFrame(columns=[], index=self.electricity_invat.index)
        self.electricity_price_exVAT = pd.DataFrame(columns=[], index=self.electricity_invat.index)
        
        self.explosives_price_rebates = pd.DataFrame(columns=[], index=self.explosives_invat.index)
        self.explosives_price_vat = pd.DataFrame(columns=[], index=self.explosives_invat.index)
        self.explosives_price_exVAT = pd.DataFrame(columns=[], index=self.explosives_invat.index)
        
        self.water_price_rebates = pd.DataFrame(columns=[], index=self.water_invat.index)
        self.water_price_vat = pd.DataFrame(columns=[], index=self.water_invat.index)
        self.water_price_exVAT = pd.DataFrame(columns=[], index=self.water_invat.index)
        
        self.labour_price_rebates = pd.DataFrame(columns=[], index=self.labour_invat.index)
        self.labour_price_vat = pd.DataFrame(columns=[], index=self.labour_invat.index)
        self.labour_price_exVAT = pd.DataFrame(columns=[], index=self.labour_invat.index)
        
   
        self.main_tunning_values = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_pricesEnergyWater = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_directCharges = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_labour = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_labour_summary  = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_processing = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.labour2_processing =  pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_transport =  pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_other_direct_charges=  pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_taxes_loyalties =  pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_maintenance =  pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_site_Overheads =  pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_corporate_charge =  pd.DataFrame(columns=[], index=self.mainInput.index)
        
        self.main_diesel = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_gasoline = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_lpg =pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_electricity =pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_explosives =pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_special = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_water =pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_labour_summary = pd.DataFrame(columns=[], index=self.mainInput.index)
        self.main_raw_cost =pd.DataFrame(columns=[], index=self.mainInput.index)
        
        
        self.year = '2019_Q4'
    
    def diesel_calcs(self):
        table = self.diesel_price_rebates.copy()
        table['Price Denomination per liter'] = self.diesel_invat['Denomination per litre']
        for col in self.diesel_invat.columns[2:]:
            for row in self.diesel_invat.index:
                res =0
                if pd.isna(self.diesel_rebates.loc[row,'2017_Q1']):
                    res = 0
                else:
                    res = self.priceRebates_func(col, table.loc[row,'Price Denomination per liter'],  self.diesel_rebates.loc[row,'Rebate Denomination per litre'], self.diesel_rebates.loc[row,'2017_Q1'])
                table.at[row,col] = res
        self.diesel_price_rebates = table   
        
        self.diesel_price_vat['Line'] = self.diesel_invat['Line']
        self.diesel_price_vat['Units'] = '#'
        for col in self.diesel_invat.columns[2:]:
            for row in self.diesel_invat.index:
                res = 0
                if pd.isna(self.diesel_price_vat.loc[row,'Line']):
                    res =0
                else:
                    res = float(self.vat.loc[self.vat['Line']==self.diesel_price_vat.loc[row,'Line'], col])
                self.diesel_price_vat.at[row,col] = res
        
        self.diesel_price_exVAT['Country Province'] = self.diesel_invat['Line']
        self.diesel_price_exVAT['Denomination per litre'] = self.diesel_invat['Denomination per litre']
        for col in self.diesel_invat.columns[2:]:
            for row in self.diesel_invat.index:
                ress = 0
                if pd.isna(self.diesel_invat.loc[row,col]) and pd.isna(self.diesel_exvat.loc[row, col]):
                    ress = 'no data'
                else:
                    if pd.isna(self.diesel_exvat.loc[row,col]):
                        ress = float(self.diesel_invat.loc[row,col])/(1+self.diesel_price_vat.loc[row,col])-float(self.diesel_price_rebates.loc[row, col ])
                    else:
                        ress = float(self.diesel_exvat.loc[row,col])-float(self.diesel_price_rebates.loc[row, col ])
                self.diesel_price_exVAT.loc[row,col] = ress
                
    def gasoline_calcs(self):
        table = self.gasoline_price_rebates.copy()
        table['Price Denomination per liter'] = self.gasoline_invat['Denomination per litre']
        for col in self.gasoline_invat.columns[2:]:
            for row in self.gasoline_invat.index:
                res =0
                if pd.isna(self.gasoline_rebates.loc[row,'2017_Q1']):
                    res = 0
                else:
                    res = self.priceRebates_func(col, table.loc[row,'Price Denomination per liter'],  self.gasoline_rebates.loc[row,'Rebate Denomination per litre'], self.gasoline_rebates.loc[row,'2017_Q1'])
                table.at[row,col] = res
        self.gasoline_price_rebates = table   
        
        self.gasoline_price_vat['Line'] = self.gasoline_invat['Line']
        self.gasoline_price_vat['Units'] = '#'
        for col in self.gasoline_invat.columns[2:]:
            for row in self.gasoline_invat.index:
                res = 0
                if pd.isna(self.gasoline_price_vat.loc[row,'Line']):
                    res =0
                else:
                    res = float(self.vat.loc[self.vat['Line']==self.gasoline_price_vat.loc[row,'Line'], col])
                self.gasoline_price_vat.at[row,col] = res
        
        self.gasoline_price_exVAT['Country Province'] = self.gasoline_invat['Line']
        self.gasoline_price_exVAT['Denomination per litre'] = self.gasoline_invat['Denomination per litre']
        for col in self.gasoline_invat.columns[2:]:
            for row in self.gasoline_invat.index:
                ress = 0
                if pd.isna(self.gasoline_invat.loc[row,col]) and pd.isna(self.gasoline_exvat.loc[row, col]):
                    ress = 'no data'
                else:
                    if pd.isna(self.gasoline_exvat.loc[row,col]):
                        ress = float(self.gasoline_invat.loc[row,col])/(1+self.gasoline_price_vat.loc[row,col])-float(self.gasoline_price_rebates.loc[row, col ])
                    else:
                        ress = float(self.gasoline_exvat.loc[row,col])-float(self.gasoline_price_rebates.loc[row, col ])
                self.gasoline_price_exVAT.loc[row,col] = ress
        
    
    def lpg_calcs(self):
        table = self.lpg_price_rebates.copy()
        table['Price Denomination per liter'] = self.lpg_invat['Denomination per litre']
        for col in self.lpg_invat.columns[2:]:
            for row in self.lpg_invat.index:
                res =0
                if pd.isna(self.lpg_rebates.loc[row,'2017_Q1']):
                    res = 0
                else:
                    res = self.priceRebates_func(col, table.loc[row,'Price Denomination per liter'],  self.lpg_rebates.loc[row,'Rebate Denomination per litre'], self.lpg_rebates.loc[row,'2017_Q1'])
                table.at[row,col] = res
        self.lpg_price_rebates = table   
        
        self.lpg_price_vat['Line'] = self.diesel_invat['Line']
        self.lpg_price_vat['Units'] = '#'
        for col in self.lpg_invat.columns[2:]:
            for row in self.lpg_invat.index:
                res = 0
                if pd.isna(self.lpg_price_vat.loc[row,'Line']):
                    res =0
                else:
                    res = float(self.vat.loc[self.vat['Line']==self.lpg_price_vat.loc[row,'Line'], col])
                self.lpg_price_vat.at[row,col] = res
        
        self.lpg_price_exVAT['Country Province'] = self.lpg_invat['Line']
        self.lpg_price_exVAT['Denomination per litre'] = self.lpg_invat['Denomination per litre']
        for col in self.lpg_invat.columns[2:]:
            for row in self.lpg_invat.index:
                ress = 0
                if pd.isna(self.lpg_invat.loc[row,col]) and pd.isna(self.lpg_exvat.loc[row, col]):
                    ress = 'no data'
                else:
                    if pd.isna(self.lpg_exvat.loc[row,col]):
                        ress = float(self.lpg_invat.loc[row,col])/(1+self.lpg_price_vat.loc[row,col])-float(self.lpg_price_rebates.loc[row, col ])
                    else:
                        ress = float(self.lpg_exvat.loc[row,col])-float(self.lpg_price_rebates.loc[row, col ])
                self.lpg_price_exVAT.loc[row,col] = ress
        
        
        
    def electricity_calcs(self):
        table = self.electricity_price_rebates.copy()
        table['Price Denomination per liter'] = self.electricity_invat['Denomination per litre']
        for col in self.electricity_invat.columns[2:]:
            for row in self.electricity_invat.index:
                res =0
                if pd.isna(self.electricity_rebates.loc[row,'2017_Q1']):
                    res = 0
                else:
                    res = self.priceRebates_func(col, table.loc[row,'Price Denomination per liter'],  self.electricity_rebates.loc[row,'Rebate Denomination per litre'], self.electricity_rebates.loc[row,'2017_Q1'])
                table.at[row,col] = res
        self.electricity_price_rebates = table   
        
        self.electricity_price_vat['Line'] = self.electricity_invat['Line']
        self.electricity_price_vat['Units'] = '#'
        for col in self.electricity_invat.columns[2:]:
            for row in self.electricity_invat.index:
                res = 0
                if pd.isna(self.electricity_price_vat.loc[row,'Line']):
                    res =0
                else:
                    res = float(self.vat.loc[self.vat['Line']==self.electricity_price_vat.loc[row,'Line'], col])
                self.electricity_price_vat.at[row,col] = res
        
        self.electricity_price_exVAT['Country Province'] = self.electricity_invat['Line']
        self.electricity_price_exVAT['Denomination per litre'] = self.electricity_invat['Denomination per litre']
        for col in self.electricity_invat.columns[2:]:
            for row in self.electricity_invat.index:
                ress = 0
                if pd.isna(self.electricity_invat.loc[row,col]) and pd.isna(self.electricity_exvat.loc[row, col]):
                    ress = 'no data'
                else:
                    if pd.isna(self.electricity_exvat.loc[row,col]):
                        ress = float(self.electricity_invat.loc[row,col])/(1+self.electricity_price_vat.loc[row,col])-float(self.electricity_price_rebates.loc[row, col ])
                    else:
                        ress = float(self.electricity_exvat.loc[row,col])-float(self.electricity_price_rebates.loc[row, col ])
                self.electricity_price_exVAT.loc[row,col] = ress
        
        
    def explosives_calcs(self):
        table = self.explosives_price_rebates.copy()
        table['Price Denomination per liter'] = self.explosives_invat['Denomination per litre']
        for col in self.explosives_invat.columns[2:]:
            for row in self.explosives_invat.index:
                res =0
                if pd.isna(self.explosives_rebates.loc[row,'2017_Q1']):
                    res = 0
                else:
                    res = self.priceRebates_func(col, table.loc[row,'Price Denomination per liter'],  self.explosives_rebates.loc[row,'Rebate Denomination per litre'], self.explosives_rebates.loc[row,'2017_Q1'])
                table.at[row,col] = res
        self.explosives_price_rebates = table   
        
        self.explosives_price_vat['Line'] = self.explosives_invat['Line']
        self.explosives_price_vat['Units'] = '#'
        for col in self.explosives_invat.columns[2:]:
            for row in self.explosives_invat.index:
                res = 0
                if pd.isna(self.explosives_price_vat.loc[row,'Line']):
                    res =0
                else:
                    res = float(self.vat.loc[self.vat['Line']==self.explosives_price_vat.loc[row,'Line'], col])
                self.explosives_price_vat.at[row,col] = res
        
        self.explosives_price_exVAT['Country Province'] = self.explosives_invat['Line']
        self.explosives_price_exVAT['Denomination per litre'] = self.explosives_invat['Denomination per litre']
        for col in self.explosives_invat.columns[2:]:
            for row in self.explosives_invat.index:
                ress = 0
                if pd.isna(self.explosives_invat.loc[row,col]) and pd.isna(self.explosives_exvat.loc[row, col]):
                    ress = 'no data'
                else:
                    if pd.isna(self.explosives_exvat.loc[row,col]):
                        ress = float(self.explosives_invat.loc[row,col])/(1+self.explosives_price_vat.loc[row,col])-float(self.explosives_price_rebates.loc[row, col ])
                    else:
                        ress = float(self.explosives_exvat.loc[row,col])-float(self.explosives_price_rebates.loc[row, col ])
                self.explosives_price_exVAT.loc[row,col] = ress
        
    def water_calcs(self):
        table = self.water_price_rebates.copy()
        table['Price Denomination per liter'] = self.water_invat['Denomination per litre']
        for col in self.water_invat.columns[2:]:
            for row in self.water_invat.index:
                res =0
                if pd.isna(self.water_rebates.loc[row,'2017_Q1']):
                    res = 0
                else:
                    res = self.priceRebates_func(col, table.loc[row,'Price Denomination per liter'],  self.water_rebates.loc[row,'Rebate Denomination per litre'], self.water_rebates.loc[row,'2017_Q1'])
                table.at[row,col] = res
        self.water_price_rebates = table   
        
        self.water_price_vat['Line'] = self.water_invat['Line']
        self.water_price_vat['Units'] = '#'
        for col in self.water_invat.columns[2:]:
            for row in self.water_invat.index:
                res = 0
                if pd.isna(self.water_price_vat.loc[row,'Line']):
                    res =0
                else:
                    res = float(self.vat.loc[self.vat['Line']==self.water_price_vat.loc[row,'Line'], col])
                self.water_price_vat.at[row,col] = res
        
        self.water_price_exVAT['Country Province'] = self.water_invat['Line']
        self.water_price_exVAT['Denomination per litre'] = self.water_invat['Denomination per litre']
        for col in self.water_invat.columns[2:]:
            for row in self.water_invat.index:
                ress = 0
                if pd.isna(self.water_invat.loc[row,col]) and pd.isna(self.water_exvat.loc[row, col]):
                    ress = 'no data'
                else:
                    if pd.isna(self.water_exvat.loc[row,col]):
                        ress = float(self.water_invat.loc[row,col])/(1+self.water_price_vat.loc[row,col])-float(self.water_price_rebates.loc[row, col ])
                    else:
                        ress = float(self.water_exvat.loc[row,col])-float(self.water_price_rebates.loc[row, col ])
                self.water_price_exVAT.loc[row,col] = ress
        
        
    def labour_calcs(self):
        table = self.labour_price_rebates.copy()
        table['Price Denomination per liter'] = self.labour_invat['Denomination per litre']
        for col in self.labour_invat.columns[2:]:
            for row in self.labour_invat.index:
                res =0
                if pd.isna(self.labour_rebates.loc[row,'2017_Q1']):
                    res = 0
                else:
                    res = self.priceRebates_func(col, table.loc[row,'Price Denomination per liter'],  self.labour_rebates.loc[row,'Rebate Denomination per litre'], self.labour_rebates.loc[row,'2017_Q1'])
                table.at[row,col] = res
        self.labour_price_rebates = table   
        
        self.labour_price_vat['Line'] = self.labour_invat['Line']
        self.labour_price_vat['Units'] = '#'
        for col in self.labour_invat.columns[2:]:
            for row in self.labour_invat.index:
                res = 0
                if pd.isna(self.labour_price_vat.loc[row,'Line']):
                    res =0
                else:
                    try:
                        res = float(self.vat.loc[self.vat['Line']==self.labour_price_vat.loc[row,'Line'], col])
                    except:
                        res = 0
                self.labour_price_vat.at[row,col] = res
        
        self.labour_price_exVAT['Country Province'] = self.labour_invat['Line']
        self.labour_price_exVAT['Denomination per litre'] = self.labour_invat['Denomination per litre']
        for col in self.labour_invat.columns[2:]:
            for row in self.labour_invat.index:
                ress = 0
                if pd.isna(self.labour_invat.loc[row,col]) and pd.isna(self.labour_exvat.loc[row, col]):
                    ress = 'no data'
                else:
                    if pd.isna(self.labour_exvat.loc[row,col]):
                        ress = float(self.labour_invat.loc[row,col])/(1+self.labour_price_vat.loc[row,col])-float(self.labour_price_rebates.loc[row, col ])
                    else:
                        ress = float(self.labour_exvat.loc[row,col])-float(self.labour_price_rebates.loc[row, col ])
                self.labour_price_exVAT.loc[row,col] = ress
        
        
    def main_calc_1(self):
        self.main_tunning_values['Mine'] = self.mainInput['Mine']
        self.main_tunning_values["Total FTE's"] = self.mainInput["Total FTE's"]
        self.main_tunning_values['Total Production'] =self.mainInput['Total Production']
        self.main_tunning_values['Labour Efficiency'] = self.main_tunning_values['Total Production']/self.main_tunning_values["Total FTE's"]
        self.main_tunning_values['Labour utilisation'] =self.main_tunning_values["Total FTE's"]/self.main_tunning_values['Total Production']
        
        self.main_tunning_values['Labour numbers'] = ''
        for col in self.mainInput.columns[7:13]:
            self.main_tunning_values[col] = self.mainInput[col] * self.main_tunning_values["Total FTE's"]
        self.main_tunning_values['Total'] = self.main_tunning_values[['Mining', 'Processing', 'Transport', 'Maintenance', 'Administration']].sum(axis=1)
    
    def main_calc_2(self):
        self.main_pricesEnergyWater['Mine']= self.mainInput['Mine']
        self.main_pricesEnergyWater['Price Line for Diesel'] = self.mainInput['Diesel Price Line']
        d = {self.diesel_invat['Line'][x]:self.diesel_invat['Denomination per litre'][x] for x in self.diesel_invat.index}
        self.main_pricesEnergyWater['Diesel - Denomination'] = [d[self.main_pricesEnergyWater.loc[i,'Price Line for Diesel']] for i in self.mainInput.index]
        l1 = {self.diesel_price_exVAT['Country Province'][x]:self.diesel_price_exVAT[self.year][x] for x in self.diesel_price_exVAT.index}
        self.main_pricesEnergyWater['Diesel - List price'] =[l1[self.main_pricesEnergyWater.loc[i,'Price Line for Diesel']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Diesel - Customised price'] =self.mainInput['Diesel Custom Price']
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_pricesEnergyWater['Diesel - Fx rate'] =[ l2[self.main_pricesEnergyWater.loc[row,'Diesel - Denomination'] ] for row in self.mainInput.index ]
        for row in  self.mainInput.index :
            res = 0
            if pd.isna(self.main_pricesEnergyWater.loc[row,'Diesel - Customised price'] ):
                res = self.main_pricesEnergyWater.loc[row, 'Diesel - List price']
            else:
                res = self.main_pricesEnergyWater.loc[row,'Diesel - Customised price']
            res = res /float(self.main_pricesEnergyWater.loc[row, 'Diesel - Fx rate'])
            self.main_pricesEnergyWater.at[row, 'Diesel - Final Price'] = res
        
        l1 = {self.gasoline_price_exVAT['Country Province'][x]:self.gasoline_price_exVAT[self.year][x] for x in self.gasoline_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_pricesEnergyWater['Price Line for Gasoline'] = self.main_pricesEnergyWater['Price Line for Diesel']
        self.main_pricesEnergyWater['Gasoline - Denomination'] =[d[self.main_pricesEnergyWater.loc[i,'Price Line for Gasoline']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Gasoline - List price'] = [l1[self.main_pricesEnergyWater.loc[i,'Price Line for Gasoline']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Gasoline - Customised price'] =self.mainInput['Gasoline Customised Price']
        self.main_pricesEnergyWater['Gasoline - Fx rate'] = [l2[self.main_pricesEnergyWater.loc[i,'Gasoline - Denomination']] for i in self.mainInput.index]
        for row in  self.mainInput.index :
            res = 0
            if pd.isna(self.main_pricesEnergyWater.loc[row,'Gasoline - Customised price'] ):
                res = self.main_pricesEnergyWater.loc[row, 'Gasoline - List price']
            else:
                res = self.main_pricesEnergyWater.loc[row,'Gasoline - Customised price']
            res = res /float(self.main_pricesEnergyWater.loc[row, 'Gasoline - Fx rate'])
            self.main_pricesEnergyWater.at[row, 'Gasoline - Final Price'] = res
        
        
        l1 = {self.lpg_price_exVAT['Country Province'][x]:self.lpg_price_exVAT[self.year][x] for x in self.lpg_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_pricesEnergyWater['Price Line for LPG'] = self.main_pricesEnergyWater['Price Line for Diesel']
        self.main_pricesEnergyWater['LPG - Denomination'] =[d[self.main_pricesEnergyWater.loc[i,'Price Line for LPG']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['LPG  - List price'] = [l1[self.main_pricesEnergyWater.loc[i,'Price Line for LPG']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['LPG  - Customised price'] =self.mainInput['LPG Gas Customised Price']
        self.main_pricesEnergyWater['LPG  - Fx rate'] =[l2[self.main_pricesEnergyWater.loc[i,'LPG - Denomination']] for i in self.mainInput.index]
        for row in  self.mainInput.index :
            res = 0
            if pd.isna(self.main_pricesEnergyWater.loc[row,'LPG  - Customised price'] ):
                res = self.main_pricesEnergyWater.loc[row, 'LPG  - List price']
            else:
                res = self.main_pricesEnergyWater.loc[row,'LPG  - Customised price']
            res = res /float(self.main_pricesEnergyWater.loc[row, 'LPG  - Fx rate'])
            self.main_pricesEnergyWater.at[row, 'LPG - Final Price'] = res
        
        l1 = {self.electricity_price_exVAT['Country Province'][x]:self.electricity_price_exVAT[self.year][x] for x in self.electricity_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_pricesEnergyWater['Price Line for Electricity'] = self.main_pricesEnergyWater['Price Line for Diesel']
        self.main_pricesEnergyWater['Electricity - Denomination'] =[d[self.main_pricesEnergyWater.loc[i,'Price Line for Electricity']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Electricity - List price'] = [l1[self.main_pricesEnergyWater.loc[i,'Price Line for Electricity']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Electricity - Customised price'] =self.mainInput['Electricity Customised Price']
        self.main_pricesEnergyWater['Electricity - Fx rate'] =[l2[self.main_pricesEnergyWater.loc[i,'Electricity - Denomination']] for i in self.mainInput.index]
        for row in  self.mainInput.index :
            res = 0
            if pd.isna(self.main_pricesEnergyWater.loc[row,'Electricity - Customised price'] ):
                res = self.main_pricesEnergyWater.loc[row, 'Electricity - List price']
            else:
                res = self.main_pricesEnergyWater.loc[row,'Electricity - Customised price']
            res = res /float(self.main_pricesEnergyWater.loc[row, 'Electricity - Fx rate'])
            self.main_pricesEnergyWater.at[row, 'Electricity - Final Price'] = res
        
        l1 = {self.explosives_price_exVAT['Country Province'][x]:self.explosives_price_exVAT[self.year][x] for x in self.explosives_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_pricesEnergyWater['Price Line for Explosives'] = self.main_pricesEnergyWater['Price Line for Diesel']
        self.main_pricesEnergyWater['Explosives - Denomination'] =[d[self.main_pricesEnergyWater.loc[i,'Price Line for Explosives']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Explosives - List price'] = [l1[self.main_pricesEnergyWater.loc[i,'Price Line for Explosives']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Explosives - Customised price'] =self.mainInput['Explosives Customised Price']
        self.main_pricesEnergyWater['Explosives - Fx rate'] =[l2[self.main_pricesEnergyWater.loc[i,'Explosives - Denomination']] for i in self.mainInput.index]
        for row in  self.mainInput.index :
            res = 0
            if pd.isna(self.main_pricesEnergyWater.loc[row,'Explosives - Customised price'] ):
                res = self.main_pricesEnergyWater.loc[row, 'Explosives - List price']
            else:
                res = self.main_pricesEnergyWater.loc[row,'Explosives - Customised price']
            res = res /float(self.main_pricesEnergyWater.loc[row, 'Explosives - Fx rate'])
            self.main_pricesEnergyWater.at[row, 'Explosives - Final Price'] = res
            
        l1 = {self.water_price_exVAT['Country Province'][x]:self.water_price_exVAT[self.year][x] for x in self.water_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_pricesEnergyWater['Price Line for Water'] = self.main_pricesEnergyWater['Price Line for Diesel']
        self.main_pricesEnergyWater['Water - Denomination'] =[d[self.main_pricesEnergyWater.loc[i,'Price Line for Water']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Water - List price'] = [l1[self.main_pricesEnergyWater.loc[i,'Price Line for Water']] for i in self.mainInput.index]
        self.main_pricesEnergyWater['Water - Customised price'] =self.mainInput['New Water Customised Price']
        self.main_pricesEnergyWater['Water - Fx rate'] =[l2[self.main_pricesEnergyWater.loc[i,'Water - Denomination']] for i in self.mainInput.index]
        for row in  self.mainInput.index :
            res = 0
            if pd.isna(self.main_pricesEnergyWater.loc[row,'Water - Customised price'] ):
                res = self.main_pricesEnergyWater.loc[row, 'Water - List price']
            else:
                res = self.main_pricesEnergyWater.loc[row,'Water - Customised price']
            res = res /float(self.main_pricesEnergyWater.loc[row, 'Water - Fx rate'])
            self.main_pricesEnergyWater.at[row, 'Water - Final Price'] = res
            
    def main_calc_3(self):
        self.main_directCharges['Mine'] =  self.mainInput['Mine']
        self.main_directCharges['Diesel'] = self.main_pricesEnergyWater['Diesel - Final Price']
        self.main_directCharges['Gasoline'] = self.main_pricesEnergyWater['Gasoline - Final Price']
        self.main_directCharges['LPG Gas'] =self.main_pricesEnergyWater['LPG - Final Price']
        self.main_directCharges['Electricity'] =self.main_pricesEnergyWater['Electricity - Final Price']
        self.main_directCharges['Explosives'] =self.main_pricesEnergyWater['Explosives - Final Price']
        self.main_directCharges['Special'] = self.mainInput['Special Price']
        
        val1 = self.main_directCharges['Diesel']*self.mainInput['Energy -Usage Rates - Diesel ']
        val2 = self.main_directCharges['Gasoline']*self.mainInput['Energy -Usage Rates - Gasoline']
        val3 = self.main_directCharges['LPG Gas']*self.mainInput['Energy -Usage Rates - LPG']
        val4 = self.main_directCharges['Electricity']*self.mainInput['Energy -Usage Rates - Electricity']
        val5 = self.main_directCharges['Explosives']*self.mainInput['Energy -Usage Rates - Explosives']
        val6 = self.main_directCharges['Special']*self.mainInput[ 'Energy -Usage Rates - Special']
       
        self.main_directCharges['Cost'] = val1+val2+val3+val4+val5+val6
        
    def main_calc_4(self):
        self.main_labour['Mine'] = self.mainInput['Mine']
        self.main_labour['Portion by Price Line 1'] = self.mainInput['Labour - Usage Rates - Portion by Price Line 1']
        self.main_labour['Price Line 1'] =self.mainInput['Labour - Usage Rates - Price Line 1']
        d = {self.labour_invat['Line'][x]:self.labour_invat['Denomination per litre'][x] for x in self.labour_invat.index}
        self.main_labour['Denomination of Line 1'] = [d[self.main_labour['Price Line 1'][x]] for x in self.mainInput.index]
        self.main_labour['Labour use of Line 1'] = self.main_labour['Portion by Price Line 1'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Mining']
        
        self.main_labour['Portion by Price Line 2'] =1 - self.main_labour['Portion by Price Line 1'] 
        self.main_labour['Price Line 2']=self.mainInput['Price Line 2']
        d = {self.labour_invat['Line'][x]:self.labour_invat['Denomination per litre'][x] for x in self.labour_invat.index}
        self.main_labour['Denomination of Line 2'] = [d[self.main_labour['Price Line 2'][x]] for x in self.mainInput.index]
        self.main_labour['Labour use of Line 2'] = self.main_labour['Portion by Price Line 2']* self.main_tunning_values['Labour utilisation']*self.mainInput['Mining']
        
        
        l1 = {self.labour_price_exVAT['Country Province'][x]:self.labour_price_exVAT[self.year][x] for x in self.labour_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        
        self.main_labour['List Price of Line 1'] = [l1[self.main_labour['Price Line 1'][x]] for x in self.mainInput.index]
        self.main_labour['Customised Price of Line 1'] = self.mainInput['Prices - Labour Price by Price Line 1 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_labour.loc[row,'Customised Price of Line 1']):
                res = self.main_labour.loc[row,'List Price of Line 1']
            else:
                res = self.main_labour.loc[row,'Customised Price of Line 1']
            self.main_labour.at[row,'Final Price of Line 1']=res
        self.main_labour['FX rate of Line 1'] =[l2[self.main_labour['Denomination of Line 1'][x]] for x in self.mainInput.index]
        self.main_labour['Final Price 1'] = self.main_labour['Final Price of Line 1'].astype(float)/self.main_labour['FX rate of Line 1'].astype(float)
        
        self.main_labour['List Price of Line 2'] = [l1[self.main_labour['Price Line 2'][x]] for x in self.mainInput.index]
        self.main_labour['Customised Price of Line 2'] = self.mainInput['Prices - Labour Price by Price Line 2 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_labour.loc[row,'Customised Price of Line 2']):
                res = self.main_labour.loc[row,'List Price of Line 2']
            else:
                res = self.main_labour.loc[row,'Customised Price of Line 2']
            self.main_labour.at[row,'Final Price of Line 2']=res
        self.main_labour['FX rate of Line 2'] =[l2[self.main_labour['Denomination of Line 2'][x]] for x in self.mainInput.index]
        self.main_labour['Final Price 2'] = self.main_labour['Final Price of Line 2'].astype(float)/self.main_labour['FX rate of Line 2'].astype(float)
        
        self.main_labour['Cost'] = (self.main_labour['Labour use of Line 1']*self.main_labour['Final Price 1'])+(self.main_labour['Labour use of Line 2']*self.main_labour['Final Price 2'])
    
    def main_calc_5(self):
        self.main_labour_summary['Mine'] = self.mainInput['Mine']
        self.main_labour_summary['Water use'] = self.mainInput['Water - Water use']
        self.main_labour_summary['Price'] = self.main_pricesEnergyWater['Water - Final Price']
        self.main_labour_summary['Cost'] = self.main_labour_summary['Water use'].astype(float)*self.main_labour_summary['Price']
        self.main_labour_summary['Mining Costs'] =self.main_labour_summary['Cost']+self.main_labour['Cost']+self.main_directCharges['Cost']
        
    def main_calc_6(self):
        self.main_processing['Mine'] =  self.mainInput['Mine']
        self.main_processing['Diesel'] = self.main_pricesEnergyWater['Diesel - Final Price']
        self.main_processing['Gasoline'] = self.main_pricesEnergyWater['Gasoline - Final Price']
        self.main_processing['LPG Gas'] =self.main_pricesEnergyWater['LPG - Final Price']
        self.main_processing['Electricity'] =self.main_pricesEnergyWater['Electricity - Final Price']
        self.main_processing['Explosives'] =self.main_pricesEnergyWater['Explosives - Final Price']
        self.main_processing['Special'] = self.mainInput['Special Price']
        
        val1 = self.main_processing['Diesel']*self.mainInput['Processing - Energy - Usage rates - Diesel']
        val2 = self.main_processing['Gasoline']*self.mainInput['Processing - Energy - Usage rates - Gasoline']
        val3 = self.main_processing['LPG Gas']*self.mainInput['Processing - Energy - Usage rates - LPG']
        val4 = self.main_processing['Electricity']*self.mainInput[ 'Processing - Energy - Usage rates - Electricity']
        val5 = self.main_processing['Explosives']*self.mainInput['Processing - Energy - Usage rates - Explosives']
        val6 = self.main_processing['Special']*self.mainInput[ 'Processing - Energy - Usage rates - Special']
       
        self.main_processing['Cost'] = val1+val2+val3+val4+val5+val6
        
    def main_calc_7(self):
        self.labour2_processing['Mine'] =  self.mainInput['Mine']
        d = {self.labour_invat['Line'][x]:self.labour_invat['Denomination per litre'][x] for x in self.labour_invat.index}
        self.labour2_processing['Portion by Line 1'] = self.main_labour['Portion by Price Line 1']
        self.labour2_processing['Price of Line 1'] = self.main_labour['Price Line 1']
        self.labour2_processing['Denomination of Line 1'] = [d[self.labour2_processing['Price of Line 1'][x]] for x in self.mainInput.index]
        self.labour2_processing['Labour use of Line 1'] =self.labour2_processing['Portion by Line 1'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Processing']
        
        self.labour2_processing['Portion by Line 2']= 1 -self.labour2_processing['Portion by Line 1']
        self.labour2_processing['Price of Line 2'] = self.main_labour['Price Line 2']
        self.labour2_processing['Denomination of Line 2'] =[d[self.labour2_processing['Price of Line 2'][x]] for x in self.mainInput.index]
        self.labour2_processing['Labour use of Line 2'] =self.labour2_processing['Portion by Line 2'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Processing']
        
        l1 = {self.labour_price_exVAT['Country Province'][x]:self.labour_price_exVAT[self.year][x] for x in self.labour_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        
        self.labour2_processing['List Price of  Line 1'] = [l1[self.labour2_processing['Price of Line 1'][x]] for x in self.mainInput.index]
        self.labour2_processing['Customised Price of Line 1'] = self.mainInput['Prices 2 - Line 1 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.labour2_processing.loc[row,'Customised Price of Line 1']):
                res = self.labour2_processing.loc[row,'List Price of  Line 1']
            else:
                res = self.labour2_processing.loc[row,'Customised Price of Line 1']
            self.labour2_processing.at[row,'Final Price of  Line 1'] =res
        self.labour2_processing['FX rate of Line 1'] = [l2[self.labour2_processing['Denomination of Line 1'][x]] for x in self.mainInput.index]
        self.labour2_processing['Final Price 1'] = self.labour2_processing['Final Price of  Line 1'].astype(float)/self.labour2_processing['FX rate of Line 1'].astype(float)
        
        self.labour2_processing['List Price of  Line 2']= [l1[self.labour2_processing['Price of Line 2'][x]] for x in self.mainInput.index]
        self.labour2_processing['Customised Price of Line 2'] = self.mainInput['Prices 2 - Line 2 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.labour2_processing.loc[row,'Customised Price of Line 2']):
                res = self.labour2_processing.loc[row,'List Price of  Line 2']
            else:
                res = self.labour2_processing.loc[row,'Customised Price of Line 2']
            self.labour2_processing.at[row,'Final Price of  Line 2'] =res
        self.labour2_processing['FX rate of Line 2']=[l2[self.labour2_processing['Denomination of Line 2'][x]] for x in self.mainInput.index]
        self.labour2_processing['Final Price 2'] = self.labour2_processing['Final Price of  Line 2'].astype(float)/self.labour2_processing['FX rate of Line 2'].astype(float)
        
        self.labour2_processing['Cost'] = (self.labour2_processing['Labour use of Line 1']*self.labour2_processing['Final Price 1'])+(self.labour2_processing['Labour use of Line 2']*self.labour2_processing['Final Price 2'])
        self.labour2_processing['Water use'] = self.mainInput['Water - Usage - Water use']
        self.labour2_processing['Price'] = self.main_pricesEnergyWater['Water - Final Price']
        self.labour2_processing['Cost 2'] =self.labour2_processing['Water use'].astype(float)*self.labour2_processing['Price'].astype(float)
        self.labour2_processing['Processing Cost'] = self.main_processing['Cost'] +self.labour2_processing['Cost']+self.labour2_processing['Cost 2']
        
        
    def main_calc_8(self):
        self.main_transport['Mine'] =  self.mainInput['Mine']
        self.main_transport['Diesel'] = self.mainInput['Transport Energy Usage rates - Diesel'].astype(float)
        self.main_transport['Gasoline'] =  self.mainInput['Transport Energy Usage rates - Gasoline'].astype(float)
        self.main_transport['LPG Gas'] =self.mainInput['Transport Energy Usage rates - LPG'].astype(float)
        self.main_transport['Electricity'] =self.mainInput[ 'Transport Energy Usage rates - Electricity'].astype(float)
        self.main_transport['Explosives'] =self.mainInput['Transport Energy Usage rates - Explosives'].astype(float)
        self.main_transport['Special'] = self.mainInput[ 'Transport Energy Usage rates - special'].astype(float)
        
        val1 = self.main_transport['Diesel']*self.main_pricesEnergyWater['Diesel - Final Price']
        val2 = self.main_transport['Gasoline']*self.main_pricesEnergyWater['Gasoline - Final Price']
        val3 = self.main_transport['LPG Gas']*self.main_pricesEnergyWater['LPG - Final Price']
        val4 = self.main_transport['Electricity']*self.main_pricesEnergyWater['Electricity - Final Price']
        val5 = self.main_transport['Explosives']*self.main_pricesEnergyWater['Explosives - Final Price']
        val6 = self.main_transport['Special']*self.mainInput['Special Price']
        self.main_transport['Cost 1'] = val1+val2+val3+val4+val5+val6
        
        d = {self.labour_invat['Line'][x]:self.labour_invat['Denomination per litre'][x] for x in self.labour_invat.index}
        self.main_transport['Portion by Line 1'] = self.main_labour['Portion by Price Line 1']
        self.main_transport['Price of Line 1'] = self.main_labour['Price Line 1']
        self.main_transport['Denomination of Line 1'] = [d[self.main_transport['Price of Line 1'][x]] for x in self.mainInput.index]
        self.main_transport['Labour use of Line 1'] =self.main_transport['Portion by Line 1'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Transport']
        
        self.main_transport['Portion by Line 2']= 1 -self.main_transport['Portion by Line 1']
        self.main_transport['Price of Line 2'] = self.main_labour['Price Line 2']
        self.main_transport['Denomination of Line 2'] =[d[self.main_transport['Price of Line 2'][x]] for x in self.mainInput.index]
        self.main_transport['Labour use of Line 2'] =self.main_transport['Portion by Line 2'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Transport']
        
        l1 = {self.labour_price_exVAT['Country Province'][x]:self.labour_price_exVAT[self.year][x] for x in self.labour_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        
        self.main_transport['List Price of  Line 1'] = [l1[self.main_transport['Price of Line 1'][x]] for x in self.mainInput.index]
        self.main_transport['Customised Price of Line 1'] = self.mainInput['Prices 3 -Line 1 Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_transport.loc[row,'Customised Price of Line 1']):
                res = self.main_transport.loc[row,'List Price of  Line 1']
            else:
                res = self.main_transport.loc[row,'Customised Price of Line 1']
            self.main_transport.at[row,'Final Price of  Line 1'] =res
        self.main_transport['FX rate of Line 1'] = [l2[self.main_transport['Denomination of Line 1'][x]] for x in self.mainInput.index]
        self.main_transport['Final Price 1'] = self.main_transport['Final Price of  Line 1'].astype(float)/self.main_transport['FX rate of Line 1'].astype(float)
        
        self.main_transport['List Price of  Line 2']= [l1[self.main_transport['Price of Line 2'][x]] for x in self.mainInput.index]
        self.main_transport['Customised Price of Line 2'] = self.mainInput['Prices 3 -Line 2 Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_transport.loc[row,'Customised Price of Line 2']):
                res = self.main_transport.loc[row,'List Price of  Line 2']
            else:
                res = self.main_transport.loc[row,'Customised Price of Line 2']
            self.main_transport.at[row,'Final Price of  Line 2'] =res
        self.main_transport['FX rate of Line 2']=[l2[self.main_transport['Denomination of Line 2'][x]] for x in self.mainInput.index]
        self.main_transport['Final Price 2'] = self.main_transport['Final Price of  Line 2'].astype(float)/self.main_transport['FX rate of Line 2'].astype(float)
        
        self.main_transport['Cost 2'] = (self.main_transport['Labour use of Line 1']*self.main_transport['Final Price 1'])+(self.main_transport['Labour use of Line 2']*self.main_transport['Final Price 2'])
        self.main_transport['Water use'] = self.mainInput['Water 3 - Water use']
        self.main_transport['Price'] = self.main_pricesEnergyWater['Water - Final Price']
        self.main_transport['Cost 3'] =self.main_transport['Water use'].astype(float)*self.main_transport['Price'].astype(float)
        self.main_transport['Special'] = self.mainInput['Special (row 312)'].fillna(0)
        self.main_transport['Transport Costs'] = self.main_transport['Cost 1'] +self.main_transport['Cost 2']+self.main_transport['Cost 3']+self.main_transport['Special']
        
        
    def main_calc_9(self):
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_other_direct_charges['Mine'] =  self.mainInput['Mine']
        self.main_other_direct_charges['Port Charges - Denominaion'] = self.mainInput['Port Charges - Denomination']
        self.main_other_direct_charges['Port Charges - Charge amount'] = self.mainInput['Port Charges - Charge amount']
        self.main_other_direct_charges['Port Charges - Fx rate'] =[l2[self.main_other_direct_charges['Port Charges - Denominaion'][x]] for x in self.mainInput.index]
        self.main_other_direct_charges['Port Charges - Charge amount 2'] =self.main_other_direct_charges['Port Charges - Charge amount'].astype(float)/self.main_other_direct_charges['Port Charges - Fx rate'].astype(float)
        
        self.main_other_direct_charges['Barging Charges - Denominaion'] = self.mainInput['Barging Charges - Denomination']
        self.main_other_direct_charges['Barging Charges - Charge amount'] = self.mainInput['Barging Charges - Charge amount']
        self.main_other_direct_charges['Barging Charges - Fx rate'] =[l2[self.main_other_direct_charges['Barging Charges - Denominaion'][x]] for x in self.mainInput.index]
        self.main_other_direct_charges['Barging Charges - Charge amount 2'] =self.main_other_direct_charges['Barging Charges - Charge amount'].astype(float)/self.main_other_direct_charges['Barging Charges - Fx rate'].astype(float)
        
        self.main_other_direct_charges['Trans. Charges - Denominaion'] = self.mainInput['Transloading charges - Denomination']
        self.main_other_direct_charges['Trans. Charges - Charge amount'] = self.mainInput['Transloading charges - Charge amount']
        self.main_other_direct_charges['Trans. Charges - Fx rate'] =[l2[self.main_other_direct_charges['Trans. Charges - Denominaion'][x]] for x in self.mainInput.index]
        self.main_other_direct_charges['Trans. Charges - Charge amount 2'] =self.main_other_direct_charges['Trans. Charges - Charge amount'].astype(float)/self.main_other_direct_charges['Trans. Charges - Fx rate'].astype(float)
        
    def main_calc_10(self): 
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_taxes_loyalties['Mine'] =  self.mainInput['Mine']
        self.main_taxes_loyalties['Charge 1 Name'] = self.mainInput['Charge 1 Name']
        self.main_taxes_loyalties['Charge 1 Denomination']= self.mainInput['Charge 1 Denomination']
        self.main_taxes_loyalties['Charge 1 Amount']= self.mainInput['Charge 1 Amount'].fillna(0).astype(float)
        self.main_taxes_loyalties['Charge 1 Fx rate'] =[l2[self.main_taxes_loyalties['Charge 1 Denomination'][x]] for x in self.mainInput.index]
        self.main_taxes_loyalties['Charge 1 Amount 2'] =self.main_taxes_loyalties['Charge 1 Amount'].astype(float)/self.main_taxes_loyalties['Charge 1 Fx rate'].astype(float)
        
        self.main_taxes_loyalties['Mine'] =  self.mainInput['Mine']
        self.main_taxes_loyalties['Charge 2 Name'] = self.mainInput['Charge 2 Name']
        self.main_taxes_loyalties['Charge 2 Denomination']= self.mainInput['Charge 2 Denomination']
        self.main_taxes_loyalties['Charge 2 Amount']= self.mainInput['Charge 2 Amount'].fillna(0).astype(float)
        self.main_taxes_loyalties['Charge 2 Fx rate'] =[l2[self.main_taxes_loyalties['Charge 2 Denomination'][x]] for x in self.mainInput.index]
        self.main_taxes_loyalties['Charge 2 Amount 2'] =self.main_taxes_loyalties['Charge 2 Amount'].astype(float)/self.main_taxes_loyalties['Charge 2 Fx rate'].astype(float)
        
        self.main_taxes_loyalties['Mine'] =  self.mainInput['Mine']
        self.main_taxes_loyalties['Charge 3 Name'] = self.mainInput['Charge 3 Name']
        self.main_taxes_loyalties['Charge 3 Denomination']= self.mainInput['Charge 3 Denomination']
        self.main_taxes_loyalties['Charge 3 Amount']= self.mainInput['Charge 3 Amount'].fillna(0).astype(float)
        self.main_taxes_loyalties['Charge 3 Fx rate'] =[l2[self.main_taxes_loyalties['Charge 3 Denomination'][x]] for x in self.mainInput.index]
        self.main_taxes_loyalties['Charge 3 Amount 2'] =self.main_taxes_loyalties['Charge 3 Amount'].astype(float)/self.main_taxes_loyalties['Charge 3 Fx rate'].astype(float)
        
        self.main_taxes_loyalties['Mine'] =  self.mainInput['Mine']
        self.main_taxes_loyalties['Charge 4 Name'] = self.mainInput['Charge 4 Name']
        self.main_taxes_loyalties['Charge 4 Denomination']= self.mainInput['Charge 4 Denomination']
        self.main_taxes_loyalties['Charge 4 Amount']= self.mainInput['Charge 4 Amount'].fillna(0).astype(float)
        self.main_taxes_loyalties['Charge 4 Fx rate'] =[l2[self.main_taxes_loyalties['Charge 4 Denomination'][x]] for x in self.mainInput.index]
        self.main_taxes_loyalties['Charge 4 Amount 2'] =self.main_taxes_loyalties['Charge 4 Amount'].astype(float)/self.main_taxes_loyalties['Charge 4 Fx rate'].astype(float)
        
        self.main_taxes_loyalties['Mine'] =  self.mainInput['Mine']
        self.main_taxes_loyalties['Charge 5 Name'] = self.mainInput['Charge 5 Name']
        self.main_taxes_loyalties['Charge 5 Denomination']= self.mainInput['Charge 5 Denomination']
        self.main_taxes_loyalties['Charge 5 Amount']= self.mainInput['Charge 5 Amount'].fillna(0).astype(float)
        self.main_taxes_loyalties['Charge 5 Fx rate'] =[l2[self.main_taxes_loyalties['Charge 5 Denomination'][x]] for x in self.mainInput.index]
        self.main_taxes_loyalties['Charge 5 Amount 2'] =self.main_taxes_loyalties['Charge 5 Amount'].astype(float)/self.main_taxes_loyalties['Charge 5 Fx rate'].astype(float)

        self.main_taxes_loyalties['Total Taxes & Royalties'] = self.main_taxes_loyalties['Charge 1 Amount 2']+self.main_taxes_loyalties['Charge 2 Amount 2']+self.main_taxes_loyalties['Charge 3 Amount 2']+self.main_taxes_loyalties['Charge 4 Amount 2']+self.main_taxes_loyalties['Charge 5 Amount 2']
        self.main_taxes_loyalties['Total Other Charges'] = self.main_other_direct_charges['Port Charges - Charge amount 2']+self.main_other_direct_charges['Barging Charges - Charge amount 2']+self.main_other_direct_charges['Trans. Charges - Charge amount 2']+self.main_taxes_loyalties['Total Taxes & Royalties']

    def main_calc_11(self): 
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_maintenance['Mine'] =  self.mainInput['Mine']
        self.main_maintenance['Materials Denomination'] = self.mainInput['Underlying Denomiations']
        self.main_maintenance['estimated capital cost'] = self.mainInput['Estimated Capital Cost']
        self.main_maintenance['Fx rate'] =[l2[self.main_maintenance['Materials Denomination'][x]] for x in self.mainInput.index]
        self.main_maintenance['estimated capital cost 2'] = self.main_maintenance['estimated capital cost'].astype(float)/self.main_maintenance['Fx rate'].astype(float)
        self.main_maintenance['Portion of capital spent as maintenance'] = self.mainInput['Portion of capital spent as maintenance ']
        self.main_maintenance['Minimun maintenance charge'] =self.mainInput['Minimum maintenance charge']
        for row in self.mainInput.index:
            val1 = float(self.main_maintenance.loc[row,'estimated capital cost 2'])*float(self.main_maintenance.loc[row,'Portion of capital spent as maintenance'])
            val2 = float(self.main_maintenance.loc[row,'Minimun maintenance charge'])
            self.main_maintenance.at[row,'Maintenance'] = max(val1,val2)
        
        d = {self.labour_invat['Line'][x]:self.labour_invat['Denomination per litre'][x] for x in self.labour_invat.index}
        self.main_maintenance['Portion by Line 1'] = self.main_labour['Portion by Price Line 1']
        self.main_maintenance['Price of Line 1'] = self.main_labour['Price Line 1']
        self.main_maintenance['Denomination of Line 1'] = [d[self.main_maintenance['Price of Line 1'][x]] for x in self.mainInput.index]
        self.main_maintenance['Labour use of Line 1'] =self.main_maintenance['Portion by Line 1'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Maintenance']
        
        self.main_maintenance['Portion by Line 2']= 1 -self.main_maintenance['Portion by Line 1']
        self.main_maintenance['Price of Line 2'] = self.main_labour['Price Line 2']
        self.main_maintenance['Denomination of Line 2'] =[d[self.main_maintenance['Price of Line 2'][x]] for x in self.mainInput.index]
        self.main_maintenance['Labour use of Line 2'] =self.main_maintenance['Portion by Line 2'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Maintenance']
        
        l1 = {self.labour_price_exVAT['Country Province'][x]:self.labour_price_exVAT[self.year][x] for x in self.labour_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        
        self.main_maintenance['List Price of  Line 1'] = [l1[self.main_maintenance['Price of Line 1'][x]] for x in self.mainInput.index]
        self.main_maintenance['Customised Price of Line 1'] = self.mainInput['Commom Distributables Prices - Line 1 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_maintenance.loc[row,'Customised Price of Line 1']):
                res = self.main_maintenance.loc[row,'List Price of  Line 1']
            else:
                res = self.main_maintenance.loc[row,'Customised Price of Line 1']
            self.main_maintenance.at[row,'Final Price of  Line 1'] =res
        self.main_maintenance['FX rate of Line 1'] = [l2[self.main_maintenance['Denomination of Line 1'][x]] for x in self.mainInput.index]
        self.main_maintenance['Final Price 1'] = self.main_maintenance['Final Price of  Line 1'].astype(float)/self.main_maintenance['FX rate of Line 1'].astype(float)
        
        self.main_maintenance['List Price of  Line 2']= [l1[self.main_maintenance['Price of Line 2'][x]] for x in self.mainInput.index]
        self.main_maintenance['Customised Price of Line 2'] = self.mainInput['Commom Distributables Prices - Line 2 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_maintenance.loc[row,'Customised Price of Line 2']):
                res = self.main_maintenance.loc[row,'List Price of  Line 2']
            else:
                res = self.main_maintenance.loc[row,'Customised Price of Line 2']
            self.main_maintenance.at[row,'Final Price of  Line 2'] =res
        self.main_maintenance['FX rate of Line 2']=[l2[self.main_maintenance['Denomination of Line 2'][x]] for x in self.mainInput.index]
        self.main_maintenance['Final Price 2'] = self.main_maintenance['Final Price of  Line 2'].astype(float)/self.main_maintenance['FX rate of Line 2'].astype(float)
        
        self.main_maintenance['Cost'] = (self.main_maintenance['Labour use of Line 1']*self.main_maintenance['Final Price 1'])+(self.main_maintenance['Labour use of Line 2']* self.main_maintenance['Final Price 2'])
   
    def main_calc_12(self): 
        self.main_site_Overheads['Mine'] =  self.mainInput['Mine']
        d = {self.labour_invat['Line'][x]:self.labour_invat['Denomination per litre'][x] for x in self.labour_invat.index}
        self.main_site_Overheads['Portion by Line 1'] = self.main_labour['Portion by Price Line 1']
        self.main_site_Overheads['Price of Line 1'] = self.main_labour['Price Line 1']
        self.main_site_Overheads['Denomination of Line 1'] = [d[self.main_site_Overheads['Price of Line 1'][x]] for x in self.mainInput.index]
        self.main_site_Overheads['Labour use of Line 1'] =self.main_site_Overheads['Portion by Line 1'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Administration']
        
        self.main_site_Overheads['Portion by Line 2']= 1 -self.main_site_Overheads['Portion by Line 1']
        self.main_site_Overheads['Price of Line 2'] = self.main_labour['Price Line 2']
        self.main_site_Overheads['Denomination of Line 2'] =[d[self.main_site_Overheads['Price of Line 2'][x]] for x in self.mainInput.index]
        self.main_site_Overheads['Labour use of Line 2'] =self.main_site_Overheads['Portion by Line 2'] * self.main_tunning_values['Labour utilisation']*self.mainInput['Administration']
        
        l1 = {self.labour_price_exVAT['Country Province'][x]:self.labour_price_exVAT[self.year][x] for x in self.labour_price_exVAT.index}
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        
        self.main_site_Overheads['List Price of  Line 1'] = [l1[self.main_site_Overheads['Price of Line 1'][x]] for x in self.mainInput.index]
        self.main_site_Overheads['Customised Price of Line 1'] = self.mainInput['Site & Overheads - Prices - Line 1 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_site_Overheads.loc[row,'Customised Price of Line 1']):
                res = self.main_site_Overheads.loc[row,'List Price of  Line 1']
            else:
                res = self.main_site_Overheads.loc[row,'Customised Price of Line 1']
            self.main_site_Overheads.at[row,'Final Price of  Line 1'] =res
        self.main_site_Overheads['FX rate of Line 1'] = [l2[self.main_site_Overheads['Denomination of Line 1'][x]] for x in self.mainInput.index]
        self.main_site_Overheads['Final Price 1'] = self.main_site_Overheads['Final Price of  Line 1'].astype(float)/self.main_site_Overheads['FX rate of Line 1'].astype(float)
        
        self.main_site_Overheads['List Price of  Line 2']= [l1[self.main_site_Overheads['Price of Line 2'][x]] for x in self.mainInput.index]
        self.main_site_Overheads['Customised Price of Line 2'] = self.mainInput['Site & Overheads - Prices - Line 2 - Customised Price']
        for row in self.mainInput.index:
            res = 0
            if pd.isna(self.main_site_Overheads.loc[row,'Customised Price of Line 2']):
                res = self.main_site_Overheads.loc[row,'List Price of  Line 2']
            else:
                res = self.main_site_Overheads.loc[row,'Customised Price of Line 2']
            self.main_site_Overheads.at[row,'Final Price of  Line 2'] =res
        self.main_site_Overheads['FX rate of Line 2']=[l2[self.main_site_Overheads['Denomination of Line 2'][x]] for x in self.mainInput.index]
        self.main_site_Overheads['Final Price 2'] = self.main_site_Overheads['Final Price of  Line 2'].astype(float)/self.main_site_Overheads['FX rate of Line 2'].astype(float)
        
        self.main_site_Overheads['Cost'] = (self.main_site_Overheads['Labour use of Line 1']*self.main_site_Overheads['Final Price 1'])+(self.main_site_Overheads['Labour use of Line 2']*self.main_site_Overheads['Final Price 2'])
        
        
    def main_calc_13(self):
        l2 = {self.currency['Currency_Code'][x]:self.currency[self.year][x] for x in self.currency.index}
        self.main_corporate_charge['Mine'] =  self.mainInput['Mine']
        self.main_corporate_charge['Denomination'] = self.mainInput['Coporate Charge - Denomination']
        self.main_corporate_charge['Charge amount'] = self.mainInput['Coporate Charge - Charge Amount'].astype(float).fillna(0)
        self.main_corporate_charge['Fx rate'] = [l2[self.main_corporate_charge['Denomination'][x]] for x in self.mainInput.index]
        self.main_corporate_charge['Charge amount'] =self.main_corporate_charge['Charge amount'].astype(float)/self.main_corporate_charge['Fx rate'].astype(float)
        self.main_corporate_charge['Total Common distributables'] = self.main_maintenance['Maintenance']+self.main_maintenance['Cost']+self.main_site_Overheads['Cost'] +self.main_corporate_charge['Charge amount']
        
    def main_calc_summary(self):
        self.main_diesel['Mining'] = self.mainInput['Energy -Usage Rates - Diesel '].fillna(0).astype(float)
        self.main_diesel['Processing'] = self.mainInput['Processing - Energy - Usage rates - Diesel'].fillna(0).astype(float)
        self.main_diesel['Transport'] = self.mainInput[ 'Transport Energy Usage rates - Diesel'].fillna(0).astype(float)
        self.main_diesel['Other direct charges'] = 0
        self.main_diesel['Common Distributables'] = 0
        self.main_diesel['Total'] = self.main_diesel[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
        
        self.main_gasoline['Mining'] = self.mainInput['Energy -Usage Rates - Gasoline'].fillna(0).astype(float)
        self.main_gasoline['Processing']= self.mainInput['Processing - Energy - Usage rates - Gasoline'].fillna(0).astype(float)
        self.main_gasoline['Transport'] = self.mainInput['Transport Energy Usage rates - Gasoline'].fillna(0).astype(float)
        self.main_gasoline['Other direct charges'] = 0
        self.main_gasoline['Common Distributables'] = 0
        self.main_gasoline['Total'] = self.main_gasoline[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
        
        self.main_lpg['Mining'] = self.mainInput['Energy -Usage Rates - LPG'].fillna(0).astype(float)
        self.main_lpg['Processing'] = self.mainInput['Processing - Energy - Usage rates - LPG'].fillna(0).astype(float)
        self.main_lpg['Transport'] = self.mainInput['Transport Energy Usage rates - LPG'].fillna(0).astype(float)
        self.main_lpg['Other direct charges'] = 0
        self.main_lpg['Common Distributables'] = 0
        self.main_lpg['Total'] = self.main_lpg[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
        
        self.main_electricity['Mining'] = self.mainInput['Energy -Usage Rates - Electricity'].fillna(0).astype(float)
        self.main_electricity['Processing'] = self.mainInput['Processing - Energy - Usage rates - Electricity'].fillna(0).astype(float)
        self.main_electricity['Transport'] = self.mainInput['Transport Energy Usage rates - Electricity'].fillna(0).astype(float)
        self.main_electricity['Other direct charges'] = 0
        self.main_electricity['Common Distributables'] = 0
        self.main_electricity['Total'] = self.main_electricity[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
        
        self.main_explosives['Mining'] = self.mainInput['Energy -Usage Rates - Explosives'].fillna(0).astype(float)
        self.main_explosives['Processing'] = self.mainInput['Processing - Energy - Usage rates - Explosives'].fillna(0).astype(float)
        self.main_explosives['Transport'] = self.mainInput['Transport Energy Usage rates - Explosives'].fillna(0).astype(float)
        self.main_explosives['Other direct charges'] = 0
        self.main_explosives['Common Distributables'] = 0
        self.main_explosives['Total'] =  self.main_explosives[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
        
        self.main_water['Mining'] = self.mainInput['Water - Water use'].fillna(0).astype(float)
        self.main_water['Processing'] = self.mainInput['Water - Usage - Water use'].fillna(0).astype(float)
        self.main_water['Transport'] = self.mainInput['Water 3 - Water use'].fillna(0).astype(float)
        self.main_water['Other direct charges']=0
        self.main_water['Common Distributables']=0
        self.main_water['Total'] =  self.main_water[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
         
        self.main_special['Mining'] = self.mainInput['Energy -Usage Rates - Special'].fillna(0).astype(float)
        self.main_special['Processing'] = self.mainInput['Processing - Energy - Usage rates - Special'].fillna(0).astype(float)
        self.main_special['Transport'] = self.mainInput['Transport Energy Usage rates - special'].fillna(0).astype(float)
        self.main_special['Other direct charges'] = 0
        self.main_special['Common Distributables'] = 0
        self.main_special['Total'] = self.main_special[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
        
        self.main_labour_summary['Mining'] = self.main_labour['Labour use of Line 1']+self.main_labour['Labour use of Line 2']
        self.main_labour_summary['Processing'] = self.labour2_processing['Labour use of Line 1'] +self.labour2_processing['Labour use of Line 2']
        self.main_labour_summary['Transport'] = self.main_transport['Labour use of Line 1']+self.main_transport['Labour use of Line 2']
        self.main_labour_summary['Other direct charges'] = self.main_maintenance['Labour use of Line 1'] +self.main_maintenance['Labour use of Line 2']
        self.main_labour_summary['Common Distributables'] =self.main_site_Overheads['Labour use of Line 1']+self.main_site_Overheads['Labour use of Line 2']
        self.main_labour_summary['Total'] = self.main_labour_summary[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)
        self.main_labour_summary['Total 2'] = 1/self.main_labour_summary['Total']
        
        # add override
        self.main_raw_cost['Mine'] =  self.mainInput['Mine']
        self.main_raw_cost['Mining'] =  self.main_labour_summary['Mining Costs'] 
        self.main_raw_cost['Processing'] = self.labour2_processing['Processing Cost']
        self.main_raw_cost['Transport'] =  self.main_transport['Transport Costs']
        self.main_raw_cost['Other direct charges'] =self.main_taxes_loyalties['Total Other Charges']
        self.main_raw_cost['Port charges'] =  self.main_other_direct_charges['Port Charges - Charge amount 2'] 
        self.main_raw_cost['Barging Charges'] =self.main_other_direct_charges['Barging Charges - Charge amount 2']
        self.main_raw_cost['Transloading charges'] =self.main_other_direct_charges['Trans. Charges - Charge amount 2']
        self.main_raw_cost['Taxes and Royalties'] =self.main_taxes_loyalties['Total Taxes & Royalties']
        self.main_raw_cost['Common Distributables'] =self.main_corporate_charge['Total Common distributables']

        for v in override_store.keys():
            col = '_'.join(v.split('_')[:-1])
            row = int(v.split('_')[-1])
            self.main_raw_cost.loc[row, col] = override_store[v]

        self.main_raw_cost['Total'] = self.main_raw_cost[['Mining','Processing','Transport','Other direct charges','Common Distributables']].sum(axis=1)

        for v in override_store.keys():
            col = '_'.join(v.split('_')[:-1])
            if col == 'Total':
                row = int(v.split('_')[0])
                self.main_raw_cost.loc[row, col] = override_store[v]
        
        
    
    
    def calc_main(self):
        self.main_calc_1()
        self.main_calc_2()
        self.main_calc_3()
        self.main_calc_4()
        self.main_calc_5()
        self.main_calc_6()
        self.main_calc_7()
        self.main_calc_8()
        self.main_calc_9()
        self.main_calc_10()
        self.main_calc_11()
        self.main_calc_12()
        self.main_calc_13()
        self.main_calc_summary()

        
        
        
        
        #functions
    def priceRebates_func(self, column, denom,ad, aff):
        res = float(self.currency.loc[self.currency['Currency_Code']==denom, column]) / float(self.currency.loc[self.currency['Currency_Code']==ad, column])
        res = float(aff) * res
        return res
   
        
        
x = qrtlymodel()
x.diesel_calcs()
x.gasoline_calcs()
x.lpg_calcs()
x.electricity_calcs()
x.explosives_calcs()
x.water_calcs()
x.labour_calcs()
x.calc_main()

writer1 = pd.ExcelWriter('outputs\\calculations\\Diesel prices and rebates.xlsx')
x.diesel_price_rebates.to_excel(writer1, sheet_name='Prices Rebates', index=False)
x.diesel_price_vat.to_excel(writer1, sheet_name='VATs as decimal', index=False)
x.diesel_price_exVAT.to_excel(writer1, sheet_name='Final Prices ', index=False)
writer1.save()

writer2 = pd.ExcelWriter('outputs\\calculations\\Gasoline prices and rebates.xlsx')
x.gasoline_price_rebates.to_excel(writer2, sheet_name='Prices Rebates', index=False)
x.gasoline_price_vat.to_excel(writer2, sheet_name='VATs as decimal', index=False)
x.gasoline_price_exVAT.to_excel(writer2, sheet_name='Final Prices ', index=False)
writer2.save()

writer3 = pd.ExcelWriter('outputs\\calculations\\LPG prices and rebates.xlsx')
# this is lpg but gasoline is used
# x.gasoline_price_rebates.to_excel(writer3, sheet_name='Prices Rebates', index=False)
# x.gasoline_price_vat.to_excel(writer3, sheet_name='VATs as decimal', index=False)
# x.gasoline_price_exVAT.to_excel(writer3, sheet_name='Final Prices ', index=False)

# assumption
x.lpg_price_rebates.to_excel(writer3, sheet_name='Prices Rebates', index=False)
x.lpg_price_vat.to_excel(writer3, sheet_name='VATs as decimal', index=False)
x.lpg_price_exVAT.to_excel(writer3, sheet_name='Final Prices ', index=False)

writer3.save()

writer4 = pd.ExcelWriter('outputs\\calculations\\Explosives prices and rebates.xlsx')
x.explosives_price_rebates.to_excel(writer4, sheet_name='Prices Rebates', index=False)
x.explosives_price_vat.to_excel(writer4, sheet_name='VATs as decimal', index=False)
x.explosives_price_exVAT.to_excel(writer4, sheet_name='Final Prices ', index=False)
writer4.save()

writer5 = pd.ExcelWriter('outputs\\calculations\\Water prices and rebates.xlsx')
x.water_price_rebates.to_excel(writer5, sheet_name='Prices Rebates', index=False)
x.water_price_vat.to_excel(writer5, sheet_name='VATs as decimal', index=False)
x.water_price_exVAT.to_excel(writer5, sheet_name='Final Prices ', index=False)
writer5.save()

writer6 = pd.ExcelWriter('outputs\\calculations\\Labour prices and rebates.xlsx')
x.labour_price_rebates.to_excel(writer6, sheet_name='Prices Rebates', index=False)
x.labour_price_vat.to_excel(writer6, sheet_name='VATs as decimal', index=False)
x.labour_price_exVAT.to_excel(writer6, sheet_name='Final Prices ', index=False)
writer6.save()

writer7 = pd.ExcelWriter('outputs\\calculations\\Electricity prices and rebates.xlsx')
x.electricity_price_rebates.to_excel(writer7, sheet_name='Prices Rebates', index=False)
x.electricity_price_vat.to_excel(writer7, sheet_name='VATs as decimal', index=False)
x.electricity_price_exVAT.to_excel(writer7, sheet_name='Final Prices ', index=False)
writer7.save()


mainWriter = pd.ExcelWriter('outputs\\Main Sheet.xlsx')
x.main_tunning_values.to_excel(mainWriter, sheet_name='Tunning Values', index=False)
x.main_pricesEnergyWater.to_excel(mainWriter, sheet_name='Prices', index=False)
x.main_directCharges.to_excel(mainWriter, sheet_name='Direct Charges', index=False)
x.main_labour.to_excel(mainWriter, sheet_name='Labour ', index=False)
x.main_labour_summary.to_excel(mainWriter, sheet_name='Labour cost and sum', index=False)
x.main_processing.to_excel(mainWriter, sheet_name='processing', index=False)
x.labour2_processing.to_excel(mainWriter, sheet_name='Labour 2 Processing', index=False)
x.main_transport.to_excel(mainWriter, sheet_name='Transport', index=False)
x.main_other_direct_charges.to_excel(mainWriter, sheet_name='Other Direct Charges', index=False)
x.main_taxes_loyalties.to_excel(mainWriter, sheet_name='Taxes and Loyalties', index=False)
x.main_maintenance.to_excel(mainWriter, sheet_name='Maintenance', index=False)
x.main_site_Overheads.to_excel(mainWriter, sheet_name='Sites and Overheads', index=False)
x.main_corporate_charge.to_excel(mainWriter, sheet_name='Corporate Charges', index=False)
#x.main_.to_excel(mainWriter, sheet_name='', index=False)
#x.main_.to_excel(mainWriter, sheet_name='', index=False)
#x.main_.to_excel(mainWriter, sheet_name='', index=False)
mainWriter.save()

writer = pd.ExcelWriter('outputs\\Main Sheet Outputs.xlsx')
x.main_diesel.to_excel(writer, sheet_name='Diesel Use', index=False)
x.main_gasoline.to_excel(writer, sheet_name='Gasoline Use', index=False)
x.main_lpg.to_excel(writer, sheet_name='LPG Use', index=False)
x.main_electricity.to_excel(writer, sheet_name='Electricity Use', index=False)
x.main_explosives.to_excel(writer, sheet_name='Explosives Use', index=False)
x.main_special.to_excel(writer, sheet_name='Special Use', index=False)
x.main_water.to_excel(writer, sheet_name='Water Use', index=False)
x.main_labour_summary.to_excel(writer, sheet_name='LAbour Use', index=False)
x.main_raw_cost.to_excel(writer, sheet_name='Raw Costs', index=False)
writer.save()

dblist = []

dblist.append(q_flat.mult_year_single_output(x.diesel_price_rebates, "Diesel Prices Rebates"))
dblist.append(q_flat.mult_year_single_output(x.diesel_price_vat, "Diesel Prices Vats"))
dblist.append(q_flat.mult_year_single_output(x.diesel_price_exVAT, "Diesel Prices final price"))
dblist.append(q_flat.mult_year_single_output(x.gasoline_price_rebates, "Gasoline Prices Rebates"))
dblist.append(q_flat.mult_year_single_output(x.gasoline_price_vat, "Gasoline Prices vats"))
dblist.append(q_flat.mult_year_single_output(x.gasoline_price_exVAT, "Gasoline Prices final"))
dblist.append(q_flat.mult_year_single_output(x.lpg_price_rebates, "LPG Prices Rebates"))
dblist.append(q_flat.mult_year_single_output(x.lpg_price_vat, "LPG Prices vats"))
dblist.append(q_flat.mult_year_single_output(x.lpg_price_exVAT, "LPG Prices final"))
dblist.append(q_flat.mult_year_single_output(x.explosives_price_rebates, "Explosives Prices Rebates"))
dblist.append(q_flat.mult_year_single_output(x.explosives_price_vat, "Explosives Prices vats"))
dblist.append(q_flat.mult_year_single_output(x.explosives_price_exVAT, "Explosives Prices final"))
dblist.append(q_flat.mult_year_single_output(x.water_price_rebates, "Water Prices Rebates"))
dblist.append(q_flat.mult_year_single_output(x.water_price_vat, "Water Prices vats"))
dblist.append(q_flat.mult_year_single_output(x.water_price_exVAT, "Water Prices final"))
dblist.append(q_flat.mult_year_single_output(x.labour_price_rebates, "Labour Prices Rebates"))
dblist.append(q_flat.mult_year_single_output(x.labour_price_vat, "Labour Prices vats"))
dblist.append(q_flat.mult_year_single_output(x.labour_price_exVAT, "Labour Prices final"))
dblist.append(q_flat.mult_year_single_output(x.electricity_price_rebates, "Electricity Prices Rebates"))
dblist.append(q_flat.mult_year_single_output(x.electricity_price_vat, "Electricity Prices vats"))
dblist.append(q_flat.mult_year_single_output(x.electricity_price_exVAT, "Electricity Prices final"))

dblist.append(q_flat.single_year_mult_out(x.main_tunning_values, "Main sheet Tunning Values"))
dblist.append(q_flat.single_year_mult_out(x.main_pricesEnergyWater, "Main sheet Prices"))
dblist.append(q_flat.single_year_mult_out(x.main_directCharges, "Main sheet Direct Charges"))
dblist.append(q_flat.single_year_mult_out(x.main_labour, "Main sheet Labour "))
dblist.append(q_flat.single_year_mult_out(x.main_labour_summary, "Main sheet Labour cost and sum"))
dblist.append(q_flat.single_year_mult_out(x.main_processing, "Main sheet processing"))
dblist.append(q_flat.single_year_mult_out(x.labour2_processing, "Main sheet Labour 2 Processing"))
dblist.append(q_flat.single_year_mult_out(x.main_transport, "Main sheet Transport"))
dblist.append(q_flat.single_year_mult_out(x.main_other_direct_charges, "Main sheet Other Direct Charges"))
dblist.append(q_flat.single_year_mult_out(x.main_taxes_loyalties, "Main sheet Taxes and Loyalties"))
dblist.append(q_flat.single_year_mult_out(x.main_maintenance, "Main sheet Maintenance"))
dblist.append(q_flat.single_year_mult_out(x.main_site_Overheads, "Main sheet Sites and Overheads"))
dblist.append(q_flat.single_year_mult_out(x.main_corporate_charge, "Main sheet Corporate Charges"))

dblist.append(q_flat.single_year_mult_out(x.main_diesel, "Main sheet output Diesel Use"))
dblist.append(q_flat.single_year_mult_out(x.main_gasoline, "Main sheet output Gasoline Use"))
dblist.append(q_flat.single_year_mult_out(x.main_lpg, "Main sheet output LPG Use"))
dblist.append(q_flat.single_year_mult_out(x.main_electricity, "Main sheet output Electricity Use"))
dblist.append(q_flat.single_year_mult_out(x.main_explosives, "Main sheet output Explosives Use"))
dblist.append(q_flat.single_year_mult_out(x.main_special, "Main sheet output Special Use"))
dblist.append(q_flat.single_year_mult_out(x.main_water, "Main sheet output Water Use"))
dblist.append(q_flat.single_year_mult_out(x.main_labour_summary, "Main sheet output LAbour Use"))
dblist.append(q_flat.single_year_mult_out(x.main_raw_cost, "Main sheet output Raw Costs"))

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, q_flat.out_col]

try:
    override_res = override_rows.values
    for i, v in enumerate(override_rows.index):
        print(snapshot_output_data.loc[v], )
        set_it = snapshot_output_data.loc[v].values
        print(override_res[i][-2:])
        set_it[-2:] = override_res[i][-2:]
        snapshot_output_data.loc[v] = set_it 
except Exception as err:
    print(err)
    print("Error caught and skipped")

snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
uploadtodb.upload(snapshot_output_data)