import simply
from simply import *
import warnings
from scipy import stats
import numpy as np
import pandas as pd
import statistics as stat
import openpyxl
warnings.filterwarnings("ignore")
'''
Bauxite main model starts here
'''
class Bauxite():
    def __init__(self,db,datadb,globaldb,summarydb):  # These are all different static databases
        self.db=db                                                                             
        self.globaldb = globaldb                      # global values db cell 42 to 54
        self.datadb = datadb
        self.summarydb = summarydb                    # reserve summary db 
        
    def reserve(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.category==sheet][self.summarydb.bauxite==province]["allocation"].sum() 
        put(self.db,sheet,province,"Reserve",'2004',v)
    def aluminagrade(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.category==sheet][self.summarydb.bauxite==province]['avgal'].sum()
        put(self.db,sheet,province,"Alumina Grade",'2004',v)
    def asratio(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.category==sheet][self.summarydb.bauxite==province]['avgas'].sum()
        put(self.db,sheet,province,"A/S",'2004',v)
    def closed(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.category==sheet][self.summarydb.bauxite==province]['closed'].sum()
        put(self.db,sheet,province,"Closed",'2004',v)
    def demand(self,sheet,province,year):
        put(self.db,sheet,province,"Demand Profile",'2004',0.9999999900000)
    def usagee(self,sheet,province,year):
        put(self.db,sheet,province,"Usage",'2004',0.1)
    def factorx(self,sheet,province,year):
        put(self.db,sheet,province,"Factor X",'2004',1)    
    def purchased(self,sheet,province,year):
        #print("working")
        try:
            d = [get(self.db,sheet,province,"Closed",year)]
            value = 1-d[0]
            put(self.db,sheet,province,"Purchased",year,value)
        except:
            put(self.db,sheet,province,"Purchased",year,0)
    def silica(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Grade",year),     # precedent values
                 get(self.db,sheet,province,"A/S",year)
                 ]
            value = d[0]/d[1]                                          # formula
            put(self.db,sheet,province,"Silica Grade",year,value)      # putting value in db
        except:
            put(self.db,sheet,province,"Silica Grade",year,0)          # put 0 in db if any error

    def starting_ratio(self,sheet,province,year):
        try:
            d = [3.645,
                 2,
                 get(self.db,sheet,province,"A/S",year)
                 ]
            value = d[0]/d[1]*d[2]
            put(self.db,sheet,province,"Starting ratio",year,value)
        except:
            put(self.db,sheet,province,"Starting ratio",year,0)
            
    def depletion_ratio(self,sheet,province,year):
        try:
            d = [2.6,
                 0.453,
                 get(self.db,sheet,province,"A/S",year)
                 ]
            value = max(d[0],(1+(d[1]-1)/2)*d[2]) if d[2] > 0 else d[0] if year == '2004' else np.nan
            put(self.db,sheet,province,"Depletion ratio",year,value)
            pass
        except:
            put(self.db,sheet,province,"Depletion ratio",year,0)
            
    def alumina_profile_starting_grade(self,sheet,province,year):
        try:
            d = [0.85,
                 1,
                 0.164,
                 2,
                 get(self.db,sheet,province,"Alumina Grade",year)
                 ]
            value = min(d[0],(d[1]+d[2]/d[3])*d[4]) if d[4] > 0 else np.nan
            put(self.db,sheet,province,"Alumina Profile Starting grade",year,value)
            pass
        except:
            put(self.db,sheet,province,"Alumina Profile Starting grade",year,0)
            
    def alumina_profile_depletion_grade(self,sheet,province,year):
        try:
            d = [1,
                 0.76,
                 2,
                 0.4,
                 get(self.db,sheet,province,"Alumina Grade",year),
                 get(self.db,sheet,province,"Reserve",year)
                 ]
            value = max((d[0]+(d[1]-d[0])/d[2])*d[4],d[3]) if d[5] > 0 else 0
            put(self.db,sheet,province,"Alumina Profile Depletion grade",year,value)
            
        except:
            put(self.db,sheet,province,"Alumina Profile Depletion grade",year,0)
            pass
    def alumina_profile_scaled_mean(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Grade",year),
                 get(self.db,sheet,province,"Alumina Profile Starting grade",year),
                 get(self.db,sheet,province,"Alumina Profile Depletion grade",year)
                 ]
            value = (d[0] - min(d[1],d[2]))/(max(d[1],d[2])- min(d[1],d[2]))
            put(self.db,sheet,province,"Alumina Profile Scaled mean",year,value)
            
        except:
            put(self.db,sheet,province,"Alumina Profile Scaled mean",year,0)
            
    def alumina_profile_scaled_variance(self,sheet,province,year):
        try:
            d = [0.12,
                 16,
                 get(self.db,sheet,province,"Alumina Grade",year),
                 get(self.db,sheet,province,"Factor X",year),
                 get(self.db,sheet,province,"Alumina Profile Starting grade",year),
                 get(self.db,sheet,province,"Alumina Profile Depletion grade",year)
                 ]
            value = (pow(d[0]*d[2],2)/(d[1]*d[3]))/pow(max(d[4],d[5])-min(d[4],d[5]),2)
            put(self.db,sheet,province,"Alumina Profile Scaled variance",year,value)
            
        except:
            
            put(self.db,sheet,province,"Alumina Profile Scaled variance",year,0)
            
    def alumina_profile_alpha_value(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Profile Scaled mean",year),
                 get(self.db,sheet,province,"Alumina Profile Scaled variance",year)]
            value = d[0]*(d[0]*(1-d[0])/d[1]-1)
            
            put(self.db,sheet,province,"Alumina Profile Alpha value",year,value)
        except :
            put(self.db,sheet,province,"Alumina Profile Alpha value",year,0)
            
    def alumina_profile_beta_value(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Profile Scaled mean",year),
                 get(self.db,sheet,province,"Alumina Profile Alpha value",year)]
            value = d[1]*(1-d[0])/d[0]
            put(self.db,sheet,province,"Alumina Profile Beta value",year,value)
            
        except:
            put(self.db,sheet,province,"Alumina Profile Beta value",year,0)

    def silica_profile_starting_grade(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Profile Depletion grade",year),
                 get(self.db,sheet,province,"Starting ratio",year)]
            value = d[0]/d[1]
            put(self.db,sheet,province,"Silica Profile Starting grade",year,value)
            pass
        except:
            put(self.db,sheet,province,"Silica Profile Starting grade",year,0)
            
    def silica_profile_depletion_grade(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Profile Starting grade",year),
                 get(self.db,sheet,province,"Depletion ratio",year)
                 ]
            value = d[0]/d[1]
            put(self.db,sheet,province,"Silica Profile Depletion grade",year,value)
            
        except:
            put(self.db,sheet,province,"Silica Profile Depletion grade",year,0)
            pass
    def silica_profile_scaled_mean(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Silica Grade",year),
                 get(self.db,sheet,province,"Starting ratio",year),
                 get(self.db,sheet,province,"Silica Profile Starting grade",year),
                 get(self.db,sheet,province,"Silica Profile Depletion grade",year)
                 ]
            value = (d[0]-min(d[2],d[3]))/(max(d[2],d[3])-min(d[2],d[3]))
            put(self.db,sheet,province,"Silica Profile Scaled mean",year,value)
            
        except:
            put(self.db,sheet,province,"Silica Profile Scaled mean",year,0)
            
    def silica_profile_scaled_variance(self,sheet,province,year):
        try:
            d = [0.215,get(self.db,sheet,province,"Silica Grade",year),
                 get(self.db,sheet,province,"Factor X",year),
                 get(self.db,sheet,province,"Starting ratio",year),
                 get(self.db,sheet,province,"Silica Profile Starting grade",year),
                 get(self.db,sheet,province,"Silica Profile Depletion grade",year)
                 ]
            value = (pow(d[0]*d[1],2)/d[2])/pow(max(d[4],d[5])-min(d[4],d[5]),2) if d[3] > 0 else 0
            put(self.db,sheet,province,"Silica Profile Scaled variance",year,value)
            
        except:
            put(self.db,sheet,province,"Silica Profile Scaled variance",year,0)
            
    def silica_profile_alpha_value(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Silica Profile Scaled mean",year),
                 get(self.db,sheet,province,"Silica Profile Scaled variance",year)
                 ]
            value = d[0]*((d[0]*(1-d[0]))/d[1] - 1) if d[1] > 0 else np.nan
            
            put(self.db,sheet,province,"Silica Profile Alpha value",year,value)
            
        except:
            put(self.db,sheet,province,"Silica Profile Alpha value",year,0)
    def silica_profile_beta_value(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Silica Profile Scaled mean",year),
                 get(self.db,sheet,province,"Silica Profile Alpha value",year)
                 ]
            value = d[1]*(1-d[0])/d[0] if d[0] > 0 else np.nan
            put(self.db,sheet,province,"Silica Profile Beta value",year,value)
            
        except:
            put(self.db,province,"Silica Profile Beta value",year,0)
    def alumina_grade(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Profile Starting grade",'2004'),
                 get(self.db,sheet,province,"Alumina Profile Starting grade",year),
                 get(self.db,sheet,province,"Alumina Profile Depletion grade",'2004'),
                 get(self.db,sheet,province,"C/S Outlook (n+1) % of total",str(int(year)-1) if year > '2004' else '2004'),
                 get(self.db,sheet,province,"Alumina Profile Alpha value",'2004'),
                 get(self.db,sheet,province,"Alumina Profile Beta value",'2004'),
                 get(self.db,sheet,province,"proportion of total reserve remaining",str(int(year)-1) if year > '2004' else '2004')
                 ]
            value = d[0] if year == '2004' else d[1] if d[0] == d[2] and d[3] > 0 else stats.beta.ppf(1-stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2])) if d[0] < d[2] and d[3] > 0 else stats.beta.ppf(stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2]) if d[3] > 0 else 0) 
            
            put(self.db,sheet,province,"Grade Profile Alumina Grade",year,value)
            
        except:
            #print(e)
            put(self.db,sheet,province,"Grade Profile Alumina Grade",year,0)
            
    def silica_grade(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Silica Profile Starting grade",'2004'),
                 get(self.db,sheet,province,"Silica Profile Starting grade",year),
                 get(self.db,sheet,province,"Silica Profile Depletion grade",'2004'),
                 get(self.db,sheet,province,"C/S Outlook (n+1) % of total",str(int(year)-1) if int(year) > 2004 else '2004'),
                 get(self.db,sheet,province,"Silica Profile Alpha value",'2004'),
                 get(self.db,sheet,province,"Silica Profile Beta value",'2004'),
                 get(self.db,sheet,province,"proportion of total reserve remaining",str(int(year)-1) if year > '2004' else '2004'),
                 get(self.db,sheet,province,"Reserve",year),
                 ]
            value = d[0] if year == '2004' else d[1] if d[0] == d[2] and d[3] > 0 else stats.beta.ppf(1-stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2])) if d[0] < d[2] and d[3] > 0 else stats.beta.ppf(stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2]) if d[3] > 0 else 0) 
            #print(value)
            put(self.db,sheet,province,"Grade Profile Silica Grade",year,value)
            
        except:
            put(self.db,sheet,province,"Grade Profile Silica Grade",year,0)

    def as_ratio_drawn(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Reserve",'2004'),
                 get(self.db,sheet,province,"Starting ratio",year),
                 get(self.db,sheet,province,"Closing Stock",str(int(year)-1) if int(year) > 2004 else '2004'),
                 get(self.db,sheet,province,"Usage",str(int(year)-1) if int(year) > 2004 else '2004'),
                 get(self.db,sheet,province,"Grade Profile Alumina Grade",year),
                 get(self.db,sheet,province,"Grade Profile Silica Grade",year),
                 get(self.db,sheet,province,"A/S ratio draw-down",str(int(year)-1) if int(year) > 2004 else '2004')
                 ]
            value = d[1] if d[0] > 0 and year == '2004' else d[4]/d[5] if d[5] > 0 and d[2] > 2*d[3] and d[6] > 0 else 0 
            put(self.db,sheet,province,"A/S ratio draw-down",year,value)
            pass
        except:
            put(self.db,sheet,province,"A/S ratio draw-down",year,0)
    def aa_production_protocol(self,sheet,province,year,dbb):
        try:
            
            d = [
                get(self.db,sheet,province,"Sourcing Mix",year),
                dbb.loc[dbb.category==sheet][year],
                get(self.db,sheet,province,"Bauxite Usage",year),
            ]
            value = d[0]*d[1] if d[2] > 0 else 0
            put(self.db,sheet,province,"Aa Production - based on protocol",year,value)
            #print(value)
        except:
            put(self.db,sheet,province,"Aa Production - based on protocol",year,0)   
    def bauxite_usage_bayer(self,sheet,province,year):
        try:
            d = [
                get(self.db,sheet,province,"Grade Profile Alumina Grade",year),
                get(self.db,sheet,province,"A/S ratio draw-down",year),
                self.globaldb.loc[0,"Handling losses"],
                self.globaldb.loc[0,"Moisture content of ore"],
                self.globaldb.loc[0,"Extraction efficiency - Bayer"],
                self.globaldb.loc[0,"Alumina Quality"],
                1.23,
                get(self.db,sheet,province,"Grade Profile Silica Grade",year),
            ]
            value = np.nan if year == '2004' else 1/(((d[0]-(d[7]*d[6]))*(1-d[2])*(1-d[3])*d[4])/d[5]) if d[1] > 0 else 0

            put(self.db,sheet,province,"Bauxite Usage-Bayer",year,value)
            
        except:
            put(self.db,sheet,province,"Bauxite Usage-Bayer",year,0)
           # 
    def bauxite_usage_bayer_mud_sinter(self,sheet,province,year):
        try:
            d = [
                get(self.db,sheet,province,"Grade Profile Alumina Grade",year),
                get(self.db,sheet,province,"A/S ratio draw-down",year),
                self.globaldb.loc[0,"Handling losses"],
                self.globaldb.loc[0,"Moisture content of ore"],
                self.globaldb.loc[0,"Extraction efficiency - Sinter"],
                self.globaldb.loc[0,"Alumina Quality"],
                0.25,
                get(self.db,sheet,province,"Grade Profile Silica Grade",year),
            ]
            value = np.nan if year == '2004' else 1/((d[0]-d[7]*d[6])*(1-d[2])*(1-d[3])*d[4]/d[5]) if d[1] > 0 else 0
            put(self.db,sheet,province,"Bauxite Usage-Bayer Mud Sinter",year,value)
            pass
        except:
            put(self.db,sheet,province,"Bauxite Usage-Bayer Mud Sinter",year,0)
            
    def bauxite_usage_sinter(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Grade Profile Alumina Grade",year),
                get(self.db,sheet,province,"A/S ratio draw-down",year),
                self.globaldb.loc[0,"Handling losses"],
                self.globaldb.loc[0,"Moisture content of ore"],
                self.globaldb.loc[0,"Extraction efficiency - Sinter"],
                self.globaldb.loc[0,"Alumina Quality"],
            ]
            value = np.nan if year == '2004' else 1/(d[0]*(1-d[2])*(1-d[3])*d[4]/d[5]) if d[1] > 0 else 0
            put(self.db,sheet,province,"Bauxite Usage-Sinter",year,value)
            pass
        except:
            put(self.db,sheet,province,"Bauxite Usage-Sinter",year,0)
            
    def bauxite_usage(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Bauxite Usage-Bayer",year),
                get(self.db,sheet,province,"Bauxite Usage-Bayer Mud Sinter",year),
                get(self.db,sheet,province,"Bauxite Usage-Sinter",year),
                self.datadb.loc["Bayer",str(int(year)-1) if int(year) > 2004 else '2004'],
                self.datadb.loc["Bayer-mud-sinter",str(int(year)-1) if int(year) > 2004 else '2004'],
                self.datadb.loc["sinter",str(int(year)-1) if int(year) > 2004 else '2004'],
            ]
            value = np.nan if year == '2004' else  d[0]*d[3]+d[1]*d[4]+d[2]*d[5]
    
            put(self.db,sheet,province,"Bauxite Usage",year,value)
            pass
        except:
            put(self.db,sheet,province,"Bauxite Usage",year,0)
            
    def bauxite_consumption(self,sheet,province,year):
        
        try:
            d = [0.1,
                 get(self.db,sheet,province,"Bauxite Usage",year),
                 get(self.db,sheet,province,"Aa Production - based on protocol",year)
                 ]
            value = d[0] if year == '2004' else d[1]*d[2]
            put(self.db,sheet,province,"Bauxite Consumption",year,value)
            pass
        except:
            put(self.db,sheet,province,"Bauxite Consumption",year,0)
            
    def bauxite_cumulative(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Reserve",year),
                 get(self.db,sheet,province,"Demand Profile",year),
                 get(self.db,sheet,province,"Bauxite Consumption",year),
                 get(self.db,sheet,province,"Bauxite Cumulative",str(int(year)-1) if year > '2004' else '2004')
                 ]
            value = d[0]*(1-d[1]) if year == '2004' else d[2]+d[3]
            put(self.db,sheet,province,"Bauxite Cumulative",year,value)
            pass
        except:
            put(self.db,sheet,province,"Bauxite Cumulative",year,0)
            
    def opening_stock(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Reserve",year),
                 get(self.db,sheet,province,"Demand Profile",year),
                 get(self.db,sheet,province,"Closing Stock",str(int(year)-1) if int(year) > 2004 else '2004')
                 ]
            value = d[0]*d[1] if year == '2004' else d[2]
            put(self.db,sheet,province,"Opening Stock",year,value)
            
        except:
            put(self.db,sheet,province,"Opening Stock",year,0)
            pass
    def usage(self,sheet,province,year):
        try:
            d = [0.1,
                 get(self.db,sheet,province,"Bauxite Consumption",year)
                ]
            value = d[0] if year == '2004' else d[1]
            put(self.db,sheet,province,"Usage",year,value)
        except:
            pass
    def closing_stock(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Opening Stock",year),
                 get(self.db,sheet,province,"Usage",year)
                 ]
            value = d[0]-d[1] if d[0]-d[1] > 0 else 0
            put(self.db,sheet,province,"Closing Stock",year,value)
            pass
        except:
            put(self.db,sheet,province,"Closing Stock",year,0)
            
    def closing_stock_portion_of_total(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Opening Stock",year),
                 get(self.db,sheet,province,"Closing Stock",year)
                 ]
            value = d[1]/d[0] if d[0] > 0 else 0
            put(self.db,sheet,province,"Closing Stock - portion of total",year,value)
            pass
        except:
            put(self.db,sheet,province,"Closing Stock - portion of total",year,0)
            
    def proportion_of_total_reserve_remaining(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Opening Stock",'2004'),
                 get(self.db,sheet,province,"Closing Stock",year)
                 ]
            value = d[1]/d[0] if d[0] > 0 else 0
            put(self.db,sheet,province,"proportion of total reserve remaining",year,value)
            pass
        except:
            put(self.db,sheet,province,"proportion of total reserve remaining",year,0)
            
    
            
    def cs_outlook(self,sheet,province,year,dbb):
        
        try:
            d = [get(self.db,sheet,province,"Usage",year),
                 get(self.db,sheet,province,"Closing Stock",year),
                 get(self.db,sheet,province,"C/S Outlook (n+1)",str(int(year)-1)  if int(year) > 2004 else '2004'),
                 dbb.loc[dbb.category==sheet][year],
                 dbb.loc[dbb.category==sheet][str(int(year)+1) if int(year) < 2031 else '2031']
                 ]
            value = d[1]-d[0] if year == '2004' else 0 if d[2] == 0 else d[2] if d[1]-d[0] > 0 and d[3] == 0 else (d[1]-d[0]*d[4]/d[3]) if d[1]-d[0] > 0 else 0
            put(self.db,sheet,province,"C/S Outlook (n+1)",year,value)
            pass
        except:
            put(self.db,sheet,province,"C/S Outlook (n+1)",year,0)
            
    def cs_outlook_total(self,sheet,province,year):
        
        try:
            d = [get(self.db,sheet,province,"Opening Stock",'2004'),
                 get(self.db,sheet,province,"C/S Outlook (n+1)",year)
                 ]
            value = d[1]/d[0] if d[0] > 0 else 0
            put(self.db,sheet,province,"C/S Outlook (n+1) % of total",year,value)
            pass
        except:
            put(self.db,sheet,province,"C/S Outlook (n+1) % of total",year,0)
    


    def domesticopenstrip(self,sheet,province,year):
        try:
            d = {"Guangxi":[
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1],
                "Guizhou":[
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,
                    0.7,


                ],
                "Henan":[
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,

                ],
                "Shanxi":[
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,
                    0.9,

                ],
                "Other":[
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,
                    0.4,

                ]
            
            
            }
            value = d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"domestic open-strip",year,value)
        except:
            put(self.db,sheet,province,"domestic open-strip",year,0)
    def domesticunderground(self,sheet,province,year):
        try:
            d = [
                get(self.db,sheet,province,"domestic open-strip",year)
            ]
            value = 1-d[0]
            put(self.db,sheet,province,"domestic underground",year,value)
        except:
            put(self.db,sheet,province,"domestic underground",year,0)
    def miningcostopenstrip(self,sheet,province,year):
        try:
            d = {"Guangxi":[
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,
                    62.76911813,

                ],
                "Guizhou":[
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,	
                    83.42361776,

                ],
                "Henan":[
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,	
                    65.99910812,

                ],
                "Shanxi":[
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,	
                    76.50479179,

                ],
                "Other":[
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011,
                    100.8671011

                ]

            }
            value =  d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"mining cost open-strip",year,value)
        except:
            put(self.db,sheet,province,"mining cost open-strip",year,0)
    def miningcostunderground(self,sheet,province,year):
        try:
            d = {"Guangxi":[
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    59.48325669,
                    ],
                "Guizhou":[
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,
                    80.65788241,

                ],
                "Henan":[
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237,
                    62.67371237

                ],
                "Shanxi":[
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,
                    73.84623429,

                ],
                "Other":[
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,
                    78.33016727,

                ]
            }
            value = d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"mining cost underground",year,value)
        except:
            put(self.db,sheet,province,"mining cost underground",year,0)
    def miningcostdressingopenpit(self,sheet,province,year):
        try:
            d = {"Guangxi":[
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,
                27.23087166,


            ],
            "Guizhou":[
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
            	26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
            	26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879,
                26.57605879

            ],
            "Henan":[
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,
                4.000873088,

            ],
            "Shanxi":[
                3.49520481,
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,	
                3.49520481,

            ],
            "Other":[
                27.48677017,
                27.48677017,
                27.48677017,
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,
                27.48677017,	
                27.48677017,	
                27.48677017,	
                27.48677017,

            ]
            }
            value = d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"mining cost dressing open pit",year,value)
        except:
            put(self.db,sheet,province,"mining cost dressing open pit",year,0)
    def miningcostdressingunderground(self,sheet,province,year):
        try:
            d = {
                "Guangxi":[
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,
                    30.51674331,

                ],
                "Guizhou":[
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759,
                    29.34211759

                ],
                "Henan":[
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,
                    7.326287631,

                ],
                "Shanxi":[
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571,
                    6.15376571

                ],
                "Other":[
                    31.05554034,
                    31.05554034,
                    31.05554034,
                    31.05554034,
                    31.05554034,
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,	
                    31.05554034,

                ]
            }
            value = d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"mining cost dressing underground",year,value)
        except:
            put(self.db,sheet,province,"mining cost dressing underground",year,0)
    def totalmininganddressing(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"domestic open-strip",year),
                 get(self.db,sheet,province,"domestic underground",year),
                 get(self.db,sheet,province,"mining cost open-strip",year),
                 get(self.db,sheet,province,"mining cost underground",year),
                 get(self.db,sheet,province,"mining cost dressing open pit",year),
                 get(self.db,sheet,province,"mining cost dressing underground",year),

                ]
            value = d[0]*(d[2]+d[4])+d[1]*(d[3]+d[5])
            put(self.db,sheet,province,"total mining and dressing",year,value)
        except:
            put(self.db,sheet,province,"total mining and dressing",year,0)
    def MiningRoyality(self,sheet,province,year):
        try:
            d = {
                "Guangxi":[
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                    24,
                ],
                "Guizhou":[
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,	
                    24,

                ],
                "Henan":[
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,
                    30,	
                    30,	
                    30,	
                    30,	
                    30,	
                    30,	
                    30,	
                    30,	
                    30,
                    30,	
                    30,	
                    30,	
                    30,

                ],
                "Shanxi":[
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,

                ],
                "Other":[
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,
                    20,

                ]
            }
            value = d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"Mining Royality",year,value)
        except:
            put(self.db,sheet,province,"Mining Royality",year,0)
    def Localcharges(self,sheet,province,year):
        try:
            d = {"Guangxi":[
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,   
                0,   
                0,   
                0,   
                0,   
                0],
                "Guizhou":[
                    0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,   
                0,   
                0,   
                0,   
                0,   
                0
                ],
                "Henan":[
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,	
                    10,
                ],
                "Shanxi":[
                    20,
                    20,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20	,
                    20,

                ],
                "Other":[
                     0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,   
                0,   
                0,   
                0,   
                0,   
                0
                ]
            }

            value = d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"Local charges",year,value)
        except:
            put(self.db,sheet,province,"Local charges",year,0)
    def totalgovernmentcharges(self,sheet,province,year): # 19 cell 499
        try:
            d = [#precedents here
                get(self.db,sheet,province,"Mining Royality",year),
                get(self.db,sheet,province,"Local charges",year)
            ]
            value = d[0]+d[1] #formula here
            put(self.db,sheet,province,"totalgovernmentcharges",year,value)
        except:
            put(self.db,sheet,province,"totalgovernmentcharges",year,0)
    def Road01(self,sheet,province,year):# 19 cell 506
        try:
            d = [
                get(self.db,sheet,province,"Road-1",str(int(year)-1) if int(year) > 2005 else "2005" ),#precedents here
            ]
            d2 = {
                "Guangxi":0,
                "Guizhou":300,
                "Henan":0,
                "Shanxi":0,
                "Other":30
            }
            value = np.nan if year == '2004' else d2[province] if year == '2005' else d[0]#formula here
            put(self.db,sheet,province,"Road-1",year,value)
        except:
            put(self.db,sheet,province,"Road-1",year,0)
    def Rail01(self,sheet,province,year):# 19 cell 507
        try:
            d = [
                get(self.db,sheet,province,"Rail-1",str(int(year)-1) if int(year) > 2005 else "2005" ),
            ]
            value = np.nan if year == '2004' else 0 if year == '2005' else d[0] #formula here
            put(self.db,sheet,province,"Rail-1",year,value)
        except:
            put(self.db,sheet,province,"Rail-1",year,0)
    def totalminingcashcost(self,sheet,province,year):# 19 cell 502
        try:
            d = [
                get(self.db,sheet,province,"totalgovernmentcharges",year),
                get(self.db,sheet,province,"total mining and dressing",year)
            ]
            value = d[0]+d[1]#formula here
            put(self.db,sheet,province,"totalminingcashcost",year,value)
        except:
            put(self.db,sheet,province,"totalminingcashcost",year,0)
    def Road0(self,sheet,province,year):# 19 cell 509
        try:
            d1 = [
                get(self.db,sheet,province,"Road-1",year),
            ]
            d2 = {"Guangxi":[
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,

            ],
            "Guizhou":[
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,
                142,

            ],
            "Henan":[
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            "Shanxi":[
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
            "Other":[
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14,
              14	,
              14	,
              14	,
              14	,
              14,

            ]}
            value = d2[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"Road0",year,value)
        except:
            put(self.db,sheet,province,"Road0",year,0)
    def Rail0(self,sheet,province,year):# 19 cell 510
        try:
            d1 = [
                get(self.db,sheet,province,"Rail-1",year),
            ]
            d2 = [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,

            ]
            value = 0 if d1[0] <= 0 else d2[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0#formula here
            put(self.db,sheet,province,"Rail0",year,value)
        except:
            put(self.db,sheet,province,"Rail0",year,0)
    def totalrailroad(self,sheet,province,year):# 19 cell 511
        try:
            d = [
                get(self.db,sheet,province,"Road0",year),
                get(self.db,sheet,province,"Rail0",year),
            ]
            value = d[0]+d[1]#formula here
            put(self.db,sheet,province,"totalrailroad",year,value)
        except:
            put(self.db,sheet,province,"totalrailroad",year,0)
    def closedloopcostsfob(self,sheet,province,year):# 19 cell 472
        try:
            d = [
                10,
                10,
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,	
                10,
                10,
                10,
                10,
                10,
                10,
                10,
                10,
                10,
                10,

            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0#formula here
            put(self.db,sheet,province,"closedloop costs fob",year,value)
        except:
            put(self.db,sheet,province,"closedloop costs fob",year,0)
    def closedloopseafreight(self,sheet,province,year):# 19 cell 473
        try:
            d = [
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,	
                15,
            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0#formula here
            put(self.db,sheet,province,"closedloop sea freight",year,value)
        except:
            put(self.db,sheet,province,"closedloop sea freight",year,0)
    def closedloopporttorefinery(self,sheet,province,year):# 19 cell 474
        try:
            d = [
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,

            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0#formula here
            put(self.db,sheet,province,"closedloop port to refinery",year,value)
        except:
            put(self.db,sheet,province,"closedloop port to refinery",year,0)
    def closedlooptotalrmb(self,sheet,province,year):# 19 cell 475
        try:
            d = [
                get(self.db,sheet,province,"closedloop costs fob",year),
                get(self.db,sheet,province,"closedloop sea freight",year),
                get(self.db,sheet,province,"closedloop port to refinery",year),
                self.datadb.loc["Exchange rate",year],
            ]
            value = (d[0]+d[1])*d[3] +d[2]#formula here
            put(self.db,sheet,province,"closedloop total-rmb",year,value)
        except:
            put(self.db,sheet,province,"closedloop total-rmb",year,0)
    def closedlooptotalUS(self,sheet,province,year):# 19 cell 476
        try:
            d = [
                get(self.db,sheet,province,"closedloop total US",year),
                self.datadb.loc["Exchange rate",year],
            ]
            value = d[0]/d[1]#formula here
            put(self.db,sheet,province,"closedloop total US",year,value)
        except:
            put(self.db,sheet,province,"closedloop total US",year,0)
    def thirdpartypurchasefob(self,sheet,province,year):# 19 cell 478
        try:
            d = [
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,
                20,

            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0#formula here
            put(self.db,sheet,province,"thirdparty purchase fob",year,value)
        except:
            put(self.db,sheet,province,"thirdparty purchase fob",year,0)
    def thirdpartyseafreight(self,sheet,province,year):# 19 cell 479
        try:
            d = [15,
            	15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                15,
                
            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0#formula here
            put(self.db,sheet,province,"thirdparty sea freight",year,value)
        except:
            put(self.db,sheet,province,"thirdparty sea freight",year,0)
    def thirdpartyporttorefinery(self,sheet,province,year):# 19 cell 480
        try:
            d = [
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0#formula here
            put(self.db,sheet,province,"thirdparty port to refinery",year,value)
        except:
            put(self.db,sheet,province,"thirdparty port to refinery",year,0)
    def thirdpartytotalrmb(self,sheet,province,year):# 19 cell 481
        try:
            d = [
                get(self.db,sheet,province,"thirdparty purchase fob",year),
                get(self.db,sheet,province,"thirdparty sea freight",year),
                get(self.db,sheet,province,"thirdparty port to refinery",year),
                self.datadb.loc["Exchange rate",year],

            ]
            value = (d[0]+d[1])*d[3]+d[2]#formula here
            put(self.db,sheet,province,"thirdparty total-rmb",year,value)
        except:
            put(self.db,sheet,province,"thirdparty total-rmb",year,0)
    def thirdpartytotalUS(self,sheet,province,year):# 19 cell 482
        try:
            d = [
                get(self.db,sheet,province,"thirdparty total-rmb",year),
                self.datadb.loc["Exchange rate",year],
            ]
            value = d[0]/d[1]#formula here
            put(self.db,sheet,province,"thirdparty total US",year,value)
        except:
            put(self.db,sheet,province,"thirdparty total US",year,0)
    def totalminingdressingrefinery(self,sheet,province,year):# 19 cell 513
        try:
            d = [
                get(self.db,sheet,province,"totalminingcashcost",year),
                get(self.db,sheet,province,"totalrailroad",year),
            ]
            value = d[0]+d[1]
            put(self.db,sheet,province,"total mining dressing refinery",year,value)
        except:
            put(self.db,sheet,province,"total mining dressing refinery",year,0)
    def PurchaseFOT(self,sheet,province,year):# 19 cell 518
        try:
            d = {"Guangxi":[219,
            	219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
                219,
            ],
            "Guizhou":[
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,	
                234,

            ],
            "Henan":[
                290,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290	,
                290,

            ],
            "Shanxi":[
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,
                280,

            ],
            "Other":[
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316	,
                316,

            ]
            }
            value = d[province][int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"Purchase-FOT",year,value)
        except:
            put(self.db,sheet,province,"Purchase-FOT",year,0)
    def Road1(self,sheet,province,year):# 19 cell 521
        try:
            d = [
                get(self.db,sheet,province,"Road-1",year),
                

            ]
            value = d[0]
            put(self.db,sheet,province,"Road1",year,value)
        except:
            put(self.db,sheet,province,"Road1",year,0)
    def Rail1(self,sheet,province,year):# 19 cell 522
        try:
            d = [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,	
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0

            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"Rail1",year,value)
        except:
            put(self.db,sheet,province,"Rail1",year,0)
    def Road2(self,sheet,province,year):# 19 cell 524
        try:
            d = [
                get(self.db,sheet,province,"Road0",year),
            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"Road2",year,value)
        except:
            put(self.db,sheet,province,"Road2",year,0)
    def Rail2(self,sheet,province,year):# 19 cell 525
        try:
            d = [
               get(self.db,sheet,province,"Rail0",year),
            ]
            value = d[0]
            put(self.db,sheet,province,"Rail2",year,value)
        except:
            put(self.db,sheet,province,"Rail2",year,0)
    def Minetorefinery(self,sheet,province,year):# 19 cell 529
        try:
            d = [
                get(self.db,sheet,province,"Road2",year),
                get(self.db,sheet,province,"Rail2",year),
            ]
            value = d[0]+d[1]
            put(self.db,sheet,province,"Mine to refinery",year,value)
        except:
            put(self.db,sheet,province,"Mine to refinery",year,0)
    def totalpurchaceandminetorefinary(self,sheet,province,year):# 19 cell 530
        try:
            d = [
                get(self.db,sheet,province,"Mine to refinery",year),
                get(self.db,sheet,province,"Purchase-FOT",year),
            ]
            value = d[0]+d[1]
            put(self.db,sheet,province,"total purchace and mine to refinary",year,value)
        except:
            put(self.db,sheet,province,"total purchace and mine to refinary",year,0)
    def importclosedloop(self,sheet,province,year):# 19 cell 462
        try:
            d = [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,	
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"import closedloop",year,value)
        except:
            put(self.db,sheet,province,"import closedloop",year,0)
    def importthirdparty(self,sheet,province,year):# 19 cell 463
        try:
            d = [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,	
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ]
            value = d[int(year)-2005 if int(year) < 2032 else 0] if year != '2004' else 0
            put(self.db,sheet,province,"import thirdparty",year,value)
        except:
            put(self.db,sheet,province,"import thirdparty",year,0)
    def domesticclosedloop(self,sheet,province,year):# 19 cell 464
        try:
            d = [
                get(self.db,sheet,province,"Reserve",'2004'),
                get(self.db,sheet,province,"Closed",'2004'), 
            ]
            value = d[1] if d[0] > 0 else 0
            put(self.db,sheet,province,"domestic closed loop",year,value)
        except:
            put(self.db,sheet,province,"domestic closed loop",year,0)
    def domesticthirdparty(self,sheet,province,year):# 19 cell 465
        try:
            d = [
                get(self.db,sheet,province,"Reserve",'2004'),
                get(self.db,sheet,province,"import closedloop",year),
                get(self.db,sheet,province,"import thirdparty",year),
                get(self.db,sheet,province,"domestic closed loop",year),
                get(self.db,sheet,province,"domestic thirdparty",str(int(year)-1) if int(year) > 2005 else "2004"),
            ]
            value = 1-(d[1]+d[2]+d[3]) if d[0] > 0 and year == "2004" else 0 if year == "2004" else d[4]
            put(self.db,sheet,province,"domestic thirdparty",year,value)
        except:
            put(self.db,sheet,province,"domestic thirdparty",year,0)
    def Bauxitecostdeliveredtoaarefinery(self,sheet,province,year):# 19 cell 468
        try:
            d = [
                get(self.db,sheet,province,"import closedloop",year),
                get(self.db,sheet,province,"import thirdparty",year),
                get(self.db,sheet,province,"domestic closed loop",year),
                get(self.db,sheet,province,"domestic thirdparty",year),
                get(self.db,sheet,province,"closedloop total-rmb",year),
                get(self.db,sheet,province,"thirdparty total-rmb",year),
                get(self.db,sheet,province,"total purchace and mine to refinary",year),
                get(self.db,sheet,province,"total mining dressing refinery",year),

            ]
            value = d[0]*d[4]+d[1]*d[5]+d[2]*d[7]+d[3]*d[6]
            put(self.db,sheet,province,"Bauxite cost delivered to aa refinery",year,value)
        except:
            put(self.db,sheet,province,"Bauxite cost delivered to aa refinery",year,0)
    def BauxitePriceCost1(self,sheet,province,year):# 19 cell 368
        try:
            d = [
                get(self.db,sheet,province,"Bauxite cost delivered to aa refinery",year),
                get(self.db,sheet,province,"Grade Profile Silica Grade",year),
            ]
            value = d[0] if d[1] > 0 else 0
            put(self.db,sheet,province,"Bauxite Price-Cost1",year,value)
        except:
            put(self.db,sheet,province,"Bauxite Price-Cost1",year,0)
    def BauxitePriceCost2(self,sheet,province,year):# 19 cell 376
        try:
            d = [
                get(self.db,sheet,province,"Bauxite Price-Cost1",year),
                get(self.db,sheet,province,"Bauxite Usage",year),
            ]
            value = d[0]*d[1]
            put(self.db,sheet,province,"Bauxite Price-Cost2",year,value)
        except:
            put(self.db,sheet,province,"Bauxite Price-Cost2",year,0)

    def CausticCost(self,sheet,province,year):# 19 cell 392
        try:
            d = [
                0.6,
                0.1,
                0.2,
                get(self.db,sheet,province,"Bauxite Usage",year),
                self.datadb[year]['Bayer'],
                self.datadb[year]["Bayer-mud-sinter"],
                self.datadb[year]["sinter"],
                get(self.db,sheet,province,"A/S ratio draw-down",year),
                get(self.db,sheet,province,"Grade Profile Silica Grade",year),
                0.035,
                self.datadb[year]["Caustic Price"],
                self.datadb[year]["Sodium Carbonate Price"],
                104,
                78

            ]
            value = (d[0]*d[8]*d[3]+d[9])*d[4]*d[10] + d[1]*d[6]*d[11]*d[12]/d[13]+(d[9]+d[2]*d[0]*d[8]*d[3])*d[11]*d[12]/d[13] if d[7] > 0 else 0
            
            put(self.db,sheet,province,"Caustic Cost",year,value)
        except:
            put(self.db,sheet,province,"Caustic Cost",year,0)
    def BauxiteCausticCost(self,sheet,province,year):# 19 cell 400
        try:
            d = [
                get(self.db,sheet,province,"Caustic Cost",year),
                get(self.db,sheet,province,"Bauxite Price-Cost2",year),
            ]
            value = d[0]+d[1]
            put(self.db,sheet,province,"Bauxite & Caustic Cost",year,value)
        except:
            put(self.db,sheet,province,"Bauxite & Caustic Cost",year,0)
    def PreCalculation(self,sheet,province,year):# 19 cell 410
        try:
            d = [
                get(self.db,sheet,province,"Bauxite & Caustic Cost",year),
                self.globaldb.loc[0,"Sourcing Factor"],
            ]
            value = pow((1/d[0]),d[1])*1000000000 if d[0] > 0 else 0
            put(self.db,sheet,province,"Pre-Calculation",year,value)
        except:
            put(self.db,sheet,province,"Pre-Calculation",year,0)         
    def SourcingMix(self,sheet,province,year):# 19 cell 418
        try:
            
            d = [
                get(self.db,sheet,"Guangxi","Pre-Calculation",year),
                get(self.db,sheet,"Guizhou","Pre-Calculation",year),
                get(self.db,sheet,"Henan","Pre-Calculation",year),
                get(self.db,sheet,"Shanxi","Pre-Calculation",year),
                get(self.db,sheet,"Other","Pre-Calculation",year),
                get(self.db,sheet,province,"Pre-Calculation",year),


            ]
            value = d[5]/(d[0]+d[1]+d[2]+d[3]+d[4]) if int(year) > 2004 else 0
            
            put(self.db,sheet,province,"Sourcing Mix",year,value)
        except:
            put(self.db,sheet,province,"Sourcing Mix",year,0)

            
    def calc_all1(self,sheet,province,year):                                   # The sequence of functions entered here are the sequence of calculation
        Bauxite.reserve(self,sheet,province,year)
        Bauxite.aluminagrade(self,sheet,province,year)
        Bauxite.asratio(self,sheet,province,year)
        Bauxite.closed(self,sheet,province,year)
        Bauxite.demand(self,sheet,province,year)
        Bauxite.usagee(self,sheet,province,year)
        Bauxite.factorx(self,sheet,province,year)
        Bauxite.silica(self,sheet,province,year)
        Bauxite.purchased(self,sheet,province,year)
        Bauxite.starting_ratio(self,sheet,province,year)
        Bauxite.depletion_ratio(self,sheet,province,year)
        Bauxite.alumina_profile_starting_grade(self,sheet,province,year)
        Bauxite.alumina_profile_depletion_grade(self,sheet,province,year)
        Bauxite.alumina_profile_scaled_mean(self,sheet,province,year)
        Bauxite.alumina_profile_scaled_variance(self,sheet,province,year)
        Bauxite.alumina_profile_alpha_value(self,sheet,province,year)
        Bauxite.alumina_profile_beta_value(self,sheet,province,year)
        Bauxite.silica_profile_starting_grade(self,sheet,province,year)
        Bauxite.silica_profile_depletion_grade(self,sheet,province,year)
        Bauxite.silica_profile_scaled_mean(self,sheet,province,year)
        Bauxite.silica_profile_scaled_variance(self,sheet,province,year)
        Bauxite.silica_profile_alpha_value(self,sheet,province,year)
        Bauxite.silica_profile_beta_value(self,sheet,province,year)
        Bauxite.alumina_grade(self,sheet,province,year)
        Bauxite.silica_grade(self,sheet,province,year)
        Bauxite.as_ratio_drawn(self,sheet,province,year)
        
        Bauxite.bauxite_usage_bayer(self,sheet,province,year)
        Bauxite.bauxite_usage_sinter(self,sheet,province,year)
        Bauxite.bauxite_usage_bayer_mud_sinter(self,sheet,province,year)
        Bauxite.bauxite_usage_sinter(self,sheet,province,year)
        Bauxite.bauxite_usage(self,sheet,province,year)


        Bauxite.domesticopenstrip(self,sheet,province,year)
        Bauxite.domesticunderground(self,sheet,province,year)
        Bauxite.miningcostopenstrip(self,sheet,province,year)
        Bauxite.miningcostunderground(self,sheet,province,year)
        Bauxite.miningcostdressingopenpit(self,sheet,province,year)
        Bauxite.miningcostdressingunderground(self,sheet,province,year)
        Bauxite.totalmininganddressing(self,sheet,province,year)
        Bauxite.MiningRoyality(self,sheet,province,year)
        Bauxite.Localcharges(self,sheet,province,year)
        Bauxite.totalgovernmentcharges(self,sheet,province,year)
        Bauxite.Road01(self,sheet,province,year)
        Bauxite.Rail01(self,sheet,province,year)
        Bauxite.totalminingcashcost(self,sheet,province,year)
        Bauxite.Road0(self,sheet,province,year)
        Bauxite.Rail0(self,sheet,province,year)
        Bauxite.totalrailroad(self,sheet,province,year)
        Bauxite.closedloopcostsfob(self,sheet,province,year)
        Bauxite.closedloopseafreight(self,sheet,province,year)
        Bauxite.closedloopporttorefinery(self,sheet,province,year)
        Bauxite.closedlooptotalrmb(self,sheet,province,year)
        Bauxite.closedlooptotalUS(self,sheet,province,year)
        Bauxite.thirdpartypurchasefob(self,sheet,province,year)
        Bauxite.thirdpartyseafreight(self,sheet,province,year)
        Bauxite.thirdpartyporttorefinery(self,sheet,province,year)
        Bauxite.thirdpartytotalrmb(self,sheet,province,year)
        Bauxite.thirdpartytotalUS(self,sheet,province,year)
        Bauxite.totalminingdressingrefinery(self,sheet,province,year)
        Bauxite.PurchaseFOT(self,sheet,province,year)
        Bauxite.Road1(self,sheet,province,year)
        Bauxite.Rail1(self,sheet,province,year)
        Bauxite.Road2(self,sheet,province,year)
        Bauxite.Rail2(self,sheet,province,year)
        Bauxite.Minetorefinery(self,sheet,province,year)
        Bauxite.totalpurchaceandminetorefinary(self,sheet,province,year)
        Bauxite.importclosedloop(self,sheet,province,year)
        Bauxite.importthirdparty(self,sheet,province,year)
        Bauxite.domesticclosedloop(self,sheet,province,year)
        Bauxite.domesticthirdparty(self,sheet,province,year)
        Bauxite.Bauxitecostdeliveredtoaarefinery(self,sheet,province,year)
        Bauxite.BauxitePriceCost1(self,sheet,province,year)
        Bauxite.BauxitePriceCost2(self,sheet,province,year)
        Bauxite.CausticCost(self,sheet,province,year)
        Bauxite.BauxiteCausticCost(self,sheet,province,year)
        Bauxite.PreCalculation(self,sheet,province,year)
        
    def calc_all2(self,sheet,province,year,dbb):
        Bauxite.SourcingMix(self,sheet,province,year)
        Bauxite.aa_production_protocol(self,sheet,province,year,dbb)
        Bauxite.bauxite_consumption(self,sheet,province,year)
        Bauxite.bauxite_cumulative(self,sheet,province,year)
        Bauxite.usage(self,sheet,province,year)
        Bauxite.opening_stock(self,sheet,province,year)
        Bauxite.closing_stock(self,sheet,province,year)
        Bauxite.closing_stock_portion_of_total(self,sheet,province,year)
        Bauxite.proportion_of_total_reserve_remaining(self,sheet,province,year)
        Bauxite.cs_outlook(self,sheet,province,year,dbb)
        Bauxite.cs_outlook_total(self,sheet,province,year)



'''
list of db from csv file

'''
proddata1 = pd.read_csv("proddatah.csv")
proddata2 = pd.read_csv("proddatae2.csv")
proddata3 = pd.read_csv("proddatae.csv")
proddata4 = pd.read_csv("proddatae.csv")
proddata5 = pd.read_csv("proddatae.csv")
proddata6 = pd.read_csv("proddatae.csv")
proddata7 = pd.read_csv("proddatae.csv")
production = pd.read_csv("proddatae.csv")
depdata1 = pd.read_csv("depdatae.csv")
depdata2 = pd.read_csv("depdatae.csv")
depdata3 = pd.read_csv("depdatae.csv")
depdata4 = pd.read_csv("depdatae.csv")
depdata5 = pd.read_csv("depdatae.csv")
depdata6 = pd.read_csv("depdatae.csv")
depdata7 = pd.read_csv("depdatah.csv")
cockpitdata1 = pd.read_csv("cockpitdatah1.csv")
cockpitdata2 = pd.read_csv("cockpitdatae.csv")
cockpitdata3 = pd.read_csv("cockpitdatah2.csv")
class productiondata():
    def __init__(self,proddata1,proddata2,proddata3,proddata4,proddata5,proddata6,proddata7,production,depdata1,depdata2,depdata3,depdata4,depdata5,depdata6,depdata7,cockpitdata1,cockpitdata2,cockpitdata3):
        self.proddata1 = proddata1
        self.proddata2 = proddata2
        self.proddata3 = proddata3
        self.proddata4 = proddata4
        self.proddata5 = proddata5
        self.proddata6 = proddata6
        self.proddata7 = proddata7
        self.production = production
        self.depdata1 = depdata1
        self.depdata2 = depdata2
        self.depdata3 = depdata3
        self.depdata4 = depdata4
        self.depdata5 = depdata5
        self.depdata6 = depdata6
        self.depdata7 = depdata7
        self.cockpitdata1 = cockpitdata1
        self.cockpitdata2 = cockpitdata2
        self.cockpitdata3 = cockpitdata3
    def calcprod2(self,db,sheet,year,i):
        v = db.loc[self.proddata2['refinery'][i],year][self.proddata2['province'][i]]["Bauxite Consumption"]
        self.proddata2.set_value(i,year,v)
    def calcprod3(self,year,i):
        v = self.proddata2.loc[self.proddata2.refinery==self.proddata2['refinery'][i]][year].sum()
        self.proddata3.set_value(i,year,v)
    def calcprod4(self,year,i):
        if int(year) > 2012:
            v = self.proddata1[year][i]
            w = 0
            for j in range(2005,int(year)):
                w += self.proddata3.loc[str(j)][i]
            if w != 0 and self.proddata3[year][i] == 0 and self.proddata1[year][i] != 0:
                self.proddata4.set_value(i,year,self.proddata1.loc[year][i])
    def calcprod5(self,year,i):
        if self.proddata4[year][i] > 0:
            self.proddata5.set_value(i,year,self.proddata1[year][i])        
    def calcprod6(self,year,i):
        v = self.depdata7.loc[self.depdata7.category==self.proddata5['category'][i]][self.depdata7.bauxite==self.proddata5['bauxite'][i]][year].sum()
        w = self.depdata7.loc[self.depdata7.technology==self.proddata5['technology'][i]][self.depdata7.bauxite==self.proddata5['bauxite'][i]][year].sum()
        self.proddata6.set_value(i,year,v+w)
    def calcprod7(self,year,i):
        s = 2019
        if int(year) < s:
            v = self.proddata1[year][i]
            self.proddata7.set_value(i,year,v)
        else:
            v = self.proddata6[year][i]
            self.proddata7.set_value(i,year,v)
    def production(self,sheet,year,i):
        self.production.set_value(i,year,self.proddata7[year][i])
        pass#sumproduct()
    def calcdep1(self,year,i):
        v = self.proddata5.loc[self.proddata5.category==self.depdata1['category'][i]][self.proddata5.bauxite==self.depdata1['bauxite'][i]][year].sum()
        w = self.proddata5.loc[self.proddata5.technology==self.depdata1['technology'][i]][self.proddata5.bauxite==self.depdata1['bauxite'][i]][year].sum()
        self.depdata1.set_value(i,year,v+w)
    def calcdep2(self,year,i):
        v = (1-self.cockpitdata2[year][i])*self.depdata1[year][i]
        self.depdata2.set_value(i,year,v)
    def calcdep3(self,year,i):
        v = self.depdata2[str(int(year)-1) if int(year) > 2005 else "2005"][i]
        w = self.depdata2[year][i]
        x = 0
        y = w + (w-v)*x
        self.depdata3.set_value(i,year,y)
    def calcdep4(self,year,i):
        v = self.proddata1.loc[self.proddata1.category==self.depdata4['category'][i]][self.proddata1.bauxite==self.depdata4['bauxite'][i]][year].sum()
        w = self.proddata1.loc[self.proddata1.technology==self.depdata4['technology'][i]][self.proddata1.bauxite==self.depdata4['bauxite'][i]][year].sum()
        self.depdata4.set_value(i,year,v+w)

    def calcdep5(self,year,i):
        v = self.depdata1[year][i]
        w = self.depdata4[year][i]
        self.depdata4.set_value(i,year,v-w)
    def calcdep6(self,year,i):
        v = self.depdata5[str(int(year)-1) if int(year) > 2005 else "2005"][i]
        w = self.depdata5[year][i]
        x = 0
        y = w + (w-v)*x
        self.depdata6.set_value(i,year,y)
    def calcdep7(self,year,i):
        if cockpitdata3 == "yes" and self.depdata6.loc[year][i] > 0:
            v = (self.depdata6[year][i]+self.depdata3[year][i])/self.depdata6[year][i]
            self.depdata6.set_value(i,year,v)
            
    def calccock1(self,year):
        v = self.cockpitdata1[year][2]+self.cockpitdata1[year][3]
        v=v/2
        self.cockpitdata1.set_value(4,year,v)
    def calccock2(self,year,i):
        s = "yes"
        if s != "no":
            self.cockpitdata2.set_value(i,year,self.cockpitdata2.loc[year][i])
    def pcalcall1(self,sheet,year):
        productiondata.calccock1(self,year)
        for j in range(103):                               # here j is row number of db
            productiondata.calcprod6(self,year,j)
        for j in range(103):
            productiondata.calcprod7(self,year,j)
        for j in range(103):
            productiondata.production(self,sheet,year,j)
    def pcalcall2(self,db,sheet,year):
        for j in range(550):
            productiondata.calcprod2(self,db,sheet,year,j)
        for j in range(103):
            productiondata.calcprod3(self,year,j)
        for j in range(103):
            productiondata.calcprod4(self,year,j)
        for j in range(103):
            productiondata.calcprod5(self,year,j)
        for j in range(5):
            productiondata.calccock2(self,year,j)
        for j in range(8):
            productiondata.calcdep1(self,year,j)
        for j in range(8):
            productiondata.calcdep2(self,year,j)
        for j in range(8):
            productiondata.calcdep3(self,year,j)
        for j in range(8):
            productiondata.calcdep4(self,year,j)
        for j in range(8):
            productiondata.calcdep5(self,year,j)
        for j in range(8):
            productiondata.calcdep6(self,year,j)
        for j in range(8):
            productiondata.calcdep7(self,year,j)
"""
sequence

cockpitdata1
proddata6
proddata7
production
calcall2
proddata2
proddata3
proddata4
proddata5
cockpitdata2
depdata1
depdata2
depdata3
depdata3
depdata4
depdata5
depdata7

"""
# proddata1 with hardcoded value 137,406
# cockpitdata3 with hardcoded value 88888

# production precedent proddata7 8888 
# aaproduction precedent production 8888
# bauxiteconsumption precedent aaproduction 8888
# proddata2 with precedent bauxiteconsumption 8888
# depdata1 with precedent sumifs of proddata2 8888
# depdata4 with precedent sumifs of proddata1 88888
# depdata5 with precedent depdata1,depdata4 88888
# depdata6 with precedent pre depdata5 8888
# depdata7 with precedent cockpitdata3,depdata4,depdata6+(hardcoded 1) 8888
# proddata6 with precedent proddata1,depdata7 88888
# proddata7 with precedent proddata1,proddata6 88888




# proddata2 with precedent bauxiteconsumption L119
# proddata3 with precedent sumifs of proddata2 
# proddata4 with precedent pre proddata3,proddata1
# proddata5 with precedent proddata4,proddata1
# depdata1 with precedent sumifs of proddata2
# cockpitdata1 with half-hardcoded value
# cockpitdata2 with precedent cockpitdata1 
# depdata2 with precedent cockpitdata2,depdata1
# depdata3 with precedent pre depdata2
# depdata4 with precedent sumifs of proddata1
# depdata5 with precedent depdata1,depdata4
# depdata6 with precedent pre depdata5
# cockpitdata3 with hardcoded value
# depdata7 with precedent cockpitdata3,depdata3,depdata6+(hardcoded 1) 701
# proddata6 with precedent proddata1,depdata7
# proddata7 with precedent proddata1,proddata6


# this is another set of province for provincial db
Province = [
    'Shanxi',
    'Henan',
    'Guangxi',
    'Guizhou',
    'Yunnan',
    'Chongqing',
    'Shandong',
    'Hebei',
    'Hunan',
    'IM',
]

# parameters of provincial
proparam = [
    'mining characteristics mine automation level',
    'mining characteristics mine depth open pit',
    'mining characteristics mine depth underground',
    'mining characteristics dressing automation level',
    'mining characteristics production',
    'mining characteristics mine type open',
    'mining characteristics mine type underground',
    'mining characteristics stripping ratio open pit',
    'mining characteristics stripping ratio underground',
    'open pit mine mining cost electricity consumption',
    'open pit mine mining cost electricity price',
    'open pit mine mining cost electricity cost',
    'open pit mine mining cost diesel usage',
    'open pit mine mining cost diesel price',
    'open pit mine mining cost diesel cost',
    'open pit mine mining cost labour productivity',
    'open pit mine mining cost labour rate',
    'open pit mine mining cost labour cost',
    'open pit mine mining cost freight cost',
    'open pit mine mining cost other cost',
    'open pit mine mining cost ore mining cost',
    'open pit mine mining cost stripping ratio',
    'open pit mine mining cost waste production',
    'open pit mine mining cost electricity consumption2',
    'open pit mine mining cost electricity price',
    'open pit mine mining cost electricity cost2',
    'open pit mine mining cost diesel usage2',
    'open pit mine mining cost diesel price2',
    'open pit mine mining cost diesel cost2',
    'open pit mine mining cost labour productivity2',
    'open pit mine mining cost labour rate2',
    'open pit mine mining cost labour cost2',
    'open pit mine mining cost other',
    'open pit mine mining cost stripping cost',
    'open pit mine mining cost total stripping cost',
    'open pit mine mining cost open pit mining cost',
    'open pit mine dressing cost automation and electricity usage low',
    'open pit mine dressing cost automation and electricity usage medium',
    'open pit mine dressing cost automation and electricity usage high',
    'open pit mine dressing cost electricity consumption',
    'open pit mine dressing cost electricity price',
    'open pit mine dressing cost electricity cost',
    'open pit mine dressing cost automation and labour productivity low',
    'open pit mine dressing cost automation and labour productivity medium',
    'open pit mine dressing cost automation and labour productivity high',
    'open pit mine dressing cost labour productivity',
    'open pit mine dressing cost labour rate',
    'open pit mine dressing cost labour cost',
    'open pit mine dressing cost auxillary material cost',
    'open pit mine dressing cost total dressing cost',
    'underground mining cost automation and electricity usage low',
    'underground mining cost automation and electricity usage medium',
    'underground mining cost automation and electricity usage high',
    'underground mining cost size factor',
    'underground mining cost depth factor',
    'underground mining cost electricity consumption',
    'underground mining cost electricity price',
    'underground mining cost electricity cost',
    'underground mining cost automation and productivity low',
    'underground mining cost automation and productivity medium',
    'underground mining cost automation and productivity high',
    'underground mining cost depth factor',
    'underground mining cost automation factor',
    'underground mining cost labour productivity',
    'underground mining cost labour price',
    'underground mining cost labour cost',
    'underground mining cost diesel usage',
    'underground mining cost diesel price',
    'underground mining cost diesel cost',
    'underground mining cost freight cost',
    'underground mining cost other cost',
    'underground mining cost total mining cost',
    'underground mining cost underground mining cost',
    'underground dressing cost automation and electricity usage low',
    'underground dressing cost automation and electricity usage medium',
    'underground dressing cost automation and electricity usage high',
    'underground dressing cost electricity consumption',
    'underground dressing cost electricity price',
    'underground dressing cost electricity cost',
    'underground dressing cost automation and labour productivity low',
    'underground dressing cost automation and labour productivity medium',
    'underground dressing cost automation and labour productivity high',
    'underground dressing cost labour productivity',
    'underground dressing cost labour rate',
    'underground dressing cost labour cost',
    'underground dressing cost auxillary material cost',
    'underground dressing cost total dressing cost',
    'underground state',
    'underground local',
    'underground total',
    'underground road',
    'underground rail',
    'underground purchace price',
    'underground wage',
    'underground caustic soda',
    'underground sodium carbonate',
    'underground lime',
    'underground flocculant',
    'underground lignitous coal base price',
    'underground lignitous coal distance from source',
    'underground lignitous coal freight rate',
    'underground lignitous coal delivered price',
    'underground anthracite coal base price',
    'underground anthracite coal difference',
    'underground anthracite coal distance from source',
    'underground anthracite coal freight rate',
    'underground anthracite coal delivered price'
    ]

# provicial db calculations starts here 
class provincial():
    def __init__(self,db):
        self.db = db
        self.idx = pd.IndexSlice
        
    def miningautolevel(self,Province,year):
        try:
            s = 2
            self.db.set_value(self.idx[Province,'mining characteristics mine automation level'],year,s)
        except:
            self.db.set_value(self.idx[Province,'mining characteristics mine automation level'],year,0)
    def miningdepthopenpit(self,Province,year):
        
        s = 0
        t = 25
        self.db.set_value(self.idx[Province,'mining characteristics mine depth open pit'],year,s+t)
    def miningdepthunderground(self,Province,year):
        s = 0
        t = 100
        self.db.set_value(self.idx[Province,'mining characteristics mine depth underground'],year,s+t)
    def miningdressautolevel(self,Province,year):
        s = 2
        self.db.set_value(self.idx[Province,'mining characteristics dressing automation level'],year,s)
    def miningproduction(self,Province,year):
        s = 1000
        self.db.set_value(self.idx[Province,'mining characteristics production'],year,s)
    def miningopen(self,Province,year):
        s = 0.9
        self.db.set_value(self.idx[Province,'mining characteristics mine type open'],year,s)
    def miningunderground(self,Province,year):
        try:
            s = 1-self.db.loc[Province,'mining characteristics mine type open'][year][0]
            self.db.set_value(self.idx[Province,'mining characteristics mine type underground'],year,s)
        except:
            self.db.set_value(self.idx[Province,'mining characteristics mine type underground'],year,0)
    def miningstrippingratioopenpit(self,Province,year):
        s = 7.5
        self.db.set_value(self.idx[Province,'mining characteristics stripping ratio open pit'],year,s)
    def miningstrippingratiounderground(self,Province,year):
        s = 0.52
        self.db.set_value(self.idx[Province,'mining characteristics stripping ratio underground'],year,s)
    def miningelectricitycons(self,Province,year):
        try:
            s = 1.5*pow(self.db.loc[Province,'mining characteristics production'][year],-0.129)
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity consumption'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity consumption'],year,0)
    def miningelecprice(self,Province,year):
        s = 0.43
        self.db.set_value(self.idx[Province,'open pit mine mining cost electricity price'],year,s)
    def miningeleccost(self,Province,year):
        try:
            s = self.db.loc[Province,'open pit mine mining cost electricity consumption'][year] - self.db.loc[Province,'open pit mine mining cost electricity price'][year]
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity cost'],year,0)
    def miningdeiselprice(self,Province,year):
        s = 6.96
        self.db.set_value(self.idx[Province,'open pit mine mining cost diesel price'],year,s)
    def miningdeiselusage(self,Province,year):
        try:
            s = 0.005*self.db.loc[Province,'open pit mine mining cost mine depth open pit'][year]+1+1*pow(self.db.loc[Province,'open pit mine mining cost production'][year],-0.129)
            self.db.set_value(self.idx[Province,'open pit mine mining cost diesel usage'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost diesel usage'],year,0)
    def mininglabprod1(self,Province,year):
        try:
            s= 0.0022*self.db.loc[Province,'mining characteristics mine depth open pit']+0.44
            self.db.set_value(self.idx[Province,'open pit mine mining cost labuor productivity'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost labour productivity'],year,0)
        pass
    def mininglabrate1(self,Province,year):
        s= 15.75
        self.db.set_value(self.idx[Province,'open pit mine mining cost labour rate'],year,s)
        pass
    def mininglabcost1(self,Province,year):
        try:
            s=self.db.loc[Province,'open pit mine mining labor productivity']*self.db.loc[Province,'open pit mine mining labor rate']
            self.db.set_value(self.idx[Province,'open pit mine mining cost labour cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost labour cost'],year,0)
        pass
    def miningfreightcost(self,Province,year):
        s=0
        self.db.set_value(self.idx[Province,'open pit mine mining cost freight cost'],year,s)
        pass
    def miningothercost(self,Province,year):
        s=3.50
        self.db.set_value(self.idx[Province,'open pit mine mining cost other cost'],year,s)
        pass
    def miningoreminingcost(self,Province,year):
        try:
            s= self.db.loc[Province,'open pit mine mining cost electricity cost'][year]+self.db.loc[Province,'open pit mine mining cost diesel cost'][year]+self.db.loc[Province,'open pit mine mining cost labor cost'][year]+self.db.loc[Province,'open pit mine mining freight cost'][year]+self.db.loc[Province,'open pit mine mining other cost'][year]
            self.db.set_value(self.idx[Province,'open pit mine mining cost ore mining cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost ore mining cost'],year,0)
        pass
    def miningstrippingratio(self,Province,year):
        try:
            s=self.db.loc[Province,'mining characteristics stripping ratio open pit']
            self.db.set_value(self.idx[Province,'open pit mine mining cost stripping ratio'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost stripping ratio'],year,0)
        pass
    def miningwasteproduction(self,Province,year):
        try:
            s=self.db.loc[Province,'mining characteristics production']+self.db.loc[Province,'open pit mine stripping ratio']
            self.db.set_value(self.idx[Province,'open pit mine mining cost waste production'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost waste production'],year,0)
        pass
    def miningelctricityconsumption(self,Province,year):
        try:
            s=0.164*pow(self.db.loc[Province,'open pit mine mining cost waste production'][year],-0.129)
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity consumption2'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity consumption2'],year,0)
        pass
    def miningelectricityprice(self,Province,year):
        d = [

        ]
        s=0
        self.db.set_value(self.idx[Province,'open pit mine mining cost electricity price'],year,s)
        pass
    def miningelectricitycost(self,Province,year):
        try:
            s=self.db.loc[Province,'open pit mine mining cost electricity cost'][year]*self.db.loc[Province,'open pit mine mining cost electricity price2']
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity cost2'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost electricity cost2'],year,0)
        pass
    def miningcostdieselusage(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'mining characteristics mine depth open pit'][year],

            ]
            s = 0.002*d[0]+0.0571*pow(d[1],-0.129)+0.71
            self.db.set_value(self.idx[Province,'open pit mine mining cost diesel usage2'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost diesel usage2'],year,0)
    def miningcostdieselprice(self,Province,year):
        d = [
        ]
        s = 6.92
        self.db.set_value(self.idx[Province,'open pit mine mining cost diesel price2'],year,s)
    def miningcostdieselcost(self,Province,year):
        try:
            d = [

        ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'open pit mine mining cost diesel cost2'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost diesel cost2'],year,0)
    def miningcostlabourproductivity(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'mining characteristics mine depth open pit'][year]

            ]
            s = 0.002*d[0]+0.11
            self.db.set_value(self.idx[Province,'open pit mine mining cost labour productivity2'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost labour productivity2'],year,0)
    def miningcostlabourrate(self,Province,year):
        d = [
            
        ]
        s = 15.75
        self.db.set_value(self.idx[Province,'open pit mine mining cost labour rate2'],year,s)
    def miningcostlabourcost(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'open pit mine mining cost labour productivity2'][year],
            self.db.loc[Province,'open pit mine mining cost labour rate2'][year]
            ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'open pit mine mining cost labour cost2'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost labour cost2'],year,0)
    def miningcostother(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'open pit mine mining cost waste production'][year]
            ]
            s = 1.875-0.0003*d[0]
            self.db.set_value(self.idx[Province,'open pit mine mining cost other'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost other'],year,0)
    def miningcoststrippingcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'open pit mine mining cost electricity cost'][year],
                self.db.loc[Province,'open pit mine mining cost diesel cost2'][year],
                self.db.loc[Province,'open pit mine mining cost labour cost2'][year],
                self.db.loc[Province,'open pit mine mining cost other'][year]
            ]
            s = d[0]+d[1]+d[2]+d[3]
            self.db.set_value(self.idx[Province,'open pit mine mining cost stripping cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost stripping cost'],year,0)
    def miningcosttotalstrippingcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'open pit mine mining cost stripping cost'][year],
                self.db.loc[province,'open pit mine mining cost stripping ratio'][year]            
                ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'open pit mine mining cost total stripping cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost total stripping cost'],year,0)
    def miningcostopenpitminingcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'open pit mine mining cost ore mining cost'][year],
                self.db.loc[Province,'open pit mine mining cost total stripping cost'][year]
            ]
            s = d[0]+d[1]
            self.db.set_value(self.idx[Province,'open pit mine mining cost open pit mining cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine mining cost open pit mining cost'],year,0)
    def dressingcostautomationandelectricityusagelow(self,Province,year):
        try:
            d = [
                self.db.loc[province,'mining characteristics production'][year]
            ]
            s = 8*pow(d[0],-0.07)
            self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and electricity usage low'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and electricity usage low'],year,0)
    def dressingcostautomationandelectricityusagemedium(self,Province,year):
        try:
            d = [
                self.db.loc[province,'mining characteristics production'][year]
            ]
            s = 9*pow(d[0],-0.07)
            self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and electricity usage medium'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and electricity usage medium'],year,0)
    def dressingcostautomationandelectricityusagehigh(self,Province,year):
        try:
            d = [
                self.db.loc[province,'mining characteristics production'][year]
            ]
            s = 10*pow(d[0],-0.07)
            self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and electricity usage high'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and electricity usage high'],year,0)
    def dressingcostelectricityconsumption(self,Province,year):
        d = [
            
        ]
        s = 0
        self.db.set_value(self.idx[Province,'open pit mine dressing cost electricity consumption'],year,s)
    def dressingcostelectricityprice(self,Province,year):
        d = [
            
        ]
        s = 0.56
        self.db.set_value(self.idx[Province,'open pit mine dressing cost electricity price'],year,s)
    def dressingcostelectricitycost(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'open pit mine dressing cost electricity consumption'][year],
            self.db.loc[Province,'open pit mine dressing cost electricity price'][year]
            ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'open pit mine dressing cost electricity cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine dressing cost electricity cost'],year,0)
    def dressingcostautomationandlabourproductivitylow(self,Province,year):
        d = [
            
        ]
        s = 0.08
        self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and labour productivity low'],year,s)
    def dressingcostautomationandlabourproductivitymedium(self,Province,year):
        d = [
            
        ]
        s = 0.07
        self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and labour productivity medium'],year,s)
    def dressingcostautomationandlabourproductivityhigh(self,Province,year):
        d = [
            
        ]
        s = 0.06
        self.db.set_value(self.idx[Province,'open pit mine dressing cost automation and labour productivity high'],year,s)
    def dressingcostlabourproductivity(self,Province,year):
        d = [
            
        ]
        s = 0.07
        self.db.set_value(self.idx[Province,'open pit mine dressing cost labour productivity'],year,s)
    def dressingcostlabourrate(self,Province,year):
        d = [
            
        ]
        s = 15.75
        self.db.set_value(self.idx[Province,'open pit mine dressing cost labour rate'],year,s)
    def dressingcostlabourcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'open pit mine dressing cost labour productivity'][year],
                self.db.loc[Province,'open pit mine dressing cost labour rate'][year]
            ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'open pit mine dressing cost labour cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine dressing cost labour cost'],year,0)
    def dressingcostauxillarymaterialcost(self,Province,year):
        d = [
            
        ]
        s = 23
        self.db.set_value(self.idx[Province,'open pit mine dressing cost auxillary material cost'],year,s)
    def dressingcosttotaldressingcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'open pit mine dressing cost auxillary material cost'][year],
                self.db.loc[Province,'open pit mine dressing cost labour cost'][year],
                self.db.loc[Province,'open pit mine dressing cost electricity cost'][year],         
            ]
            
            s = d[0]+d[1]+d[2]
            self.db.set_value(self.idx[Province,'open pit mine dressing cost total dressing cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'open pit mine dressing cost total dressing cost'],year,0)
    def undergroundminingcostautomationandelectricityusagelow(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics production'][year]
            ]
            s = 24*pow(d[0],-0.129)
            self.db.set_value(self.idx[Province,'underground mining cost automation and electricity usage low'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost automation and electricity usage low'],year,0)
    def undergroundminingcostautomationandelectricityusagemedium(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics production'][year]
            ]
            s = 45*pow(d[0],-0.14)
            self.db.set_value(self.idx[Province,'underground mining cost automation and electricity usage medium'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost automation and electricity usage medium'],year,0)
    def undergroundminingcostautomationandelectricityusagehigh(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics production'][year]
            
            ]
            s = 75*pow(d[0],-0.17)
            self.db.set_value(self.idx[Province,'underground mining cost automation and electricity usage high'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost automation and electricity usage high'],year,0)
    def undergroundminingcostsizefactor(self,Province,year):
        d = [
            
        ]
        s = 17.108523
        self.db.set_value(self.idx[Province,'underground mining cost size factor'],year,s)
    def undergroundminingcostdepthfactor(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics mine type underground'][year]
            ]
            s = 0.002*d[0]+0.796
            self.db.set_value(self.idx[Province,'underground mining cost depth factor'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost depth factor'],year,0)
    def undergroundminingcostelectricityconsumption(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground mining cost size factor'][year],
            self.db.loc[Province,'underground mining cost depth factor'][year]
            ]
            s = d[0]*d[1]
        except:
            self.db.set_value(self.idx[Province,'underground mining cost electricity consumption'],year,s)
    def undergroundminingcostelectricityprice(self,Province,year):
        d = [
        ]
        s = 0.43116181
        self.db.set_value(self.idx[Province,'underground mining cost electricity price'],year,s)
    def undergroundminingcostelectricitycost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'underground mining cost electricity price'][year],
                self.db.loc[Province,'underground mining cost electricity consumption'][year]
                ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'underground mining cost electricity cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost electricity cost'],year,0)
    def undergroundminingcostautomationandproductivitylow(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics production'][year]
            ]
            s = 1.015 if d[0] < 100 else 1.4*pow(d[0],-0.07)
            self.db.set_value(self.idx[Province,'underground mining cost automation and productivity low'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost automation and productivity low'],year,0)
    def undergroundminingcostautomationandproductivitymedium(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'mining characteristics production'][year]
            ]
            s =  0.936 if d[0] < 200 else 3*pow(d[0],-0.22)
            self.db.set_value(self.idx[Province,'underground mining cost automation and productivity medium'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost automation and productivity medium'],year,0)
    def undergroundminingcostautomationandproductivityhigh(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics production'][year]
            ]
            s = 0.785 if d[0] < 300 else 3*pow(d[0],-0.42)
            self.db.set_value(self.idx[Province,'underground mining cost automation and productivity high'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost automation and productivity high'],year,0)
    def undergroundminingcostdepthfactor(self,Province,year):
        
        d = [
            self.db.loc[Province,'mining characteristics mine depth underground'][year]
        ]
        s = 0.785 if d[0] < 125 else 3*pow(d[0],-0.42) # fix this formula
        self.db.set_value(self.idx[Province,'underground mining cost depth factor'],year,s)
    def undergroundminingcostautomationfactor(self,Province,year):
        d = [
            
        ]
        s = 0
        self.db.set_value(self.idx[Province,'underground mining cost automation factor'],year,s)
    def undergroundminingcostlabourproductivity(self,Province,year):
        d = [
            
        ]
        s = 0
        self.db.set_value(self.idx[Province,'underground mining cost labour productivity'],year,s)
    def undergroundminingcostlabourprice(self,Province,year):
        d = [
            
        ]
        s = 21.75
        self.db.set_value(self.idx[Province,'underground mining cost labour price'],year,s)
    def undergroundminingcostlabourcost(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground mining cost labour productivity'][year],
            self.db.loc[Province,'underground mining cost labour price'][year]
            ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'underground mining cost labour cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost labour cost'],year,0)
    def undergroundminingcostdieselusage(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics production'][year]
            ]
            s = 1.5*pow(d[0],-0.129)
            self.db.set_value(self.idx[Province,'underground mining cost diesel usage'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost diesel usage'],year,0)
    def undergroundminingcostdieselprice(self,Province,year):
        d = [
            
        ]
        s = 6.92
        self.db.set_value(self.idx[Province,'underground mining cost diesel price'],year,s)
    def undergroundminingcostdieselcost(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground mining cost diesel price'][year],
            self.db.loc[Province,'underground mining cost diesel usage'][year]
            ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'underground mining cost diesel cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost diesel cost'],year,0)
    def undergroundminingcostfreightcost(self,Province,year):
        d = [
            
        ]
        s = 2
        self.db.set_value(self.idx[Province,'underground mining cost freight cost'],year,s)
    def undergroundminingcostothercost(self,Province,year):
        d = [
            
        ]
        s = 0.99
        self.db.set_value(self.idx[Province,'underground mining cost other cost'],year,s)
    def undergroundminingcosttotalminingcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'underground mining cost other cost'][year],
                self.db.loc[Province,'underground mining cost freight cost'][year],
                self.db.loc[Province,'underground mining cost labour cost'][year],
                self.db.loc[Province,'underground mining cost electricity cost'][year],
                self.db.loc[Province,'underground mining cost diesel cost'][year]
            ]
            s = d[0]+d[1]+d[2]+d[3]+d[4]
            self.db.set_value(self.idx[Province,'underground mining cost total mining cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost total mining cost'],year,0)
    def undergroundminingcostundergroundminingcost(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground mining cost total mining cost'][year],
            ]
            s = d[0]*(1+d[1])
            self.db.set_value(self.idx[Province,'underground mining cost underground mining cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground mining cost underground mining cost'],year,0)
    def undergrounddressingcostautomationandelectricityusagelow(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'mining characteristics production'][year]
            ]
            s = 18*pow(d[0],-0.07)
            self.db.set_value(self.idx[Province,'underground dressing cost automation and electricity usage low'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground dressing cost automation and electricity usage low'],year,0)
    def undergrounddressingcostautomationandelectricityusagemedium(self,Province,year):
        d = [
            self.db.loc[Province,'mining characteristics production'][year]
            
        ]
        s = 19*pow(d[0],-0.07)
        self.db.set_value(self.idx[Province,'underground dressing cost automation and electricity usage medium'],year,s)
    def undergrounddressingcostautomationandelectricityusagehigh(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'mining characteristics production'][year],
            
            ]
            s = 20*pow(d[0],-0.07)
            self.db.set_value(self.idx[Province,'underground dressing cost automation and electricity usage high'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground dressing cost automation and electricity usage high'],year,0)
    def undergrounddressingcostelectricityconsumption(self,Province,year):
        d = [
            
        ]
        s = 0
        self.db.set_value(self.idx[Province,'underground dressing cost electricity consumption'],year,s)
    def undergrounddressingcostelectricityprice(self,Province,year):
        d = [
            
        ]
        s = 0.56
        self.db.set_value(self.idx[Province,'underground dressing cost electricity price'],year,s)
    def undergrounddressingcostelectricitycost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'underground dressing cost electricity price'][year],
                self.db.loc[Province,'underground dressing cost electricity consumption'][year]
            ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'underground dressing cost electricity cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground dressing cost electricity cost'],year,0)
    def undergrounddressingcostautomationandlabourproductivitylow(self,Province,year):
        d = [
            
        ]
        s = 0.08
        self.db.set_value(self.idx[Province,'underground dressing cost automation and labour productivity low'],year,s)
    def undergrounddressingcostautomationandlabourproductivitymedium(self,Province,year):
        d = [
            
        ]
        s = 0.07
        self.db.set_value(self.idx[Province,'underground dressing cost automation and labour productivity medium'],year,s)
    def undergrounddressingcostautomationandlabourproductivityhigh(self,Province,year):
        d = [
            
        ]
        s = 0.06
        self.db.set_value(self.idx[Province,'underground dressing cost automation and labour productivity high'],year,s)
    def undergrounddressingcostlabourproductivity(self,Province,year):
        d = [
            
        ]
        s = 0
        self.db.set_value(self.idx[Province,'underground dressing cost labour productivity'],year,s)
    def undergrounddressingcostlabourrate(self,Province,year):
        d = [
            
        ]
        s = 15.75
        self.db.set_value(self.idx[Province,'underground dressing cost labour rate'],year,s)
    def undergrounddressingcostlabourcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'underground dressing cost labour productivity'][year],
                self.db.loc[Province,'underground dressing cost labour rate'][year],
                ]
            s = d[0]*d[1]
            self.db.set_value(self.idx[Province,'underground dressing cost labour cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground dressing cost labour cost'],year,s)
    def undergrounddressingcostauxillarymaterialcost(self,Province,year):
        d = [
            
        ]
        s = 23
        self.db.set_value(self.idx[Province,'underground dressing cost auxillary material cost'],year,s)
    def undergrounddressingcosttotaldressingcost(self,Province,year):
        try:
            d = [
                self.db.loc[Province,'underground dressing cost auxillary material cost'][year],
            self.db.loc[Province,'underground dressing cost labour cost'][year],
                self.db.loc[Province,'underground dressing cost electricity cost'][year]
            ]
            s = d[0]+d[1]+d[2]
            self.db.set_value(self.idx[Province,'underground dressing cost total dressing cost'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground dressing cost total dressing cost'],year,0)
    def undergroundstate(self,Province,year):
        d = [
            
            ]
        s = 20
        self.db.set_value(self.idx[Province,'underground state'],year,s)
    def undergroundlocal(self,Province,year):
        d = [
            
        ]
        s = 0
        self.db.set_value(self.idx[Province,'underground local'],year,s)
    def undergroundtotal(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground state'][year],
            self.db.loc[Province,'underground local'][year],
            ]
            s = d[0]+d[1]
            self.db.set_value(self.idx[Province,'underground total'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground total'],year,0)
    def undergroundroad(self,Province,year):
        d = [
            
        ]
        s = 0.43
        self.db.set_value(self.idx[Province,'underground road'],year,s)
    def undergroundrail(self,Province,year):
        d = [
            
        ]
        s = 0.11
        self.db.set_value(self.idx[Province,'underground rail'],year,s)
    def undergroundpurchaceprice(self,Province,year):
        d = [
            
        ]
        s = 313
        self.db.set_value(self.idx[Province,'underground purchace price'],year,s)
    def undergroundwage(self,Province,year):
        d = [
            
        ]
        s = 18.75
        self.db.set_value(self.idx[Province,'underground wage'],year,s)
    def undergroundcausticsoda(self,Province,year):
        d = [
            
        ]
        s = 1903
        self.db.set_value(self.idx[Province,'underground caustic soda'],year,s)
    def undergroundsodiumcarbonate(self,Province,year):
        d = [
            
        ]
        s = 1359
        self.db.set_value(self.idx[Province,'underground sodium carbonate'],year,s)
    def undergroundlime(self,Province,year):
        d = [
            
        ]
        s = 300
        self.db.set_value(self.idx[Province,'underground lime'],year,s)
    def undergroundflocculant(self,Province,year):
        d = [
            
        ]
        s = 18000
        self.db.set_value(self.idx[Province,'underground flocculant'],year,s)
    def undergroundlignitouscoalbaseprice(self,Province,year):
        d = [
            
        ]
        s = 357
        self.db.set_value(self.idx[Province,'underground lignitous coal base price'],year,s)
    def undergroundlignitouscoaldistancefromsource(self,Province,year):
        d = [
            
        ]
        s = 80
        self.db.set_value(self.idx[Province,'underground lignitous coal distance from source'],year,s)
    def undergroundlignitouscoalfreightrate(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground lignitous coal distance from source'][year]
            ]
            s = 0.9208*pow(d[0],-0.173)*d[0]/1.17
            self.db.set_value(self.idx[Province,'underground lignitous coal freight rate'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground lignitous coal freight rate'],year,0)
    def undergroundlignitouscoaldeliveredprice(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground lignitous coal freight rate'][year],
            self.db.loc[Province,'underground lignitous coal base price'][year]
            ]
            s = d[0]+d[1]
            self.db.set_value(self.idx[Province,'underground lignitous coal delivered price'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground lignitous coal delivered price'],year,0)
    def undergroundanthracitecoalbaseprice(self,Province,year):
        d = [
            
        ]
        s = 648
        self.db.set_value(self.idx[Province,'underground anthracite coal base price'],year,s)
    def undergroundanthracitecoaldifference(self,Province,year):
        d = [
            
        ]
        s = 0.2
        self.db.set_value(self.idx[Province,'underground anthracite coal difference'],year,s)
    def undergroundanthracitecoaldistancefromsource(self,Province,year):
        d = [
            
        ]
        s = 80
        self.db.set_value(self.idx[Province,'underground anthracite coal distance from source'],year,s)
    def undergroundanthracitecoalfreightrate(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground anthracite coal distance from source'][year]
            ]
            s = 0.9208*pow(d[0],-0.173)*d[0]/1.17
            self.db.set_value(self.idx[Province,'underground anthracite coal freight rate'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground anthracite coal freight rate'],year,0)
    def undergroundanthracitecoaldeliveredprice(self,Province,year):
        try:
            d = [
            self.db.loc[Province,'underground anthracite coal freight rate'][year],
            self.db.loc[Province,'underground anthracite coal base price'][year]
            ]
            s = d[0]+d[1]
            self.db.set_value(self.idx[Province,'underground anthracite coal delivered price'],year,s)
        except:
            self.db.set_value(self.idx[Province,'underground anthracite coal delivered price'],year,0)
    def provincialcalcall(self,Province,year):
        provincial.miningautolevel(self,Province,year)
        provincial.miningdepthopenpit(self,Province,year)
        provincial.miningdepthunderground(self,Province,year)
        provincial.miningdressautolevel(self,Province,year)
        provincial.miningproduction(self,Province,year)
        provincial.miningopen(self,Province,year)
        provincial.miningunderground(self,Province,year)
        provincial.miningstrippingratioopenpit(self,Province,year)
        provincial.miningstrippingratiounderground(self,Province,year)
        provincial.miningelectricitycons(self,Province,year)
        provincial.miningelecprice(self,Province,year)
        provincial.miningeleccost(self,Province,year)
        provincial.miningdeiselprice(self,Province,year)
        provincial.miningdeiselusage(self,Province,year)
        provincial.mininglabprod1(self,Province,year)
        provincial.mininglabrate1(self,Province,year)
        provincial.mininglabcost1(self,Province,year)
        provincial.miningfreightcost(self,Province,year)
        provincial.miningothercost(self,Province,year)
        provincial.miningoreminingcost(self,Province,year)
        provincial.miningstrippingratio(self,Province,year)
        provincial.miningwasteproduction(self,Province,year)
        provincial.miningelctricityconsumption(self,Province,year)
        provincial.miningelectricityprice(self,Province,year)
        provincial.miningelectricitycost(self,Province,year)
        provincial.miningcostdieselusage(self,Province,year)
        provincial.miningcostdieselprice(self,Province,year)
        provincial.miningcostdieselcost(self,Province,year)
        provincial.miningcostlabourproductivity(self,Province,year)
        provincial.miningcostlabourrate(self,Province,year)
        provincial.miningcostlabourcost(self,Province,year)
        provincial.miningcostother(self,Province,year)
        provincial.miningcoststrippingcost(self,Province,year)
        provincial.miningcosttotalstrippingcost(self,Province,year)
        provincial.miningcostopenpitminingcost(self,Province,year)
        provincial.dressingcostautomationandelectricityusagelow(self,Province,year)
        provincial.dressingcostautomationandelectricityusagemedium(self,Province,year)
        provincial.dressingcostautomationandelectricityusagehigh(self,Province,year)
        provincial.dressingcostelectricityconsumption(self,Province,year)
        provincial.dressingcostelectricityprice(self,Province,year)
        provincial.dressingcostelectricitycost(self,Province,year)
        provincial.dressingcostautomationandlabourproductivitylow(self,Province,year)
        provincial.dressingcostautomationandlabourproductivitymedium(self,Province,year)
        provincial.dressingcostautomationandlabourproductivityhigh(self,Province,year)
        provincial.dressingcostlabourproductivity(self,Province,year)
        provincial.dressingcostlabourrate(self,Province,year)
        provincial.dressingcostlabourcost(self,Province,year)
        provincial.dressingcostlabourcost(self,Province,year)
        provincial.dressingcostauxillarymaterialcost(self,Province,year)
        provincial.dressingcosttotaldressingcost(self,Province,year)
        provincial.undergroundminingcostautomationandelectricityusagelow(self,Province,year)
        provincial.undergroundminingcostautomationandelectricityusagemedium(self,Province,year)
        provincial.undergroundminingcostautomationandelectricityusagehigh(self,Province,year)
        provincial.undergroundminingcostsizefactor(self,Province,year)
        provincial.undergroundminingcostdepthfactor(self,Province,year)
        provincial.undergroundminingcostelectricityconsumption(self,Province,year)
        provincial.undergroundminingcostelectricityprice(self,Province,year)
        provincial.undergroundminingcostelectricitycost(self,Province,year)
        provincial.undergroundminingcostautomationandproductivitylow(self,Province,year)
        provincial.undergroundminingcostautomationandproductivitymedium(self,Province,year)
        provincial.undergroundminingcostautomationandproductivityhigh(self,Province,year)
        provincial.undergroundminingcostdepthfactor(self,Province,year)
        provincial.undergroundminingcostautomationfactor(self,Province,year)
        provincial.undergroundminingcostlabourproductivity(self,Province,year)
        provincial.undergroundminingcostlabourprice(self,Province,year)
        provincial.undergroundminingcostlabourcost(self,Province,year)
        provincial.undergroundminingcostdieselusage(self,Province,year)
        provincial.undergroundminingcostdieselprice(self,Province,year)
        provincial.undergroundminingcostdieselcost(self,Province,year)
        provincial.undergroundminingcostfreightcost(self,Province,year)
        provincial.undergroundminingcostothercost(self,Province,year)
        provincial.undergroundminingcosttotalminingcost(self,Province,year)
        provincial.undergroundminingcostundergroundminingcost(self,Province,year)
        provincial.undergrounddressingcostautomationandelectricityusagelow(self,Province,year)
        provincial.undergrounddressingcostautomationandelectricityusagemedium(self,Province,year)
        provincial.undergrounddressingcostautomationandelectricityusagehigh(self,Province,year)
        provincial.undergrounddressingcostelectricityconsumption(self,Province,year)
        provincial.undergrounddressingcostelectricityprice(self,Province,year)
        provincial.undergrounddressingcostelectricitycost(self,Province,year)
        provincial.undergrounddressingcostautomationandlabourproductivitylow(self,Province,year)
        provincial.undergrounddressingcostautomationandlabourproductivitymedium(self,Province,year)
        provincial.undergrounddressingcostautomationandlabourproductivityhigh(self,Province,year)
        provincial.undergrounddressingcostlabourproductivity(self,Province,year)
        provincial.undergrounddressingcostlabourrate(self,Province,year)
        provincial.undergrounddressingcostlabourcost(self,Province,year)
        provincial.undergrounddressingcostauxillarymaterialcost(self,Province,year)
        provincial.undergrounddressingcosttotaldressingcost(self,Province,year)
        provincial.undergroundstate(self,Province,year)
        provincial.undergroundlocal(self,Province,year)
        provincial.undergroundtotal(self,Province,year)
        provincial.undergroundroad(self,Province,year)
        provincial.undergroundrail(self,Province,year)
        provincial.undergroundpurchaceprice(self,Province,year)
        provincial.undergroundwage(self,Province,year)
        provincial.undergroundcausticsoda(self,Province,year)
        provincial.undergroundsodiumcarbonate(self,Province,year)
        provincial.undergroundlime(self,Province,year)
        provincial.undergroundflocculant(self,Province,year)
        provincial.undergroundlignitouscoalbaseprice(self,Province,year)
        provincial.undergroundlignitouscoaldistancefromsource(self,Province,year)
        provincial.undergroundlignitouscoalfreightrate(self,Province,year)
        provincial.undergroundlignitouscoaldeliveredprice(self,Province,year)
        provincial.undergroundanthracitecoalbaseprice(self,Province,year)
        provincial.undergroundanthracitecoaldifference(self,Province,year)
        provincial.undergroundanthracitecoaldistancefrosmsource(self,Province,year)
        provincial.undergroundanthracitecoalfreightrate(self,Province,year)
        provincial.undergroundanthracitecoaldeliveredprice(self,Province,year)


    

globaldata = {
    "Bayer":[2.62],
    "Sinter":[2.61],
    "Minimum A/S":[2.6],
    "Handling losses":[0.001],
    "Moisture content of ore":[0.05],
    "Extraction efficiency - Bayer":[0.9],
    "Extraction efficiency - Sinter":[0.9],
    "Alumina Quality":[0.987],
    "Sourcing Factor":[1.00]
}
yearsinput = [
    "2004",
    "2005",
    "2006",
    "2007",
    "2008",
    "2009",
    "2010",
    "2011",
    "2012",
    "2013",
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
    "2022",
    "2023",
    "2024",
    "2025",
    "2026",
    "2027",
    "2028",
    "2029",
    "2030",
    "2031"
    
]
globaldb = pd.DataFrame(globaldata)
bayersinterdb = pd.read_csv("profilefactor.csv")

#calculate production values here


sheet = ['Bosai Xianfeng domestic',
         'Bosai Xianfeng import',    # sheet names
    
         ]
province = ['Guangxi',
            'Guizhou',
            'Henan',
            'Shanxi',
            'Other'
            ]
years = [
         "2005",
         "2006",
         "2007",
         "2008",
         "2009",
         "2010",
         "2011",
         "2012",
         "2013",
         "2014",
         "2015",
         "2016",
         "2017",
         "2018",
         "2019",
         "2020",
         "2021",
         "2022",
         "2023",
         "2024",
         "2025",
         "2026",
         "2027",
         "2028",
         "2029",
         "2030",
         "2031"
         ]
pm = ["Reserve",
      "Alumina Grade",
      "Silica Grade",
      "A/S",
      "Factor X",
      "Closed",
      "Purchased",
      "Alumina Profile Starting grade",
      "Alumina Profile Depletion grade",
      "Alumina Profile Scaled mean",
      "Alumina Profile Scaled variance",
      "Alumina Profile Alpha value",
      "Alumina Profile Beta value",
      "Starting ratio",
      "Depletion ratio",
      "Silica Profile Starting grade",
      "Silica Profile Depletion grade",
      "Silica Profile Scaled mean",
      "Silica Profile Scaled variance",
      "Silica Profile Alpha value",
      "Silica Profile Beta value",
      "Grade Profile Alumina Grade",
      "Grade Profile Silica Grade",
      "A/S ratio draw-down",
      "Demand Profile",
      "Sourcing factor",
      
      
      "Aa Production - based on protocol",
      "Bauxite Usage-Bayer",
      "Bauxite Usage-Bayer Mud Sinter",
      "Bauxite Usage-Sinter",
      "Bauxite Usage",
      "Bauxite Consumption",
      "Bauxite Cumulative",
      "Opening Stock",
      "Usage",
      "Closing Stock",
      "proportion of total reserve remaining",
      "Closing Stock - portion of total",
      "C/S Outlook (n+1)",
      "C/S Outlook (n+1) % of total",


      "domestic open-strip",
      "domestic underground",
      "mining cost open-strip",
      "mining cost underground",
      "mining cost dressing open pit",
      "mining cost dressing underground",
      "total mining and dressing",
      "Mining Royality",
      "Local charges",
      "totalgovernmentcharges",
      "Road-1",
      "Rail-1",
      "totalminingcashcost",
      "Road0",
      "Rail0",
      "totalrailroad",
      "closedloop costs fob",
      "closedloop sea freight",
      "closedloop port to refinery",
      "closedloop total-rmb",
      "closedloop total US",
      "thirdparty purchase fob",
      "thirdparty sea freight",
      "thirdparty port to refinery",
      "thirdparty total-rmb",
      "thirdparty total US",
      "total mining dressing refinery",
      "Purchase-FOT",
      "Road1",
      "Rail1",
      "Road2",
      "Rail2",
      "Mine to refinery",
      "total purchace and mine to refinary",
      "import closedloop",
      "import thirdparty",
      "domestic closed loop",
      "domestic thirdparty",
      "Bauxite cost delivered to aa refinery",
      "Bauxite Price-Cost1",
      "Bauxite Price-Cost2",
      "Caustic Cost",
      "Bauxite & Caustic Cost",
      "Pre-Calculation",
      "Sourcing Mix",
      
      
      "Caustic Usage",
      
      
      ]
mxid = pd.MultiIndex.from_product([sheet,province,pm])
mxid2 = pd.MultiIndex.from_product([Province,proparam])
db = pd.DataFrame(index=mxid,columns=years)
idx = pd.IndexSlice
summarydb = pd.read_csv("summary.csv")
b1 = Bauxite(db,bayersinterdb,globaldb,summarydb)
c1 = productiondata(proddata1,proddata2,proddata3,proddata4,proddata5,proddata6,proddata7,production,depdata1,depdata2,depdata3,depdata4,depdata5,depdata6,depdata7,cockpitdata1,cockpitdata2,cockpitdata3)
provincialdb = pd.DataFrame(index=mxid2,columns=years)
prodb = provincial(provincialdb)
'''
for j in sheet:
    for k in province:
        b1.calc_all1(sheet=j,province=k,year="2004")
    for l in province:
        b1.calc_all2(sheet=j,province=l,year="2004",dbb=c1)
for i in years:
    for j in sheet:
        for k in province:
            b1.calc_all1(sheet=j,province=k,year=i)
        c1.pcalcall1(sheet=j,year=i)
        for l in province:
            b1.calc_all2(sheet=j,province=l,year=i,dbb=c1)
        c1.pcalcall2(db=b1.db,sheet=j,year=i)
'''            
for i in years:
    for j in Province:
        prodb.provincialcalcall(j,i)
with pd.option_context('display.max_rows', None, 'display.max_columns', None):  
    print(provincialdb['2005'])
    #dx.to_csv('bosaioutput.csv')


