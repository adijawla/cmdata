
import simply
from simply import *
import warnings
from scipy import stats
import numpy as np
import pandas as pd
import statistics as stat
import openpyxl

from collections import defaultdict
from newprovincial import provincial
from reservesummary import summary
warnings.filterwarnings("ignore")
'''

Bauxite main model starts here
'''
class Bauxite():
    def __init__(self,db,datadb,globaldb,summarydb,provincialdb,otherlist,exlist):  # These are all different static databases
        self.db=db                                                                             
        self.globaldb = globaldb                      # global values db cell 42 to 54
        self.datadb = datadb
        self.summarydb = summarydb                    # reserve summary db 
        self.provincialdb = provincialdb
        self.causticdata = pd.read_csv('caustic.csv')
        self.otherlist = otherlist
        self.sheetdata = self.datadb["sheetname"].tolist()
        self.exlist = exlist
        self.factx = pd.read_csv('factx.csv')
        constant = pd.read_csv('formulainput.csv')
        self.constant = constant['value']
    def reserve(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.province==province][sheet+" Allocation"].sum()
        put(self.db,sheet,province,"Reserve",'2004',v)
    def aluminagrade(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.province==province][sheet+" Avg %Al2O3"].sum()
        put(self.db,sheet,province,"Alumina Grade",'2004',v)
    def asratio(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.province==province][sheet+" Avg A/S"].sum()
        put(self.db,sheet,province,"A/S",'2004',v)
    def closed(self,sheet,province,year):
        v = self.summarydb.loc[self.summarydb.province==province][sheet+" Closed"].sum()
        put(self.db,sheet,province,"Closed",'2004',v)

        #constant1
    def demand(self,sheet,province,year):
        put(self.db,sheet,province,"Demand Profile",'2004',0.99999999)

        #constant2
    def usagee(self,sheet,province,year):
        put(self.db,sheet,province,"Usage",'2004',0.1)
    def factorx(self,sheet,province,year):
        put(self.db,sheet,province,"Factor X",year,self.factx.loc[self.factx.Sheetname==sheet][self.factx.province==province]['factorx'].sum())    
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


        # constant 3 , 4
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

        # constant 5 , 6
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


        # constant 7 , 8 , 9 , 10
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

        # constant 11 , 12  , 13 , 14
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

        # constant 15 , 16
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

        # constant 17
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
            put(self.db,sheet,province,"Silica Profile Beta value",year,0)

        
    def alumina_grade(self,sheet,province,year):
        try:
            d = [get(self.db,sheet,province,"Alumina Profile Starting grade",'2004'),
                 get(self.db,sheet,province,"Alumina Profile Starting grade",year),
                 get(self.db,sheet,province,"Alumina Profile Depletion grade",'2004'),
                 get(self.db,sheet,province,"C/S Outlook (n+1) % of total",str(int(year)-1) if int(year) > 2004 else '2004'),
                 get(self.db,sheet,province,"Alumina Profile Alpha value",'2004'),
                 get(self.db,sheet,province,"Alumina Profile Beta value",'2004'),
                 get(self.db,sheet,province,"proportion of total reserve remaining",str(int(year)-1) if year > '2004' else '2004'),
                 get(self.db,sheet,province,"Alumina Grade",'2004'),
                 self.datadb.loc[self.datadb.sheetname==sheet]["switch"].sum(),
                 ]

            if year == '2004':
                value = d[0]
            elif d[8] == 1:
                value = d[7]
            else:
                if  d[0] == d[2] and d[3] > 0 :
                    value = d[1]
                else:
                    if  d[0] < d[2] and d[3] > 0:
                        value = stats.beta.ppf(1-stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2]))
                    else:
                        value = stats.beta.ppf(stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2])) if d[3] > 0 else 0   
            
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
                 get(self.db,sheet,province,"Alumina Profile Beta value",'2004'),
                 get(self.db,sheet,province,"Silica Grade",'2004'),
                 self.datadb.loc[self.datadb.sheetname==sheet]["switch"].sum(),
                 ]
            
            #print(value)
            if d[10] == 1:
                value = d[9]
            else:
                value = d[0] if year == '2004' else d[1] if d[0] == d[2] and d[3] > 0 else stats.beta.ppf(1-stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2])) if d[0] < d[2] and d[3] > 0 else stats.beta.ppf(stat.mean([d[3],d[6]]),d[4],d[5],min(d[0],d[2]),max(d[0],d[2])-min(d[0],d[2]) if d[3] > 0 else 0) 
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
                dbb.production.loc[dbb.production.refinery==sheet][year if int(year) > 2004 else '2005'].sum(),
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
                self.factx.loc[self.factx.Sheetname==sheet][self.factx.province==province]['bayerconstant'].sum(),
                get(self.db,sheet,province,"Grade Profile Silica Grade",year),
            ]
            value = np.nan if year == '2004' else 1/(((d[0]-(d[7]*d[6]))*(1-d[2])*(1-d[3])*d[4])/d[5]) if d[1] > 0 else 0
            put(self.db,sheet,province,"Bauxite Usage-Bayer",year,value)
            
        except:
            put(self.db,sheet,province,"Bauxite Usage-Bayer",year,0)
            
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
                self.datadb.loc[self.datadb.sheetname==sheet]["bayer"].sum(),
                self.datadb.loc[self.datadb.sheetname==sheet]["bayer mud sinter"].sum(),
                self.datadb.loc[self.datadb.sheetname==sheet]["sinter"].sum(),
            ]
            value = np.nan if year == '2004' else  d[0]*d[3]+d[1]*d[4]+d[2]*d[5]
    
            put(self.db,sheet,province,"Bauxite Usage",year,value)
            pass
        except:
            put(self.db,sheet,province,"Bauxite Usage",year,0)
            
    def bauxite_consumption(self,sheet,province,year):
        
        try:
            d = [self.factx.loc[self.factx.Sheetname==sheet][self.factx.province==province]['Bauxite Consumption'].sum(),
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
            d = [self.factx.loc[self.factx.Sheetname==sheet][self.factx.province==province]['Bauxite Consumption'].sum(),
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
            d = [get(self.db,sheet,province,"Opening Stock",'2004'),
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
                 dbb.production.loc[dbb.production.refinery==sheet][year if year != "2004" else "2005"].sum(),
                 dbb.production.loc[dbb.production.refinery==sheet][str(int(year)+1) if int(year) < 2031 else '2031'].sum() if int(year) < 2032 else 0
                 ]
            
            if  year == '2004' :
                value = d[1]-d[0]
            else:
                if d[2] == 0:
                    value = 0
                else:
                    if d[3] == 0:
                        value = d[2]
                    elif d[1]-d[0] > 0:
                        value = (d[1]-d[0]*d[4]/d[3])
                    else:
                        value = 0
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
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'mining characteristics mine type open']["2005"].sum()
                value = s if year != "2004" else np.nan
                put(self.db,sheet,province,"domestic open-strip",year,value)
            else:
                s = self.provincialdb.loc[province,'mining characteristics mine type open']["2005"].sum()
                value = s if year != "2004" else np.nan
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
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                d = self.provincialdb.loc[province1,'open pit mine mining cost open pit mining cost']["2005"].sum()
                value =  d if year != "2004" else np.nan
                put(self.db,sheet,province,"mining cost open-strip",year,value)
            else:
                d = self.provincialdb.loc[province,'open pit mine mining cost open pit mining cost']["2005"].sum()
                value =  d if year != "2004" else np.nan
                put(self.db,sheet,province,"mining cost open-strip",year,value)
            
        except:
            put(self.db,sheet,province,"mining cost open-strip",year,0)
    def miningcostunderground(self,sheet,province,year):
        try:
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                d = self.provincialdb.loc[province1,'underground mining cost underground mining cost']["2005"].sum()
                value = d if year != "2004" else np.nan
                put(self.db,sheet,province,"mining cost underground",year,value)
            else:
                d = self.provincialdb.loc[province,'underground mining cost underground mining cost']["2005"].sum()
                value = d if year != "2004" else np.nan
                put(self.db,sheet,province,"mining cost underground",year,value)
            
        except:
            put(self.db,sheet,province,"mining cost underground",year,0)
    def miningcostdressingopenpit(self,sheet,province,year):
        try:
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                d = self.provincialdb.loc[province1,'open pit mine dressing cost total dressing cost']["2005"].sum()
                value = d if year != "2004" else np.nan
                put(self.db,sheet,province,"mining cost dressing open pit",year,value)
            else:
                d = self.provincialdb.loc[province,'open pit mine dressing cost total dressing cost']["2005"].sum()
                value = d if year != "2004" else np.nan
                put(self.db,sheet,province,"mining cost dressing open pit",year,value)
        except:
            put(self.db,sheet,province,"mining cost dressing open pit",year,0)
    def miningcostdressingunderground(self,sheet,province,year):
        try:
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                d = self.provincialdb.loc[province1,'underground dressing cost total dressing cost']["2005"].sum()
                value = d if year != "2004" else np.nan
                put(self.db,sheet,province,"mining cost dressing underground",year,value)
            else:
                d = self.provincialdb.loc[province,'underground dressing cost total dressing cost']["2005"].sum()
                value = d if year != "2004" else np.nan
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
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'underground state']["2005"].sum()
                value = s if year != "2004" else np.nan
                #here1
                put(self.db,sheet,province,"Mining Royality",year,value)
            else:
                s = self.provincialdb.loc[province,'underground state']["2005"].sum()
                value = s if year != "2004" else np.nan
                #here1
                put(self.db,sheet,province,"Mining Royality",year,value)
        except:
            put(self.db,sheet,province,"Mining Royality",year,0)
    def Localcharges(self,sheet,province,year):
        try:
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'underground local']["2005"].sum()
                value = s if year != "2004" else np.nan
                #here
                put(self.db,sheet,province,"Local charges",year,value)
            else:
                s = self.provincialdb.loc[province,'underground local']["2005"].sum()
                value = s if year != "2004" else np.nan
                #here
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

    # constant 18,19,20,21
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
            
            value = np.nan if year == '2004' else self.summarydb.loc[self.summarydb.province==province][sheet+' Avg Distance'].sum() if year == '2005' else d[0]#formula here
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
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'underground road']["2005"].sum()
                value = s*d1[0] if year != "2004" else np.nan
                put(self.db,sheet,province,"Road0",year,value)

            else:
                s = self.provincialdb.loc[province,'underground road']["2005"].sum()
                value = s*d1[0] if year != "2004" else np.nan
                put(self.db,sheet,province,"Road0",year,value)
        except:
            put(self.db,sheet,province,"Road0",year,0)

        # constant 22
    def Rail0(self,sheet,province,year):# 19 cell 510
        try:
            d1 = [
                get(self.db,sheet,province,"Rail-1",year),
            ]
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'underground rail']["2005"].sum()
                value = s*d1[0]+41 if year != "2004" and d1[0] > 0 else 0
                put(self.db,sheet,province,"Rail0",year,value)
            else:
                s = self.provincialdb.loc[province,'underground rail']["2005"].sum()
                value = s*d1[0]+41 if year != "2004" and d1[0] > 0 else 0
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

        #constant 23 - years
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

        #constant 24 - years
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

            #constant 25 - years
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
                self.exlist[int(year)-2005 if int(year) > 2004 else 0],
            ]
            
            value = (d[0]+d[1])*d[3] +d[2]#formula here
            put(self.db,sheet,province,"closedloop total-rmb",year,value)
        except:
            put(self.db,sheet,province,"closedloop total-rmb",year,0)
    def closedlooptotalUS(self,sheet,province,year):# 19 cell 476
        try:
            d = [
                get(self.db,sheet,province,"closedloop total US",year),
                self.exlist[int(year)-2005 if int(year) > 2004 else 0],
            ]
            
            value = d[0]/d[1]#formula here
            put(self.db,sheet,province,"closedloop total US",year,value)
        except:
            put(self.db,sheet,province,"closedloop total US",year,0)

            # constant 26 - years
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

            # constant 27 - years
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

            # constant 28 - years
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
                self.exlist[int(year)-2005 if int(year) > 2004 else 0],

            ]
            
            value = (d[0]+d[1])*d[3]+d[2]#formula here
            put(self.db,sheet,province,"thirdparty total-rmb",year,value)
        except:
            put(self.db,sheet,province,"thirdparty total-rmb",year,0)
    def thirdpartytotalUS(self,sheet,province,year):# 19 cell 482
        try:
            d = [
                get(self.db,sheet,province,"thirdparty total-rmb",year),
                self.exlist[int(year)-2005 if int(year) > 2004 else 0],
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
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'underground purchace price']["2005"].sum()
                value = s if year != "2004" else np.nan
                put(self.db,sheet,province,"Purchase-FOT",year,value)
            else:
                s = self.provincialdb.loc[province,'underground purchace price']["2005"].sum()
                value = s if year != "2004" else np.nan
                put(self.db,sheet,province,"Purchase-FOT",year,value)
        except:
            put(self.db,sheet,province,"Purchase-FOT",year,0)
    def Road1(self,sheet,province,year):# 19 cell 521
        try:
            d = [
                get(self.db,sheet,province,"Road-1",year)
                

            ]
            
            value = d[0]
            put(self.db,sheet,province,"Road1",year,value)
        except:
            put(self.db,sheet,province,"Road1",year,0)
    def Rail1(self,sheet,province,year):# 19 cell 522
        try:
            
            value =0
            put(self.db,sheet,province,"Rail1",year,value)
        except:
            put(self.db,sheet,province,"Rail1",year,0)
    def Road2(self,sheet,province,year):# 19 cell 524
        try:
            d = [
                get(self.db,sheet,province,"Road1",year),
            ]
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'underground road']["2005"].sum()
                value = s*d[0] if year != "2004" else np.nan
                #here

                put(self.db,sheet,province,"Road2",year,value)
            else:
                s = self.provincialdb.loc[province,'underground road']["2005"].sum()
                value = s*d[0] if year != "2004" else np.nan
                #here

                put(self.db,sheet,province,"Road2",year,value)
        except:
            put(self.db,sheet,province,"Road2",year,0)

            # constant 29
    def Rail2(self,sheet,province,year):# 19 cell 525
        try:
            d = [
               get(self.db,sheet,province,"Rail1",year),
            ]
            if province == "Other":
                province1 = self.otherlist[self.sheetdata.index(sheet)]
                s = self.provincialdb.loc[province1,'underground rail']["2005"].sum()
                value = s*d[0]+41 if year != "2004" and d[0] > 0 else 0.0
                #here
                put(self.db,sheet,province,"Rail2",year,value)
            else:
                s = self.provincialdb.loc[province,'underground rail']["2005"].sum()
                value = s*d[0]+41 if year != "2004" and d[0] > 0 else 0.0
                #here
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

        #constant 30 - years
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

        #constant 31 - years
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

        # constant 32,33,34,35,36,37
    def CausticCost(self,sheet,province,year):# 19 cell 392
        try:
            d = [
                0.6,
                0.1,
                0.2,
                get(self.db,sheet,province,"Bauxite Usage",year),
                self.datadb.loc[self.datadb.sheetname==sheet,'bayer'].sum(),
                self.datadb.loc[self.datadb.sheetname==sheet,"bayer mud sinter"].sum(),
                self.datadb.loc[self.datadb.sheetname==sheet,"sinter"].sum(),
                get(self.db,sheet,province,"A/S ratio draw-down",year),
                get(self.db,sheet,province,"Grade Profile Silica Grade",year),
                0.035,
                self.causticdata.loc[self.datadb.sheetname==sheet,"causticPrice"].sum(),
                self.causticdata.loc[self.datadb.sheetname==sheet,"sodiumCarbonate"].sum(),
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

        # constant 38
    def PreCalculation(self,sheet,province,year):# 19 cell 410
        try:
            d = [
                get(self.db,sheet,province,"Bauxite & Caustic Cost",year),
                self.datadb.loc[self.datadb.sheetname==sheet]["sourcing factor"].sum(),
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
        
    def calc_all3(self,sheet,province,year,dbb):
        Bauxite.cs_outlook(self,sheet,province,year,dbb)
        Bauxite.cs_outlook_total(self,sheet,province,year)


'''
list of db from csv file

'''

capdata1 = pd.read_csv("capbase.csv") #capacity 
depcap1 = pd.read_csv("proddatae.csv")
proddata1 = pd.read_csv("proddatah.csv")
proddata2 = pd.read_csv("proddatae2.csv") # bauxite consumption
proddata3 = pd.read_csv("proddatae.csv")
proddata4 = pd.read_csv("proddatae.csv")
proddata5 = pd.read_csv("proddatae.csv")
proddata6 = pd.read_csv("proddatae.csv")
proddata7 = pd.read_csv("proddatae.csv")
proddata8 = pd.read_csv("proddatae.csv")
proddata9 = pd.read_csv("proddatae.csv")
proddata10 = pd.read_csv("proddatae.csv")
production = pd.read_csv("proddatae.csv")
prodout = pd.read_csv("proddatae.csv")# production
depdata1 = pd.read_csv("depdatae.csv")
depdata2 = pd.read_csv("depdatae.csv")
depdata3 = pd.read_csv("depdatae.csv")
depdata4 = pd.read_csv("depdatae.csv")
depdata5 = pd.read_csv("depdatae.csv")
depdata6 = pd.read_csv("depdatae.csv")
depdata7 = pd.read_csv("depdatah.csv")
depdata8 = pd.read_csv("depdatae.csv")
depdata9 = pd.read_csv("depdatae.csv")
depdata10 = pd.read_csv("depdatae.csv")
depdata11 = pd.read_csv("depdatae.csv")
depdata12 = pd.read_csv("depdatae.csv")
depdata13 = pd.read_csv("depdatae2.csv")
depdata14 = pd.read_csv("depdatae2.csv")
depdata15 = pd.read_csv("depdatae.csv")
depdata16 = pd.read_csv("depdatae2.csv")
depdata17 = pd.read_csv("depdatae2.csv")
depdata18 = pd.read_csv("depdatae2.csv")
depdata19 = pd.read_csv("depdatae2.csv")
bauxitedata1 = pd.read_csv("bauxitedatae.csv")
bauxitedata2 = pd.read_csv("bauxitedatae.csv")
cockpitdata1 = pd.read_csv("cockpitdatah1.csv")
plotlink = pd.read_csv('plotlink.csv')

cockpitdata2 = pd.read_csv("cockpitdatae.csv")
cockpitdata3 = pd.read_csv("cockpitdatah2.csv")
silicadata = pd.read_csv("proddatae2.csv") #grade profile silica grade
aluminadata = pd.read_csv("proddatae2.csv")#grade profile alumina grade
capout = pd.read_csv("proddatae.csv")
class productiondata():
    def __init__(self,capdata1,plotlink,depcap1,capout,proddata1,proddata2,proddata3,proddata4,proddata5,proddata6,proddata7,proddata8,proddata9,proddata10,production,prodout,depdata1,depdata2,depdata3,depdata4,depdata5,depdata6,depdata7,depdata8,depdata9,depdata10,depdata11,depdata12,depdata13,depdata14,depdata15,depdata16,depdata17,depdata18,depdata19,bauxitedata1,bauxitedata2,cockpitdata1,cockpitdata2,cockpitdata3,silicadata,aluminadata):
        self.capdata1 = capdata1
        self.depcap1 = depcap1
        self.proddata1 = proddata1 # cap prod 137
        self.proddata2 = proddata2 # collector 1256
        self.proddata3 = proddata3 # collector 1828
        self.proddata4 = proddata4 # collector 1959
        self.proddata5 = proddata5 # cap prod 531
        self.proddata6 = proddata6 # cap prod 720
        self.proddata7 = proddata7 # cap prod 866
        self.proddata8 = proddata8 # cap prod 1589
        self.proddata9 = proddata9 # cap prod 1713
        self.proddata10 = proddata10 # cap prod 1004
        self.production = production
        self.depdata1 = depdata1 # cap prod 644
        self.depdata2 = depdata2 # not needed
        self.depdata3 = depdata3 # cap prod 657
        self.depdata4 = depdata4 # cap prod 250
        self.depdata5 = depdata5 # not needed 
        self.depdata6 = depdata6 # cap prod 667
        self.depdata7 = depdata7 # cap prod 701
        self.depdata8 = depdata8 # 
        self.depdata9 = depdata9 # collector 2073
        self.depdata10 = depdata10 # cockpit 102
        self.depdata11 = depdata11 # cockpit 159
        self.depdata12 = depdata12 # cockpit 172
        self.depdata13 = depdata13 # collector 2343
        self.depdata14 = depdata14 # collector 2350
        self.depdata15 = depdata15 # collector 1949
        self.depdata16 = depdata16 # collector 2400
        self.depdata17 = depdata17 # collector 2409
        self.depdata18 = depdata18 # collector 2417
        self.depdata19 = depdata19 # collector 2085
        self.bauxitedata1 = bauxitedata1
        self.bauxitedata2 = bauxitedata2
        self.cockpitdata1 = cockpitdata1
        self.cockpitdata2 = cockpitdata2
        self.cockpitdata3 = cockpitdata3
        self.silicadata = silicadata
        self.aluminadata = aluminadata
        self.prodout = prodout
        self.smalldata = pd.read_csv("importe.csv")
        self.capout = capout
        self.plotlink = plotlink
    def calcplotlink(self,db,year,i):
        self.plotlink[year] = self.plotlink[year].astype(float)
        self.plotlink["Factor X"] = self.plotlink["Factor X"].astype(float)
        self.plotlink["Alumina Grade"] = self.plotlink["Alumina Grade"].astype(float)
        self.plotlink["A/S"] = self.plotlink["A/S"].astype(float)
        self.plotlink["open stock"] = self.plotlink["open stock"].astype(float)
        v0 = db.loc[self.plotlink['refinery'][i],year][self.plotlink['province'][i]]["Closing Stock - portion of total"] if self.plotlink['refinery'][i] != 'spare' else 0
        v1 = db.loc[self.plotlink['refinery'][i],'2004'][self.plotlink['province'][i]]["Reserve"] if self.plotlink['refinery'][i] != 'spare' else 0
        v2 = db.loc[self.plotlink['refinery'][i],'2004'][self.plotlink['province'][i]]["Factor X"] if self.plotlink['refinery'][i] != 'spare' else 0
        v3 = db.loc[self.plotlink['refinery'][i],'2004'][self.plotlink['province'][i]]["Alumina Grade"] if self.plotlink['refinery'][i] != 'spare' else 0
        v4 = db.loc[self.plotlink['refinery'][i],'2004'][self.plotlink['province'][i]]["A/S"] if self.plotlink['refinery'][i] != 'spare' else 0
        v0 = 0 if pd.isnull(v0) else v0
        v1 = 0 if pd.isnull(v1) else v1
        v2 = 0 if pd.isnull(v2) else v2
        v3 = 0 if pd.isnull(v3) else v3
        v4 = 0 if pd.isnull(v4) else v4
        v1 = v1 if self.plotlink["bauxite"][i] == "Domestic" else 0
        self.plotlink.set_value(i,year,v0)
        self.plotlink.set_value(i,"Factor X",v2)
        self.plotlink.set_value(i,"Alumina Grade",v3)
        self.plotlink.set_value(i,"A/S",v4)
        self.plotlink.set_value(i,"open stock",v1)
    def calccapout(self,year,i):
        self.capdata1[year] = self.capdata1[year].astype(float)
        v = self.capdata1[year][i]*1000
        self.capout.set_value(i,year,v)
    def calcprod2(self,db,sheet,year,i):
        self.proddata2[year] = self.proddata2[year].astype(float)
        v = db.loc[self.proddata2['refinery'][i],year][self.proddata2['province'][i]]["Bauxite Consumption"] if self.silicadata['refinery'][i] != 'spare' else 0
        self.proddata2.set_value(i,year,v)
    def silica(self,db,sheet,year,i):
        self.silicadata[year] = self.silicadata[year].astype(float)
        v = db.loc[self.silicadata['refinery'][i],year][self.silicadata['province'][i]]["Grade Profile Silica Grade"] if self.silicadata['refinery'][i] != 'spare' else 0
        self.silicadata.set_value(i,year,v)

    def alumina(self,db,sheet,year,i):
        self.aluminadata[year] = self.aluminadata[year].astype(float)
        v = db.loc[self.aluminadata['refinery'][i],year][self.aluminadata['province'][i]]["Grade Profile Alumina Grade"] if self.silicadata['refinery'][i] != 'spare' else 0
        self.aluminadata.set_value(i,year,v)
    
    #capacity 
    def depcap(self,year,i):
        self.depcap1[year] = self.depcap1[year].astype(float)
        v = self.capdata1[year][i] if self.proddata4[year][i] > 0 else 0
        self.depcap1.set_value(i,year,v)
    def calcprod3(self,year,i):
        self.proddata3[year] = self.proddata3[year].astype(float)
        v = self.proddata2.loc[self.proddata2.refinery==self.proddata3['refinery'][i]][year].sum()
        self.proddata3.set_value(i,year,v)
    def calcprod4(self,year,i):
        self.proddata4[year] = self.proddata4[year].astype(float)
        if int(year) > 2012:
            v = self.proddata1[year][i]
            w = 0
            for j in range(2005,int(year)):
                w += self.proddata3[str(j)][i]
            if w != 0 and self.proddata3[year][i] == 0 and self.proddata1[year][i] != 0:
                self.proddata4.set_value(i,year,self.proddata1[year][i]*1000)
    def calcprod5(self,year,i):
        self.proddata5[year] = self.proddata5[year].astype(float)
        if self.proddata4[year][i] > 0:
            self.proddata5.set_value(i,year,self.proddata1[year][i])        
    def calcprod6(self,year,i):
        v = self.depdata7.loc[self.depdata7.category==self.proddata5['category'][i]][self.depdata7.bauxite==self.proddata5['bauxite'][i]][year].sum()
        w = self.depdata7.loc[self.depdata7.technology==self.proddata5['technology'][i]][self.depdata7.bauxite==self.proddata5['bauxite'][i]][year].sum()
        z = self.proddata1[year][i].sum()
        self.proddata6[year] = self.proddata6[year].astype(float)
        self.proddata6.set_value(i,year,(v+w)*z)
    def calcprod7(self,year,i):
        s = 2020
        self.proddata7[year] = self.proddata7[year].astype(float)
        if int(year) < s:
            v = self.proddata1[year][i]
            self.proddata7.set_value(i,year,v)
        else:
            v = self.proddata6[year][i]
            self.proddata7.set_value(i,year,v)
    def calcprod8(self,year,i):
        self.proddata8[year] = self.proddata8[year].astype(float)
        if self.proddata8["bauxite"][i] == "Domestic":
            a = 1
        else:
            a=0
        if self.depcap1[year][i] <= 0:
            b = self.proddata1[year][i]
        else:
            b = (1-self.cockpitdata1.loc[self.cockpitdata1.category==self.proddata8["category"][i]][year].sum())*self.proddata1[year][i]
        self.proddata8.set_value(i,year,a*b)
    def calcprod9(self,year,i):
        self.proddata9[year] = self.proddata9[year].astype(float)
        if self.proddata9["bauxite"][i] == "Domestic" or self.proddata9["bauxite"][i] == "Imported":
            v = proddata1[year][i]-proddata8[year][i]
            self.proddata9.set_value(i,year,v)
    def calcprod10(self,year,i):
        self.proddata10[year] = self.proddata10[year].astype(float)
        if year == "2005":
            v = self.proddata1[year][i]
        else:
            v = self.proddata7[year][i]+self.proddata10[str(int(year)-1)][i]
        self.proddata10.set_value(i,year,v)
    def calcprodout(self,db,year,i):
        self.prodout[year] = self.prodout[year].astype(float)
        d = [
            db.loc[self.prodout['refinery'][i],year]["Guangxi"]["Bauxite Usage"].sum(),
             db.loc[self.prodout['refinery'][i],year]["Guizhou"]["Bauxite Usage"].sum(),
             db.loc[self.prodout['refinery'][i],year]["Henan"]["Bauxite Usage"].sum(),
             db.loc[self.prodout['refinery'][i],year]['Shanxi']["Bauxite Usage"].sum(),
             db.loc[self.prodout['refinery'][i],year]['Other']["Bauxite Usage"].sum(),
            ]
        w = [
            db.loc[self.prodout['refinery'][i],year]["Guangxi"]["Sourcing Mix"] if self.prodout['refinery'][i] != 'spare' else 0,
             db.loc[self.prodout['refinery'][i],year]["Guizhou"]["Sourcing Mix"] if self.prodout['refinery'][i] != 'spare' else 0,
             db.loc[self.prodout['refinery'][i],year]["Henan"]["Sourcing Mix"] if self.prodout['refinery'][i] != 'spare' else 0,
             db.loc[self.prodout['refinery'][i],year]['Shanxi']["Sourcing Mix"] if self.prodout['refinery'][i] != 'spare' else 0,
             db.loc[self.prodout['refinery'][i],year]['Other']["Sourcing Mix"] if self.prodout['refinery'][i] != 'spare' else 0,
            ]
        
        t = d[0]*w[0]+d[1]*w[1]+d[2]*w[2]+d[3]*w[3]+d[4]*w[4]
        if t > 0:
            v = self.production[year][i]
        else:
            v = 0
        self.prodout.set_value(i,year,v)
    def production(self,sheet,year,i):
        self.production[year] = self.production[year].astype(float)
        self.production.set_value(i,year,self.proddata7[year][i]*1000)
        pass#sumproduct()
    def calcdep1(self,year,i):
        v = self.proddata5.loc[self.proddata5.category==self.depdata1['category'][i]][self.proddata5.bauxite==self.depdata1['bauxite'][i]][year].sum()
        w = self.proddata5.loc[self.proddata5.technology==self.depdata1['technology'][i]][self.proddata5.bauxite==self.depdata1['bauxite'][i]][year].sum()
        self.depdata1[year] = self.depdata1[year].astype(float)
        self.depdata1.set_value(i,year,v+w)
    
    def calcdep3(self,year,i):
        self.depdata3[year] = self.depdata3[year].astype(float)
        v = (1-self.cockpitdata2[year][i])*self.depdata1[year][i]
        self.depdata3.set_value(i,year,v)
    def calcdep4(self,year,i):
        self.depdata4[year] = self.depdata4[year].astype(float)
        v = self.proddata1.loc[self.proddata1.category==self.depdata4['category'][i]][self.proddata1.bauxite==self.depdata4['bauxite'][i]][year].sum()
        w = self.proddata1.loc[self.proddata1.technology==self.depdata4['technology'][i]][self.proddata1.bauxite==self.depdata4['bauxite'][i]][year].sum()
        self.depdata4.set_value(i,year,v+w)
    def calcdep6(self,year,i):
        self.depdata6[year] = self.depdata6[year].astype(float)
        v = self.depdata1[year][i]
        w = self.depdata4[year][i]
        self.depdata6.set_value(i,year,w-v)
    def calcdep7(self,year,i):
        self.depdata7[year] = self.depdata7[year].astype(float)
        if self.cockpitdata3[year][i] == "yes" and self.depdata6[str(int(year)-1) if int(year) > 2005 else "2005"][i].sum() > 0:
            v = (self.depdata6[str(int(year)-1) if int(year) > 2005 else "2005"][i]+self.depdata3[str(int(year)-1) if int(year) > 2005 else "2005"][i])/self.depdata6[str(int(year)-1) if int(year) > 2005 else "2005"][i]
            self.depdata7.set_value(i,year,v)
            
    def calcdep8(self,year,i):
        self.depdata8[year] = self.depdata8[year].astype(float)
        v = self.prodout.loc[self.prodout.category==self.depdata8['category'][i]][self.prodout.bauxite==self.depdata8['bauxite'][i]][year].sum()
        w = self.prodout.loc[self.prodout.technology==self.depdata8['technology'][i]][self.prodout.bauxite==self.depdata8['bauxite'][i]][year].sum()
        self.depdata8.set_value(i,year,v+w)
        # continue here .......
    def calcdep9(self,year,i):
        self.depdata9[year] = self.depdata9[year].astype(float)
        v = self.proddata4.loc[self.proddata4.category==self.depdata9['category'][i]][self.proddata4.bauxite==self.depdata9['bauxite'][i]][year].sum()
        w = self.proddata4.loc[self.proddata4.technology==self.depdata9['technology'][i]][self.proddata4.bauxite==self.depdata9['bauxite'][i]][year].sum()
        self.depdata9.set_value(i,year,v+w)
    def calcdep10(self,year,i):
        self.depdata10[year] = self.depdata10[year].astype(float)
        if i < 5:
            v = (self.cockpitdata2[year][i]*self.depdata9[year][i])/1000
        else:
            v = self.depdata8[year][i]/1000
        self.depdata10.set_value(i,year,v)
        pass # doubt cockpit row 102

    def calcdep11(self,year,i):
        self.depdata11[year] = self.depdata11[year].astype(float)
        b = self.smalldata[year][1] if self.depdata10["technology"][i] == "L-B" else self.smalldata[year][0]
        v = self.depdata10[year][i]*b
        self.depdata11.set_value(i,year,v)

    def calcdep12(self,year,i):
        self.depdata12[year] = self.depdata12[year].astype(float)
        b = self.smalldata[year][1] if self.depdata10["technology"][i] == "L-B" else self.smalldata[year][0]
        v = self.depdata9[year][i]*b/1000
        self.depdata12.set_value(i,year,v)

    def calcdep13(self,year,i):
        self.depdata13[year] = self.depdata13[year].astype(float)
        if i < 4:
            v = self.proddata2.loc[self.proddata2.Province==self.depdata13['category'][i]][self.proddata2.bauxite==self.depdata13['bauxite'][i]][year].sum()
        else:
            v = (self.proddata2.loc[self.proddata2.bauxite==self.depdata13['bauxite'][i]][year].sum())-(self.depdata13[year][0]+self.depdata13[year][1]+self.depdata13[year][2]+self.depdata13[year][3])
        self.depdata13.set_value(i,year,v)

    def calcdep14(self,year,i):
        self.depdata14[year] = self.depdata14[year].astype(float)
        v = self.depdata13[year][i]/self.depdata8[year][i] if self.depdata8[year][i] > 0 else 0
        self.depdata14.set_value(i,year,v)
    def calcdep15(self,year,i):
        self.depdata15[year] = self.depdata15[year].astype(float)
        v = self.proddata3.loc[self.proddata3.category==self.depdata15['category'][i]][self.proddata3.bauxite==self.depdata15['bauxite'][i]][year].sum()
        w = self.proddata3.loc[self.proddata3.technology==self.depdata15['technology'][i]][self.proddata3.bauxite==self.depdata15['bauxite'][i]][year].sum()
        self.depdata15.set_value(i,year,v+w)
    def calcdep16(self,year,i):
        self.depdata16[year] = self.depdata16[year].astype(float)
        v = 0
        for j in range(2005,int(year)+1):
            v+=self.depdata15[str(j)][i]
        self.depdata16.set_value(i,year,v)
    def calcdep17(self,year,i):
        self.depdata17[year] = self.depdata17[year].astype(float)
        v = 0
        for j in range(2005,int(year)+1):
            v+=self.depdata8[str(j)][i]
        self.depdata17.set_value(i,year,v)
    def calcdep18(self,year,i):
        self.depdata18[year] = self.depdata18[year].astype(float)
        v = self.depdata16[year][i]/self.depdata17[year][i]
        self.depdata18.set_value(i,year,v)

    def calcdep19(self,year,i):
        self.depdata19[year] = self.depdata19[year].astype(float)
        v = self.depdata9[year][i]/self.depdata4[year][i]/1000
        self.depdata19.set_value(i,year,v)
    def calcbauxitedata1(self,year):
        self.bauxitedata1[year] =self.bauxitedata1[year].astype(float)
        v1 = self.depdata11[year][5]+self.depdata11[year][6]
        v2 = self.depdata12[year][8]
        v3 = self.depdata11[year][0]+self.depdata11[year][1]+self.depdata11[year][2]+self.depdata11[year][3]+self.depdata11[year][4]
        v4 = v1+v3
        self.bauxitedata1.set_value(0,year,v1)
        self.bauxitedata1.set_value(1,year,v2)
        
        self.bauxitedata1.set_value(2,year,v3)
        self.bauxitedata1.set_value(3,year,v4)

    def calcbauxitedata2(self,year):
        self.bauxitedata2[year] =self.bauxitedata2[year].astype(float)
        v1 = self.bauxitedata1[year][0]/self.smalldata[year][0]
        v2 = self.bauxitedata1[year][1]/self.smalldata[year][0]
        v3 = self.bauxitedata1[year][2]/self.smalldata[year][0]
        v4 = self.bauxitedata1[year][3]/self.smalldata[year][0]
        self.bauxitedata2.set_value(0,year,v1)
        self.bauxitedata2.set_value(1,year,v2)
        self.bauxitedata2.set_value(2,year,v3)
        self.bauxitedata2.set_value(3,year,v4)

    def calccock1(self,year):
        v = self.cockpitdata1[year][2]+self.cockpitdata1[year][3]
        v=v/2
        self.cockpitdata1.set_value(4,year,v)
    def calccock2(self,year,i):
        self.cockpitdata2[year] = self.cockpitdata2[year].astype(float)
        s = "yes"
        if s != "no":
            self.cockpitdata2.set_value(i,year,self.cockpitdata1[year][i])
    def pcalcall1(self,sheet,year):
        productiondata.calccock1(self,year)
        for j in range(5):
            productiondata.calcdep7(self,year,j)
        depdata7.set_value(8,year,depdata7[year].sum())
        for j in range(self.proddata6.shape[0]):                               # here j is row number of db
            productiondata.calcprod6(self,year,j)
        for j in range(self.proddata7.shape[0]):
            productiondata.calcprod7(self,year,j)
        for j in range(self.production.shape[0]):
            productiondata.production(self,sheet,year,j)
    def pcalcall2(self,db,sheet,year):
        for j in range(self.proddata2.shape[0]):
            productiondata.calcprod2(self,db,sheet,year,j)
        for j in range(self.proddata3.shape[0]):
            productiondata.calcprod3(self,year,j)
        for j in range(self.proddata4.shape[0]):
            productiondata.calcprod4(self,year,j)   
        for j in range(self.proddata5.shape[0]):
            productiondata.calcprod5(self,year,j)
        for j in range(5):
            productiondata.calccock2(self,year,j)
        for j in range(5):
            productiondata.calcdep1(self,year,j)
        depdata1.set_value(8,year,depdata1[year].sum())
        for j in range(5):
            productiondata.calcdep3(self,year,j)
        depdata3.set_value(8,year,depdata3[year].sum())
        for j in range(8):
            productiondata.calcdep4(self,year,j)
        depdata4.set_value(8,year,depdata4[year].sum())
        for j in range(5):
            productiondata.calcdep6(self,year,j)
        depdata6.set_value(8,year,depdata6[year].sum())
            
        for j in range(self.depcap1.shape[0]):
            productiondata.depcap(self, year, j)
        for j in range(self.prodout.shape[0]):
            productiondata.calccapout(self, year, j)
        for j in range(self.silicadata.shape[0]):
            productiondata.silica(self,db,sheet,year,j)
        for j in range(self.aluminadata.shape[0]):
            productiondata.alumina(self,db,sheet,year,j)
    def pcalcall3(self,db,year):
        for j in range(self.prodout.shape[0]):
            productiondata.calcprodout(self,db,year,j)
        for j in range(self.proddata8.shape[0]):
            productiondata.calcprod8(self,year,j)
        for j in range(self.proddata9.shape[0]):
            productiondata.calcprod9(self,year,j)
        for j in range(self.proddata10.shape[0]):
            productiondata.calcprod10(self,year,j)
        for j in range(8):
            productiondata.calcdep8(self,year,j)
        depdata7.set_value(8,year,depdata7[year].sum())
        for j in range(5):
            productiondata.calcdep9(self,year,j)
        depdata9.set_value(8,year,depdata9[year].sum())
        for j in range(8):
            productiondata.calcdep10(self,year,j)
        depdata10.set_value(8,year,depdata10[year].sum())
        for j in range(8):
            productiondata.calcdep11(self,year,j)
        depdata11.set_value(8,year,depdata11[year].sum())
        for j in range(8):
            productiondata.calcdep12(self,year,j)
        depdata12.set_value(8,year,depdata12[year].sum())
        for j in range(5):
            productiondata.calcdep13(self,year,j)
        depdata13.set_value(5,year,depdata13[year].sum()) #checked formula
        for j in range(5):
            productiondata.calcdep14(self,year,j)
        depdata14.set_value(8,year,depdata14[year].sum())
        for j in range(8):
            productiondata.calcdep15(self,year,j)   
        depdata15.set_value(8,year,depdata15[year].sum())
        for j in range(5):
            productiondata.calcdep16(self,year,j)
        depdata16.set_value(5,year,depdata16[year].sum()) #checked formul
        for j in range(5):
            productiondata.calcdep17(self,year,j)
        depdata17.set_value(5,year,depdata17[year].sum())
        for j in range(5):
            productiondata.calcdep18(self,year,j)
        depdata18.set_value(5,year,depdata18[year].sum())
        for j in range(5):
            productiondata.calcdep19(self,year,j)
        productiondata.calcbauxitedata1(self,year)
        productiondata.calcbauxitedata2(self,year)
    def pcalcall4(self,db,year):
        for j in range(self.plotlink.shape[0]):
            productiondata.calcplotlink(self,db,year,j)
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
    'Hubei',
    'Hunan',
    'IM'
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
    'open pit mine mining cost electricity price2',
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
exlist = [
     8.19,
     7.97,
     7.61,
     6.95,
     6.83,
     6.77,
     6.46,
     6.31,
     6.15,
     6.15,
     6.10,
     6.10,
     6.00,
     6.00,
     5.90,
     5.90,
     5.90,
     5.80,
     5.80,
     5.80,
     5.80,
     5.80,
     5.70,
     5.70,
     5.70,
     5.70,
     5.70 

]
globaldb = pd.DataFrame(globaldata)
bayersinterdb = pd.read_csv("caustic.csv")
sheet = bayersinterdb['sheetname'].tolist()
#calculate production values here


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
otherlist = proddata1['province']
provicialcalc = provincial()

for j in Province:
    provicialcalc.provincialcalcall(j)
r = summary()
r.calcall1()
mxid = pd.MultiIndex.from_product([sheet,province,pm])
mxid2 = pd.MultiIndex.from_product([Province,proparam])
db = pd.DataFrame(index=mxid,columns=yearsinput)
idx = pd.IndexSlice
summarydb = r.hdb
b1 = Bauxite(db,bayersinterdb,globaldb,summarydb,provicialcalc.db,otherlist,exlist)
c1 = productiondata(capdata1,plotlink,depcap1,capout,proddata1,proddata2,proddata3,proddata4,proddata5,proddata6,proddata7,proddata8,proddata9,proddata10,production,prodout,depdata1,depdata2,depdata3,depdata4,depdata5,depdata6,depdata7,depdata8,depdata9,depdata10,depdata11,depdata12,depdata13,depdata14,depdata15,depdata16,depdata17,depdata18,depdata19,bauxitedata1,bauxitedata2,cockpitdata1,cockpitdata2,cockpitdata3,silicadata,aluminadata)
provincialdb = pd.DataFrame(index=mxid2,columns=years)


for j in sheet:
    for k in province:
        b1.calc_all1(sheet=j,province=k,year="2004")
    for l in province:
        b1.calc_all2(sheet=j,province=l,year="2004",dbb=c1)
    for m in province:
        b1.calc_all3(sheet=j,province=m,year="2004",dbb=c1)


#2005 starts
c1.pcalcall1(sheet=j,year="2005")
for i in years:
    
    for j in sheet:
        for k in province:
            b1.calc_all1(sheet=j,province=k,year=i)
            
    c1.pcalcall1(sheet=j,year=i)
    for j in sheet:
        for l in province:
            b1.calc_all2(sheet=j,province=l,year=i,dbb=c1)
    c1.pcalcall2(db=b1.db,sheet=j,year=i)
    c1.pcalcall1(sheet=j,year=str(int(i)+1)) if int(i) < 2031 else print("ok")
    for j in sheet:
        for l in province:
            b1.calc_all3(sheet=j,province=l,year=i,dbb=c1)
    c1.pcalcall3(db=b1.db,year=i)
    c1.pcalcall4(db=b1.db,year=i)
r.calcall2(c1.proddata10)
    
            

with pd.option_context('display.max_rows',None, 'display.max_columns', None):
    b1.db.to_csv('outputdata/wholerefinery.csv')
    c1.proddata1.to_csv('outputdata/capprod137_proddata1output.csv')
    c1.proddata2.to_csv('outputdata/collector1256_proddata2output.csv')
    c1.proddata3.to_csv('outputdata/collector1828_proddata3output.csv')
    c1.proddata4.to_csv('outputdata/collector1959_proddata4output.csv')
    c1.proddata5.to_csv('outputdata/capprod531_proddata5output.csv')
    c1.proddata6.to_csv('outputdata/capprod720_proddata6output.csv')
    c1.proddata7.to_csv('outputdata/capprod866_proddata7output.csv')
    c1.proddata8.to_csv('outputdata/capprod1589_proddata8output.csv')
    c1.proddata9.to_csv('outputdata/capprod1713_proddata9output.csv')
    c1.proddata10.to_csv('outputdata/capprod1004_proddata10output.csv')
    c1.depdata1.to_csv('outputdata/capprod644_depdata1output.csv')
    c1.capdata1.to_csv('outputdata/depcap1output.csv')
    #c1.depdata2.to_csv('outputdata/depdata2output.csv')
    c1.depdata3.to_csv('outputdata/capprod657_depdata3output.csv',index=False)
    c1.depdata4.to_csv('outputdata/capprod250_depdata4output.csv',index=False)
    #c1.depdata5.to_csv('outputdata/depdata5output.csv')
    c1.depdata6.to_csv('outputdata/capprod667_depdata6output.csv',index=False)
    c1.depdata7.to_csv('outputdata/capprod701_depdata7output.csv',index=False)
    c1.depdata8.to_csv('outputdata/depdata8output.csv',index=False)
    c1.depdata9.to_csv('outputdata/collector2073_depdata9output.csv',index=False)
    c1.depdata10.to_csv('outputdata/cockpit102_depdata10output.csv',index=False)
    c1.depdata11.to_csv('outputdata/cockpit159_depdata11output.csv',index=False)
    c1.depdata12.to_csv('outputdata/cockpit172_depdata12output.csv',index=False)
    c1.depdata13.to_csv('outputdata/collector2343_depdata13output.csv',index=False)
    c1.depdata14.to_csv('outputdata/collector2350_barbyrefoutput.csv',index=False)
    c1.depdata15.to_csv('outputdata/collector1949_depdata15output.csv',index=False)
    c1.depdata16.to_csv('outputdata/collector2400_depdata16output.csv',index=False)
    c1.depdata17.to_csv('outputdata/collector2409_depdata17output.csv',index=False)
    c1.depdata18.to_csv('outputdata/collector2417_CumulativedomesticBXtoAA.csv',index=False)
    c1.depdata19.to_csv('outputdata/collector2085_depdata19output.csv',index=False)
    c1.bauxitedata1.to_csv('outputdata/bauxitedata1output.csv')
    c1.bauxitedata2.to_csv('outputdata/bauxitedata2output.csv')
    c1.cockpitdata1.to_csv('outputdata/cockpitdata1output.csv')
    c1.cockpitdata2.to_csv('outputdata/cockpitdata2output.csv')
    c1.cockpitdata3.to_csv('outputdata/cockpitdata3output.csv')
    c1.aluminadata.to_csv('outputdata/aluminagradeoutput.csv')
    c1.plotlink.to_csv('outputdata/plotlinkoutput.csv')
    c1.silicadata.to_csv('outputdata/silicagradeoutput.csv')
    c1.production.to_csv('outputdata/productionoutput.csv')
    c1.prodout.to_csv('outputdata/prodoutoutput.csv')
    b1.provincialdb.to_csv("outputdata/provincialoutput.csv")
    r.db.to_csv('outputdata/db.csv',index=False)
    r.provt.to_csv('outputdata/provtdb.csv',index=False)
    r.bfdb.to_csv('outputdata/bfdb.csv',index=False)
    r.hdb.to_csv('outputdata/hdb.csv',index=False)
    r.totaldb.to_csv('outputdata/totaldb.csv',index=False)
    r.newallocdb.to_csv('outputdata/newallocdb.csv',index=False)
    for i in sheet:
        hh = b1.db.loc[i]
        hh.to_csv("outputdata/refinery/"+i+" output.csv")

