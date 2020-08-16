# import connect
import simply
from simply import *
import warnings
from scipy import stats
import numpy as np
import pandas as pd
import statistics as stat
# import openpyxl
from ddm import newprovincial
from ddm import reservesummary
from collections import defaultdict
from ddm.newprovincial import provincial
from ddm.reservesummary import summary
from ddm.codetimer.timer import Timer
from flatdb.flatdbconverter import Flatdbconverter
from outputdb.uploadtodb import upload
import time
import asyncio
#from aiostream import stream,pipe
import sys
warnings.filterwarnings("ignore")
# a = connect.to_db()
# cnxn = a.outputstart()
db_conv = Flatdbconverter("Draw Down Model")
bayersinterdb = pd.read_csv("ddm/caustic.csv")
sheets = bayersinterdb['refinery'].tolist()
sheets = np.array(sheets)
_, idx = np.unique(sheets, return_index=True)
sheets = sheets[np.sort(idx)]
print(sheets)
Provinces =['Guangxi',
            'Guizhou',
            'Henan',
            'Shanxi',
            'Other'
            ]
print("ddm sql connected")
'''

Bauxite main model starts here
'''
# print(sheets)

# excel files 

'''
list of db from csv file

'''
# capdata1 = pd.read_csv("ddm/capbase.csv") #capacity 
# depcap1 = pd.read_csv("ddm/proddatae.csv")
# proddata1 = pd.read_csv("ddm/proddatah.csv")
# proddata2 = pd.read_csv("ddm/proddatae2.csv") # bauxite consumption
# proddata3 = pd.read_csv("ddm/proddatae.csv")
# proddata4 = pd.read_csv("ddm/proddatae.csv")
# proddata5 = pd.read_csv("ddm/proddatae.csv")
# proddata6 = pd.read_csv("ddm/proddatae.csv")
# proddata7 = pd.read_csv("ddm/proddatae.csv")
# proddata8 = pd.read_csv("ddm/proddatae.csv")
# proddata9 = pd.read_csv("ddm/proddatae.csv")
# proddata10 = pd.read_csv("ddm/proddatae.csv")
# production = pd.read_csv("ddm/proddatae.csv")
# prodout = pd.read_csv("ddm/proddatae.csv") # production
# depdata1 = pd.read_csv("ddm/depdatae.csv")
# depdata2 = pd.read_csv("ddm/depdatae.csv")
# depdata3 = pd.read_csv("ddm/depdatae.csv")
# depdata4 = pd.read_csv("ddm/depdatae.csv")
# depdata5 = pd.read_csv("ddm/depdatae.csv")
# depdata6 = pd.read_csv("ddm/depdatae.csv")
# depdata7 = pd.read_csv("ddm/QuickSummarySwitches.csv")
# depdata8 = pd.read_csv("ddm/depdatae.csv")
# depdata9 = pd.read_csv("ddm/depdatae.csv")
# depdata10 = pd.read_csv("ddm/depdatae.csv")
# depdata11 = pd.read_csv("ddm/depdatae.csv")
# depdata12 = pd.read_csv("ddm/depdatae.csv")
# depdata13 = pd.read_csv("ddm/depdatae2.csv")
# depdata14 = pd.read_csv("ddm/depdatae2.csv")
# depdata15 = pd.read_csv("ddm/depdatae.csv")
# depdata16 = pd.read_csv("ddm/depdatae2.csv")
# depdata17 = pd.read_csv("ddm/depdatae2.csv")
# depdata18 = pd.read_csv("ddm/depdatae2.csv")
# depdata19 = pd.read_csv("ddm/depdatae2.csv")
# bauxitedata1 = pd.read_csv("ddm/bauxitedatae.csv")
# bauxitedata2 = pd.read_csv("ddm/bauxitedatae.csv")
# cockpitdata1 = pd.read_csv("ddm/cockpit3rdPartySwitching.csv")
# plotlink = pd.read_csv('ddm/plotlink.csv')

# cockpitdata2 = pd.read_csv("ddm/cockpitdatae.csv")
# cockpitdata3 = pd.read_csv("ddm/cockpitAllow3rdPartyTrade.csv")
# silicadata = pd.read_csv("ddm/proddatae2.csv") #grade profile silica grade
# aluminadata = pd.read_csv("ddm/proddatae2.csv")#grade profile alumina grade
# capout = pd.read_csv("ddm/proddatae.csv")



timer = Timer("bauxite", txt=True)
class Bauxite():
    def __init__(self,db,datadb,globaldb,summarydb,provincialdb,otherlist,exlist):  # These are all different static databases
        timer.start()
        self.db = db                                                                             
        self.globaldb = globaldb                      # global values db cell 42 to 54
        self.datadb = datadb
        self.summarydb = summarydb                    # reserve summary db 
        self.provincialdb = provincialdb
        self.capdata1 = pd.read_csv("ddm/capbase.csv") #capacity 
        self.depcap1 = pd.read_csv("ddm/proddatae.csv")
        self.proddata1 = pd.read_csv("ddm/proddatah.csv") # cap prod 137
        self.proddata2 = pd.read_csv("ddm/proddatae2.csv") # bauxite consumption # collector 1256
        self.proddata3 = pd.read_csv("ddm/proddatae.csv") # collector 1828
        self.proddata4 = pd.read_csv("ddm/proddatae.csv") # collector 1959
        self.proddata5 = pd.read_csv("ddm/proddatae.csv") # cap prod 531
        self.proddata6 = pd.read_csv("ddm/proddatae.csv") # cap prod 720
        self.proddata7 = pd.read_csv("ddm/proddatae.csv") # cap prod 867
        self.proddata8 = pd.read_csv("ddm/proddatae.csv") # cap prod 1589
        self.proddata9 = pd.read_csv("ddm/proddatae.csv") # cap prod 1713
        self.proddata10 = pd.read_csv("ddm/proddatae.csv") # cap prod 1004
        self.production = pd.read_csv("ddm/proddatae.csv")
        self.prodout = pd.read_csv("ddm/proddatae.csv") # production
        self.depdata1 = pd.read_csv("ddm/depdatae.csv") # cap prod 644
        self.depdata2 = pd.read_csv("ddm/depdatae.csv") # not needed
        self.depdata3 = pd.read_csv("ddm/depdatae.csv") # cap prod 657
        self.depdata4 = pd.read_csv("ddm/depdatae.csv") # cap prod 250
        self.depdata5 = pd.read_csv("ddm/depdatae.csv") # not needed 
        self.depdata6 = pd.read_csv("ddm/depdatae.csv") # cap prod 667
        self.depdata7 = pd.read_csv("ddm/QuickSummarySwitches.csv") # cap prod 701
        self.depdata8 = pd.read_csv("ddm/depdatae.csv") # collector 2333
        self.depdata9 = pd.read_csv("ddm/depdatae.csv") # collector 2073
        self.depdata10 = pd.read_csv("ddm/depdatae.csv") # cockpit 102
        self.depdata11 = pd.read_csv("ddm/depdatae.csv") # cockpit 159
        self.depdata12 = pd.read_csv("ddm/depdatae.csv") # cockpit 172
        self.depdata13 = pd.read_csv("ddm/depdatae2.csv") # collector 2343
        self.depdata14 = pd.read_csv("ddm/depdatae2.csv") # collector 2350
        self.depdata15 = pd.read_csv("ddm/depdatae.csv") # collector 1949
        self.depdata16 = pd.read_csv("ddm/depdatae2.csv") # collector 2400
        self.depdata17 = pd.read_csv("ddm/depdatae2.csv") # collector 2409
        self.depdata18 = pd.read_csv("ddm/depdatae2.csv") # collector 2417
        self.depdata19 = pd.read_csv("ddm/depdatae2.csv") # collector 2085
        self.bauxitedata1 = pd.read_csv("ddm/bauxitedatae.csv") # # cockpit 188
        self.bauxitedata2 = pd.read_csv("ddm/bauxitedatae.csv") # cockpit 204
        self.cockpitdata1 = pd.read_csv("ddm/cockpit3rdPartySwitching.csv")
        self.plotlink = pd.read_csv('ddm/plotlink.csv')
        self.cockpitdata2 = pd.read_csv("ddm/cockpitdatae.csv")
        self.cockpitdata3 = pd.read_csv("ddm/cockpitAllow3rdPartyTrade.csv")
        self.silicadata = pd.read_csv("ddm/proddatae2.csv") #grade profile silica grade
        self.aluminadata = pd.read_csv("ddm/proddatae2.csv")#grade profile alumina grade
        self.capout = pd.read_csv("ddm/proddatae.csv")

        self.smalldata = pd.read_csv("ddm/importe.csv")
        # print(self.plotlink)
        # print(plotlink.index)
        self.causticdata = pd.read_csv('ddm/caustic.csv')
        # self.otherlist = otherlist
        province1 = 'Chongqing'
        self.sheetdata = self.datadb["refinery"].tolist()
        self.exlist = exlist
        self.factx = pd.read_csv('ddm/DrawDownTechFactorInputs_1.csv')
        constant = pd.read_csv('ddm/DrawDownTechFactorInputs_2.csv')
        self.constant = constant['value']
        self.max_col = len(self.db.columns)
        self.init_col = '2004'
        self.pr1 = ['Lubei', 'Bochuang', 'Nanhan', 'Datang Tuoketuo Fly Ash', 'Mengxi Fly Ash']
        # self.pr2 = []
        # self.pr2 = [a.lower() for a in self.pr2]
        self.pr1 = [a.lower() for a in self.pr1]
        self.pr = [*self.pr1]
        print(self.pr)
        timer.stop()

    def reserve(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                v = self.summarydb.loc[(self.summarydb.province==province), sheet+" Allocation"].sum()
                # print(self.summarydb[self.summarydb.province==province])
                val = self.plotlink.loc[(self.plotlink['refinery'] == sheet) & (self.plotlink['province'] == province) & (self.plotlink['bauxite'] == 'Domestic') , 'open stock'].values
                if len(val) > 0:
                    self.plotlink.loc[(self.plotlink['refinery'] == sheet) & (self.plotlink['province'] == province) & (self.plotlink['bauxite'] == 'Domestic') , 'open stock'] = v
                else:
                    self.plotlink.loc[(self.plotlink['refinery'] == sheet) & (self.plotlink['province'] == province) & (self.plotlink['bauxite'] == 'Domestic') , 'open stock'] = 0
                put(self.db,sheet,province,"Reserve",self.init_col,v)
        
                # print('reserve')
        timer.stop()


    def aluminagrade(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                v = self.summarydb.loc[self.summarydb.province==province, sheet+" Avg %Al2O3"].sum()
                self.plotlink.loc[(self.plotlink['refinery'] == sheet) & (self.plotlink['province'] == province), 'Alumina Grade'] = v 
                put(self.db,sheet,province,"Alumina Grade",self.init_col,v)
        timer.stop()

    def asratio(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                v = self.summarydb.loc[self.summarydb.province==province, sheet+" Avg A/S"].sum()
                self.plotlink.loc[(self.plotlink['refinery'] == sheet) & (self.plotlink['province'] == province), 'A/S'] = v 
                put(self.db,sheet,province,"A/S",self.init_col,v)
        timer.stop()

    def closed(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                v = self.summarydb.loc[self.summarydb.province==province, sheet+" Closed"].sum()
                put(self.db,sheet,province,"Closed",self.init_col,v)
        timer.stop([v])

        #constant1 global
    def demand(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                v = 0.99999999
                put(self.db,sheet,province,"Demand Profile",self.init_col,v)
        timer.stop()

        #constant2 
    def usagee(self,year):
        timer.start()
        dh = ['Bosai Wanzhou','Tuoyuan', 'Jinjiang Chongzuo import', 'Xinfa Jingxi', 'Huafei', 'Qizheng Chemical Added LT', 'Kaiman', 'Kaiman Import', 'East Hope Mianchi', 'East Hope Mianchi Added LT', 'Huiyuan Lushan', 'Shenhuo', 'Wanji', 'Wanji import', 'Weilai', 'Yifeng Gongyi (Yulian Xinya)', 'Yima Yixiang', 'Shaoxing', 'Kaisheng', 'Zhongmei', 'Zhongchao', 'Jialu Xianfeng', 'Jinshi', 'Huixiang', 'Mengxi import', 'Tiangui', 'Jinjiang Lianyungang', 'Chalco Lianyungang', 'SPIC - Wuchuan', 'Chalco Shandong sinter import', 'Aokaida', 'Huajin', 'Dongxu Tianyuan', 'Dongxu Tianyuan import', 'East Hope Lingshi import', 'Huaqing', 'Taixing', 'Wusheng', 'Xinfa - Jiaokou', 'Xingan Chemical', 'Xingan Chemical import', 'Yangquan Zhaofeng', 'Gangyuan Jiaohua', 'Xinghua (Chalco Jiaokou) import', 'YMG - Wenshan', 'Senze', 'Fusheng', 'Tiantong']
        dh = [a.lower() for a in dh]
        for sheet in sheets:
            for province in Provinces:
                if year == '2004':
                    if (province == 'Other') and (sheet.lower() in dh)  :
                        v = 0.2
                    else:
                        v = 0.1
                else:
                    v = get(self.db,sheet,province,"Bauxite Consumption",year)
                put(self.db,sheet,province,"Usage",year,v)
        timer.stop()

    def factorx(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                v = self.factx.loc[self.factx.refinery==sheet][self.factx.province==province]['factorx'].sum()
                self.plotlink.loc[(self.plotlink['refinery'] == sheet) & (self.plotlink['province'] == province), 'Factor X'] = v 
                put(self.db,sheet,province,"Factor X",self.init_col,v)    
        timer.stop()

    def purchased(self):
        timer.start()
        # try:
        for sheet in sheets:
            for province in Provinces:
                a = get(self.db,sheet,province,"Closed",self.init_col)
                b = get(self.db,sheet,province,"Reserve",self.init_col)
                d = 1 - a if b > 0 else 0
                value = d
                put(self.db,sheet,province,"Purchased",self.init_col,value)
        timer.stop()

    def silica(self):
        timer.start()
        # try:
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Alumina Grade",self.init_col),     # precedent values
                        get(self.db,sheet,province,"A/S",self.init_col),
                        self.constant[0]
                        ]
                value = d[0]/d[1] if d[1] > d[2] else d[2]                                         # formula
                put(self.db,sheet,province,"Silica Grade",self.init_col,value)      # putting value in db
        timer.stop([value])

        # constant 3 , 4
    def starting_ratio(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [self.constant[10],
                    get(self.db,sheet,province,"A/S",self.init_col)
                    ]
                value = d[0]*d[1]
                put(self.db,sheet,province,"Starting ratio",self.init_col,value)
        timer.stop()

        # constant 5 , 6
    def depletion_ratio(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [self.constant[12],
                    self.constant[11],
                    get(self.db,sheet,province,"A/S",self.init_col),
                    ]
                value = max(d[0], d[1] * d[2])
                put(self.db,sheet,province,"Depletion ratio",self.init_col,value)
        timer.stop()


        # constant 7 , 8 , 9 , 10
    def alumina_profile_starting_grade(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [self.constant[2],
                        1,
                        0.164,
                        2,
                        get(self.db,sheet,province,"Alumina Grade",self.init_col),
                        self.constant[3]
                        ]
                value = min(d[0],(d[5])*d[4])
                put(self.db,sheet,province,"Alumina Profile Starting grade",self.init_col,value)
        timer.stop()

        # constant 11 , 12  , 13 , 14
    def alumina_profile_depletion_grade(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [1,
                        0.76,
                        2,
                        self.constant[5],
                        get(self.db,sheet,province,"Alumina Grade",self.init_col),
                        get(self.db,sheet,province,"Reserve",self.init_col),
                        self.constant[0],
                        self.constant[4]
                        ]
                value = max((d[7])*d[4],d[3]) if d[5] > d[6] else d[6]
                put(self.db,sheet,province,"Alumina Profile Depletion grade",self.init_col,value)
        timer.stop([value])

    def alumina_profile_scaled_mean(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Alumina Grade",self.init_col),
                        get(self.db,sheet,province,"Alumina Profile Starting grade",self.init_col),
                        get(self.db,sheet,province,"Alumina Profile Depletion grade",self.init_col),
                        get(self.db,sheet,province,"Reserve",self.init_col),
                        self.constant[0]
                        ]
                value = abs(d[0] - min(d[1],d[2]))/(max(d[1],d[2])- min(d[1],d[2])) if d[3] > 0 else 0
                # value = 0 if value < 0 else value
                put(self.db,sheet,province,"Alumina Profile Scaled mean",self.init_col,value)
        timer.stop()
        # constant 15 , 16
    def alumina_profile_scaled_variance(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [self.constant[6],
                        self.constant[7],
                        get(self.db,sheet,province,"Alumina Grade",self.init_col),
                        get(self.db,sheet,province,"Factor X",self.init_col),
                        get(self.db,sheet,province,"Alumina Profile Starting grade",self.init_col),
                        get(self.db,sheet,province,"Alumina Profile Depletion grade",self.init_col),
                        get(self.db,sheet,province,"Reserve",self.init_col),
                        self.constant[0],
                        self.constant[8]
                        ]
                value = (pow(d[0]*d[2],d[8])/(d[1]*d[3]))/pow(max(d[4],d[5])-min(d[4],d[5]),d[8]) if d[6] > d[7] else d[7]
                put(self.db,sheet,province,"Alumina Profile Scaled variance",self.init_col,value)
        timer.stop()
            
    def alumina_profile_alpha_value(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Alumina Profile Scaled mean",self.init_col),
                        get(self.db,sheet,province,"Alumina Profile Scaled variance",self.init_col),
                        get(self.db,sheet,province,"Reserve",self.init_col),
                        self.constant[0]
                        ]
                value = d[0]*(d[0]*(1-d[0])/d[1]-1) if d[2] > d[3] else d[3]
                value = 0 if value <= 0 else value
                put(self.db,sheet,province,"Alumina Profile Alpha value",self.init_col,value)
        timer.stop()
            
    def alumina_profile_beta_value(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Alumina Profile Scaled mean",self.init_col),
                    get(self.db,sheet,province,"Alumina Profile Alpha value",self.init_col),
                    get(self.db,sheet,province,"Reserve",self.init_col),
                    self.constant[0]
                    ]
                value = d[1]*(1-d[0])/d[0] if d[2] > d[3] else d[3]
                if value != value:
                    print(d)
                put(self.db,sheet,province,"Alumina Profile Beta value",self.init_col,value)
        timer.stop()

    def silica_profile_starting_grade(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Alumina Profile Depletion grade",self.init_col),
                    get(self.db,sheet,province,"Starting ratio",self.init_col),
                    self.constant[0]
                        ]
                value = d[0]/d[1] if d[1] > d[2] else d[2]
                put(self.db,sheet,province,"Silica Profile Starting grade",self.init_col,value)
        timer.stop()
            
    def silica_profile_depletion_grade(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Alumina Profile Starting grade",self.init_col),
                    get(self.db,sheet,province,"Depletion ratio",self.init_col)
                    ]
                value = d[0]/d[1]
                put(self.db,sheet,province,"Silica Profile Depletion grade",self.init_col,value)
        timer.stop()

    def silica_profile_scaled_mean(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Silica Grade",self.init_col),
                    get(self.db,sheet,province,"Starting ratio",self.init_col),
                    get(self.db,sheet,province,"Silica Profile Starting grade",self.init_col),
                    get(self.db,sheet,province,"Silica Profile Depletion grade",self.init_col),
                    self.constant[0]
                    ]
                value = (d[0]-min(d[2],d[3]))/(max(d[2],d[3])-min(d[2],d[3])) if d[1] > 0 else 0
                put(self.db,sheet,province,"Silica Profile Scaled mean",self.init_col,value)   
        timer.stop()

        # constant 17
    def silica_profile_scaled_variance(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [self.constant[13],
                    get(self.db,sheet,province,"Silica Grade",self.init_col),
                    get(self.db,sheet,province,"Factor X",self.init_col),
                    get(self.db,sheet,province,"Starting ratio",self.init_col),
                    get(self.db,sheet,province,"Silica Profile Starting grade",self.init_col),
                    get(self.db,sheet,province,"Silica Profile Depletion grade",self.init_col),
                    self.constant[0],
                    self.constant[8]
                    ]
                value = (pow(d[0]*d[1],d[7])/d[2])/pow(max(d[4],d[5])-min(d[4],d[5]),d[7]) if d[3] > 0 else 0
                put(self.db,sheet,province,"Silica Profile Scaled variance",self.init_col,value)
        timer.stop()
            
    def silica_profile_alpha_value(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Silica Profile Scaled mean",self.init_col),
                    get(self.db,sheet,province,"Silica Profile Scaled variance",self.init_col),
                    self.constant[0],
                    self.constant[9],
                    ]
                value = d[0]*((d[0]*(1-d[0]))/d[1] - d[3]) if d[1] > d[2] else d[2]
                put(self.db,sheet,province,"Silica Profile Alpha value",self.init_col,value)
        timer.stop()


    def silica_profile_beta_value(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [get(self.db,sheet,province,"Silica Profile Scaled mean",self.init_col),
                    get(self.db,sheet,province,"Silica Profile Alpha value",self.init_col),
                    self.constant[0]
                    ]
                value = d[1]*(1-d[0])/d[0] if d[0] > d[2] else d[2] 
                put(self.db,sheet,province,"Silica Profile Beta value",self.init_col,value)    
        timer.stop()

    def get_grade(self, sg, dg, alpha, beta, d115, d118):
            value = 0
            if d118 > 0:
                if sg == dg:
                    value = sg
                else:
                    if sg < dg:
                        value = stats.beta.ppf(1-stat.mean([d115, d118]),alpha,beta,min(sg,dg),max(sg,dg)-min(sg,dg))
                    else:
                        value = stats.beta.ppf(stat.mean([d115, d118]),alpha,beta,min(sg,dg),max(sg,dg)-min(sg,dg)) 
            return value


    def bauxite_first_col_rest(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                idx = pd.IndexSlice
                a = get(self.db,sheet,province,"Demand Profile",self.init_col)
                b = get(self.db,sheet,province,"Reserve",self.init_col)
                c = get(self.db,sheet,province,"Usage",self.init_col)
                s_alpha = get(self.db,sheet,province,"Silica Profile Alpha value",self.init_col)
                s_beta  =  get(self.db,sheet,province,"Silica Profile Beta value",self.init_col)
                a_alpha = get(self.db,sheet,province,"Alumina Profile Alpha value",self.init_col)
                a_beta  =  get(self.db,sheet,province,"Alumina Profile Beta value",self.init_col)
                sp_sg = get(self.db,sheet,province,"Silica Profile Starting grade",self.init_col)
                sp_dg = get(self.db,sheet,province,"Silica Profile Depletion grade",self.init_col)
                ap_sg = get(self.db,sheet,province,"Alumina Profile Starting grade",self.init_col)
                ap_dg = get(self.db,sheet,province,"Alumina Profile Depletion grade",self.init_col)
                as_sr = get(self.db,sheet,province,"Starting ratio",self.init_col)
                as_dr = get(self.db,sheet,province,"Depletion ratio",self.init_col)
                
                d112 = a * b
                d114 = d112 - c if d112 - c > 0 else 0
                d117 = d114 - c
                d115 = d114/d112 if d112 > 0 else 0
                d118 = d117/d112 if d112 > 0 else 0
                d98  = as_sr if b > 0 else 0
                # get first alumina and silica grade
                d96 = ap_sg
                d97 = sp_sg if b > 0 else 0
                # get second alumina and silica grade 

                if (province == 'Other') and (sheet.lower() in self.pr1):
                    ag = [d96, 0]
                    sg = [d97, 0]
                    ag[1] = get(self.db,sheet,province,"Alumina Grade",self.init_col)
                    sg[1] = get(self.db,sheet,province,"Silica Grade",self.init_col)
                    s_ag = ag[1]
                    s_sg = sg[1]
                # elif (province == 'Other') and (sheet.lower() in self.pr2):
                #     ag = [d96, d96] 
                #     sg = [d97, d97]
                #     s_ag = ag[1]
                #     s_sg = sg[1]
                else:
                    s_ag = self.get_grade(ap_sg, ap_dg, a_alpha, a_beta, d115, d118)
                    s_sg = self.get_grade(sp_sg, sp_dg, s_alpha, s_beta, d115, d118)
                    ag = [d96, s_ag]
                    sg = [d97, s_sg]

                s_rdd =  s_ag/s_sg if d98 > 0 and d114 - c > c and s_sg > 0 else 0
                rdd = [d98, s_rdd]
                

                put(self.db,sheet,province, "Opening Stock",self.init_col, d112)
                put(self.db,sheet,province, "Closing Stock",self.init_col, d114)
                put(self.db,sheet,province, "Closing Stock - portion of total",self.init_col, d115)
                put(self.db,sheet,province, "C/S Outlook (n+1)",self.init_col, d117)
                put(self.db,sheet,province, "C/S Outlook (n+1) % of total",self.init_col, d118)
                self.db.at[idx[sheet,province, "Grade Profile Alumina Grade"], self.init_col:str(int(self.init_col)+1)] = ag
                self.db.at[idx[sheet,province, "Grade Profile Silica Grade"], self.init_col:str(int(self.init_col)+1)] = sg
                self.db.at[idx[sheet,province, "A/S ratio draw-down"], self.init_col:str(int(self.init_col)+1)] = rdd
        # print('reserve', b)
        # print(self.db.loc[(sheet, province, "Grade Profile Alumina Grade")])
        timer.stop()

    # calc cockpits
    def calccock1(self):
        timer.start()
        for year in map(str, list(range(2005, 2032))):
            v = self.cockpitdata1[year][2]+self.cockpitdata1[year][3]
            v=v/2
            self.cockpitdata1.at[4,year] = v
        timer.stop()

    def calccock2(self):
        timer.start()
        s = "yes"
        for year in map(str, list(range(2005, 2032))):
            self.cockpitdata2[year] = self.cockpitdata2[year].astype(float)
            for i in range(5):
                if s != "no":
                    self.cockpitdata2.at[i,year] = self.cockpitdata1[year][i]
                else:
                    self.cockpitdata2.at[i,year] = 0
        timer.stop()

    def calcdep4(self):
        timer.start()
        # print(self.depdata4.columns[5:])
        for year in self.depdata4.columns[5:]:
            for i in range(8):
                self.depdata4[year] = self.depdata4[year].astype(float)
                v = self.proddata1.loc[self.proddata1.category==self.depdata4['category'][i]][self.proddata1.bauxite==self.depdata4['bauxite'][i]][year].sum()
                w = self.proddata1.loc[self.proddata1.technology==self.depdata4['technology'][i]][self.proddata1.bauxite==self.depdata4['bauxite'][i]][year].sum()
                self.depdata4.at[i,year] = v+w
            self.depdata4.at[8,year] = self.depdata4[year].sum()
        # print(self.depdata4)
        timer.stop()

    def alumina_silica_ratio_grade(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                a_alpha = get(self.db,sheet,province,"Alumina Profile Alpha value",self.init_col)
                a_beta  =  get(self.db,sheet,province,"Alumina Profile Beta value",self.init_col)
                ap_sg = get(self.db,sheet,province,"Alumina Profile Starting grade",self.init_col)
                ap_dg = get(self.db,sheet,province,"Alumina Profile Depletion grade",self.init_col)

                s_alpha = get(self.db,sheet,province,"Silica Profile Alpha value",self.init_col)
                s_beta  =  get(self.db,sheet,province,"Silica Profile Beta value",self.init_col)
                sp_sg = get(self.db,sheet,province,"Silica Profile Starting grade",self.init_col)
                sp_dg = get(self.db,sheet,province,"Silica Profile Depletion grade",self.init_col)
                prev_year = str(int(year) - 1)
                usage = get(self.db,sheet,province,"Usage", prev_year)
                cs = get(self.db,sheet,province, "Closing Stock", prev_year)
                prev_rdd = get(self.db,sheet,province, "A/S ratio draw-down", prev_year)


                cs_outlook_tot = get(self.db,sheet,province,"C/S Outlook (n+1) % of total", prev_year)
                close_stock = get(self.db,sheet,province,"Closing Stock - portion of total", prev_year)
                if (province == 'Other') and (sheet.lower() in self.pr1):
                    a_result = get(self.db,sheet,province,"Alumina Grade",self.init_col)
                    s_result = get(self.db,sheet,province,"Silica Grade",self.init_col)
                # elif (province == 'Other') and (sheet.lower() in self.pr2):
                #     a_result = get(self.db,sheet,province,"Grade Profile Alumina Grade",self.init_col)
                #     s_result = get(self.db,sheet,province,"Grade Profile Silica Grade",self.init_col)
                else:
                    a_result = self.get_grade(ap_sg, ap_dg, a_alpha, a_beta, close_stock, cs_outlook_tot)
                    s_result = self.get_grade(sp_sg, sp_dg, s_alpha, s_beta, close_stock, cs_outlook_tot)
                rdd_result = a_result/s_result if prev_rdd > 0 and cs - usage >  usage and s_result > 0 else 0
                self.aluminadata.loc[(self.aluminadata['refinery'] == sheet) & (self.aluminadata['province'] == province), 'Alumina Grade'] = a_result
                self.silicadata.loc[(self.silicadata['refinery'] == sheet) & (self.silicadata['province'] == province), 'Alumina Grade'] = s_result

                put(self.db,sheet,province,"Grade Profile Alumina Grade",year,a_result)
                put(self.db,sheet,province,"Grade Profile Silica Grade",year,s_result)
                put(self.db,sheet,province,"A/S ratio draw-down",year, rdd_result)
                if a_result != a_result or s_result != s_result:
                    print(a_alpha, a_beta, ap_sg, ap_dg, s_alpha, s_beta, sp_sg, sp_dg, usage, cs, prev_rdd, cs_outlook_tot, close_stock )
                    print(a_result, s_result, rdd_result)
                    raise RuntimeError('Error wliek')
                    
        timer.stop(['alumina grade', a_result, 'silica grade', s_result, "A/S ratio", rdd_result])

    def aa_production_protocol(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                a = get(self.db,sheet,province,"Sourcing Mix",year)
                b = self.proddata7.loc[self.proddata7.refinery==sheet, year].values[0]
                c = get(self.db,sheet,province,"Bauxite Usage",year)
                result = a*b*1000 if c > 0 else 0 
                # value = d[0]*d[1] if d[2] > 0 else 0
                put(self.db,sheet,province,"Aa Production - based on protocol",year,result)
        timer.stop() 

    def bauxite_usage_bayer(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                ag = get(self.db,sheet,province,"Grade Profile Alumina Grade",year)
                sg = get(self.db,sheet,province,"Grade Profile Silica Grade",year)
                rdd = get(self.db,sheet,province,"A/S ratio draw-down",year)
                d = [
                ag,
                rdd,
                self.globaldb.loc[0,"Handling losses"],
                self.globaldb.loc[0,"Moisture content of ore"],
                self.globaldb.loc[0,"Extraction efficiency - Bayer"],
                self.globaldb.loc[0,"Alumina Quality"],
                self.factx.loc[self.factx.refinery==sheet][self.factx.province==province]['bayerconstant'].sum(),
                sg,
                self.constant[18]
                ]
                value = 1/(((d[0]-(d[7]*d[6]))*(1-d[2])*(1-d[3])*d[4])/d[5]) if d[1] > 0 else 0 # d[6] changed to d[8]
                put(self.db,sheet,province,"Bauxite Usage-Bayer",year,value)
        timer.stop()
            
    def bauxite_usage_bayer_mud_sinter(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                ag = get(self.db,sheet,province,"Grade Profile Alumina Grade",year)
                sg = get(self.db,sheet,province,"Grade Profile Silica Grade",year)
                rdd = get(self.db,sheet,province,"A/S ratio draw-down",year)
                d = [
                        ag,
                        rdd,
                        self.globaldb.loc[0,"Handling losses"],
                        self.globaldb.loc[0,"Moisture content of ore"],
                        self.globaldb.loc[0,"Extraction efficiency - Sinter"],
                        self.globaldb.loc[0,"Alumina Quality"],
                        self.constant[19],
                        sg
                    ]
                value = 1/((d[0]-d[7]*d[6])*(1-d[2])*(1-d[3])*d[4]/d[5]) if d[1] > 0 else 0
                put(self.db,sheet,province,"Bauxite Usage-Bayer Mud Sinter",year,value)
        timer.stop()
            
    def bauxite_usage_sinter(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                ag = get(self.db,sheet,province,"Grade Profile Alumina Grade",year)
                rdd = get(self.db,sheet,province,"A/S ratio draw-down",year)
                d = [
                        ag,
                        rdd,
                        self.globaldb.loc[0,"Handling losses"],
                        self.globaldb.loc[0,"Moisture content of ore"],
                        self.globaldb.loc[0,"Extraction efficiency - Sinter"],
                        self.globaldb.loc[0,"Alumina Quality"]
                    ]
                value = 1/(d[0]*(1-d[2])*(1-d[3])*d[4]/d[5]) if d[1] > 0 else 0
                put(self.db,sheet,province,"Bauxite Usage-Sinter",year,value)
        timer.stop()
            
    def bauxite_usage(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Bauxite Usage-Bayer",year),
                        get(self.db,sheet,province,"Bauxite Usage-Bayer Mud Sinter",year),
                        get(self.db,sheet,province,"Bauxite Usage-Sinter",year),
                        self.datadb.loc[self.datadb.refinery==sheet]["bayer"].sum(),
                        self.datadb.loc[self.datadb.refinery==sheet]["bayer mud sinter"].sum(),
                        self.datadb.loc[self.datadb.refinery==sheet]["sinter"].sum(),
                    ]
                value = d[0]*d[3]+d[1]*d[4]+d[2]*d[5]
                put(self.db,sheet,province,"Bauxite Usage",year,value)
        timer.stop()
            
    def bauxite_consumption(self,year):
        timer.start()
        for sheet in sheets:
            total = 0
            for province in Provinces:
                d = [
                    self.factx.loc[self.factx.refinery==sheet][self.factx.province==province]['Bauxite Consumption'].sum(),
                    get(self.db,sheet,province,"Bauxite Usage",year),
                    get(self.db,sheet,province,"Aa Production - based on protocol",year)
                    ]
                value = d[0] if year == '2004' else d[1]*d[2]
                total += value
                if int(year) > 2004:
                    self.proddata2.loc[(self.proddata2['refinery'] == sheet) & (self.proddata2['province'] == province), year] = value 
                put(self.db,sheet,province,"Bauxite Consumption",year,value)
            self.proddata3.loc[(self.proddata3['refinery'] == sheet), year] = total
            prod4 = 0
            if year == '2005':
                prod4 = 0
            else:
                # print(self.proddata3.loc[(self.proddata3['refinery'] == sheet), '2005':str(int(year)-1) ].values)
                if sum(np.array(self.proddata3.loc[(self.proddata3['refinery'] == sheet), '2005':str(int(year)-1) ].values).flatten()) == 0 :
                    prod4 = 0
                else:
                    if (total == 0) and (self.proddata1.loc[(self.proddata1['refinery'] == sheet), year].sum()*1 != 0):
                        prod4 = self.proddata1.loc[(self.proddata1['refinery'] == sheet), year].sum() * 1000
                    else:
                        prod4 = 0
            self.proddata4.loc[(self.proddata4['refinery'] == sheet), year] = prod4
            self.proddata5.loc[(self.proddata5['refinery'] == sheet), year] = self.proddata1.loc[(self.proddata1['refinery'] == sheet), year].sum() if prod4 > 0 else 0     
        timer.stop()

    def calcdep1(self,year):
        timer.start()
        for i in range(5):
            v = self.proddata5.loc[self.proddata5.category==self.depdata1['category'][i]][self.proddata5.bauxite==self.depdata1['bauxite'][i]][year].sum()
            w = self.proddata5.loc[self.proddata5.technology==self.depdata1['technology'][i]][self.proddata5.bauxite==self.depdata1['bauxite'][i]][year].sum()
            self.depdata1[year] = self.depdata1[year].astype(float)
            self.depdata1.at[i,year] = v+w
        self.depdata1.at[8,year] = self.depdata1[year].sum()
        timer.stop()

    #capacity 
    def depcap(self,year): #
        timer.start()
        self.depcap1[year] = self.depcap1[year].astype(float)
        for refinery in sheets:
            v = self.capdata1.loc[(self.capdata1['refinery'] == refinery), year].values[0] if self.proddata4.loc[(self.proddata4['refinery'] == refinery), year].values[0] > 0 else 0
            self.depcap1.loc[(self.depcap1['refinery'] == refinery), year] = v
        timer.stop()

    def calcdep3(self,year):
        timer.start()
        for i in range(5):
            self.depdata3[year] = self.depdata3[year].astype(float)
            v = (1-self.cockpitdata2[year][i])*self.depdata1[year][i]
            self.depdata3.at[i,year] = v
        self.depdata3.at[8,year] = self.depdata3[year].sum()
        timer.stop()

    def calcdep6(self,year):
        timer.start()
        for i in range(5):
            self.depdata6[year] = self.depdata6[year].astype(float)
            v = self.depdata1[year][i]
            w = self.depdata4[year][i]
            self.depdata6.at[i,year] = w-v
        self.depdata6.at[8,year] = self.depdata6[year].sum()
        timer.stop()
    
    def calcdep7(self,year):
        timer.start()
        for i in range(5):
            self.depdata7[year] = self.depdata7[year].astype(float)
            if self.cockpitdata3[year][i] == "yes" and self.depdata6[str(int(year)-1) if int(year) > 2005 else "2005"][i].sum() > 0:
                v = (self.depdata6[str(int(year)-1) if int(year) > 2005 else "2005"][i]+self.depdata3[str(int(year)-1) if int(year) > 2005 else "2005"][i])/self.depdata6[str(int(year)-1) if int(year) > 2005 else "2005"][i]
                self.depdata7.at[i,year] = v
            else:
                self.depdata7.at[i,year] = 1
        self.depdata7.at[8,year] = self.depdata7[year].sum()
        timer.stop()

    def calcprod6(self,year):
        timer.start()
        for refinery in sheets:
            tp = self.proddata6[self.proddata6['refinery'] == refinery]
            v = self.depdata7.loc[ (self.depdata7.category == tp['category'].values[0]) & (self.depdata7.bauxite == tp['bauxite'].values[0]), year ].sum()
            w = self.depdata7.loc[ (self.depdata7.technology == tp['technology'].values[0]) & (self.depdata7.bauxite == tp['bauxite'].values[0]), year ].sum()
            z = self.proddata1.loc[self.proddata1['refinery'] == refinery, year].sum()
            # print(v,w,z)
            if np.isnan(v):
                print(refinery)
                v = 0
            elif np.isnan(w):
                print(refinery)
                w = 0
            self.proddata6[year] = self.proddata6[year].astype(float)
            self.proddata6.loc[self.proddata6['refinery'] == refinery,year] = (v+w)*z
        timer.stop()

    # const needs to extracted 
    def calcprod7(self,year):
        timer.start()
        s = 2020
        for refinery in sheets:
            self.proddata7[year] = self.proddata7[year].astype(float)
            if int(year) < s:
                v = self.proddata1.loc[self.proddata1['refinery'] == refinery, year].sum()
                self.proddata7.loc[self.proddata7['refinery'] == refinery, year] = v
            else:
                v = self.proddata6.loc[self.proddata6['refinery'] == refinery,year].values
                self.proddata7.at[self.proddata7['refinery'] == refinery,year] = v
        timer.stop()

    def calcprod8(self,year):
        timer.start()
        self.proddata8[year] = self.proddata8[year].astype(float)
        for refinery in sheets:
            if self.proddata8.loc[ self.proddata8['refinery'] == refinery , "bauxite"].values[0] == "Domestic":
                a = 1
            else:
                a=0
            if self.depcap1.loc[ self.depcap1['refinery'] == refinery , year].sum() <= 0:
                b = self.proddata1.loc[ self.proddata1['refinery'] == refinery , year]
            else:
                m = self.proddata1.loc[ self.proddata1['refinery'] == refinery , year]
                cat = self.proddata8.loc[self.proddata8['refinery'] == refinery, 'category'].values[0]
                b = (1-self.cockpitdata1.loc[self.cockpitdata1['category'] == cat, year].sum()) * m
            self.proddata8.loc[ self.proddata8['refinery'] == refinery , year] = a*b
        timer.stop()

    def calcprod9(self,year):
        timer.start()
        self.proddata9[year] = self.proddata9[year].astype(float)
        for refinery in sheets:
            j_17 = self.proddata9.loc[(self.proddata9["refinery"] == refinery), 'bauxite'].values[0]
            if j_17 == "Domestic" or j_17 == "Imported":
                prod_1 = self.proddata1.loc[self.proddata1['refinery'] == refinery, year]
                prod_8 = self.proddata8.loc[self.proddata8['refinery'] == refinery, year]
                v = prod_1 - prod_8
            else: 
                v = 0
            self.proddata9.loc[self.proddata9['refinery'] == refinery, year] = v
        timer.stop()

    def calcprod10(self,year):
        timer.start()
        self.proddata10[year] = self.proddata10[year].astype(float)
        for refinery in sheets:
            if year == "2005":
                v = self.proddata7.loc[self.proddata7['refinery'] == refinery, year]
            else:
                v =self.proddata7.loc[self.proddata7['refinery'] == refinery, year] + self.proddata10.loc[self.proddata10['refinery'] == refinery, str(int(year)-1)]
            self.proddata10.loc[self.proddata10['refinery'] == refinery, year] = v
        timer.stop()

    def calcprodout(self,year): # collector 2219
        timer.start()
        self.prodout[year] = self.prodout[year].astype(float)
        for refinery in sheets:
            d = np.array(self.db.loc[(refinery, slice(None), "Bauxite Usage"), year])
            w = np.array(self.db.loc[(refinery, slice(None), "Sourcing Mix"), year])
            bu = sum(d*w)
            if bu > 0:
                v = self.proddata7.loc[self.proddata7['refinery'] == refinery,year] 
            else:
                v = 0
            self.prodout.loc[self.prodout['refinery'] == refinery,year] = v
        timer.stop()

    def calcdep8(self,year):
        timer.start()
        self.depdata8[year] = self.depdata8[year].astype(float)
        for i in range(8):
            v = self.prodout.loc[(self.prodout['category'] == self.depdata8['category'][i]) & (self.prodout['bauxite'] == self.depdata8['bauxite'][i]), year].sum()
            w = self.prodout.loc[(self.prodout['technology'] == self.depdata8['technology'][i]) & (self.prodout['bauxite'] == self.depdata8['bauxite'][i]), year].sum()
            self.depdata8.at[i,year] = v + w
        self.depdata8.at[8,year] = self.depdata8[year].sum()
        timer.stop()

        # continue here .......
    def calcdep9(self,year):
        timer.start()
        self.depdata9[year] = self.depdata9[year].astype(float)
        for i in range(8):
            v = self.proddata4.loc[(self.proddata4['category'] == self.depdata9['category'][i]) & (self.proddata4['bauxite'] ==self.depdata9['bauxite'][i]), year].sum()
            w = self.proddata4.loc[(self.proddata4['technology'] == self.depdata9['technology'][i]) & (self.proddata4['bauxite'] ==self.depdata9['bauxite'][i]), year].sum()
            self.depdata9.at[i,year] = v+w
        self.depdata9.at[8,year] = self.depdata9[year].sum()
        timer.stop()

    def calcdep10(self,year):  # cockpit 102
        timer.start()
        self.depdata10[year] = self.depdata10[year].astype(float)
        for i in range(8):
            if i < 5:
                v = self.cockpitdata2[year][i]*self.depdata8[year][i]/1000
            else:
                v = self.depdata8[year][i]/1000
            self.depdata10.at[i,year] = v
        self.depdata10.at[8,year] = self.depdata10[year].sum()
        timer.stop()

    def calcdep11(self,year):  # cockpit 159
        timer.start()
        self.depdata11[year] = self.depdata11[year].astype(float)
        self.smalldata[year] = self.smalldata[year].astype(float)
        for i in range(8):
            b = self.smalldata[year][1] if self.depdata10["technology"][i] == "L-B" else self.smalldata[year][0]
            v = self.depdata10[year][i] * b
            self.depdata11.at[i,year] = v
        self.depdata11.at[8,year] = self.depdata11[year].sum()
        timer.stop()

    def calcdep12(self,year):  # cockpit 172
        timer.start()
        self.depdata12[year] = self.depdata12[year].astype(float)
        self.smalldata[year] = self.smalldata[year].astype(float)
        for i in range(8):
            b = self.smalldata[year][1] if self.depdata12["technology"][i] == "L-B" else self.smalldata[year][0]
            v = self.depdata9[year][i]* b/1000
            self.depdata12.at[i,year] = v
        self.depdata12.at[8,year] = self.depdata12[year].sum()
        timer.stop()

    def calcdep13(self,year): #cockpit 2343
        timer.start()
        self.depdata13[year] = self.depdata13[year].astype(float)
        for i in range(5):
            if i < 4:
                v = self.proddata2.loc[self.proddata2.Province==self.depdata13['category'][i]][self.proddata2.bauxite==self.depdata13['bauxite'][i]][year].sum()
            else:
                v = (self.proddata2.loc[self.proddata2.bauxite==self.depdata13['bauxite'][i]][year].sum())-(self.depdata13[year][0]+self.depdata13[year][1]+self.depdata13[year][2]+self.depdata13[year][3])
            self.depdata13.at[i,year] = v
        self.depdata13.at[5,year] = self.proddata2.loc[self.proddata2.bauxite== self.depdata13.loc[4, 'bauxite']][year].sum() #checked formula
        timer.stop()

    def calcdep14(self,year):
        timer.start()
        self.depdata14[year] = self.depdata14[year].astype(float)
        for i in range(5):
            v = self.depdata13[year][i]/self.depdata8[year][i] if self.depdata8[year][i] > 0 else 0
            self.depdata14.at[i,year] = v
        self.depdata14.at[5,year] = self.depdata13[year][5]/self.depdata8[year].sum()
        timer.stop()

    def calcdep15(self,year):
        timer.start()
        self.depdata15[year] = self.depdata15[year].astype(float)
        for i in range(8):
            v = self.proddata3.loc[self.proddata3.category==self.depdata15['category'][i]][self.proddata3.bauxite==self.depdata15['bauxite'][i]][year].sum()
            w = self.proddata3.loc[self.proddata3.technology==self.depdata15['technology'][i]][self.proddata3.bauxite==self.depdata15['bauxite'][i]][year].sum()
            self.depdata15.at[i,year] = v+w
        self.depdata15.at[8,year] = self.depdata15[year].sum()
        timer.stop()

    def calcdep16(self,year):
        timer.start()
        self.depdata16[year] = self.depdata16[year].astype(float)
        v = 0
        for i in range(5):
            for j in range(2005,int(year)+1):
                v+=self.depdata15[str(j)][i]
            self.depdata16.at[i,year] = v
        self.depdata16.at[5,year] = self.depdata16[year].sum()
        timer.stop()

    def calcdep17(self,year):
        timer.start()
        self.depdata17[year] = self.depdata17[year].astype(float)
        v = 0
        for i in range(5):
            for j in range(2005,int(year)+1):
                v+=self.depdata8[str(j)][i]
            self.depdata17.at[i,year] = v
        self.depdata17.at[5,year] = self.depdata17[year].sum()
        timer.stop()

    def calcdep18(self,year):
        timer.start()
        self.depdata18[year] = self.depdata18[year].astype(float)
        for i in range(5):
            v = self.depdata16[year][i]/self.depdata17[year][i]
            self.depdata18.at[i,year] = v
        self.depdata18.at[5,year] = self.depdata16[year].sum()/self.depdata17[year].sum()
        timer.stop()

    def calcdep19(self,year):
        timer.start()
        self.depdata19[year] = self.depdata19[year].astype(float)
        for i in range(5):
            v = self.depdata9[year][i]/self.depdata4[year][i]/1000
            self.depdata19.at[i,year] = v
        timer.stop()

    def calcbauxitedata1(self,year):
        timer.start()
        self.bauxitedata1[year] = self.bauxitedata1[year].astype(float)
        v1 = self.depdata11[year][5] + self.depdata11[year][6]
        v2 = self.depdata12[year].sum()/2
        v3 = self.depdata11[year][0]+self.depdata11[year][1]+self.depdata11[year][2]+self.depdata11[year][3]+self.depdata11[year][4]
        v4 = v1+v3
        self.bauxitedata1.at[0,year] = v1
        self.bauxitedata1.at[1,year] = v2
        
        self.bauxitedata1.at[2,year] = v3
        self.bauxitedata1.at[3,year] = v4
        timer.stop()

    def calcbauxitedata2(self,year):
        timer.start()
        self.bauxitedata2[year] =self.bauxitedata2[year].astype(float)
        self.smalldata[year] = self.smalldata[year].astype(float)
        v1 = self.bauxitedata1[year][0]/self.smalldata[year][0]
        v2 = self.bauxitedata1[year][1]/self.smalldata[year][0]
        v3 = self.bauxitedata1[year][2]/self.smalldata[year][0]
        v4 = self.bauxitedata1[year][3]/self.smalldata[year][0]
        self.bauxitedata2.at[0,year] = v1
        self.bauxitedata2.at[1,year] = v2
        self.bauxitedata2.at[2,year] = v3
        self.bauxitedata2.at[3,year] = v4
        timer.stop()
            
    def bauxite_cumulative(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Reserve",year),
                        get(self.db,sheet,province,"Demand Profile",year),
                        get(self.db,sheet,province,"Bauxite Consumption",year),
                        get(self.db,sheet,province,"Bauxite Cumulative",str(int(year)-1) if year > '2004' else '2004')
                    ]
                value = d[0]*(1-d[1]) if year == '2004' else d[2]+d[3]
                put(self.db,sheet,province,"Bauxite Cumulative",year,value)
        timer.stop()

    # starts here   
    def opening_stock(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Reserve",year),
                        get(self.db,sheet,province,"Demand Profile",year),
                        get(self.db,sheet,province,"Closing Stock",str(int(year)-1) if int(year) > 2004 else '2004')
                    ]
                value = d[0]*d[1] if year == '2004' else d[2]
                put(self.db,sheet,province,"Opening Stock",year,value)
        timer.stop()

    def usage(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        self.factx.loc[self.factx.refinery==sheet][self.factx.province==province]['Bauxite Consumption'].sum(),
                        get(self.db,sheet,province,"Bauxite Consumption",year).sum()
                    ]
                value = d[0] if year == '2004' else d[1]
                put(self.db,sheet,province,"Usage",year,value)
        timer.stop()

    def closing_stock(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Closing Stock", str(int(year)-1) if int(year) > 2004 else '2004'),
                        get(self.db,sheet,province,"Usage",year)
                    ]
                value = d[0]-d[1] if d[0]-d[1] > 0 else 0
                put(self.db,sheet,province,"Closing Stock",year,value)
        timer.stop()
            
    def closing_stock_portion_of_total(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Opening Stock",'2004'),
                        get(self.db,sheet,province,"Closing Stock",year)
                    ]
                value = d[1]/d[0] if d[0] > 0 else 0
                if int(year) > 2004:
                    self.plotlink.loc[(self.plotlink['refinery'] == sheet) & (self.plotlink['province'] == province), year] = value
                put(self.db,sheet,province,"Closing Stock - portion of total",year,value)
        timer.stop()
            
    def cs_outlook(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Usage",year),
                        get(self.db,sheet,province,"Closing Stock",year),
                        get(self.db,sheet,province,"C/S Outlook (n+1)",str(int(year)-1)),
                        self.proddata7.loc[self.proddata6.refinery==sheet, year].values[0],
                        self.proddata7.loc[self.proddata6.refinery==sheet, str(int(year)+1) if int(year) < 2031 else '2031'].values[0]
                        # dbb.production.loc[dbb.production.refinery==sheet][year].sum(),
                        # dbb.production.loc[dbb.production.refinery==sheet][str(int(year)+1) if int(year) < 2031 else '2031'].sum() if int(year) < 2031 else 0
                    ]
                if d[2] == 0:
                    value = 0
                else:
                    if d[1]-d[0] > 0:
                        if d[3] == 0:
                            value = d[2]
                        else:
                            value = (d[1]-d[0]*d[4]/d[3])
                    else:
                        value = 0
                put(self.db,sheet,province,"C/S Outlook (n+1)",year,value)            
        timer.stop()
            
    def cs_outlook_total(self,year):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Opening Stock",'2004'),
                        get(self.db,sheet,province,"C/S Outlook (n+1)",year)
                    ]
                value = d[1]/d[0] if d[0] > 0 else 0
                put(self.db,sheet,province,"C/S Outlook (n+1) % of total",year,value)
        timer.stop()
    
    def domesticopenstrip(self):
        timer.start()
        for sheet in sheets:
            # print(self.otherlist)
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    # province1 = 'IM' if province1 == 'Inner Mongolia' else province1
                    result = np.array([0, *self.provincialdb.loc[province1,'mining characteristics mine type open']]).flatten()
                    if len(result) > 28:
                        print(self.provincialdb.loc[province1,'mining characteristics mine type open'])
                        raise RuntimeError('I quit')
                    put(self.db,sheet,province,"domestic open-strip",None,result)
                else:
                    result = np.array([0, *self.provincialdb.loc[province,'mining characteristics mine type open']]).flatten()
                    if len(result) > 28:
                        print(self.provincialdb.loc[province,'mining characteristics mine type open'])
                        raise RuntimeError('I quit')
                    put(self.db,sheet,province,"domestic open-strip",None, result)
        timer.stop(result)

    def domesticunderground(self):
        timer.start()
        # print(get(self.db,'spare','Guangxi',"domestic open-strip"))
        # print(self.db.loc[('spare','Guangxi',"domestic open-strip")])
        for sheet in sheets:
            for province in Provinces:
                # print(sheet, province)
                d = np.array(get(self.db,sheet,province,"domestic open-strip")).flatten()
                # print(get(self.db,sheet,province,"domestic open-strip"))
                result = 1 - d
                # print(d, result)
                put(self.db,sheet,province,"domestic underground", None,result)
        timer.stop()

    def miningcostopenstrip(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    result = [0, *self.provincialdb.loc[province1,'open pit mine mining cost open pit mining cost'].to_list()]
                    put(self.db,sheet,province,"mining cost open-strip", None, result)
                else:
                    result = [0, *self.provincialdb.loc[province,'open pit mine mining cost open pit mining cost'].to_list()]
                    put(self.db,sheet,province,"mining cost open-strip", None,result)
        timer.stop(result)

    def miningcostunderground(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    result = [0, *self.provincialdb.loc[province1,'underground mining cost underground mining cost']]
                    put(self.db,sheet,province,"mining cost underground", None, result)
                else:
                    result = [0, *self.provincialdb.loc[province,'underground mining cost underground mining cost']]
                    put(self.db,sheet,province,"mining cost underground", None, result)
        timer.stop(result)

    def miningcostdressingopenpit(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    result = [0, *self.provincialdb.loc[province1,'open pit mine dressing cost total dressing cost'].to_list()]
                    put(self.db,sheet,province,"mining cost dressing open pit", None, result)
                else:
                    result = [0, *self.provincialdb.loc[province,'open pit mine dressing cost total dressing cost'].to_list()]
                    put(self.db,sheet,province,"mining cost dressing open pit", None, result)
        timer.stop(result)

    def miningcostdressingunderground(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    result = [0, *self.provincialdb.loc[province1,'underground dressing cost total dressing cost'].to_list()]
                    put(self.db,sheet,province,"mining cost dressing underground", None, result)
                else:
                    result = [0, *self.provincialdb.loc[province,'underground dressing cost total dressing cost'].to_list()]
                    put(self.db,sheet,province,"mining cost dressing underground", None, result)
        timer.stop(result)

    def totalmininganddressing(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"domestic open-strip")),
                        np.array(get(self.db,sheet,province,"domestic underground")),
                        np.array(get(self.db,sheet,province,"mining cost open-strip")),
                        np.array(get(self.db,sheet,province,"mining cost underground")),
                        np.array(get(self.db,sheet,province,"mining cost dressing open pit")),
                        np.array(get(self.db,sheet,province,"mining cost dressing underground"))
                    ]
                # print(d)
                result = d[0]*(d[2]+d[4])+d[1]*(d[3]+d[5])
                put(self.db,sheet,province,"total mining and dressing", None, result)
        timer.stop(result)

    def MiningRoyality(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    result = [0, *self.provincialdb.loc[province1,'underground state'].to_list()]
                    put(self.db,sheet,province,"Mining Royality", None,result)
                else:
                    result = [0, *self.provincialdb.loc[province,'underground state']]
                    put(self.db,sheet,province,"Mining Royality", None, result)
        timer.stop(result)

    def Localcharges(self):
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    result = [0, *self.provincialdb.loc[province1,'underground local'].to_list()]
                    put(self.db,sheet,province,"Local charges",None, result)
                else:
                    result = [0, *self.provincialdb.loc[province,'underground local'].to_list()]
                    put(self.db,sheet,province,"Local charges",None, result)
        timer.stop(result)

    def totalgovernmentcharges(self): # 19 cell 499
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"Mining Royality")),
                        np.array(get(self.db,sheet,province,"Local charges"))
                    ]
                result = d[0]+d[1] #formula here
                put(self.db,sheet,province,"totalgovernmentcharges",None, result)
        timer.stop(result)

    # constant 18,19,20,21
    def Road01(self):# 19 cell 506
        timer.start()
        d2 = {
            "Guangxi":0,
            "Guizhou":300,
            "Henan":0,
            "Shanxi":0,
            "Other":30
        } 
        for sheet in sheets:
            for province in Provinces:
                value = self.summarydb.loc[self.summarydb.province==province][sheet+' Avg Distance'].sum()
                result = [0, *np.full(self.max_col - 1, value)]
                put(self.db,sheet,province,"Road-1", None, result)
        timer.stop(result)

    def Rail01(self):# 19 cell 507
        timer.start()    
        for sheet in sheets:
            for province in Provinces:    
                val = 0
                result = [0, *np.full(self.max_col-1, val)]
                put(self.db,sheet,province,"Rail-1", None, result)
        timer.stop(result)

    def totalminingcashcost(self):# 19 cell 502
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                    np.array(get(self.db,sheet,province,"totalgovernmentcharges")),
                    np.array(get(self.db,sheet,province,"total mining and dressing"))
                ]
                result = d[0]+d[1] #formula here
                put(self.db,sheet,province,"totalminingcashcost", None, result)
        timer.stop(result)

    def Road0(self):# 19 cell 509
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d1 = np.array(get(self.db,sheet,province,"Road-1"))
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    arr = np.array([0, *self.provincialdb.loc[province1,'underground road'].to_list()])
                    result = d1 * arr
                    put(self.db,sheet,province,"Road0", None, result)
                else:
                    arr = [0, *self.provincialdb.loc[province,'underground road']]
                    result = d1 * arr
                    put(self.db,sheet,province,"Road0", None, result)
        timer.stop(result)

        # constant 22
    def Rail0(self):# 19 cell 510
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d1 = np.array(self.db.loc[(sheet,province,"Rail-1")])
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    arr = [0, *self.provincialdb.loc[province1,'underground rail'].to_list()]
                    result = [41 + arr[i]*d1[i] if d1[i] > 0 else 0 for i in np.arange(len(d1))]
                    put(self.db,sheet,province,"Rail0", None, result)
                else:
                    arr = [0, *self.provincialdb.loc[province,'underground rail'].to_list()]
                    result = [41 + arr[i]*d1[i] if d1[i] > 0 else 0 for i in np.arange(len(d1))]
                    put(self.db,sheet,province,"Rail0", None, result)
        timer.stop(result)

    def totalrailroad(self):# 19 cell 511
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"Road0")),
                        np.array(get(self.db,sheet,province,"Rail0"))
                    ]
                result = d[0]+d[1]#formula here
                put(self.db,sheet,province,"totalrailroad", None, result)
        timer.stop(result)

        #constant 23 - years
    def closedloopcostsfob(self):# 19 cell 472
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.full(self.max_col-1, 10)]
                put(self.db,sheet,province,"closedloop costs fob",None, result)
        timer.stop(result)

        #constant 24 - years
    def closedloopseafreight(self):# 19 cell 473
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.full(self.max_col-1, 15)]
                put(self.db,sheet,province,"closedloop sea freight", None, result)
        timer.stop(result)

        #constant 25 - years
    def closedloopporttorefinery(self):# 19 cell 474
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.full(self.max_col-1, 100)]
                put(self.db,sheet,province,"closedloop port to refinery", None, result)
        timer.stop(result)

    def closedlooptotalrmb(self):# 19 cell 475
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"closedloop costs fob")),
                        np.array(get(self.db,sheet,province,"closedloop sea freight")),
                        np.array(get(self.db,sheet,province,"closedloop port to refinery")),
                        np.array([0, *self.exlist])
                    ]
                result = (d[0]+d[1])*d[3]+d[2]#formula here
                put(self.db,sheet,province,"closedloop total-rmb", None, result)
        timer.stop(result)

    def closedlooptotalUS(self):# 19 cell 476
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"closedloop total-rmb")),
                        np.array([0, *self.exlist])
                    ]
                a , b = d[0], d[1]
                result = np.divide(a, b, out=np.zeros_like(a), where=b!=0)#formula here
                put(self.db,sheet,province,"closedloop total US", None, result)
        timer.stop(result)

            # constant 26 - years
    def thirdpartypurchasefob(self):# 19 cell 478
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.full(self.max_col-1, 20)]
                put(self.db,sheet,province,"thirdparty purchase fob", None, result)
        timer.stop(result)

            # constant 27 - years
    def thirdpartyseafreight(self):# 19 cell 479
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.full(self.max_col - 1, 15)]
                put(self.db,sheet,province,"thirdparty sea freight", None, result)
        timer.stop(result)

            # constant 28 - years
    def thirdpartyporttorefinery(self):# 19 cell 480
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.full(self.max_col - 1, 100)]
                put(self.db,sheet,province,"thirdparty port to refinery", None, result)
        timer.stop(result)

    def thirdpartytotalrmb(self):# 19 cell 481
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"thirdparty purchase fob")),
                        np.array(get(self.db,sheet,province,"thirdparty sea freight")),
                        np.array(get(self.db,sheet,province,"thirdparty port to refinery")),
                        np.array([0, *self.exlist])
                    ] 
                result = (d[0]+d[1])*d[3]+d[2] #formula here
                put(self.db,sheet,province,"thirdparty total-rmb", None, result)
        timer.stop(result)
        
    def thirdpartytotalUS(self):# 19 cell 482
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"thirdparty total-rmb")),
                        np.array([0, *self.exlist])
                    ]
                a, b = d[0], d[1]#formula here
                result = np.divide(a, b, out=np.zeros_like(a), where=b!=0)#formula here
                put(self.db,sheet,province,"thirdparty total US", None, result)
        timer.stop(result)

    def totalminingdressingrefinery(self):# 19 cell 513
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                a = get(self.db,sheet,province,"totalminingcashcost")
                b = get(self.db,sheet,province,"totalrailroad")
                result = a + b
                put(self.db,sheet,province,"total mining dressing refinery", None, result)
        timer.stop(result)

    def PurchaseFOT(self):# 19 cell 518
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    result = [0, *self.provincialdb.loc[province1,'underground purchace price']]
                    put(self.db,sheet,province,"Purchase-FOT", None, result)
                else:
                    result = [0, *self.provincialdb.loc[province,'underground purchace price']]
                    put(self.db,sheet,province,"Purchase-FOT", None, result)
        timer.stop(result)

    def Road1(self):# 19 cell 521
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = get(self.db,sheet,province,"Road-1")    
                put(self.db,sheet,province,"Road1", None, result)
        timer.stop(result)

    def Rail1(self):# 19 cell 522
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.zeros(self.max_col - 1)]
                put(self.db,sheet,province,"Rail1", None, result)
        timer.stop(result)

    def Road2(self):# 19 cell 524
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = get(self.db,sheet,province,"Road1")
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    arr = [0, *self.provincialdb.loc[province1,'underground road']]
                    result = arr * d
                    #here
                    put(self.db,sheet,province,"Road2", None, result)
                else:
                    arr = np.array([0, *self.provincialdb.loc[province,'underground road']])
                    result = arr * d
                    #here
                    put(self.db,sheet,province,"Road2", None, result)
        timer.stop(result)

            # constant 29
    def Rail2(self):# 19 cell 525
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = np.array(get(self.db,sheet,province,"Rail1")).flatten()
                if province == "Other":
                    # province1 = self.otherlist[self.sheetdata.index(sheet)]
                    province1 = 'Chongqing'
                    arr = [0, *self.provincialdb.loc[province1,'underground rail']]
                    result = [41 + arr[i]*d[i] if d[i] > 0 else 0 for i in np.arange(len(d))]
                    #here
                    put(self.db,sheet,province,"Rail2", None, result)
                else:
                    arr = [0, *self.provincialdb.loc[province,'underground rail']]
                    result = [41 + arr[i]*d[i] if d[i] > 0 else 0 for i in np.arange(len(d))]
                    #here
                    put(self.db,sheet,province,"Rail2", None, result)
        timer.stop(result)

    def Minetorefinery(self):# 19 cell 529
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"Road2")),
                        np.array(get(self.db,sheet,province,"Rail2"))
                    ]
                result = d[0]+d[1]
                put(self.db,sheet,province,"Mine to refinery", None, result)
        timer.stop(result)

    def totalpurchaceandminetorefinary(self):# 19 cell 530
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"Mine to refinery")),
                        np.array(get(self.db,sheet,province,"Purchase-FOT")),
                    ]
                result = d[0]+d[1]
                put(self.db,sheet,province,"total purchace and mine to refinary",None, result)
        timer.stop(result)

        #constant 30 - years
    def importclosedloop(self):# 19 cell 462
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.zeros(self.max_col - 1)]
                put(self.db,sheet,province,"import closedloop", None, result)
        timer.stop(result)

        #constant 31 - years
    def importthirdparty(self):# 19 cell 463
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                result = [0, *np.zeros(self.max_col - 1)]
                put(self.db,sheet,province,"import thirdparty",None, result)
        timer.stop(result)

    def domesticclosedloop(self):# 19 cell 464
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"Reserve",'2004')),
                        np.array(get(self.db,sheet,province,"Closed",'2004'))
                    ]
                value = d[1] if d[0] > 0 else 0
                result = [0, *np.full(self.max_col-1, value)]
                put(self.db,sheet,province,"domestic closed loop", None, result)
        timer.stop(result)

    def domesticthirdparty(self):# 19 cell 465
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Reserve",'2004').sum(),
                        get(self.db,sheet,province,"import closedloop", '2005').sum(),
                        get(self.db,sheet,province,"import thirdparty", '2005').sum(),
                        get(self.db,sheet,province,"domestic closed loop", '2005').sum()
                    ]
                value = 1-(d[1]+d[2]+d[3]) if d[0] > 0 else 0
                result = [0, *np.full(self.max_col-1, value)]
                put(self.db,sheet,province,"domestic thirdparty", None, result)
        print(d)
        timer.stop(result)

    def Bauxitecostdeliveredtoaarefinery(self):# 19 cell 468
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        np.array(get(self.db,sheet,province,"import closedloop")),
                        np.array(get(self.db,sheet,province,"import thirdparty")),
                        np.array(get(self.db,sheet,province,"domestic closed loop")),
                        np.array(get(self.db,sheet,province,"domestic thirdparty")),
                        np.array(get(self.db,sheet,province,"closedloop total-rmb")),
                        np.array(get(self.db,sheet,province,"thirdparty total-rmb")),
                        np.array(get(self.db,sheet,province,"total purchace and mine to refinary")),
                        np.array(get(self.db,sheet,province,"total mining dressing refinery")),
                    ]
                result = d[0]*d[4]+d[1]*d[5]+d[2]*d[7]+d[3]*d[6]
                # print(result)
                put(self.db,sheet,province,"Bauxite cost delivered to aa refinery", None, result)
        timer.stop(result)

    #  back to WORK 
    def BauxitePriceCost1(self,year):# 19 cell 368
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Bauxite cost delivered to aa refinery",'2005' if year == '2004' else year),
                        get(self.db,sheet,province,"Grade Profile Silica Grade",year),
                    ]
                value = d[0] if d[1] > 0 else 0
                put(self.db,sheet,province,"Bauxite Price-Cost1",year,value)
                # put(self.db,sheet,province,"Bauxite Price-Cost1",year,0)
        timer.stop()

    # start here , code cautic usage and first bauxite usage
    def BauxitePriceCost2(self,year):# 19 cell 376
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Bauxite Price-Cost1",year),
                        get(self.db,sheet,province,"Bauxite Usage",year),
                    ]
                value = d[0]*d[1]
                put(self.db,sheet,province,"Bauxite Price-Cost2",year,value)
        timer.stop()

        # constant 32,33,34,35,36,37
    def CausticCost(self,year):# 19 cell 392
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        self.constant[20],
                        self.constant[24],
                        self.constant[25],
                        get(self.db,sheet,province,"Bauxite Usage",year),
                        self.datadb.loc[self.datadb.refinery==sheet,'bayer'].sum(),
                        self.datadb.loc[self.datadb.refinery==sheet,"bayer mud sinter"].sum(),
                        self.datadb.loc[self.datadb.refinery==sheet,"sinter"].sum(),
                        get(self.db,sheet,province,"A/S ratio draw-down",year),
                        get(self.db,sheet,province,"Grade Profile Silica Grade",year),
                        self.constant[21],
                        self.causticdata.loc[self.datadb.refinery==sheet,"causticPrice"].sum(),
                        self.causticdata.loc[self.datadb.refinery==sheet,"sodiumCarbonate"].sum(),
                        self.constant[22],
                        self.constant[23]*2

                    ]
                value = (d[0]*d[8]*d[3]+d[9])*d[4]*d[10] + d[1]*d[6]*d[11]*d[12]/d[13]+(d[9]+d[2]*d[0]*d[8]*d[3])*d[11]*d[12]/d[13] if d[7] > 0 else 0
                
                put(self.db,sheet,province,"Caustic Cost",year,value)
        timer.stop([value])

    def BauxiteCausticCost(self,year):# 19 cell 400
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Caustic Cost",year),
                        get(self.db,sheet,province,"Bauxite Price-Cost2",year),
                    ]
                value = d[0]+d[1]
                put(self.db,sheet,province,"Bauxite & Caustic Cost",year,value)
        timer.stop([value])

        # constant 38
    def PreCalculation(self,year):# 19 cell 410
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        get(self.db,sheet,province,"Bauxite & Caustic Cost",year),
                        self.datadb.loc[self.datadb.refinery==sheet]["sourcing factor"].sum(),
                    ]
                value = pow((1/d[0]),d[1])*1000000000 if d[0] > 0 else 0
                put(self.db,sheet,province,"Pre-Calculation",year,value)
        timer.stop()

    def SourcingMix(self,year):# 19 cell 418
        timer.start()
        for sheet in sheets:
            for province in Provinces:
                d = [
                        self.db.loc[(sheet,slice(None),"Pre-Calculation"), year].sum(),
                        get(self.db,sheet,province,"Pre-Calculation",year),
                    ]
                value = d[1]/d[0] if d[0] > 0 else 0 
                put(self.db,sheet,province,"Sourcing Mix",year,value)
        timer.stop(end=True)

    def calc_cir_ref(self):
        pass

    def calc_all0(self):
        Bauxite.reserve(self)
        Bauxite.aluminagrade(self)
        Bauxite.asratio(self)
        Bauxite.closed(self)
        Bauxite.demand(self)
        Bauxite.usagee(self, '2004')
        Bauxite.factorx(self)
        Bauxite.silica(self)
        Bauxite.purchased(self)
        Bauxite.starting_ratio(self)
        Bauxite.depletion_ratio(self)
        Bauxite.alumina_profile_starting_grade(self)
        Bauxite.alumina_profile_depletion_grade(self)
        Bauxite.alumina_profile_scaled_mean(self)
        Bauxite.alumina_profile_scaled_variance(self)
        Bauxite.alumina_profile_alpha_value(self)
        Bauxite.alumina_profile_beta_value(self)
        Bauxite.silica_profile_starting_grade(self)
        Bauxite.silica_profile_depletion_grade(self)
        Bauxite.silica_profile_scaled_mean(self)
        Bauxite.silica_profile_scaled_variance(self)
        Bauxite.silica_profile_alpha_value(self)
        Bauxite.silica_profile_beta_value(self)
        Bauxite.bauxite_first_col_rest(self)
        # calc cockpit
        Bauxite.calccock1(self)
        Bauxite.calccock2(self)
        Bauxite.calcdep4(self)
        Bauxite.calcprod6(self, '2005')
        Bauxite.calcprod7(self, '2005')

    def calc_bauxite_cost(self):
        Bauxite.domesticopenstrip(self)
        Bauxite.domesticunderground(self)
        Bauxite.miningcostopenstrip(self)
        Bauxite.miningcostunderground(self)
        Bauxite.miningcostdressingopenpit(self)
        Bauxite.miningcostdressingunderground(self)
        Bauxite.totalmininganddressing(self)
        Bauxite.MiningRoyality(self)
        Bauxite.Localcharges(self)
        Bauxite.totalgovernmentcharges(self)
        Bauxite.Road01(self)
        Bauxite.Rail01(self)
        Bauxite.totalminingcashcost(self)
        Bauxite.Road0(self)
        Bauxite.Rail0(self)
        Bauxite.totalrailroad(self)
        Bauxite.closedloopcostsfob(self)
        Bauxite.closedloopseafreight(self)
        Bauxite.closedloopporttorefinery(self)
        Bauxite.closedlooptotalrmb(self)
        Bauxite.closedlooptotalUS(self)
        Bauxite.thirdpartypurchasefob(self)
        Bauxite.thirdpartyseafreight(self)
        Bauxite.thirdpartyporttorefinery(self)
        Bauxite.thirdpartytotalrmb(self)
        Bauxite.thirdpartytotalUS(self)
        Bauxite.totalminingdressingrefinery(self)
        Bauxite.PurchaseFOT(self)
        Bauxite.Road1(self)
        Bauxite.Rail1(self)
        Bauxite.Road2(self)
        Bauxite.Rail2(self)
        Bauxite.Minetorefinery(self)
        Bauxite.totalpurchaceandminetorefinary(self)
        Bauxite.importclosedloop(self)
        Bauxite.importthirdparty(self)
        Bauxite.domesticclosedloop(self)
        Bauxite.domesticthirdparty(self)
        Bauxite.Bauxitecostdeliveredtoaarefinery(self)

    def calc_in_seq(self, year):
        post_year = str(int(year) + 1)

        Bauxite.bauxite_usage_bayer(self,year)
        Bauxite.bauxite_usage_sinter(self,year)
        Bauxite.bauxite_usage_bayer_mud_sinter(self,year)
        Bauxite.bauxite_usage_sinter(self,year)
        Bauxite.bauxite_usage(self,year)

        Bauxite.BauxitePriceCost1(self,year)
        Bauxite.BauxitePriceCost2(self,year)
        Bauxite.CausticCost(self,year)
        Bauxite.BauxiteCausticCost(self,year)
        Bauxite.PreCalculation(self,year)
        Bauxite.SourcingMix(self, year)

        Bauxite.aa_production_protocol(self,year)
        Bauxite.bauxite_consumption(self,year)
        Bauxite.depcap(self, year)
        Bauxite.calcdep1(self, year)
        Bauxite.calcdep3(self,year)
        Bauxite.calcdep6(self, year)
        Bauxite.calcprod8(self, year)
        Bauxite.calcprod9(self, year)
        Bauxite.calcprod10(self, year)
        Bauxite.calcprodout(self, year)
        Bauxite.calcdep8(self, year)
        Bauxite.calcdep9(self, year)
        Bauxite.calcdep10(self, year)
        Bauxite.calcdep11(self, year)
        Bauxite.calcdep12(self, year)
        Bauxite.calcdep13(self, year)
        Bauxite.calcdep14(self, year)
        Bauxite.calcdep15(self, year)
        Bauxite.calcdep16(self, year)
        Bauxite.calcdep17(self, year)
        Bauxite.calcdep18(self, year)
        Bauxite.calcdep19(self, year)

        Bauxite.calcbauxitedata1(self, year)
        Bauxite.calcbauxitedata2(self, year)

        Bauxite.usagee(self,year)
        Bauxite.opening_stock(self,year)
        Bauxite.closing_stock(self,year)

        # prod data rqd by aa prod and cs outlook
        if int(post_year) < 2032:
            Bauxite.calcdep7(self, post_year)
            Bauxite.calcprod6(self, post_year)
            Bauxite.calcprod7(self, post_year)

        Bauxite.cs_outlook(self,year)
        Bauxite.closing_stock_portion_of_total(self,year)
        Bauxite.cs_outlook_total(self,year)




        if int(post_year) < 2032:
            Bauxite.alumina_silica_ratio_grade(self,post_year)

            

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
    8.1943,
    7.9734,
    7.6075,
    6.9451,
    6.83,
    6.7703,
    6.4615,
    6.3123,
    6.152292,
    6.152292,
    6.1,
    6.1,
    6,
    6,
    5.9,
    5.9,
    5.9,
    5.8,
    5.8,
    5.8,
    5.8,
    5.8,
    5.7,
    5.7,
    5.7,
    5.7,
    5.7
]

globaldb = pd.DataFrame(globaldata)
bayersinterdb = pd.read_csv("ddm/caustic.csv")
# sheet = bayersinterdb['refinery'].tolist()
# sheet = sheets
# _, idx = np.unique(sheet, return_index=True)
# sheet = sheet[np.sort(idx)]
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
#pass#print("here")
proddata1 = pd.read_csv("ddm/proddatah.csv")

otherlist = proddata1['province']
provicialcalc = provincial()

st = time.perf_counter()
provicialcalc.pdi_calc_all()
provicialcalc.provincialcalcall()

print('TIme taken to run provincial {0} secs'.format(time.perf_counter() - st))
sm = time.perf_counter()
r = summary()
r.calcall1()
smt = time.perf_counter() - sm
print('TIme taken to run economic {0}'.format(smt))
pass#print("completed reservesummary")
mxid = pd.MultiIndex.from_product([sheets,province,pm])
mxid2 = pd.MultiIndex.from_product([Province,proparam])
db = pd.DataFrame(index=mxid,columns=yearsinput)
idx = pd.IndexSlice
summarydb = r.hdb 
summarydb.to_csv('ddm/calc_sample_output.csv')
# summarydb = pd.read_csv('ddm/calc_sample_output.csv')
b1 = Bauxite(db,bayersinterdb,globaldb,summarydb,provicialcalc.db,otherlist,exlist)
# c1 = productiondata(capdata1,plotlink,depcap1,capout,proddata1,proddata2,proddata3,proddata4,proddata5,proddata6,proddata7,proddata8,proddata9,proddata10,production,prodout,depdata1,depdata2,depdata3,depdata4,depdata5,depdata6,depdata7,depdata8,depdata9,depdata10,depdata11,depdata12,depdata13,depdata14,depdata15,depdata16,depdata17,depdata18,depdata19,bauxitedata1,bauxitedata2,cockpitdata1,cockpitdata2,cockpitdata3,silicadata,aluminadata)
provincialdb = pd.DataFrame(index=mxid2,columns=years)

print('starts baxite')

async def aiter(t):
    for l in t:
        yield l

st = time.perf_counter()

def calc1():
    b1.calc_all0()
    l = time.perf_counter() - st
    print('Time taken to initialise single cols: {0} secs'.format(l))
    b1.calc_bauxite_cost()
    print('Time taken to run bauxite prod cost for all years {0} secs'.format(time.perf_counter() - st - l))
    
def calc2():
    for y in years:
        print(y)
        b1.calc_in_seq(y)
        # break

calc1()
print('Start of bauxite circular ref')
print(b1.plotlink)
calc2()
print('Bauxite circular ref done')
r.calcall2(b1.proddata10)
# loop = asyncio.new_event_loop()
# asyncio.set_event_loop(loop)
# loop.run_until_complete(calc2())

print('Total time taken to run bauxite {0} secs'.format(time.perf_counter() - st))

b1.db.to_csv('ddm/outputdata/wholerefinery.csv')
b1.proddata1.to_csv('ddm/outputdata/capprod137_proddata1output.csv',index=False)
b1.proddata2.to_csv('ddm/outputdata/collector1256_proddata2output.csv',index=False)
b1.proddata3.to_csv('ddm/outputdata/collector1828_proddata3output.csv',index=False)
b1.proddata4.to_csv('ddm/outputdata/collector1959_proddata4output.csv',index=False)
b1.proddata5.to_csv('ddm/outputdata/capprod531_proddata5output.csv',index=False)
b1.proddata6.to_csv('ddm/outputdata/capprod720_proddata6output.csv',index=False)
b1.proddata7.to_csv('ddm/outputdata/capprod866_proddata7output.csv',index=False)
b1.proddata8.to_csv('ddm/outputdata/capprod1589_proddata8output.csv',index=False)
b1.proddata9.to_csv('ddm/outputdata/capprod1713_proddata9output.csv',index=False)
b1.proddata10.to_csv('ddm/outputdata/capprod1004_proddata10output.csv',index=False)
b1.depdata1.to_csv('ddm/outputdata/capprod644_depdata1output.csv',index=False)
b1.capdata1.to_csv('ddm/outputdata/depcap1output.csv',index=False)
#b1.depdata2.to_csv('outputdata/depdata2output.csv')
b1.depdata3.to_csv('ddm/outputdata/capprod657_depdata3output.csv',index=False)
b1.depdata4.to_csv('ddm/outputdata/capprod250_depdata4output.csv',index=False)
#b1.depdata5.to_csv('outputdata/depdata5output.csv')
b1.depdata6.to_csv('ddm/outputdata/capprod667_depdata6output.csv',index=False)
b1.depdata7.to_csv('ddm/outputdata/capprod701_depdata7output.csv',index=False)
b1.depdata8.to_csv('ddm/outputdata/collector2333_depdata8output.csv',index=False)
b1.depdata9.to_csv('ddm/outputdata/collector2073_depdata9output.csv',index=False)
b1.depdata10.to_csv('ddm/outputdata/cockpit102_depdata10output.csv',index=False)
b1.depdata11.to_csv('ddm/outputdata/cockpit159_depdata11output.csv',index=False)
b1.depdata12.to_csv('ddm/outputdata/cockpit172_depdata12output.csv',index=False)
b1.depdata13.to_csv('ddm/outputdata/collector2343_depdata13output.csv',index=False)
b1.depdata14.to_csv('ddm/outputdata/collector2350_barbyrefoutput.csv',index=False)
b1.depdata15.to_csv('ddm/outputdata/collector1949_depdata15output.csv',index=False)
b1.depdata16.to_csv('ddm/outputdata/collector2400_depdata16output.csv',index=False)
b1.depdata17.to_csv('ddm/outputdata/collector2409_depdata17output.csv',index=False)
b1.depdata18.to_csv('ddm/outputdata/collector2417_CumulativedomesticBXtoAA.csv',index=False)
b1.depdata19.to_csv('ddm/outputdata/collector2085_depdata19output.csv',index=False)
b1.bauxitedata1.to_csv('ddm/outputdata/bauxitedata1output.csv',index=False)
b1.bauxitedata2.to_csv('ddm/outputdata/bauxitedata2output.csv',index=False)
b1.cockpitdata1.to_csv('ddm/outputdata/cockpitdata1.csv',index=False)
b1.cockpitdata2.to_csv('ddm/outputdata/cockpitdata2.csv',index=False)
b1.cockpitdata3.to_csv('ddm/outputdata/cockpitdata3.csv',index=False)
b1.aluminadata.to_csv('ddm/outputdata/aluminagradeoutput.csv',index=False)
b1.plotlink.to_csv('ddm/outputdata/plotlinkoutput.csv',index=False)
b1.silicadata.to_csv('ddm/outputdata/silicagradeoutput.csv',index=False)
# b1.production.to_csv('outputdata/productionoutput.csv',index=False)
b1.prodout.to_csv('ddm/outputdata/prodoutoutput.csv',index=False)
provicialcalc.db.to_csv("ddm/outputdata/provincialoutput.csv",index=False)
r.db.to_csv('ddm/outputdata/db.csv',index=False)
r.provt.to_csv('ddm/outputdata/provtdb.csv',index=False)
r.bfdb.to_csv('ddm/outputdata/bfdb.csv',index=False)
r.hdb.to_csv('ddm/outputdata/hdb.csv',index=False)
r.totaldb.to_csv('ddm/outputdata/totaldb.csv',index=False)
r.newallocdb.to_csv('ddm/outputdata/newallocdb.csv',index=False)

print("Convert to flab db output")
dbflat_time = time.perf_counter()
# @halim there is dataframe called whole refinery which is b1.db is missing , if defined please name it "All Refinery Sheet Codeups Output"

b1_db = db_conv.multi_year_multi_out(b1.db, "All Refinery Sheet Codeups", col_params=[(0, 'Refinery'), (1, 'Province'), (2, 'Fields')])
b1_proddata1 = db_conv.mult_year_single_output(b1.proddata1, 'Base Production')
b1_proddata2 = db_conv.mult_year_single_output(b1.proddata2, 'Tonnages of bauxite consumed by each refinery split by major province ')
b1_proddata3 = db_conv.mult_year_single_output(b1.proddata3, 'Total Tonnages of bauxite consumed by each refinery')
b1_proddata4 = db_conv.mult_year_single_output(b1.proddata4, 'Scheduled production tonnages of alumina per refinery based on depleted bauxite reserves ')
b1_proddata5 = db_conv.mult_year_single_output(b1.proddata5, 'Depleted Production Output ')
b1_proddata6 = db_conv.mult_year_single_output(b1.proddata6, 'Capprod 720')
b1_proddata7 = db_conv.mult_year_single_output(b1.proddata7, 'Productions Sent to Refinery Sheets ')
b1_proddata8 = db_conv.mult_year_single_output(b1.proddata8, 'AA Prodction from Domestic Bx')
b1_proddata9 = db_conv.mult_year_single_output(b1.proddata9, 'AA Production from Import Bx')
b1_proddata10 = db_conv.mult_year_single_output(b1.proddata10, 'Capprod 1004')
b1_depdata1 = db_conv.mult_year_single_output(b1.depdata1, 'Capprod 644')
b1_capdata1 = db_conv.mult_year_single_output(b1.capdata1, 'decap1output')
#b1.depdata2.to_csv('outputdata/depdata2output.csv')
b1_depdata3 = db_conv.mult_year_single_output(b1.depdata3, 'Capprod 657')
b1_depdata4 = db_conv.mult_year_single_output(b1.depdata4, 'Base production splits by region')
    #b1.depdata5.to_csv('outputdata/depdata5output.csv')
b1_depdata6 = db_conv.mult_year_single_output(b1.depdata6, 'Capprod 667')
b1_depdata7 = db_conv.mult_year_single_output(b1.depdata7, 'Capprod 701')
b1_depdata8 = db_conv.mult_year_single_output(b1.depdata8, 'Collector 2333')
b1_depdata9 = db_conv.mult_year_single_output(b1.depdata9, 'Depleted tonnages by Major Province ')
b1_depdata10 = db_conv.mult_year_single_output(b1.depdata10, 'Cockpit 102')
b1_depdata11 = db_conv.mult_year_single_output(b1.depdata11, 'Cockpit 159')
b1_depdata12 = db_conv.mult_year_single_output(b1.depdata12, 'Cockpit 172')
b1_depdata13 = db_conv.mult_year_single_output(b1.depdata13, 'Fraction Depleted by Major Province ')
b1_depdata14 = db_conv.mult_year_single_output(b1.depdata14, 'BAR by Refinery Alcoa Work ')
b1_depdata15 = db_conv.mult_year_single_output(b1.depdata15, 'Collector 1949')
b1_depdata16 = db_conv.mult_year_single_output(b1.depdata16, 'Collector 2400')
b1_depdata17 = db_conv.mult_year_single_output(b1.depdata17, 'Collector 2409')
b1_depdata18 = db_conv.mult_year_single_output(b1.depdata18, 'Cumulative BAR by Major Prov collector2417_CumulativedomesticBXtoAA')
b1_depdata19 = db_conv.mult_year_single_output(b1.depdata19, 'Collector 2085')
b1_bauxitedata1 = db_conv.mult_year_single_output(b1.bauxitedata1, 'Bauxite data 1')
b1_bauxitedata2 = db_conv.mult_year_single_output(b1.bauxitedata2, 'Cockpit Import Bx Demand Summary as AA ')
b1_cockpitdata1 = db_conv.mult_year_single_output(b1.cockpitdata1, 'Cockpit Import Bx Transitioning Output ')
b1_cockpitdata2 = db_conv.mult_year_single_output(b1.cockpitdata2, 'Cockpit data 2')
b1_cockpitdata3 = db_conv.mult_year_single_output(b1.cockpitdata3, 'Cockpit Allow 3rd Party Domestic Bx Trade Output ')
b1_aluminadata = db_conv.mult_year_single_output(b1.aluminadata, 'Alumina grades of bauxite consumed by each refinery split by major province collector')
b1_plotlink = db_conv.mult_year_single_output(b1.plotlink, 'Plotlink')
b1_silicadata = db_conv.mult_year_single_output(b1.silicadata, 'Silica a grades of bauxite consumed by each refinery split by major province')
# b1_production = db_conv.mult_year_single_output(b1.production, 'Production')
b1_prodout = db_conv.mult_year_single_output(b1.prodout, 'Product')
b1_provincialdb = db_conv.multi_year_multi_out(provicialcalc.db, 'Provincial', col_params=[(0, 'Province'), (1, 'Field')])
r_db = db_conv.single_year_mult_out(r.db, 'Inventroy Processing output ')
r_provt = db_conv.single_year_mult_out(r.provt, 'Reserve Summary Prov Summaries ')
r_bfdb = db_conv.single_year_mult_out(r.bfdb, 'bfdb')
r_hdb = db_conv.single_year_mult_out(r.hdb, 'hdb')
r_totaldb = db_conv.single_year_mult_out(r.totaldb, 'Reserve Summary Totals output ')
r_newallocdb = db_conv.single_year_mult_out(r.newallocdb, 'Bx allocation Tonnage')

snapshot_output_data = pd.DataFrame(columns=db_conv.out_col)
dblist = [
snapshot_output_data,
b1_db,
b1_proddata1,
b1_provincialdb,
b1_proddata2,
b1_proddata3,
b1_proddata4,
b1_proddata5,
b1_proddata6,
b1_proddata7,
b1_proddata8,
b1_proddata9,
b1_proddata10,
b1_depdata1,
b1_capdata1,
b1_depdata3,
b1_depdata4,
b1_depdata6,
b1_depdata7,
b1_depdata8,
b1_depdata9,
b1_depdata10,
b1_depdata11,
b1_depdata12,
b1_depdata13,
b1_depdata14,
b1_depdata15,
b1_depdata16,
b1_depdata17,
b1_depdata18,
b1_depdata19,
b1_bauxitedata1,
b1_bauxitedata2,
b1_cockpitdata1,
b1_cockpitdata2,
b1_cockpitdata3,
b1_aluminadata,
b1_plotlink,
b1_silicadata,
# b1_production,
b1_prodout,
r_db,
r_provt,
r_bfdb,
r_hdb,
r_totaldb,
r_newallocdb]
snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
print("Time taken to convet to flat db: {0}".format(time.perf_counter() - dbflat_time))
# for i in sheet:
#     dblist.append(b1.db.loc[i])   
# tdb = reltoflat(dblist,cnxn)            
# pd.to_csv("ddm/outputdata/snapshot_output_data.csv")
'''
with pd.option_context('display.max_rows',None, 'display.max_columns', None):
    
    b1.db.to_csv('outputdata/wholerefinery.csv')
    b1.proddata1.to_csv('outputdata/capprod137_proddata1output.csv',index=False)
    b1.proddata2.to_csv('outputdata/collector1256_proddata2output.csv',index=False)
    c1.proddata3.to_csv('outputdata/collector1828_proddata3output.csv',index=False)
    c1.proddata4.to_csv('outputdata/collector1959_proddata4output.csv',index=False)
    c1.proddata5.to_csv('outputdata/capprod531_proddata5output.csv',index=False)
    c1.proddata6.to_csv('outputdata/capprod720_proddata6output.csv',index=False)
    c1.proddata7.to_csv('outputdata/capprod866_proddata7output.csv',index=False)
    c1.proddata8.to_csv('outputdata/capprod1589_proddata8output.csv',index=False)
    c1.proddata9.to_csv('outputdata/capprod1713_proddata9output.csv',index=False)
    c1.proddata10.to_csv('outputdata/capprod1004_proddata10output.csv',index=False)
    c1.depdata1.to_csv('outputdata/capprod644_depdata1output.csv',index=False)
    c1.capdata1.to_csv('outputdata/depcap1output.csv',index=False)
    #c1.depdata2.to_csv('outputdata/depdata2output.csv')
    c1.depdata3.to_csv('outputdata/capprod657_depdata3output.csv',index=False)
    c1.depdata4.to_csv('outputdata/capprod250_depdata4output.csv',index=False)
    #c1.depdata5.to_csv('outputdata/depdata5output.csv')
    c1.depdata6.to_csv('outputdata/capprod667_depdata6output.csv',index=False)
    c1.depdata7.to_csv('outputdata/capprod701_depdata7output.csv',index=False)
    c1.depdata8.to_csv('outputdata/collector2333_depdata8output.csv',index=False)
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
    c1.bauxitedata1.to_csv('outputdata/bauxitedata1output.csv',index=False)
    c1.bauxitedata2.to_csv('outputdata/bauxitedata2output.csv',index=False)
    c1.cockpitdata1.to_csv('outputdata/cockpitdata1.csv',index=False)
    c1.cockpitdata2.to_csv('outputdata/cockpitdata2.csv',index=False)
    c1.cockpitdata3.to_csv('outputdata/cockpitdata3.csv',index=False)
    c1.aluminadata.to_csv('outputdata/aluminagradeoutput.csv',index=False)
    c1.plotlink.to_csv('outputdata/plotlinkoutput.csv',index=False)
    c1.silicadata.to_csv('outputdata/silicagradeoutput.csv',index=False)
    c1.production.to_csv('outputdata/productionoutput.csv',index=False)
    c1.prodout.to_csv('outputdata/prodoutoutput.csv',index=False)
    b1.provincialdb.to_csv("outputdata/provincialoutput.csv",index=False)
    r.db.to_csv('outputdata/db.csv',index=False)
    r.provt.to_csv('outputdata/provtdb.csv',index=False)
    r.bfdb.to_csv('outputdata/bfdb.csv',index=False)
    r.hdb.to_csv('outputdata/hdb.csv',index=False)
    r.totaldb.to_csv('outputdata/totaldb.csv',index=False)
    r.newallocdb.to_csv('outputdata/newallocdb.csv',index=False)
    for i in sheet:
        hh = b1.db.loc[i]
        hh.to_csv("outputdata/refinery/"+i+" output.csv")
'''


print('uploading to output db')
upload(snapshot_output_data)

print("done")
