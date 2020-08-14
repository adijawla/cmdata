# -*- coding: utf-8 -*-
"""
Created on Sat Feb  1 12:38:05 2020

@author: vrun
"""
import simply
from simply import *
import warnings
from ddm.codetimer.timer import Timer
import numpy as np
import pandas as pd
import statistics as stat
import time
#import openpyxl
import warnings
warnings.filterwarnings("ignore")
Provinces = [
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
    'underground mining cost depth factor1',
    'underground mining cost electricity consumption',
    'underground mining cost electricity price',
    'underground mining cost electricity cost',
    'underground mining cost automation and productivity low',
    'underground mining cost automation and productivity medium',
    'underground mining cost automation and productivity high',
    'underground mining cost depth factor2',
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
    'underground mining cost waste ratio',
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
    'underground lignitous coal source',
    'underground lignitous coal base price',
    'underground lignitous coal distance from source',
    'underground lignitous coal freight rate',
    'underground lignitous coal delivered price',
    'underground anthracite coal source',
    'underground anthracite coal base price',
    'underground anthracite coal difference',
    'underground anthracite coal distance from source',
    'underground anthracite coal freight rate',
    'underground anthracite coal delivered price'
    ]

#mxid = pd.MultiIndex.from_product([sheet,province,pm])
#mxid2 = pd.MultiIndex.from_product([Province,proparam])
#db = pd.DataFrame(index=mxid,columns=years)
#idx = pd.IndexSlice
#summarydb = pd.read_csv("ddm/summary.csv")
#b1 = Bauxite(db,bayersinterdb,globaldb,summarydb)
#c1 = productiondata(proddata1,proddata2,proddata3,proddata4,proddata5,proddata6,proddata7,production,depdata1,depdata2,depdata3,depdata4,depdata5,depdata6,depdata7,cockpitdata1,cockpitdata2,cockpitdata3)
#provincialdb = pd.DataFrame(index=mxid2,columns=years)
#prodb = provincial(provincialdb)

#df = provincialinput = pd.read_csv('ddm/provincialdb.csv', delimiter=',')

# provicial db calculations starts here 
pwi_rows = ['Power',
            'Labour Dressing',
            'Labour UG',
            'Labour General',
            'Diesel Price',
            'Mining Transport',
            'Other Mining',
            'Dressing Material',
            'Government Charges',
            'Freight Rates',
            'Price Domestic',
            'Caustic Soda',
            'Sodium Carbonate',
            'Lime',
            'Flocculant',
            'Lignitous Coal',
            'Anthracite Coal'
            ]
pwi_rows_c = ['Cost Change', 'Cost Index']

prov_timer = Timer("Provincial", txt=True)
class provincial():
    def __init__(self):
        prov_timer.start()
        mxid = pd.MultiIndex.from_product([Provinces,proparam])
        mxid1 = pd.MultiIndex.from_product([pwi_rows, pwi_rows_c])
        self.years = ["2005", "2006", "2007", "2008", '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025', '2026', '2027', '2028', '2029', '2030', '2031']
        self.db = pd.DataFrame(index=mxid,columns=self.years)
        self.pdi = pd.DataFrame(index=mxid1, columns=self.years)
        self.df = pd.read_csv('ddm/provincialdb.csv')
        self.pdi_input = pd.read_csv('ddm/price_cost_index_input.csv')
        self.idx = pd.IndexSlice 
        self.max_col = len(self.years)
        self.depth_decrease = 0
        prov_timer.stop()

    def pdi_helper_gen(self, indexes):
        prov_timer.start()
        map_rows = [f'{a.lower()} cost change' for a in pwi_rows]
        for i in np.arange(len(map_rows)):
            a = np.full(self.max_col, self.pdi_input[map_rows[i]])
            self.pdi.loc[indexes[i], 'Cost Change'] = a
        ci = np.array(self.pdi.loc[::-2])
        ci[0][:6] = np.ones(6)
        wc = np.array(self.pdi.loc[::2])[::-1]
        ci = [[*ci[0]] for a in ci]
        for i in np.arange(len(ci)):
            for j in np.arange(6, len(ci[i])):
                ci[i][j] = ci[i][j-1]*(1+wc[i][j])
        self.pdi.loc[(slice(None), 'Cost Index'), '2005':'2031'] = ci
        prov_timer.stop()

    def miningautolevel(self):
        prov_timer.start()
        for Province in Provinces:
            d = self.df.loc[self.df.Province==Province]['MineAutomation'].sum()
            self.db.at[self.idx[Province,'mining characteristics mine automation level']] = np.full(self.max_col, d)
        prov_timer.stop()

    def miningdepthopenpit(self):
        prov_timer.start()
        for Province in Provinces:
            d = [self.df.loc[self.df.Province==Province]['MDOpenPit'].sum()]
            s = d[0]
            result = [s]
            for i in np.arange(1,self.max_col):
                result.append(result[i-1]+self.depth_decrease)
            self.db.at[self.idx[Province,'mining characteristics mine depth open pit']] = result
        prov_timer.stop()

    def miningdepthunderground(self):
        prov_timer.start()
        for Province in Provinces:
            d = [self.df.loc[self.df.Province==Province]['MDUnderground'].sum()]
            s = d[0]
            result = [s]
            for i in np.arange(1,self.max_col):
                result.append(result[i-1]+self.depth_decrease)
            self.db.at[self.idx[Province,'mining characteristics mine depth underground']] = result
        prov_timer.stop()

    def miningdressautolevel(self):
        prov_timer.start()
        for Province in Provinces:
            d = [self.df.loc[self.df.Province==Province]['DressingAutomation'].sum()]
            s = d[0]
            self.db.at[self.idx[Province,'mining characteristics dressing automation level']] = np.full(self.max_col, s)
        prov_timer.stop()

    def miningproduction(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['ProductionScale'].sum()]
            s = d[0]
            self.db.at[self.idx[Province,'mining characteristics production']] = np.full(self.max_col, s)
        prov_timer.stop()

    def miningopen(self):
        prov_timer.start()
        for Province in Provinces:
            d = [self.df.loc[self.df.Province==Province]['PortionOpenpit'].sum()]
            s = d[0]
            self.db.at[self.idx[Province,'mining characteristics mine type open']] = np.full(self.max_col, s)
        prov_timer.stop()

    def miningunderground(self):
        prov_timer.start()
        for Province in Provinces:
            d = 1 - np.array([self.db.loc[Province,'mining characteristics mine type open']])
            self.db.at[self.idx[Province,'mining characteristics mine type underground']] = d
        prov_timer.stop()

    def miningstrippingratioopenpit(self):
        prov_timer.start()
        for Province in Provinces:
            s = self.df.loc[self.df.Province==Province]['SWROpenPit'].sum()
            self.db.at[self.idx[Province,'mining characteristics stripping ratio open pit']] = np.full(self.max_col, s)
        prov_timer.stop()

    def miningcharmdunderground(self):
        prov_timer.start()
        for Province in Provinces:
            s = self.df.loc[self.df.Province==Province]['SWRUnderground'].sum()
            self.db.at[self.idx[Province,'mining characteristics stripping ratio underground']] = np.full(self.max_col, s)
        prov_timer.stop()

        # gb
    def undergroundminingcostwasteratio(self):
        prov_timer.start()
        for Province in Provinces:
            s = self.db.loc[Province,'mining characteristics stripping ratio underground']
            # print(s)
            self.db.at[self.idx[Province,'underground mining cost waste ratio']] = s
        prov_timer.stop()

    def miningelectricitycons(self):
        prov_timer.start()
        for Province in Provinces:
            d = np.array(self.db.loc[Province,'mining characteristics production'])
            s = 1.5*pow(d,-0.129)
            self.db.at[self.idx[Province,'open pit mine mining cost electricity consumption']] = s
        prov_timer.stop()
            
    def miningelecprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Power'].sum()][0]
            pci = self.pdi.loc[('Power', 'Cost Index'), self.years[0]:self.years[-1]]
            # print(d*pci)
            result = d*pci
            self.db.at[self.idx[Province,'open pit mine mining cost electricity price']] = result
        prov_timer.stop()

    def miningeleccost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost electricity price'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost electricity consumption'])
            result = a * b
            self.db.at[self.idx[Province,'open pit mine mining cost electricity cost']] = result
        prov_timer.stop()

    def miningdeiselprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['DieselPrice'].sum()]
            s = d[0]
            dpci = self.pdi.loc[('Diesel Price', 'Cost Index'), self.years[0]:self.years[-1]]
            result = s*dpci
            self.db.at[self.idx[Province,'open pit mine mining cost diesel price']] = result
        prov_timer.stop()

    def miningdeiselusage(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics mine depth open pit'])
            b = np.array(self.db.loc[Province,'mining characteristics production'])
            result = 0.005*a+1+1*pow((b),-0.129)
            self.db.at[self.idx[Province,'open pit mine mining cost diesel usage']] = result
        prov_timer.stop()

    def miningdeiselcost(self):
        prov_timer.start()
        for Province in Provinces:
            a =  np.array(self.db.loc[Province,'open pit mine mining cost diesel usage'])
            b =  np.array(self.db.loc[Province,'open pit mine mining cost diesel price'])
            result = a * b
            self.db.at[self.idx[Province,'open pit mine mining cost diesel cost']] = result
        prov_timer.stop()

    def mininglabprod1(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics mine depth open pit'])
            result = 0.0022*a+0.44
            self.db.at[self.idx[Province,'open pit mine mining cost labour productivity']] = result
        prov_timer.stop()

    def mininglabrate1(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['LaborMining'].sum()]
            s = d[0]*12/2000
            lrci = self.pdi.loc[('Labour Dressing', 'Cost Index'), self.years[0]:self.years[-1]]
            result = s * lrci
            self.db.at[self.idx[Province,'open pit mine mining cost labour rate']] = result
        prov_timer.stop()

    def mininglabcost1(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost labour productivity'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost labour rate'])
            result = a * b
            self.db.at[self.idx[Province,'open pit mine mining cost labour cost']] = result
        prov_timer.stop()

    def miningfreightcost(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['MTCOpenpit'].sum() ]
            s = d[0]*0
            mtci = np.array(self.pdi.loc[('Mining Transport', 'Cost Index'), self.years[0]:self.years[-1]])
            result = s * mtci
            self.db.at[self.idx[Province,'open pit mine mining cost freight cost']] = result
        prov_timer.stop()

    def miningothercost(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['OMCOpenpit'].sum(),
            self.db.loc[Province,'mining characteristics production'][self.years[0]]]
            s = d[0]-0.0015*d[1]
            omci = np.array(self.pdi.loc[('Other Mining', 'Cost Index'), self.years[0]:self.years[-1]])
            result = s * omci
            self.db.at[self.idx[Province,'open pit mine mining cost other cost']] = result
        prov_timer.stop()

    def miningoreminingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost electricity cost'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost diesel cost'])
            c = np.array(self.db.loc[Province,'open pit mine mining cost labour cost'])
            d = np.array(self.db.loc[Province,'open pit mine mining cost freight cost'])
            e = np.array(self.db.loc[Province,'open pit mine mining cost other cost'])        
            result = a+b+c+d+e
            self.db.at[self.idx[Province,'open pit mine mining cost ore mining cost']] = result
        prov_timer.stop()

    def miningstrippingratio(self):
        prov_timer.start()
        for Province in Provinces:
            result = self.db.loc[Province,'mining characteristics stripping ratio open pit']  
            self.db.at[self.idx[Province,'open pit mine mining cost stripping ratio']] = result
        prov_timer.stop()

    def miningwasteproduction(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost stripping ratio'])
            b = np.array(self.db.loc[Province,'mining characteristics production'])
            result = a * b
            self.db.at[self.idx[Province,'open pit mine mining cost waste production']] = result
        prov_timer.stop()

    def miningelctricityconsumption(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost waste production'])
            result =1.64*pow(a,-0.129)
            self.db.at[self.idx[Province,'open pit mine mining cost electricity consumption2']] = result
        prov_timer.stop()

    def miningelectricityprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Power'].sum()]
            s = d[0]
            pci = np.array(self.pdi.loc[('Power', 'Cost Index'), self.years[0]:self.years[-1]])
            result = s * pci
            self.db.at[self.idx[Province,'open pit mine mining cost electricity price2']] = result
        prov_timer.stop()

    def miningelectricitycost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost electricity consumption2'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost electricity price2'])    
            result = a * b
            self.db.at[self.idx[Province,'open pit mine mining cost electricity cost2']] = result
        prov_timer.stop()

    def miningcostdieselusage(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics mine depth open pit'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost waste production'])
            result = 0.002*a+0.0571*pow(b,-0.129)+0.71
            self.db.at[self.idx[Province,'open pit mine mining cost diesel usage2']] = result
        prov_timer.stop()

    def miningcostdieselprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['DieselPrice'].sum()]
            s = d[0]
            dpci = np.array(self.pdi.loc[('Diesel Price', 'Cost Index'), self.years[0]:self.years[-1]])
            result = dpci * s
            self.db.at[self.idx[Province,'open pit mine mining cost diesel price2']] = result
        prov_timer.stop()

    def miningcostdieselcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost diesel usage2'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost diesel price2']) 
            result = a*b
            self.db.at[self.idx[Province,'open pit mine mining cost diesel cost2']] = result
        
        prov_timer.stop()
    def miningcostlabourproductivity(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics mine depth open pit'])
            result = 0.0006*a+0.11
            self.db.at[self.idx[Province,'open pit mine mining cost labour productivity2']] = result
        prov_timer.stop()

    def miningcostlabourrate(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.df.loc[self.df.Province==Province]['LaborMining'])
            result = a*12/2000
            self.db.at[self.idx[Province,'open pit mine mining cost labour rate2']] = result
        prov_timer.stop()

    def miningcostlabourcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost labour productivity2'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost labour rate2'])
            result = a * b
            self.db.at[self.idx[Province,'open pit mine mining cost labour cost2']] = result
        prov_timer.stop()

    def miningcostother(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost waste production'])
            result = 1.875-0.0003*a if Province != 'Yunnan' else 0 
            self.db.at[self.idx[Province,'open pit mine mining cost other']] = result
        prov_timer.stop()

    def miningcoststrippingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost electricity cost2'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost diesel cost2'])
            c = np.array(self.db.loc[Province,'open pit mine mining cost labour cost2'])
            d = np.array(self.db.loc[Province,'open pit mine mining cost other']) 
            # c = c if type(d[3]) == int else d[3].sum()
            result = a + b + c + d
            self.db.at[self.idx[Province,'open pit mine mining cost stripping cost']] = result
        prov_timer.stop()

    def miningcosttotalstrippingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost stripping cost'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost stripping ratio'])            
            result = a * b
            self.db.at[self.idx[Province,'open pit mine mining cost total stripping cost']] = result
        prov_timer.stop()

    def miningcostopenpitminingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine mining cost ore mining cost'])
            b = np.array(self.db.loc[Province,'open pit mine mining cost total stripping cost'])
            result = a + b
            self.db.at[self.idx[Province,'open pit mine mining cost open pit mining cost']] = result
        prov_timer.stop()

    def dressingcostautomationandelectricityusagelow(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics production'])
            result = 8*pow(a,-0.07)
            # print(result)
            self.db.at[self.idx[Province,'open pit mine dressing cost automation and electricity usage low']] = result
        prov_timer.stop()

    def dressingcostautomationandelectricityusagemedium(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics production'])
            result = 9*pow(a,-0.07)
            self.db.at[self.idx[Province,'open pit mine dressing cost automation and electricity usage medium']] = result
        prov_timer.stop()

    def dressingcostautomationandelectricityusagehigh(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics production'])
            result = 10*pow(a,-0.07)
            self.db.at[self.idx[Province,'open pit mine dressing cost automation and electricity usage high']] = result
        prov_timer.stop()

    def dressingcostelectricityconsumption(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics production'])
            b = np.array(self.db.loc[Province,'mining characteristics dressing automation level'])
            c = [np.array(self.db.loc[Province,'open pit mine dressing cost automation and electricity usage low']),
                np.array(self.db.loc[Province,'open pit mine dressing cost automation and electricity usage medium']),
                np.array(self.db.loc[Province,'open pit mine dressing cost automation and electricity usage high'])]
            result = [c[int(b[i])-1][i] if a[i] > 0 else 0 for i in np.arange(len(a))]
            # for i in np.arange(len(a)):
            #     if a[i] > 0:
            #         if b[i] == 1:
            #             result.append(c[i])
            #         elif b[i] == 2:
            #             result.append(d[i])
            #         elif b[i] == 3:
            #             result.append(e[i])
            #         else:
            #             result.append(0.000)
            #     else:
            #         result.append(0.00)
            self.db.at[self.idx[Province,'open pit mine dressing cost electricity consumption']] = result
        prov_timer.stop()

    def dressingcostelectricityprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Power'].sum()]
            s = d[0]
            pci = np.array(self.pdi.loc[('Power', 'Cost Index'), self.years[0]:self.years[-1]])
            result = pci * s
            self.db.at[self.idx[Province,'open pit mine dressing cost electricity price']] = result
        prov_timer.stop()

    def dressingcostelectricitycost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine dressing cost electricity consumption'])
            b = np.array(self.db.loc[Province,'open pit mine dressing cost electricity price'])
            result = a * b
            self.db.at[self.idx[Province,'open pit mine dressing cost electricity cost']] = result
        prov_timer.stop()

    def dressingcostautomationandlabourproductivitylow(self):
        prov_timer.start()
        for Province in Provinces:
            # set as input val
            s = 0.08
            result = np.full(self.max_col, s)
            self.db.at[self.idx[Province,'open pit mine dressing cost automation and labour productivity low']] = result
        prov_timer.stop()

    def dressingcostautomationandlabourproductivitymedium(self):
        prov_timer.start()
        for Province in Provinces:
            # set as input val
            s = 0.07
            result = np.full(self.max_col, s)
            self.db.at[self.idx[Province,'open pit mine dressing cost automation and labour productivity medium']] = result
        prov_timer.stop()

    def dressingcostautomationandlabourproductivityhigh(self):
        prov_timer.start()
        for Province in Provinces:
            # set as input val
            s = 0.06
            result = np.full(self.max_col, s)
            self.db.at[self.idx[Province,'open pit mine dressing cost automation and labour productivity high']] = result
        prov_timer.stop()

    def dressingcostlabourproductivity(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics dressing automation level'])
            b = [np.array(self.db.loc[Province,'open pit mine dressing cost automation and labour productivity low']),
            np.array(self.db.loc[Province,'open pit mine dressing cost automation and labour productivity medium']),
            np.array(self.db.loc[Province,'open pit mine dressing cost automation and labour productivity high'])]
            result = [b[int(a[i])-1][i] for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'open pit mine dressing cost labour productivity']] = result
        prov_timer.stop()

    def dressingcostlabourrate(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['LaborMining'].sum()]
            s = d[0]*12/2000
            ldci = np.array(self.pdi.loc[('Labour Dressing', 'Cost Index'), self.years[0]:self.years[-1]])
            result = s * ldci
            self.db.at[self.idx[Province,'open pit mine dressing cost labour rate']] = result
        prov_timer.stop()

    def dressingcostlabourcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine dressing cost labour productivity'])
            b = np.array(self.db.loc[Province,'open pit mine dressing cost labour rate'])
            result = a * b
            self.db.at[self.idx[Province,'open pit mine dressing cost labour cost']] = result
        prov_timer.stop()

    def dressingcostauxillarymaterialcost(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['DMCUnderground'].sum()]
            s = d[0]
            dmci = np.array(self.pdi.loc[('Dressing Material', 'Cost Index'), self.years[0]:self.years[-1]])
            result = dmci * s
            self.db.at[self.idx[Province,'open pit mine dressing cost auxillary material cost']] = result
        prov_timer.stop()

    def dressingcosttotaldressingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'open pit mine dressing cost auxillary material cost'])
            b = np.array(self.db.loc[Province,'open pit mine dressing cost labour cost'])
            c = np.array(self.db.loc[Province,'open pit mine dressing cost electricity cost'])         
            result = a + b + c
            self.db.at[self.idx[Province,'open pit mine dressing cost total dressing cost']] = result
        prov_timer.stop()

    def undergroundminingcostautomationandelectricityusagelow(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics production'])      
            result = 24*pow(a,-0.129)
            self.db.at[self.idx[Province,'underground mining cost automation and electricity usage low']] = result
        prov_timer.stop()

    def undergroundminingcostautomationandelectricityusagemedium(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics production'])
            result = 45*pow(a,-0.14)
            self.db.at[self.idx[Province,'underground mining cost automation and electricity usage medium']] = result
        prov_timer.stop()

    def undergroundminingcostautomationandelectricityusagehigh(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics production'])
            result = 75*pow(a,-0.17)
            self.db.at[self.idx[Province,'underground mining cost automation and electricity usage high']] = result
        prov_timer.stop()

    def undergroundminingcostsizefactor(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics mine automation level'])
            b = [self.db.loc[Province,'underground mining cost automation and electricity usage low'],
                self.db.loc[Province,'underground mining cost automation and electricity usage medium'],
                self.db.loc[Province,'underground mining cost automation and electricity usage high']]
            result = [b[int(a[i])-1][i] for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'underground mining cost size factor']] = result
        prov_timer.stop()
            
    def undergroundminingcostdepthfactor(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'mining characteristics mine depth underground'])
            result = 0.002*a+0.796
            self.db.at[self.idx[Province,'underground mining cost depth factor1']] = result
        prov_timer.stop()

    def undergroundminingcostelectricityconsumption(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'underground mining cost size factor'])
            b =np.array(self.db.loc[Province,'underground mining cost depth factor1'])
            result = a * b
            self.db.at[self.idx[Province,'underground mining cost electricity consumption']] = result
        prov_timer.stop()

    def undergroundminingcostelectricityprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Power'].sum()]
            s = d[0]
            pci = np.array(self.pdi.loc[('Power', 'Cost Index'), self.years[0]:self.years[-1]])
            result = pci * s
            self.db.at[self.idx[Province,'underground mining cost electricity price']] = result
        prov_timer.stop()

    def undergroundminingcostelectricitycost(self):
        prov_timer.start()
        for Province in Provinces:
            a = np.array(self.db.loc[Province,'underground mining cost electricity price'])
            b = np.array(self.db.loc[Province,'underground mining cost electricity consumption'])
            result = a * b
            self.db.at[self.idx[Province,'underground mining cost electricity cost']] = result
        prov_timer.stop()

    def undergroundminingcostautomationandproductivitylow(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics production'] 
            result = [1.015 if a[i] < 100 else 1.4*pow(a[i], -0.07) for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'underground mining cost automation and productivity low']] = result
        prov_timer.stop()

    def undergroundminingcostautomationandproductivitymedium(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics production']
            result = [0.936 if a[i] < 200 else 3*pow(a[i], -0.22) for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'underground mining cost automation and productivity medium']] = result
        prov_timer.stop()

    def undergroundminingcostautomationandproductivityhigh(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics production']
            result = [0.785 if a[i] < 300 else 8.6*pow(a[i], -0.42) for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'underground mining cost automation and productivity high']] = result

        prov_timer.stop()
    def undergroundminingcostdepthfactor2(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics mine depth underground']
            result = []
            for i in np.arange(len(a)):
                if a[i] < 125:
                    result.append(2)
                elif a[i] < 450: 
                    result.append(0.008*a[i]+1)
                else:
                    result.append(4.4)
            self.db.at[self.idx[Province,'underground mining cost depth factor2']] = result
        prov_timer.stop()

    def undergroundminingcostautomationfactor(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics mine automation level']
            b = [ self.db.loc[Province,'underground mining cost automation and productivity low'],
                self.db.loc[Province,'underground mining cost automation and productivity medium'],
                self.db.loc[Province,'underground mining cost automation and productivity high']]
            result = [b[int(a[i]) - 1][i] for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'underground mining cost automation factor']] = result
        prov_timer.stop()

    def undergroundminingcostlabourproductivity(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground mining cost depth factor2']
            b = self.db.loc[Province,'underground mining cost automation factor']
            result = a * b + 0.2
            self.db.at[self.idx[Province,'underground mining cost labour productivity']] = result
        prov_timer.stop()

    def undergroundminingcostlabourprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Laborug'].sum()]
            s = d[0]*12/2000
            lugci = np.array(self.pdi.loc[('Labour UG', 'Cost Index'), self.years[0]:self.years[-1]])
            result = lugci * s
            self.db.at[self.idx[Province,'underground mining cost labour price']] = result
        prov_timer.stop()

    def undergroundminingcostlabourcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground mining cost labour productivity']
            b = self.db.loc[Province,'underground mining cost labour price']
            result = a * b
            self.db.at[self.idx[Province,'underground mining cost labour cost']] = result
        prov_timer.stop()

    def undergroundminingcostdieselusage(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics production']
            result = 1.5*pow(a,-0.129)
            self.db.at[self.idx[Province,'underground mining cost diesel usage']] = result
        prov_timer.stop()

    def undergroundminingcostdieselprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['DieselPrice'].sum()]
            s = d[0]
            dpci = np.array(self.pdi.loc[('Diesel Price', 'Cost Index'), self.years[0]:self.years[-1]])
            result = dpci * s
            self.db.at[self.idx[Province,'underground mining cost diesel price']] = result
        prov_timer.stop()

    def undergroundminingcostdieselcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground mining cost diesel price']
            b = self.db.loc[Province,'underground mining cost diesel usage']
            result = a * b
            self.db.at[self.idx[Province,'underground mining cost diesel cost']] = result
        prov_timer.stop()

    def undergroundminingcostfreightcost(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['MTCUnderground'].sum()]
            s = d[0]
            dpci = np.array(self.pdi.loc[('Mining Transport', 'Cost Index'), self.years[0]:self.years[-1]])
            result = dpci * s
            self.db.at[self.idx[Province,'underground mining cost freight cost']] = result
        prov_timer.stop()

    def undergroundminingcostothercost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.df.loc[self.df.Province==Province]['OMCUnderground'].sum()
            b = self.db.loc[Province,'underground mining cost automation and electricity usage low']
            s = a - 0.0015 * b
            omci = np.array(self.pdi.loc[('Other Mining', 'Cost Index'), self.years[0]:self.years[-1]])
            result = omci * s
            self.db.at[self.idx[Province,'underground mining cost other cost']] = result
        prov_timer.stop()

    def undergroundminingcosttotalminingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground mining cost other cost']
            b = self.db.loc[Province,'underground mining cost freight cost']
            c = self.db.loc[Province,'underground mining cost labour cost']
            d = self.db.loc[Province,'underground mining cost electricity cost']
            e = self.db.loc[Province,'underground mining cost diesel cost']    
            result = a + b + c + d + e
            self.db.at[self.idx[Province,'underground mining cost total mining cost']] = result
        prov_timer.stop()

    def undergroundminingcostundergroundminingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground mining cost total mining cost']
            b = self.db.loc[Province, 'mining characteristics stripping ratio underground']
            result = a * (1 + b)
            self.db.at[self.idx[Province,'underground mining cost underground mining cost']] = result
        prov_timer.stop()

    def undergrounddressingcostautomationandelectricityusagelow(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics production']
            result = 18 * pow(a, -0.07)
            self.db.at[self.idx[Province,'underground dressing cost automation and electricity usage low']] = result
        prov_timer.stop()

    def undergrounddressingcostautomationandelectricityusagemedium(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics production']
            result = 19*pow(a,-0.07)
            self.db.at[self.idx[Province,'underground dressing cost automation and electricity usage medium']] = result
        prov_timer.stop()

    def undergrounddressingcostautomationandelectricityusagehigh(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics production']
            result = 20*pow(a,-0.07)
            self.db.at[self.idx[Province,'underground dressing cost automation and electricity usage high']] = result
        prov_timer.stop()

    def undergrounddressingcostelectricityconsumption(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics dressing automation level']
            b = self.db.loc[Province,'mining characteristics production']
            c = [self.db.loc[Province,'underground dressing cost automation and electricity usage low'],
                self.db.loc[Province,'underground dressing cost automation and electricity usage medium'],
                self.db.loc[Province,'underground dressing cost automation and electricity usage high']] 
            result = [c[int(a[i]) - 1][i] if isinstance(b[i], (int, float)) else 0 for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'underground dressing cost electricity consumption']] = result
        prov_timer.stop()

    def undergrounddressingcostelectricityprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Power'].sum()]
            s = d[0]
            pci = np.array(self.pdi.loc[('Power', 'Cost Index'), self.years[0]:self.years[-1]])
            result = pci * s
            self.db.at[self.idx[Province,'underground dressing cost electricity price']] = result
        prov_timer.stop()

    def undergrounddressingcostelectricitycost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground dressing cost electricity price']
            b = self.db.loc[Province,'underground dressing cost electricity consumption']
            result = a * b
            self.db.at[self.idx[Province,'underground dressing cost electricity cost']] = result
        prov_timer.stop()

    def undergrounddressingcostautomationandlabourproductivitylow(self):
        prov_timer.start()
        for Province in Provinces:
            # hardcoded
            s = 0.08
            result = np.full(self.max_col, s)
            self.db.at[self.idx[Province,'underground dressing cost automation and labour productivity low']] = result
        prov_timer.stop()

    def undergrounddressingcostautomationandlabourproductivitymedium(self):
        prov_timer.start()
        for Province in Provinces:
            # hardcoded
            s = 0.07
            result = np.full(self.max_col, s)
            self.db.at[self.idx[Province,'underground dressing cost automation and labour productivity medium']] = result
        prov_timer.stop()

    def undergrounddressingcostautomationandlabourproductivityhigh(self):
        prov_timer.start()
        for Province in Provinces:
            # hardcoded
            s = 0.06
            result = np.full(self.max_col, s)
            self.db.at[self.idx[Province,'underground dressing cost automation and labour productivity high']] = result
        prov_timer.stop()

    def undergrounddressingcostlabourproductivity(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'mining characteristics dressing automation level']
            b = [self.db.loc[Province,'underground dressing cost automation and labour productivity low'],
                self.db.loc[Province,'underground dressing cost automation and labour productivity medium'],
                self.db.loc[Province,'underground dressing cost automation and labour productivity high']]
            result = [b[int(a[i]) - 1][i] for i in np.arange(len(a))]
            self.db.at[self.idx[Province,'underground dressing cost labour productivity']] = result
        prov_timer.stop()

    def undergrounddressingcostlabourrate(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['LaborMining'].sum()]
            s = d[0]*12/2000
            ldci = np.array(self.pdi.loc[('Labour Dressing', 'Cost Index'), self.years[0]:self.years[-1]])
            result = ldci * s
            
            self.db.at[self.idx[Province,'underground dressing cost labour rate']] = result
        prov_timer.stop()

    def undergrounddressingcostlabourcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground dressing cost labour productivity']
            b = self.db.loc[Province,'underground dressing cost labour rate']
            result = a * b
            self.db.at[self.idx[Province,'underground dressing cost labour cost']] = result
        prov_timer.stop()

    def undergrounddressingcostauxillarymaterialcost(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['DMCUnderground'].sum()]
            s = d[0]
            dmci = np.array(self.pdi.loc[('Dressing Material', 'Cost Index'), self.years[0]:self.years[-1]])
            result = dmci * s
            self.db.at[self.idx[Province,'underground dressing cost auxillary material cost']] = result 
        prov_timer.stop()

    def undergrounddressingcosttotaldressingcost(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground dressing cost auxillary material cost']
            b = self.db.loc[Province,'underground dressing cost labour cost']
            c = self.db.loc[Province,'underground dressing cost electricity cost']
            result = a + b + c
            self.db.at[self.idx[Province,'underground dressing cost total dressing cost']] = result
        prov_timer.stop()
            
    def undergroundstate(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['GCProvinceState'].sum()]
            s = d[0]
            gcci = np.array(self.pdi.loc[('Government Charges', 'Cost Index'), self.years[0]:self.years[-1]])
            result = gcci * s
            self.db.at[self.idx[Province,'underground state']] = result
        prov_timer.stop()

    def undergroundlocal(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['GCCountyLocal'].sum()]
            s = d[0]
            # print(s)
            gcci = np.array(self.pdi.loc[('Government Charges', 'Cost Index'), self.years[0]:self.years[-1]])
            result = gcci * s
            self.db.at[self.idx[Province,'underground local']] = result
        prov_timer.stop()

    def undergroundtotal(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground state']
            b = self.db.loc[Province,'underground local']
            result = a + b
            self.db.at[self.idx[Province,'underground total']] = result
        prov_timer.stop()

    def undergroundroad(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['FRRoad'].sum()]
            s = d[0]
            frci = np.array(self.pdi.loc[('Freight Rates', 'Cost Index'), self.years[0]:self.years[-1]])
            result = frci * s
            self.db.at[self.idx[Province,'underground road']] = result
        prov_timer.stop()

    def undergroundrail(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['FRRail'].sum()]
            s = d[0]
            frci = np.array(self.pdi.loc[('Freight Rates', 'Cost Index'), self.years[0]:self.years[-1]])
            result = frci * s
            self.db.at[self.idx[Province,'underground rail']] = result
        prov_timer.stop()

    def undergroundpurchaceprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['PriceDomesticBxFAW'].sum()]
            s = d[0]
            pdci = np.array(self.pdi.loc[('Price Domestic', 'Cost Index'), self.years[0]:self.years[-1]])
            result = pdci * s
            self.db.at[self.idx[Province,'underground purchace price']] = result
        prov_timer.stop()

    def undergroundwage(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['LaborGeneral'].sum()]
            s = d[0]*12/2000
            lgci = np.array(self.pdi.loc[('Labour General', 'Cost Index'), self.years[0]:self.years[-1]])
            result = lgci * s
            self.db.at[self.idx[Province,'underground wage']] = result
        prov_timer.stop()

    def undergroundcausticsoda(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['CausticSoda'].sum()]
            s = d[0]
            csci = np.array(self.pdi.loc[('Caustic Soda', 'Cost Index'), self.years[0]:self.years[-1]])
            result = csci * s
            self.db.at[self.idx[Province,'underground caustic soda']] = result
        prov_timer.stop()

    def undergroundsodiumcarbonate(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['SodiumCarbonate'].sum()]
            s = d[0]
            sdci = np.array(self.pdi.loc[('Sodium Carbonate', 'Cost Index'), self.years[0]:self.years[-1]])
            result = sdci * s
            self.db.at[self.idx[Province,'underground sodium carbonate']] = sdci
        prov_timer.stop()

    def undergroundlime(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Lime'].sum()]
            s = d[0]
            lci = np.array(self.pdi.loc[('Lime', 'Cost Index'), self.years[0]:self.years[-1]])
            result = lci * s
            self.db.at[self.idx[Province,'underground lime']] = result
        prov_timer.stop()

    def undergroundflocculant(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['Flocculant'].sum()]
            s = d[0]
            fci = np.array(self.pdi.loc[('Flocculant', 'Cost Index'), self.years[0]:self.years[-1]])
            result = fci * s
            self.db.at[self.idx[Province,'underground flocculant']] = result
        prov_timer.stop()

    def undergroundlignitouscoalbaseprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['LCBasepricefaw'].sum()]
            s = d[0]
            lcci = np.array(self.pdi.loc[('Lignitous Coal', 'Cost Index'), self.years[0]:self.years[-1]])
            result = lcci * s
            self.db.at[self.idx[Province,'underground lignitous coal base price']] = result
        prov_timer.stop()

    def undergroundlignitouscoaldistancefromsource(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['LCdistancetorefinery'].sum()]
            s = d[0]
            result = np.full(self.max_col, s)
            self.db.at[self.idx[Province,'underground lignitous coal distance from source']] = result
        prov_timer.stop()

    def undergroundlignitouscoalfreightrate(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground lignitous coal distance from source']
            result = 0.9208*pow(a,-0.173)*a/1.17
            self.db.at[self.idx[Province,'underground lignitous coal freight rate']] = result
        prov_timer.stop()

    def undergroundlignitouscoaldeliveredprice(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground lignitous coal freight rate']
            b = self.db.loc[Province,'underground lignitous coal base price']
            result = a + b
            self.db.at[self.idx[Province,'underground lignitous coal delivered price']] = a * b
        prov_timer.stop()

    def undergroundanthracitecoalbaseprice(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['ACBasepricefaw'].sum()]
            s = d[0]
            acci = np.array(self.pdi.loc[('Anthracite Coal', 'Cost Index'), self.years[0]:self.years[-1]])
            result = acci * s
            self.db.at[self.idx[Province,'underground anthracite coal base price']] = result
        prov_timer.stop()

    def undergroundanthracitecoaldifference(self):
        prov_timer.start()
        for Province in Provinces:
            s = 0.2
            self.db.at[self.idx[Province,'underground anthracite coal difference']] = s
        prov_timer.stop()

    def undergroundanthracitecoaldistancefromsource(self):
        prov_timer.start()
        for Province in Provinces:
            d = [ self.df.loc[self.df.Province==Province]['ACdistancetorefinery'].sum()]
            s = d[0]
            self.db.at[self.idx[Province,'underground anthracite coal distance from source']] = s
        prov_timer.stop()

    def undergroundanthracitecoalfreightrate(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground anthracite coal distance from source']
            result = 0.9208*pow(a,-0.173)*a/1.17
            self.db.at[self.idx[Province,'underground anthracite coal freight rate']] = result
        prov_timer.stop()

    def undergroundanthracitecoaldeliveredprice(self):
        prov_timer.start()
        for Province in Provinces:
            a = self.db.loc[Province,'underground anthracite coal freight rate']
            b = self.db.loc[Province,'underground anthracite coal base price']
            result = a + b
            self.db.at[self.idx[Province,'underground anthracite coal delivered price']] = result
        prov_timer.stop()
            
    def undergroundlignitouscoalsource(self):
        prov_timer.start()
        for Province in Provinces:
            s = self.df.loc[self.df.Province==Province]['lsource'].to_list()[0]
            self.db.loc[Province, 'underground lignitous coal source'] = s
        prov_timer.stop()

    def undergroundanthracitecoalsource(self):
        prov_timer.start()
        for Province in Provinces:
            s = self.df.loc[self.df.Province==Province]['asource'].to_list()
            self.db.loc[Province, 'underground anthracite coal source'] = s
        prov_timer.stop()


    def pdi_calc_all(self):
        provincial.pdi_helper_gen(self, pwi_rows)

    def save(self):
        prov_timer.start()
        self.db.to_csv("ddm/newprovoutput.csv", index=False)
        prov_timer.stop(end=True)
    
    def provincialcalcall(self):
        provincial.miningautolevel(self)
        provincial.miningdepthopenpit(self)
        provincial.miningdepthunderground(self)
        provincial.miningdressautolevel(self)
        provincial.miningproduction(self)
        provincial.miningopen(self)
        provincial.miningunderground(self)
        provincial.miningstrippingratioopenpit(self)
        provincial.miningdepthunderground(self)
        provincial.miningcharmdunderground(self)
        provincial.undergroundminingcostwasteratio(self)
        provincial.miningelectricitycons(self)
        provincial.miningelecprice(self)
        provincial.miningeleccost(self)
        provincial.miningdeiselprice(self)
        provincial.miningdeiselusage(self)
        provincial.miningdeiselcost(self)
        provincial.mininglabprod1(self)
        provincial.mininglabrate1(self)
        provincial.mininglabcost1(self)
        provincial.miningfreightcost(self)
        provincial.miningothercost(self)
        provincial.miningoreminingcost(self)
        provincial.miningstrippingratio(self)
        provincial.miningwasteproduction(self)
        provincial.miningelctricityconsumption(self)
        provincial.miningelectricityprice(self)
        provincial.miningelectricitycost(self)
        provincial.miningcostdieselusage(self)
        provincial.miningcostdieselprice(self)
        provincial.miningcostdieselcost(self)
        provincial.miningcostlabourproductivity(self)
        provincial.miningcostlabourrate(self)
        provincial.miningcostlabourcost(self)
        provincial.miningcostother(self)
        provincial.miningcoststrippingcost(self)
        provincial.miningcosttotalstrippingcost(self)
        provincial.miningcostopenpitminingcost(self)
        provincial.dressingcostautomationandelectricityusagelow(self)
        provincial.dressingcostautomationandelectricityusagemedium(self)
        provincial.dressingcostautomationandelectricityusagehigh(self)
        provincial.dressingcostelectricityconsumption(self)
        provincial.dressingcostelectricityprice(self)
        provincial.dressingcostelectricitycost(self)
        provincial.dressingcostautomationandlabourproductivitylow(self)
        provincial.dressingcostautomationandlabourproductivitymedium(self)
        provincial.dressingcostautomationandlabourproductivityhigh(self)
        provincial.dressingcostlabourproductivity(self)
        provincial.dressingcostlabourrate(self)
        provincial.dressingcostlabourcost(self)
        provincial.dressingcostauxillarymaterialcost(self)
        provincial.dressingcosttotaldressingcost(self)
        provincial.undergroundminingcostautomationandelectricityusagelow(self)
        provincial.undergroundminingcostautomationandelectricityusagemedium(self)
        provincial.undergroundminingcostautomationandelectricityusagehigh(self)
        provincial.undergroundminingcostsizefactor(self)
        provincial.undergroundminingcostdepthfactor(self)
        provincial.undergroundminingcostelectricityconsumption(self)
        provincial.undergroundminingcostelectricityprice(self)
        provincial.undergroundminingcostelectricitycost(self)
        provincial.undergroundminingcostautomationandproductivitylow(self)
        provincial.undergroundminingcostautomationandproductivitymedium(self)
        provincial.undergroundminingcostautomationandproductivityhigh(self)
        provincial.undergroundminingcostdepthfactor2(self)
        provincial.undergroundminingcostautomationfactor(self)
        provincial.undergroundminingcostlabourproductivity(self)
        provincial.undergroundminingcostlabourprice(self)
        provincial.undergroundminingcostlabourcost(self)
        provincial.undergroundminingcostdieselusage(self)
        provincial.undergroundminingcostdieselprice(self)
        provincial.undergroundminingcostdieselcost(self)
        provincial.undergroundminingcostfreightcost(self)
        provincial.undergroundminingcostothercost(self)
        provincial.undergroundminingcosttotalminingcost(self)
        provincial.undergroundminingcostundergroundminingcost(self)
        provincial.undergrounddressingcostautomationandelectricityusagelow(self)
        provincial.undergrounddressingcostautomationandelectricityusagemedium(self)
        provincial.undergrounddressingcostautomationandelectricityusagehigh(self)
        provincial.undergrounddressingcostelectricityconsumption(self)
        provincial.undergrounddressingcostelectricityprice(self)
        provincial.undergrounddressingcostelectricitycost(self)
        provincial.undergroundanthracitecoaldistancefromsource(self)
        provincial.undergrounddressingcostautomationandlabourproductivitylow(self)
        provincial.undergrounddressingcostautomationandlabourproductivitymedium(self)
        provincial.undergrounddressingcostautomationandlabourproductivityhigh(self)
        provincial.undergrounddressingcostlabourproductivity(self)
        provincial.undergrounddressingcostlabourrate(self)
        provincial.undergrounddressingcostlabourcost(self)
        provincial.undergrounddressingcostauxillarymaterialcost(self)
        provincial.undergrounddressingcosttotaldressingcost(self)
        provincial.undergroundstate(self)
        provincial.undergroundlocal(self)
        provincial.undergroundtotal(self)
        provincial.undergroundroad(self)
        provincial.undergroundrail(self)
        provincial.undergroundpurchaceprice(self)
        provincial.undergroundwage(self)
        provincial.undergroundcausticsoda(self)
        provincial.undergroundsodiumcarbonate(self)
        provincial.undergroundlime(self)
        provincial.undergroundflocculant(self)
        provincial.undergroundlignitouscoalbaseprice(self)
        provincial.undergroundlignitouscoaldistancefromsource(self)
        provincial.undergroundlignitouscoalfreightrate(self)
        provincial.undergroundlignitouscoaldeliveredprice(self)
        provincial.undergroundanthracitecoalbaseprice(self)
        provincial.undergroundanthracitecoaldifference(self)
        provincial.undergroundanthracitecoalfreightrate(self)
        provincial.undergroundanthracitecoaldeliveredprice(self)
        provincial.undergroundlignitouscoalsource(self)
        provincial.undergroundanthracitecoalsource(self)
# t = time.perf_counter()
# d = provincial()
# d.pdi_calc_all()
# for a in Province:
#     d.provincialcalcall(a)
# # d.save()
# print(time.perf_counter() - t)