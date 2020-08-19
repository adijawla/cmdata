import pandas as pd
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb

db_conv = Flatdbconverter("DDM Collector for Alcoa")

class DDMcollector():
    def __init__(self):
        self.allocationByCountry = pd.read_csv('DDM Collector Inputs\Allocations by Country ex DDM.csv')
        self.finalProductions1 = pd.read_csv('DDM Collector Inputs\Final productions - from Domestic Bx CBIX$55 T1.csv')
        self.finalProductions2 = pd.read_csv('DDM Collector Inputs\Final productions - from Domestic Bx CBIX$55 T2.csv')
        self.finalProductions3 = pd.read_csv('DDM Collector Inputs\Final productions - from Domestic Bx CBIX$55 T3.csv')
        self.originalBxAllocationByRefinery = pd.read_csv('DDM Collector Inputs\Original bx alloctions by refinery by county.csv')
        self.TTBauxiteConsumed = pd.read_csv('DDM Collector Inputs\Tt of bauxite consumed by each refinery split by major province.csv')
        self.engine1 = pd.read_csv('DDM Collector Inputs\engine1.csv')
        self.depletedProd = pd.read_csv('DDM Collector Inputs\AA productions.csv')
        
        self.resummingOriginal = pd.DataFrame(columns = self.originalBxAllocationByRefinery.columns)
        self.nomalisingAllocation = pd.DataFrame(columns = self.engine1.columns)
        self.bauxiteConsumed = pd.DataFrame(columns =  self.engine1.columns)
        self.alumProp = pd.DataFrame(columns =  self.engine1.columns)
        self.AAprop = pd.DataFrame(columns =  self.engine1.columns)
        self.domesticBX = pd.DataFrame(columns =  self.engine1.columns)
        self.domesticBX2 = pd.DataFrame(columns =  self.engine1.columns)
        self.domesticBX3 = pd.DataFrame(columns =  self.engine1.columns)
        self.extraAAprod = pd.DataFrame(columns =  self.engine1.columns)
        self.mappedBX = pd.DataFrame(columns =  self.engine1.columns)
        self.totalDomestic =  pd.DataFrame(columns =  self.engine1.columns)
        self.finalTab =  pd.DataFrame(columns =  self.engine1.columns)
        self.finalTabDemand =  pd.DataFrame(columns =  self.engine1.columns)
        self.finalThirdParty = pd.DataFrame(columns =  self.engine1.columns)
        self.importBauxite = pd.DataFrame(columns =  self.engine1.columns)
        self.TfactorsAllocation = pd.DataFrame(columns =self.engine1.columns)
        self.TfactorsThirdParty = pd.DataFrame(columns =self.engine1.columns)
        self.TfactorsImpBauxite = pd.DataFrame(columns =self.engine1.columns)
        self.supplyTab = pd.DataFrame(columns =self.engine1.columns)
        self.genThirdPartySupply = pd.DataFrame(columns =self.engine1.columns)
        
        self.cols = ['Ref. No.','Refinery','Refinery Province','Refinery Province Category', 'Technology','Refinery Ownership','Owner',	'Bauxite Source', 'Bauxite Self Supply', 'Bauxite Origin Province', 'Bauxite Origin Province Category', 'Bauxite Origin County']
        self.years = list(map(str,range(2018, 2032)))
        self.demandSetUp = pd.DataFrame(columns=list(map(str,range(1,46))))
        self.supplySetUp = pd.DataFrame(columns=list(map(str,range(1,23))))
        
        
        self.impt_from_price_forcast = [2.50, 2.50, 2.50, 2.50, 2.50, 2.50, 2.50, 2.50, 2.64, 2.66,	2.67, 2.68, 2.70, 2.66, 2.67, 2.68, 2.69, 2.69,	2.70, 2.70, 2.66, 2.65 ]
    def cal1_originalBx(self, row):
        d = ['Henan', 'Shanxi', 'Guangxi', 'Guizhou']
        self.originalBxAllocationByRefinery.loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row,'6'] if self.originalBxAllocationByRefinery.loc[row,'6'] in d else 'Other'
    
    #Re-summing original allocation tonnages by province by refinery - starting point for new modeling
    def cal2_resumOriginal(self, row):
        self.resummingOriginal.loc[row,'2'] = self.engine1.loc[row, '3']
        self.resummingOriginal.loc[row,'3'] = self.engine1.loc[row, '4']
        self.resummingOriginal.loc[row,'4'] = self.engine1.loc[row, '5']
        self.resummingOriginal.loc[row,'5'] = self.engine1.loc[row, '6']
        self.resummingOriginal.loc[row,'6'] = self.engine1.loc[row, '7']
        self.resummingOriginal.loc[row,'7'] = self.engine1.loc[row, '8']
        self.resummingOriginal.loc[row,'8'] = self.engine1.loc[row, '9']
        self.resummingOriginal.loc[row,'9'] = self.engine1.loc[row, '10']
        self.resummingOriginal.loc[row,'10'] = self.engine1.loc[row, '11']
        
    def cal3_resumOriginal_2(self, row):
        d = [_ for _ in range(6,515,5)]
        if row in d:
            value1 = self.originalBxAllocationByRefinery.loc[(self.originalBxAllocationByRefinery['3']==self.resummingOriginal.loc[row,'3']), '16'].astype(float).sum()
            value2 = self.resummingOriginal.loc[row-4:row-1,'16'].astype(float).sum()
            self.resummingOriginal.loc[row,'16'] = value1 - value2
        else:
            value = self.originalBxAllocationByRefinery.loc[(self.originalBxAllocationByRefinery['3']==self.resummingOriginal.loc[row,'3'])&(self.originalBxAllocationByRefinery['6']==self.resummingOriginal.loc[row,'4']) , '16'].astype(float)
            self.resummingOriginal.loc[row,'16'] = value.sum()
            
    # Normalising original allocation tonnages by province by refinery - to DDM values - end year closing stocks   
    def cal4_normalising(self, row):
        self.nomalisingAllocation['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        try:
            self.nomalisingAllocation.loc[row,'2'] = self.originalBxAllocationByRefinery.loc[row+2,'2']
            self.nomalisingAllocation.loc[row,'3'] = self.originalBxAllocationByRefinery.loc[row+2,'3']
            self.nomalisingAllocation.loc[row,'4'] = self.originalBxAllocationByRefinery.loc[row+2,'4']
            self.nomalisingAllocation.loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row+2,'5']
            self.nomalisingAllocation.loc[row,'6'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.nomalisingAllocation.loc[row,'7'] = self.originalBxAllocationByRefinery.loc[row+2,'7']
            self.nomalisingAllocation.loc[row,'10'] = self.originalBxAllocationByRefinery.loc[row+2,'10']
        except:
            pass
      
        b = {97:49.48006235,
             98:385.7787116,
             99:14.75089588,
             100:168.9084207,
             101:1890.014541,
             102:64.89129801,
             227:965.6984744,
             228:446.8470457}
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 97,'14' ] = b[97]
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 98,'14' ] = b[98]
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 99,'14' ] = b[99]
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 100,'14' ] = b[100]
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 101,'14' ] = b[101]
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 102,'14' ] = b[102]
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 227,'14' ] = b[227]
        self.nomalisingAllocation.loc[self.nomalisingAllocation['1'] == 228,'14' ] = b[228]
        self.nomalisingAllocation.loc[1:'14'].fillna(0)
    
    def cal4_normalising2(self, row):
        value1 = self.resummingOriginal.loc[(self.resummingOriginal['4']==self.nomalisingAllocation.loc[row,'5'])&(self.resummingOriginal['2']==self.nomalisingAllocation.loc[row,'2']) , '16'].astype(float).sum()
        value2 = self.engine1.loc[(self.engine1['5']==self.nomalisingAllocation.loc[row,'5'])&(self.engine1['3']==self.nomalisingAllocation.loc[row,'2']) , '17'].astype(float).sum()
        value = 0 if value1 <= 0 else (float(self.originalBxAllocationByRefinery.loc[row+2,'16']) * value2 / value1 )
        self.nomalisingAllocation.loc[row, '16'] = value if pd.isna(self.nomalisingAllocation.loc[row, '14']) else (value + self.nomalisingAllocation.loc[row, '14'])
        for i in [str(_) for _ in range(17, len(self.bauxiteConsumed.columns))]:
            self.nomalisingAllocation.loc[row, i] = 0
            newC = str(int(i)-1)
            try:
                self.nomalisingAllocation.loc[row, i] = self.nomalisingAllocation.loc[row, newC]- self.totalDomestic.loc[row, i]
                print('solving all cross referencing values')
            except:
                print('code needs to run again')
        #the logic is i'll have to run this from cal4_normalising again to provide the corrections
        
    #t/t of bauxite consumed by each refinery by county
    def cal5_bauxiteConsumed(self, row):
        self.bauxiteConsumed['1'] =  [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        try:
            self.bauxiteConsumed.loc[row,'2'] = self.originalBxAllocationByRefinery.loc[row+2,'2']
            self.bauxiteConsumed.loc[row,'3'] = self.originalBxAllocationByRefinery.loc[row+2,'3']
            self.bauxiteConsumed.loc[row,'4'] = self.originalBxAllocationByRefinery.loc[row+2,'4']
            self.bauxiteConsumed.loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row+2,'5']
            self.bauxiteConsumed.loc[row,'6'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.bauxiteConsumed.loc[row,'7'] = self.originalBxAllocationByRefinery.loc[row+2,'7']
            self.bauxiteConsumed.loc[row,'10'] = self.originalBxAllocationByRefinery.loc[row+2,'10']
        except:
            pass
        self.bauxiteConsumed.loc[row, '16'] = self.TTBauxiteConsumed.loc[(self.TTBauxiteConsumed['4']==self.bauxiteConsumed.loc[row,'5'])&(self.TTBauxiteConsumed['2']==self.bauxiteConsumed.loc[row,'2']) , '16'].astype(float).sum()
        d = [str(_) for _ in range(16, len(self.bauxiteConsumed.columns))]
        for i in d:
            self.bauxiteConsumed.loc[row, i] = self.TTBauxiteConsumed.loc[(self.TTBauxiteConsumed['4']==self.bauxiteConsumed.loc[row,'5'])&(self.TTBauxiteConsumed['2']==self.bauxiteConsumed.loc[row,'2']) , i].astype(float).sum()
    
    
    #Proportion of alumina make by source for each refinery
    def cal6_aluminaProportion(self, row):
        self.alumProp['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        try:
            self.alumProp.loc[row,'2'] = self.originalBxAllocationByRefinery.loc[row+2,'2']
            self.alumProp.loc[row,'3'] = self.originalBxAllocationByRefinery.loc[row+2,'3']
            self.alumProp.loc[row,'4'] = self.originalBxAllocationByRefinery.loc[row+2,'4']
            self.alumProp.loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row+2,'5']
            self.alumProp.loc[row,'6'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.alumProp.loc[row,'7'] = self.originalBxAllocationByRefinery.loc[row+2,'7']
            self.alumProp.loc[row,'10'] = self.originalBxAllocationByRefinery.loc[row+2,'10']
        except:
            pass
        for i in [str(_) for _ in range(17, len(self.bauxiteConsumed.columns))]:
            self.alumProp.loc[row,i] = (self.nomalisingAllocation.loc[row,'16']/self.bauxiteConsumed.loc[row, i]) if self.bauxiteConsumed.loc[row, i] > 0 else 0
            
    #Proportion of AA make from each source per refinery
    def cal7_AAproportion(self, row):
        self.AAprop['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        try:
            self.AAprop.loc[row,'2'] = self.originalBxAllocationByRefinery.loc[row+2,'2']
            self.AAprop.loc[row,'3'] = self.originalBxAllocationByRefinery.loc[row+2,'3']
            self.AAprop.loc[row,'4'] = self.originalBxAllocationByRefinery.loc[row+2,'4']
            self.AAprop.loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row+2,'5']
            self.AAprop.loc[row,'6'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.AAprop.loc[row,'7'] = self.originalBxAllocationByRefinery.loc[row+2,'7']
            self.AAprop.loc[row,'10'] = self.originalBxAllocationByRefinery.loc[row+2,'10']
        except:
            pass
        for i in [str(_) for _ in range(17, len(self.bauxiteConsumed.columns))]:
            val = self.alumProp.loc[(self.alumProp['2']==self.AAprop.loc[row,'2']) , i].astype(float).sum()
            self.AAprop.loc[row,i] = (self.alumProp.loc[row, i]/val) if val > 0  else 0
        
        
    
    #Domestic Bx Consumption by Refineries by source ex scheduled production - non depleted
    def cal8_domesticBx(self, row):
        self.domesticBX ['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        try:
            self.domesticBX .loc[row,'2'] = self.originalBxAllocationByRefinery.loc[row+2,'2']
            self.domesticBX .loc[row,'3'] = self.originalBxAllocationByRefinery.loc[row+2,'3']
            self.domesticBX .loc[row,'4'] = self.originalBxAllocationByRefinery.loc[row+2,'4']
            self.domesticBX .loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row+2,'5']
            self.domesticBX .loc[row,'6'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.domesticBX .loc[row,'7'] = self.originalBxAllocationByRefinery.loc[row+2,'7']
            self.domesticBX .loc[row,'10'] = self.originalBxAllocationByRefinery.loc[row+2,'10']
        except:
            pass
        for i in [str(_) for _ in range(17, len(self.bauxiteConsumed.columns))]:
            value = self.finalProductions2.loc[(self.finalProductions2['2']==self.domesticBX.loc[row,'2']) , i].astype(float).sum()
            self.domesticBX.loc[row,i] = (value * self.AAprop.loc[row,i] * self.bauxiteConsumed.loc[row,i] *1000 ) if self.bauxiteConsumed.loc[row,i] > 0 else 0
        
        
    #Domestic Bx Consumption by Refineries by source ex scheduled production - depleted amounts
    def cal8_domesticBx2(self, row):
        self.domesticBX2['1'] = [''] + [_ for _ in range(1,len(self.finalProductions2)+4)]
        try:
            self.domesticBX2.loc[row,'2'] = self.finalProductions3.loc[row+2,'2']
            self.domesticBX2.loc[row,'3'] = self.finalProductions3.loc[row+2,'3']
            self.domesticBX2.loc[row,'4'] = self.finalProductions3.loc[row+2,'4']
            self.domesticBX2.loc[row,'5'] = self.finalProductions3.loc[row+2,'5']
            self.domesticBX2.loc[row,'6'] = self.finalProductions3.loc[row+2,'6']
            self.domesticBX2.loc[row,'7'] = self.finalProductions3.loc[row+2,'7']
            self.domesticBX2.loc[row,'8'] = self.finalProductions3.loc[row+2,'8']
            self.domesticBX2.loc[row,'9'] = self.finalProductions3.loc[row+2,'9']
        except:
            pass
        for i in [str(_) for _ in range(17, len(self.domesticBX2.columns))]:
                val1 = self.finalProductions2.loc[(self.finalProductions2['2']==self.domesticBX2.loc[row,'2']) , i].astype(float).sum()
                val2 = self.TTBauxiteConsumed.loc[(self.TTBauxiteConsumed['2']==self.domesticBX2.loc[row,'2']) , i].astype(float).sum()
                d = [1 if (val1> 0 and val2<=0) else 0, 1 if (self.domesticBX2.loc[row,'9'] =='Domestic') else 0 ]
                self.domesticBX2.loc[row,i] = d[0] * d[1]
        
    #Non-Depeleted production
    def cal8_domesticBx3(self, row):
        self.domesticBX3['1'] = [''] + [_ for _ in range(1,len(self.finalProductions2)+4)]
        try:
            self.domesticBX3.loc[row,'2'] = self.finalProductions3.loc[row+2,'2']
            self.domesticBX3.loc[row,'3'] = self.finalProductions3.loc[row+2,'3']
            self.domesticBX3.loc[row,'4'] = self.finalProductions3.loc[row+2,'4']
            self.domesticBX3.loc[row,'5'] = self.finalProductions3.loc[row+2,'5']
            self.domesticBX3.loc[row,'6'] = self.finalProductions3.loc[row+2,'6']
            self.domesticBX3.loc[row,'7'] = self.finalProductions3.loc[row+2,'7']
            self.domesticBX3.loc[row,'8'] = self.finalProductions3.loc[row+2,'8']
            self.domesticBX3.loc[row,'9'] = self.finalProductions3.loc[row+2,'9']
            self.domesticBX3.loc[row,['12','13','14','15','16']] =1
        except:
            pass
        for i in [str(_) for _ in range(17, len(self.domesticBX3.columns))]:
            val1 = self.finalProductions2.loc[(self.finalProductions2['2']==self.domesticBX3.loc[row,'2']) , i].astype(float).sum()
            val2 = self.TTBauxiteConsumed.loc[(self.TTBauxiteConsumed['2']==self.domesticBX3.loc[row,'2']) , i].astype(float).sum()
            d = [1 if (val1 > 0 and val2 > 0) else 0, 1 if (self.domesticBX3.loc[row,'9'] =='Domestic') else 0 ]
            self.domesticBX3.loc[row,i] = d[0] * d[1]
    
    def cal9_AAprod(self):
        row1 = [_ for _ in range(2,7)]
        row2 = [_ for _ in range(11,16)]
        row3 = [_ for _ in range(20,25)]
        for i in [str(_) for _ in range(17, len(self.depletedProd.columns)+1)]:
            for row in row1:
                val1 =self.domesticBX2.loc[(self.domesticBX2['5'] ==self.depletedProd.loc[row, '5']) &(self.domesticBX2['9'] ==self.depletedProd.loc[row, '9']), i].astype(float)
                val =[ _ for _  in self.finalProductions2.loc[[ind+2 for ind in val1.index],i].astype(float)] * val1
                self.depletedProd.loc[row, i] = val.sum()
            self.depletedProd.loc[7,i] = self.depletedProd.loc[[2,3,4,5,6], i].sum()
            for row in row2:
                val1 =self.domesticBX3.loc[(self.domesticBX3['5'] ==self.depletedProd.loc[row, '5']) &(self.domesticBX3['9'] ==self.depletedProd.loc[row, '9']), i].astype(float)
                val =[ _ for _  in self.finalProductions2.loc[[ind+2 for ind in val1.index],i].astype(float)] * val1
                self.depletedProd.loc[row, i] = val.sum()
            self.depletedProd.loc[16,i] = self.depletedProd.loc[[11,12,13,14,15], i].sum()
        for i in [str(_) for _ in range(17, len(self.depletedProd.columns)+1)]:
            for row in row3:
                self.depletedProd.loc[row,i] = ((self.depletedProd.loc[row-18, i] + self.depletedProd.loc[row-9, i])/self.depletedProd.loc[row-9, i] ) if self.depletedProd.loc[row-9, i] > 0 else 1

    
    def cal10_extraAA(self, row):
        try:
            self.extraAAprod.loc[row,'2'] = self.finalProductions3.loc[row+2,'2']
            self.extraAAprod.loc[row,'3'] = self.finalProductions3.loc[row+2,'3']
            self.extraAAprod.loc[row,'4'] = self.finalProductions3.loc[row+2,'4']
            self.extraAAprod.loc[row,'5'] = self.finalProductions3.loc[row+2,'5']
            self.extraAAprod.loc[row,'6'] = self.finalProductions3.loc[row+2,'6']
            self.extraAAprod.loc[row,'7'] = self.finalProductions3.loc[row+2,'7']
            self.extraAAprod.loc[row,'8'] = self.finalProductions3.loc[row+2,'8']
            self.extraAAprod.loc[row,'9'] = self.finalProductions3.loc[row+2,'9']
        except:
            pass
        try:
            df = self.depletedProd.loc[20:24,:]
            for i in [str(_) for _ in range(12, len(self.finalProductions3.columns)+1)]:
                value = df.loc[(df['5']== self.extraAAprod.loc[row,'5']) & (df['9']== self.extraAAprod.loc[row,'9']),i].astype(float).sum()
                self.extraAAprod.loc[row, i] = (value-1) * float(self.domesticBX3.loc[row,i]) * float(self.finalProductions2.loc[row+2,i])
        except:
            pass
        
    #Mapped onto BX use - extra Extra AA productions from non-depeleted domestic bx producers
    def cal11_mappedBX(self, row):
        self.mappedBX ['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        self.totalDomestic['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        
        try:
            self.mappedBX.loc[row,'2'] = self.originalBxAllocationByRefinery.loc[row+2,'2']
            self.totalDomestic.loc[row,'2'] = self.originalBxAllocationByRefinery.loc[row+2,'2']
            self.mappedBX.loc[row,'3'] = self.originalBxAllocationByRefinery.loc[row+2,'3']
            self.totalDomestic.loc[row,'3'] = self.originalBxAllocationByRefinery.loc[row+2,'3']
            self.mappedBX.loc[row,'4'] = self.originalBxAllocationByRefinery.loc[row+2,'4']
            self.totalDomestic.loc[row,'4'] = self.originalBxAllocationByRefinery.loc[row+2,'4']
            self.mappedBX.loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row+2,'5']
            self.totalDomestic.loc[row,'5'] = self.originalBxAllocationByRefinery.loc[row+2,'5']
            self.mappedBX.loc[row,'6'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.totalDomestic.loc[row,'6'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.mappedBX.loc[row,'7'] = self.originalBxAllocationByRefinery.loc[row+2,'7']
            self.totalDomestic.loc[row,'7'] = self.originalBxAllocationByRefinery.loc[row+2,'7']
            self.mappedBX.loc[row,'10'] = self.originalBxAllocationByRefinery.loc[row+2,'10']
            self.totalDomestic.loc[row,'10'] = self.originalBxAllocationByRefinery.loc[row+2,'10']
        except:
            pass
        df = self.depletedProd.loc[28:34,:]
        for i in [str(_) for _ in range(17, len(self.finalProductions3.columns)+1)]:
            if self.bauxiteConsumed.loc[row,i] > 0:
                res = self.extraAAprod.loc[self.extraAAprod['2']== self.mappedBX.loc[row,'2'],i ].sum() * self.AAprop.loc[row,i]* 1000
                res = res * df.loc[df['6']==self.mappedBX.loc[row,'5'], i].astype(float).sum()
            else:
                res = 0
            self.mappedBX.loc[row,i] = res
        for i in [str(_) for _ in range(17, len(self.finalProductions3.columns)+1)]:
            self.totalDomestic.loc[row, i] = self.domesticBX.loc[row,i] + self.mappedBX.loc[row,i]
            
    def cal12_finalTab_pratcial(self, row):
        self.finalTab['1'] =  [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        self.finalTab.loc[row,'2'] = self.totalDomestic.loc[row,'2']
        self.finalTab.loc[row,'3'] = self.totalDomestic.loc[row,'3']
        self.finalTab.loc[row,'4'] = self.totalDomestic.loc[row,'4']
        self.finalTab.loc[row,'10'] = self.totalDomestic.loc[row,'10']
        self.finalTab.loc[row,'11'] = self.totalDomestic.loc[row,'7']
        self.finalTab.loc[row,'12'] = self.totalDomestic.loc[row,'5']
        self.finalTab.loc[row,'13'] = self.totalDomestic.loc[row,'7']
        
        key = [_ for _ in self.extraAAprod['2']]
        value1 = [_ for _ in self.extraAAprod['5']]
        value2 = [_ for _ in self.extraAAprod['6']]
        value3 = [_ for _ in self.extraAAprod['7']]
        value4 = [_ for _ in self.extraAAprod['8']]
        value5 = [_ for _ in self.extraAAprod['9']]
        d1 = {key[x]:value1[x] for x in range(len(key))}
        d2 = {key[x]:value2[x] for x in range(len(key))}
        d3 = {key[x]:value3[x] for x in range(len(key))}
        d4 = {key[x]:value4[x] for x in range(len(key))}
        d5 = {key[x]:value5[x] for x in range(len(key))}
        try:
            self.finalTab.loc[row,'5'] = d1[self.finalTab.loc[row,'2']]
            self.finalTab.loc[row,'6'] = d2[self.finalTab.loc[row,'2']]
            self.finalTab.loc[row,'7'] = d3[self.finalTab.loc[row,'2']]
            self.finalTab.loc[row,'8'] = d4[self.finalTab.loc[row,'2']]
            self.finalTab.loc[row,'9'] = d5[self.finalTab.loc[row,'2']]
            self.finalTab.loc[row,'14'] = self.originalBxAllocationByRefinery.loc[row+2,'6']
            self.finalTab.loc[row, '15'] = self.finalTab.loc[row, '13']+'~'+ self.finalTab.loc[row, '14']
        except:
            pass
        for i in [str(_) for _ in range(17, len(self.finalProductions3.columns)+1)]:
            self.finalTab.loc[row,i] = self.nomalisingAllocation.loc[row,i]
        
    
    def cal13_finalDemand(self, row):
        self.finalTabDemand['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        for col in list(map(str,range(2,14))):
            self.finalTabDemand.loc[row,col] = self.finalTab.loc[row,col]
        for col in list(map(str,range(17, len(self.finalProductions3.columns)+1))):
            self.finalTabDemand.loc[row,col] = self.domesticBX.loc[row,col]
            
    def cal14_finalThirdParty(self, row):
        for col in list(map(str,range(2,10))):
            try:
                self.finalThirdParty.loc[row, col] = self.extraAAprod.loc[row, col]
            except:
                print('third pary error> ', row, col)
        self.finalThirdParty.loc[row, '10'] = 0
        self.finalThirdParty.loc[row, '11'] = self.finalThirdParty.loc[row, '5']
        self.finalThirdParty.loc[row, '12'] = 'General 3rd Party Domestic Supply'
        self.finalThirdParty.loc[row, '13'] = '-'
        df = self.depletedProd.loc[28:34,:]
        for col in list(map(str,range(17,len(self.finalProductions3.columns)+1))):
            res = self.domesticBX2.loc[row, col] * float(self.finalProductions2.loc[row+2,col]) * 1000
            self.finalThirdParty.loc[row, col] = res * df.loc[df['6']==self.finalThirdParty.loc[row,'5'], col].astype(float).sum()
    
    #Imported Bauxite
    def cal15_importedBauxite(self, row):
        for col in list(map(str,range(2,9))):
            self.importBauxite.loc[row,col] = self.finalThirdParty.loc[row, col]
        d = ['Imported', 0,	'-','General Imports',	'-']
        self.importBauxite.loc[row, '9'] = d[0]
        self.importBauxite.loc[row, '10'] = d[1]
        self.importBauxite.loc[row, '11'] = d[2]
        self.importBauxite.loc[row, '12'] = d[3]
        self.importBauxite.loc[row, '13'] = d[4]
        for col in list(map(str,range(17, len(self.finalProductions3.columns)+1))):
            self.importBauxite.loc[row,col] = self.impt_from_price_forcast[int(col)-17] *1000 * float(self.finalProductions3.loc[row+2,col])
    
    def cal16_tFactors(self, row):
        self.TfactorsAllocation['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        for col in list(map(str,range(2,14))):
            self.TfactorsAllocation.loc[row, col] = self.finalTabDemand.loc[row, col]
        for col in list(map(str,range(17, len(self.finalProductions3.columns)))):
            self.TfactorsAllocation.loc[row, col] = self.bauxiteConsumed.loc[row, col]
    def cal16_tFactors2(self, row):
        for col in list(map(str,range(2,14))):
            self.TfactorsThirdParty.loc[row, col] = self.finalThirdParty.loc[row, col]
        df = self.depletedProd.loc[28:34,:]
        for col in list(map(str,range(17, len(self.finalProductions3.columns)+1))):
            self.TfactorsThirdParty.loc[row, col] = df.loc[df['6']==self.TfactorsThirdParty.loc[row,'5'], col].astype(float).sum() if self.finalThirdParty.loc[row, col] > 0 else 0
        
            
    def cal17_tFactors(self, row):
        self.TfactorsImpBauxite ['1'] = [''] + [_ for _ in range(1,len(self.originalBxAllocationByRefinery))]
        for col in list(map(str,range(2,8))):
            self.TfactorsImpBauxite[row, col]= self.finalTabDemand.loc[row, col]
        d = ['Imported', 0,	'-','General Imports',	'-']
        self.TfactorsImpBauxite.loc[row, '9'] = d[0]
        self.TfactorsImpBauxite.loc[row, '10'] = d[1]
        self.TfactorsImpBauxite.loc[row, '11'] = d[2]
        self.TfactorsImpBauxite.loc[row, '12'] = d[3]
        self.TfactorsImpBauxite.loc[row, '13'] = d[4]
        for col in list(map(str,range(17,25))):
            self.TfactorsImpBauxite.loc[row,col] = 2.5
        for col in list(map(str,range(24,len(self.finalProductions3.columns)+1))):
            self.TfactorsImpBauxite.loc[row,col] = self.impt_from_price_forcast[int(col)-17]
            

    def cal18_supplyTab(self, row):
        for col in list(map(str,range(2,8))):
            self.supplyTab.loc[row,col] = self.domesticBX.loc[row, col]
        self.supplyTab.loc[row,'8'] = 'China'
        self.supplyTab.loc[row,'10'] = self.domesticBX.loc[row,'10']
        for col in list(map(str,range(17, len(self.finalProductions3.columns)+1))):
            self.supplyTab.loc[row, col] = self.domesticBX.loc[row, col]
        
        
    def cal19_thirdPartySupply(self, row):
        for col in list(map(str,[2,4,5,6,7])):
            self.genThirdPartySupply.loc[row, col] = self.mappedBX.loc[row, col]  
        self.genThirdPartySupply.loc[row, '3'] = 'General 3rd Party Domestic Supply'
        self.genThirdPartySupply.loc[row, '8'] = 'China'
        self.genThirdPartySupply.loc[row, '10'] = 0
        for col in list(map(str,range(17, len(self.finalProductions3.columns)+1))):
            self.genThirdPartySupply.loc[row, col] = self.mappedBX.loc[row, col]

    
    
    def cal_demand_setup(self):
        for col in list(map(str,range(4,16))):
            self.demandSetUp.at[0,col] = self.cols[int(col)-4]
            for ind in range(1, len(self.finalTabDemand)-13):
                self.demandSetUp.at[ind, col] = self.finalTabDemand.loc[ind, str(int(col)-2)]
            self.demandSetUp.at[236, col] = ' '
            for ind in range(1, len(self.finalThirdParty)):
                self.demandSetUp.at[ind+237, col] = self.finalThirdParty.loc[ind, str(int(col)-2)]
                
            self.demandSetUp.at[339, col] = ' '  
            for ind in range(1, len(self.finalThirdParty)):
                self.demandSetUp.at[ind+340, col] = self.importBauxite.loc[ind, str(int(col)-2)]
                
        for col in list(map(str,range(17,31))):
            self.demandSetUp.at[0,col] = self.years[int(col)-17]
            for ind in range(1, len(self.finalTabDemand)-13):
                self.demandSetUp.at[ind, col] = self.finalTabDemand.loc[ind, str(int(col)+8)]
                
            self.demandSetUp.at[236, col] = ' '
            for ind in range(1, len(self.finalThirdParty)):
                self.demandSetUp.at[ind+237, col] = self.finalThirdParty.loc[ind, str(int(col)+8)]
                
            self.demandSetUp.at[339, col] = ' '
            for ind in range(1, len(self.finalThirdParty)):
                self.demandSetUp.at[ind+340, col] = self.importBauxite.loc[ind, str(int(col)+8)]
                
        for col in list(map(str,range(32, 46))):
            self.demandSetUp.at[0,col] = self.years[int(col)-32]
            for ind in range(1, len(self.finalTabDemand)-13):
                self.demandSetUp.at[ind, col] = self.TfactorsAllocation.loc[ind, str(int(col)-7)]
                
            self.demandSetUp.at[236, col] = ' '   
            for ind in range(1, len(self.finalThirdParty)):
                self.demandSetUp.at[ind+237, col] = self.TfactorsThirdParty.loc[ind, str(int(col)-7)]
                
            self.demandSetUp.at[339, col] = ' '
            for ind in range(1, len(self.finalThirdParty)):
                self.demandSetUp.at[ind+340, col] = self.impt_from_price_forcast[int(col)-24] if self.importBauxite.loc[ind, str(int(col)-7)] > 0 else 0
                
            
        
        
    def cal_supply_setup(self):
        cols = ['Bx Origin Cty', 'Bx origin Prov', 'Country', 'Destination', 'Refiney Prov', 'Bx Prov category', 'Self Supply']
        for col in list(map(str,range(3,10))):
            self.supplySetUp.at[0, col] = cols[int(col)-3]
            for ind in range(1, len(self.finalTabDemand)-13):
                self.supplySetUp.at[ind, '3']   = self.supplyTab.loc[ind, '7']
                self.supplySetUp.at[ind, '4']   = self.supplyTab.loc[ind, '6']
                self.supplySetUp.at[ind, '5']   = self.supplyTab.loc[ind, '8']
                self.supplySetUp.at[ind, '6']   = self.supplyTab.loc[ind, '3']
                self.supplySetUp.at[ind, '7']   = self.supplyTab.loc[ind, '4']
                self.supplySetUp.at[ind, '8']   = self.supplyTab.loc[ind, '5']
                self.supplySetUp.at[ind, '9']   = self.supplyTab.loc[ind, '10']
                
            self.supplySetUp.at[236, col] = ' '
            
            for ind in range(1, len(self.genThirdPartySupply)):
                self.supplySetUp.at[ind+ 237, '3'] = self.genThirdPartySupply.loc[ind, '7']
                self.supplySetUp.at[ind+ 237, '4'] = self.genThirdPartySupply.loc[ind, '6']
                self.supplySetUp.at[ind+ 237, '5'] = self.genThirdPartySupply.loc[ind, '8']
                self.supplySetUp.at[ind+ 237, '6'] = self.genThirdPartySupply.loc[ind, '3']
                self.supplySetUp.at[ind+ 237, '7'] = self.genThirdPartySupply.loc[ind, '4']
                self.supplySetUp.at[ind+ 237, '8'] = self.genThirdPartySupply.loc[ind, '5']
                self.supplySetUp.at[ind+ 237, '9'] = self.genThirdPartySupply.loc[ind, '10']
            
            
        for col in list(map(str,range(10, len(self.supplySetUp.columns) +1))):
            self.supplySetUp.at[0, col] = self.years[int(col)-10]
            for ind in range(1, len(self.finalTabDemand)-13):
                self.supplySetUp.at[ind, col] = self.supplyTab.loc[ind,str(int(col)+15) ]
            
            self.supplySetUp.at[236, col] = ' '
    
            for ind in range(1, len(self.genThirdPartySupply)):
                self.supplySetUp.at[ind+237, col] =  self.genThirdPartySupply.loc[ind,str(int(col)+15) ]
            
    
    
     
    
    
    
    
    def cal_1(self):
        for i in range(3, len(self.originalBxAllocationByRefinery)):
            DDMcollector.cal1_originalBx(self, i)
    def cal_2(self):
        for i in range(2, len(self.engine1)):
            DDMcollector.cal2_resumOriginal(self, i)
            DDMcollector.cal3_resumOriginal_2(self, i)
    def cal_3(self):
        for i in range(1, len(self.originalBxAllocationByRefinery)):
            DDMcollector.cal4_normalising(self, i)
            DDMcollector.cal4_normalising2(self, i)
            DDMcollector.cal5_bauxiteConsumed(self, i)
            DDMcollector.cal6_aluminaProportion(self, i)
    def cal_4(self):
        for i in range(1, len(self.originalBxAllocationByRefinery)):
            DDMcollector.cal7_AAproportion(self, i)
            DDMcollector.cal8_domesticBx(self, i)
    def cal_5(self):
        for i in range(1, len(self.finalProductions2)):
            DDMcollector.cal8_domesticBx2(self, i)
            DDMcollector.cal8_domesticBx3(self, i)
        DDMcollector.cal9_AAprod(self)
    def cal_6(self):
        for i in range(1, len(self.finalProductions2)):
            DDMcollector.cal10_extraAA(self, i)
    def cal_7(self):
        for i in range(1, len(self.originalBxAllocationByRefinery)):      
            DDMcollector.cal11_mappedBX(self, i)
    def cal_8(self):
        for i in range(1, len(self.originalBxAllocationByRefinery)):  
            DDMcollector.cal12_finalTab_pratcial(self, i)
    def cal_9(self):
        for i in range(1, len(self.originalBxAllocationByRefinery)):  
            DDMcollector.cal13_finalDemand(self, i)
    def cal_10(self):
        for i in range(1, len(self.finalProductions2)-2):   
            DDMcollector.cal14_finalThirdParty(self, i)
            DDMcollector.cal15_importedBauxite(self,i)
    def cal_11(self):
        for i in range(1, len(self.originalBxAllocationByRefinery)):    
            DDMcollector.cal16_tFactors(self, i)
            DDMcollector.cal17_tFactors(self, i)
    def cal_12(self):
        for i in range(1, len(self.finalProductions2)-2):   
            DDMcollector.cal16_tFactors2(self, i)
    def cal_13(self):
        for i in range(1, len(self.originalBxAllocationByRefinery)): 
            DDMcollector.cal18_supplyTab(self,i)     
            DDMcollector.cal19_thirdPartySupply(self,i)
    
    def cal_finals(self):
        DDMcollector.cal_demand_setup(self)
        DDMcollector.cal_supply_setup(self)
        
    
    
x = DDMcollector()
x.cal_1()
x.cal_2()
x.cal_3()
x.cal_4()
x.cal_5()
x.cal_6()
x.cal_7()
x.cal_8()
x.cal_9()
x.cal_10()
x.cal_11()
x.cal_12()
x.cal_13()
x.cal_3()
x.cal_4()
x.cal_5()
x.cal_6()
x.cal_7()
x.cal_8()
x.cal_9()
x.cal_10()
x.cal_11()
x.cal_12()
x.cal_13()
x.cal_finals()


c1 = ['updated',
 'Refiney No.',
 'Refiney Name',
 'Refiney Prov',
 'Bx Prov category',
 'Bx origin Prov',
 'Bx Origin Cty',
 'Owner',
 'Bauxite',
 'Self Supply',
 'column 1',
 'column 2',
 "column 3",
 'column 4',
 'column 5',
 'Allocation']

c2 = ['No',
 'Ref .No.',
 'Refiney Name',
 'Province',
 'Province category',
 'Technology',
 'Ownership',
 'Owner',
 'Bauxite',
 'ref cell',
 'ref cell row',
 'column 1',
 '2005',
 '2006',
 '2007',
 '2008',
 '2009',
 '2010',
 '2011',
 '2012',
 '2013',
 '2014',
 '2015',
 '2016',
 '2017',
 '2018',
 '2019',
 '2020',
 '2021',
 '2022',
 '2023',
 '2024',
 '2025',
 '2026',
 '2027',
 '2028',
 '2029',
 '2030',
 '2031']

x.depletedProd.columns = x.depletedProd.loc[0]
x.depletedProd.drop([0], axis=0, inplace=True)
x.depletedProd.reset_index(drop=True, inplace=True)

x.originalBxAllocationByRefinery.to_csv('DDM Collector Outputs\Original bx alloctions by refinery by county.csv', index=False, header=c1)
x.resummingOriginal.to_csv('DDM Collector Outputs\Resumming orignal allocation by refinery.csv', index=False, header=c1)
x.nomalisingAllocation.to_csv('DDM Collector Outputs\\Normalising original allocations.csv', index=False, header=c2)
x.bauxiteConsumed.to_csv('DDM Collector Outputs\Tt of Bauxite Consumed by each refinery.csv', index=False, header=c2)
x.alumProp.to_csv('DDM Collector Outputs\Proportion of Alumina make by source for each refinery.csv', index=False, header=c2)
x.AAprop.to_csv('DDM Collector Outputs\Proportion of AA make from each source per refinery', index=False, header=c2)
x.domesticBX.to_csv('DDM Collector Outputs\Domestic Bx 1 - Non depleted Amounts.csv', index=False, header=c2)
x.domesticBX2.to_csv('DDM Collector Outputs\Domestic Bx 2 - Depleted Amounts.csv', index=False, header=c2)
x.domesticBX3.to_csv('DDM Collector Outputs\Domestic Bx 3 - Non Depleted Productions.csv',index=False, header=c2)
x.depletedProd.to_csv('DDM Collector Outputs\Depleted productions.csv', index = False)#, header=c2)
x.extraAAprod.to_csv('DDM Collector Outputs\Extra AA productions.csv', index = False, header=c2)
x.mappedBX.to_csv('DDM Collector Outputs\Mapped unto Bx.csv', index = False, header=c2)
x.totalDomestic.to_csv('DDM Collector Outputs\Total Domestic Bx.csv', index=False, header=c2)
x.finalTab.to_csv('DDM Collector Outputs\Final Tables for patricia - China Reserves.csv', index=False, header=c2)
x.finalTabDemand.to_csv('DDM Collector Outputs\Final Tables for patricia -  Demand.csv', index=False, header=c2)
x.finalThirdParty.to_csv('DDM Collector Outputs\\Final Tables for patricia - General Third Party Use.csv', index=False, header=c2)
x.importBauxite.to_csv('DDM Collector Outputs\Imported Bauxite from price forecast.csv', index=False, header=c2)
x.supplyTab.to_csv('DDM Collector Outputs\Supply Allocation Tonnages.csv', index=False, header=c2)
x.genThirdPartySupply.to_csv('DDM Collector Outputs\General Third Party Supply.csv', index=False, header=c2)


x.originalBxAllocationByRefinery.columns  = c1
x.resummingOriginal.columns  = c1
x.nomalisingAllocation.columns  = c2
x.bauxiteConsumed.columns  = c2
x.alumProp.columns  = c2
x.AAprop.columns  = c2
x.domesticBX.columns  = c2
x.domesticBX2.columns  = c2
x.domesticBX3.columns  = c2
x.extraAAprod.columns  = c2
x.mappedBX.columns  = c2
x.totalDomestic.columns  = c2
x.finalTab.columns  = c2
x.finalTabDemand.columns  = c2
x.finalThirdParty.columns  = c2
x.importBauxite.columns  = c2
x.supplyTab.columns  = c2
x.genThirdPartySupply.columns  = c2


x.demandSetUp.columns = x.demandSetUp.loc[0]
x.demandSetUp.drop([0], axis=0, inplace=True)
x.demandSetUp.reset_index(drop=True, inplace=True)
x.demandSetUp.dropna(how='all').dropna(how='all', axis=1)
cols = []
count = 1
for column in x.demandSetUp.columns:
    if column in cols:
        c = cols.count(column)
        cols.append(f'{column}_{c}')
    else:
        cols.append(column)
x.demandSetUp.columns = cols
x.supplySetUp.columns = x.supplySetUp.loc[0]
x.supplySetUp.drop([0], axis=0, inplace=True)
x.supplySetUp.reset_index(drop=True, inplace=True)
x.demandSetUp.to_csv('DDM Collector Outputs\Demand n TperT set up.csv', index=False)
x.supplySetUp.to_csv('DDM Collector Outputs\Supply n TperT set up.csv', index=False)


print(x.demandSetUp)
print(x.supplySetUp)

dblist = [
    db_conv.single_year_mult_out(x.originalBxAllocationByRefinery, "Original bx alloctions by refinery by county."),
    db_conv.single_year_mult_out(x.resummingOriginal, "Resumming orignal allocation by refinery"),
    db_conv.mult_year_single_output(x.nomalisingAllocation, "Normalising original allocations"),
    db_conv.mult_year_single_output(x.bauxiteConsumed, "Tt of Bauxite Consumed by each refinery"),
    db_conv.mult_year_single_output(x.alumProp, "Proportion of Alumina make by source for each refinery"),
    db_conv.mult_year_single_output(x.AAprop, "Proportion of AA make from each source per refineryp"),
    db_conv.mult_year_single_output(x.domesticBX, "Domestic Bx 1 - Non depleted Amounts"),
    db_conv.mult_year_single_output(x.domesticBX2, "Domestic Bx 2 - Non depleted Amounts"),
    db_conv.mult_year_single_output(x.domesticBX3, "Domestic Bx 3 - Non depleted Amounts"),
    db_conv.single_year_mult_out(x.depletedProd, "Depleted productions"),
    db_conv.mult_year_single_output(x.extraAAprod, "Extra AA productions"),
    db_conv.mult_year_single_output(x.mappedBX, "Mapped unto Bx"),
    db_conv.mult_year_single_output(x.totalDomestic, "Total Domestic Bx"),
    db_conv.mult_year_single_output(x.finalTab, "Final Tables for patricia - China Reserves"),
    db_conv.mult_year_single_output(x.finalTabDemand, "Final Tables for patricia -  Demand"),
    db_conv.mult_year_single_output(x.finalThirdParty, "Final Tables for patricia - General Third Party Use"),
    db_conv.mult_year_single_output(x.importBauxite, "Imported Bauxite from price forecast"),
    db_conv.mult_year_single_output(x.supplyTab, "Supply Allocation Tonnages"),
    db_conv.mult_year_single_output(x.genThirdPartySupply, "General Third Party Supply"),
    db_conv.mult_year_single_output(x.demandSetUp, "Demand n TperT set up"),
    db_conv.mult_year_single_output(x.supplySetUp, "Supply n TperT set up"),
]

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]

snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
uploadtodb.upload(snapshot_output_data)