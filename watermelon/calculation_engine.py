import pandas as pd
import warnings
import numpy as np
import timer
from flatdb.flatdbconverter import Flatdbconverter, read_from_database
from outputdb import uploadtodb
from watermelon_script import restruct

warnings.filterwarnings("ignore")


db_conv = Flatdbconverter("Watermelon charts")
t = timer.Timer()

meidata = read_from_database('meiaadata')
basedata = read_from_database('basedata')

# stockpiles and year mappings
year = read_from_database("year")
year_map = dict(year[["year_id", "year"]].values)
other_controls = read_from_database("other_controls").replace(year_map)
includestockpile = other_controls["includestockpile"][0]
current_year = other_controls["current_year_for_watermelon"][0]

#input restructure
basedata_r = restruct(basedata, 'ind', 'year_id', 'Value')
meidata_r = restruct(meidata, 'province_id', 'year_id', 'value')
meidata_r = meidata_r.rename(columns={'province_id': 'province'})


class watermelondatamodel():
    def __init__(self):
        years = list(map(str,range(2005,2032)))
        self.years = years
        indx1 = ["Domestic Bx","CBIX US$45","CBIX US$55","CBIX US$65","Depletion","Planned Additional - Imports","Existing Demand - Imports","Imported Alumina","domestic L","CBIX US$45 L","CBIX US$55 L","CBIX US$65 L","Demand L"]
        indx2 = ["Guangxi","Guizhou","Henan","Shanxi","Other"]
        indx3 = ["Existing","Planned Additional Imports","Depletion Imports","CBIX U$65","CBIX U$55","CBIX U$45","Stockpile building","TOTAL"]
        indx4 = ["AA made from import bauxite","AA from domestic source inc. fly ash"]
        indx5 = ["domestic","import","alumina","total","Difference"]
        indx6 = ["Continuing domestic demand",
"Import demand - economic depletion of resources",
"Import demand - outright depletion of resources",
"Import demand - exsiting inland converted & blending consumption",
"Demand already curtailed due to import competition / inland bauxite scarcity",
"Forecast domestic actual",
"Forecast domestic demand",
"Nominal domestic demand - no Bx shortage"
]
        self.indx1 = indx1
        self.indx2 = indx2
        self.indx3 = indx3
        self.indx4 = indx4
        self.indx5 = indx5
        self.indx6 = indx6
        # self.basedata = pd.read_csv('inputs/basedata.csv')
        # self.meiaadata = pd.read_csv('inputs/smeiaadata.csv')
        self.basedata = basedata_r
        self.meiaadata = meidata_r
        self.current_year = current_year
        self.includestockpile = str(includestockpile)
        
        self.aluminadata = pd.DataFrame(columns=years,index=indx1)
        self.bauxitedata = pd.DataFrame(columns=years,index=indx1)
        self.meipropdata = pd.DataFrame(columns=years,index=indx2)
        self.bauxitedata2 = pd.DataFrame(columns=years,index=indx2)
        self.allcbixdata = pd.DataFrame(columns=years,index=indx3)
        self.cbix55data = pd.DataFrame(columns=years,index=indx3)
        self.allcbixdatav2 = pd.DataFrame(columns=years,index=indx3)
        self.cbix55datav2 = pd.DataFrame(columns=years,index=indx3)
        self.meidata = pd.DataFrame(columns=years,index=indx4)
        self.cbixus65 = pd.DataFrame(columns=years,index=indx4)
        self.cbixus55 = pd.DataFrame(columns=years,index=indx4)
        self.cbixus45 = pd.DataFrame(columns=years,index=indx4)
        self.bxdemandcbix55 = pd.DataFrame(columns=years,index=indx5)
        self.chartingbxAA = pd.DataFrame(columns=years,index=indx6)
        self.chartingbxBx = pd.DataFrame(columns=years,index=indx6)
        self.basedata.name = 'basedata'
        self.meiaadata.name = 'meiaadata'
        self.aluminadata.name = 'aluminadata'
        self.bauxitedata.name = 'bauxitedata'
        self.meipropdata.name = 'meipropdata'
        self.bauxitedata2.name = 'bauxitedata2'
        self.allcbixdata.name = 'allcbixdata'
        self.cbix55data.name = 'cbix55data'
        self.bauxite_charting = None

    def basedata_cbix65(self,col):
        t.start()
        value = self.basedata[col][15]
        self.basedata.at[21,col] = value
        t.stop()

    def basedata_cbix55(self,col):
        t.start()
        value = self.basedata[col][15]
        self.basedata.at[22,col] = value
        t.stop()

    def basedata_cbix45(self,col):
        t.start()
        value = self.basedata[col][15]
        self.basedata.at[23,col] = value
        t.stop()

    def meidatacalc1(self,col):
        t.start()
        value = self.basedata[col][3]
        self.meidata.at[self.indx4[0],col] = value
        t.stop()
    def meidatacalc2(self,col):
        t.start()
        value = self.basedata[col][15]+self.basedata[col][4]
        self.meidata.at[self.indx4[1],col] = value
        t.stop()
########
    def cbix65calc1(self,col):
        t.start()
        value = self.basedata[col][5]
        self.cbixus65.at[self.indx4[0],col] = value
        t.stop()
    def cbix65calc2(self,col):
        t.start()
        value = self.basedata[col][21]+self.basedata[col][6]
        self.cbixus65.at[self.indx4[1],col] = value
        t.stop()

    def cbix55calc1(self,col):
        t.start()
        value = self.basedata[col][7]
        self.cbixus55.at[self.indx4[0],col] = value
        t.stop()
    def cbix55calc2(self,col):
        t.start()
        value = self.basedata[col][22]+self.basedata[col][8]
        self.cbixus55.at[self.indx4[1],col] = value
        t.stop()

    def cbix45calc1(self,col):
        t.start()
        value = self.basedata[col][9]
        self.cbixus45.at[self.indx4[0],col] = value
        t.stop()
    def cbix45calc2(self,col):
        t.start()
        value = self.basedata[col][23]+self.basedata[col][4]
        self.cbixus45.at[self.indx4[1],col] = value
        t.stop()

    def blendbar(self,col):
        t.start()
        d = [ self.basedata[col][12],self.basedata[col][0] ,self.cbixus55[col][0],self.cbixus55[col][1] ]
        value = (d[0]*d[3]+d[1]*d[2])/(d[2]+d[3])
        self.basedata.at[20,col] = value
        t.stop()

##############
    def basedata_planned(self,col):
        t.start()
        d = [self.basedata[col][3],self.basedata[col][11]]
        value = d[0] if int(col) < 2019 else d[1]
        self.basedata.at[11,col] = value
        t.stop()

    def basedata_estimates(self,col):
        t.start()
        d = [self.basedata[col][0],self.basedata[col][12]]
        value = d[0] if int(col) < 2019 else d[1]
        self.basedata.at[20,col] = value
        t.stop()

    def alumina_Domestic(self,col):
        t.start()
        d = [self.basedata[col][1],self.aluminadata[col][1:8].sum()]
        value = d[0]-d[1]
        self.aluminadata.at["Domestic Bx",col] = value
        t.stop()

    def alumina_CBIXUS45(self,col):
        d = [self.basedata[col][9],self.aluminadata[col][2:7].sum()]
        value = d[0]-d[1] if int(col) < 2016 or int(col) > 2018 else 0
        self.aluminadata.at["CBIX US$45",col] = value
    def alumina_CBIXUS55(self,col):
        d = [self.basedata[col][7],self.aluminadata[col][3:7].sum()]
        value = d[0]-d[1] if int(col) != 2018 else 0
        self.aluminadata.at["CBIX US$55",col] = value
    def alumina_CBIXUS65(self,col):
        d = [self.basedata[col][5],self.aluminadata[col][4:7].sum()]
        value = d[0]-d[1]
        self.aluminadata.at["CBIX US$65",col] = value
    def alumina_Depletion(self,col):
        d = [self.basedata[col][3],self.aluminadata[col][5:7].sum()]
        value = d[0]-d[1]
        self.aluminadata.at["Depletion",col] = value
    def alumina_PlannedAdditionalImports(self,col): #It follows different formula
        d = [self.basedata[col][11],self.aluminadata[col][6]]
        value = d[0]-d[1]
        self.aluminadata.at["Planned Additional - Imports",col] = value
    def alumina_ExistingDemandImports(self,col):
        d = [self.basedata[col][3],self.basedata['2019'][3]]
        value = d[0] if int(col) < 2019 else d[1]
        self.aluminadata.at["Existing Demand - Imports",col] = value
    def alumina_ImportedAlumina(self,col):
        d = [self.basedata[col][2]]
        value = d[0]
        self.aluminadata.at["Imported Alumina",col] = value
    def alumina_domesticL(self,col):
        d = [self.aluminadata[col][0]]
        value = d[0]
        self.aluminadata.at["domestic L",col] = value
    def alumina_CBIXUS45L(self,col):
        d = [self.aluminadata[col][0:2].sum()]
        value = d[0]
        self.aluminadata.at["CBIX US$45 L",col] = value
    def alumina_CBIXUS55L(self,col):
        d = [self.aluminadata[col][0:3].sum()]
        value = d[0]
        self.aluminadata.at["CBIX US$55 L",col] = value
    def alumina_CBIXUS65L(self,col):
        d = [self.aluminadata[col][0:4].sum()]
        value = d[0]
        self.aluminadata.at["CBIX US$65 L",col] = value
    def alumina_DemandL(self,col):
        d = [self.basedata[col][1]]
        value = d[0]
        self.aluminadata.at["Demand L",col] = value



    def bauxite_Domestic(self,col):
        d = [self.basedata[col][12],self.aluminadata[col][0]]
        value = d[0]*d[1]
        self.bauxitedata.at["Domestic Bx",col] = value

    def bauxite_CBIXUS45(self,col):
        d = [self.basedata[col][12],self.aluminadata[col][1]]
        value = d[0]*d[1]
        self.bauxitedata.at["CBIX US$45",col] = value
    def bauxite_CBIXUS55(self,col):
        d = [self.basedata[col][12],self.aluminadata[col][2]]
        value = d[0]*d[1]
        self.bauxitedata.at["CBIX US$55",col] = value
    def bauxite_CBIXUS65(self,col):
        d = [self.basedata[col][12],self.aluminadata[col][3]]
        value = d[0]*d[1]
        self.bauxitedata.at["CBIX US$65",col] = value
    def bauxite_Depletion(self,col):
        d = [self.basedata[col][0],self.aluminadata[col][4]]
        value = d[0]*d[1]
        self.bauxitedata.at["Depletion",col] = value
    def bauxite_PlannedAdditionalImports(self,col):
        d = [self.basedata[col][0],self.aluminadata[col][5]]
        value = d[0]*d[1]
        self.bauxitedata.at["Planned Additional - Imports",col] = value
    def bauxite_ExistingDemandImports(self,col):
        d = [self.basedata[col][0],self.aluminadata[col][6]]
        value = d[0]*d[1]
        self.bauxitedata.at["Existing Demand - Imports",col] = value
    def bauxite_Importedbauxite(self,col):
        d = [self.basedata[col][20],self.aluminadata[col][7]]
        value = d[0]*d[1]
        self.bauxitedata.at["Imported Alumina",col] = value
    def bauxite_domesticL(self,col):
        d = [self.basedata[col][12],self.aluminadata[col][8]]
        value = d[0]*d[1]
        self.bauxitedata.at["domestic L",col] = value
    def bauxite_CBIXUS45L(self,col):
        d = [self.bauxitedata[col][0:2].sum()]
        value = d[0]
        self.bauxitedata.at["CBIX US$45 L",col] = value
    def bauxite_CBIXUS55L(self,col):
        d = [self.bauxitedata[col][0:3].sum()]
        value = d[0]
        self.bauxitedata.at["CBIX US$55 L",col] = value
    def bauxite_CBIXUS65L(self,col):
        d = [self.bauxitedata[col][0:4].sum()]
        value = d[0]
        self.bauxitedata.at["CBIX US$65 L",col] = value
    def bauxite_DemandL(self,col):
        d = [self.bauxitedata[col][0:8].sum()]
        value = d[0]
        self.bauxitedata.at["Demand L",col] = value

    def demandcalc1(self,col):
        value = self.basedata[col][12]*self.cbixus55[col][self.indx4[1]]
        self.bxdemandcbix55.at[self.indx5[0],col] = value
    def demandcalc2(self,col):
        value = self.basedata[col][0]*self.cbixus55[col][self.indx4[0]]
        self.bxdemandcbix55.at[self.indx5[1],col] = value
    def demandcalc3(self,col):
        value = self.basedata[col][20]*self.basedata[col][2]
        self.bxdemandcbix55.at[self.indx5[2],col] = value
    def demandcalc4(self,col):
        value = self.bxdemandcbix55[col][0:3].sum()
        self.bxdemandcbix55.at[self.indx5[3],col] = value
    def demandcalc5(self,col):
        value = self.bauxitedata[col][12] - self.bxdemandcbix55[col][3]
        self.bxdemandcbix55.at[self.indx5[4],col] = value

    def meiprop_calc(self,indx,col):
        d = [self.meiaadata[col][indx],self.meiaadata[col].sum()]
        value = d[0]/d[1]
        self.meipropdata.at[self.indx2[indx],col] = value



    def bauxitedata2_calc(self,indx,col):
        d = [self.meipropdata[col][indx],self.bauxitedata[col][0]]
        # print(self.bauxitedata)
        value = d[0]*d[1] if int(col) <= self.current_year else d[0]*self.bauxitedata.loc['Domestic Bx':'CBIX US$65', col].sum()
        self.bauxitedata2.at[self.indx2[indx],col] = value

    def allcbix(self,col):
        d = [self.aluminadata[col][6],self.aluminadata[col][5],self.aluminadata[col][4],self.aluminadata[col][3],self.aluminadata[col][2],self.aluminadata[col][1],self.basedata[col][0]]
        value1 = d[0]*d[6] if int(col) > self.current_year else self.basedata[col][13]
        value2 = d[1]*d[6]
        value3 = d[2]*d[6]
        value4 = d[3]*d[6]
        value5 = d[4]*d[6]
        value6 = d[5]*d[6]
        value7 = 0 if int(col) <= self.current_year else self.basedata[col][14] if self.includestockpile == "yes" else 0
        print(self.basedata[col])
        #value7 =  self.basedata[col][13] if self.includestockpile == "yes" else 0
        print('all cibix')
        print(value7)
        value8 = value1+value2+value3+value4+value5+value6+value7
        self.allcbixdata.at[self.indx3[0],col] = value1
        self.allcbixdata.at[self.indx3[1],col] = value2
        self.allcbixdata.at[self.indx3[2],col] = value3
        self.allcbixdata.at[self.indx3[3],col] = value4
        self.allcbixdata.at[self.indx3[4],col] = value5
        self.allcbixdata.at[self.indx3[5],col] = value6
        self.allcbixdata.at[self.indx3[6],col] = value7
        self.allcbixdata.at[self.indx3[7],col] = value8

    def cbix55(self,col):
        d = [self.allcbixdata[col][0],self.allcbixdata[col][1],self.allcbixdata[col][2],self.allcbixdata[col][3],self.allcbixdata[col][4]]
       
        

        value1 = d[0]
        value2 = d[1]
        value3 = d[2]
        value4 = d[3]
        value5 = d[4]
        value6 = 0
        value7 = 0 if int(col) < self.current_year else self.basedata[col][14] if self.includestockpile == "yes" else 0
        #value7 = self.basedata[col][13] if self.includestockpile == "yes" else 0
    
        
        
        print(value7)
        value8 = d[0]+d[1]+d[2]+d[3]+d[4]+value6+value7
        self.cbix55data.at[self.indx3[0],col] = value1
        self.cbix55data.at[self.indx3[1],col] = value2
        self.cbix55data.at[self.indx3[2],col] = value3
        self.cbix55data.at[self.indx3[3],col] = value4
        self.cbix55data.at[self.indx3[4],col] = value5
        self.cbix55data.at[self.indx3[5],col] = value6
        self.cbix55data.at[self.indx3[6],col] = value7
        self.cbix55data.at[self.indx3[7],col] = value8

    def allcbix2(self,col):
        d = [self.allcbixdata[col][0],self.allcbixdata[col][1],self.allcbixdata[col][2],self.allcbixdata[col][3],self.allcbixdata[col][4],self.allcbixdata[col][5],self.allcbixdata[col][6]]
        value1 = d[0] if d[0] > 1 else 0
        value2 = d[1] if d[1] > 1 else 0
        value3 = d[2] if d[2] > 1 else 0
        value4 = d[3] if d[3] > 1 else 0
        value5 = d[4] if d[4] > 1 else 0
        value6 = d[5] if d[5] > 1 else 0
        value7 = d[6] if d[6] > 1 else 0
        value8 = value1+value2+value3+value4+value5+value6+value7
        self.allcbixdatav2.at[self.indx3[0],col] = value1
        self.allcbixdatav2.at[self.indx3[1],col] = value2
        self.allcbixdatav2.at[self.indx3[2],col] = value3
        self.allcbixdatav2.at[self.indx3[3],col] = value4
        self.allcbixdatav2.at[self.indx3[4],col] = value5
        self.allcbixdatav2.at[self.indx3[5],col] = value6
        self.allcbixdatav2.at[self.indx3[6],col] = value7
        self.allcbixdatav2.at[self.indx3[7],col] = value8

    def cbix552(self,col):
        d = [self.cbix55data[col][0],self.cbix55data[col][1],self.cbix55data[col][2],self.cbix55data[col][3],self.cbix55data[col][4],self.cbix55data[col][5],self.cbix55data[col][6]]
        value1 = d[0] if d[0] > 1 else 0
        value2 = d[1] if d[1] > 1 else 0
        value3 = d[2] if d[2] > 1 else 0
        value4 = d[3] if d[3] > 1 else 0
        value5 = d[4] if d[4] > 1 else 0
        value6 = d[5] if d[5] > 1 else 0
        value7 = d[6] if d[6] > 1 else 0
        value8 = value1+value2+value3+value4+value5+value6+value7
        self.cbix55datav2.at[self.indx3[0],col] = value1
        self.cbix55datav2.at[self.indx3[1],col] = value2
        self.cbix55datav2.at[self.indx3[2],col] = value3
        self.cbix55datav2.at[self.indx3[3],col] = value4
        self.cbix55datav2.at[self.indx3[4],col] = value5
        self.cbix55datav2.at[self.indx3[5],col] = value6
        self.cbix55datav2.at[self.indx3[6],col] = value7
        self.cbix55datav2.at[self.indx3[7],col] = value8


    def baux_charting(self):
        yrs = list(map(str,list(range(2019, 2032))))
        baux = self.bauxitedata.copy().reset_index()
        alum = self.aluminadata.copy().reset_index()
        self.bauxite_charting = pd.DataFrame(columns=["Field", *yrs ])
        inds = ["Future Domestic Bx Demand ie CBIX 45 line","Future Imported Bx Demand","Exsiting Converted Demand","Curtailed","Domestic Demand","Real Demand L","Namely Demand L"]
        self.bauxite_charting["Field"] = inds
        # store values
        self.bauxite_charting.loc[0, yrs] = baux.loc[0:1, yrs].sum().values
        self.bauxite_charting.loc[1, yrs] = baux.loc[2:3, yrs].sum().values + ((self.basedata.loc[17, yrs].values - alum.loc[0:3, yrs].sum().values) * self.basedata.loc[0,yrs].values)
        self.bauxite_charting.loc[2, yrs] = self.basedata.loc[18, yrs].values * self.basedata.loc[0, yrs].values
        self.bauxite_charting.loc[3, yrs] = (self.basedata.loc[16, yrs].values - (self.basedata.loc[17, yrs].values + self.basedata.loc[18, yrs].values)) * self.basedata.loc[0, yrs].values
        self.bauxite_charting.loc[4, yrs] = self.bauxite_charting.loc[0, yrs]
        self.bauxite_charting.loc[5, yrs] = self.bauxite_charting.loc[0:2, yrs].sum()
        self.bauxite_charting.loc[6, yrs] = self.bauxite_charting.loc[0:3, yrs].sum()
    def baux_aa(self,col):
        v1 = 0 if int(col) < 2019 else self.aluminadata[col][0:2].sum() 
        v2 = 0 if int(col) < 2019 else self.aluminadata[col][2:4].sum()   
        v3 = 0 if int(col) < 2019 else self.aluminadata[col][4]   
        v4 = 0 if int(col) < 2019 else self.basedata[col][18]+self.basedata[col][19]   
        v5 = 0 if int(col) < 2019 else self.basedata[col][16] - (v1+v2+v3+v4)
        v6 = 0 if int(col) < 2019 else v1 
        v7 = 0 if int(col) < 2019 else v1+v2+v3 
        v8 = 0 if int(col) < 2019 else self.basedata[col][16] 
        self.chartingbxAA.at[self.indx6[0],col] = v1
        self.chartingbxAA.at[self.indx6[1],col] = v2
        self.chartingbxAA.at[self.indx6[2],col] = v3
        self.chartingbxAA.at[self.indx6[3],col] = v4
        self.chartingbxAA.at[self.indx6[4],col] = v5
        self.chartingbxAA.at[self.indx6[5],col] = v6
        self.chartingbxAA.at[self.indx6[6],col] = v7
        self.chartingbxAA.at[self.indx6[7],col] = v8
        
    def baux_bx(self,col):
        v1 = 0 if int(col) < 2019 else self.chartingbxAA[col][0]*self.basedata[col][12]
        v2 = 0 if int(col) < 2019 else self.chartingbxAA[col][1]*self.basedata[col][12] 
        v3 = 0 if int(col) < 2019 else self.chartingbxAA[col][2]*self.basedata[col][12] 
        v4 = 0 if int(col) < 2019 else self.chartingbxAA[col][3]*self.basedata[col][12] 
        v5 = 0 if int(col) < 2019 else self.chartingbxAA[col][4]*self.basedata[col][12] 
        v6 = 0 if int(col) < 2019 else self.chartingbxAA[col][5]*self.basedata[col][12] 
        v7 = 0 if int(col) < 2019 else self.chartingbxAA[col][6]*self.basedata[col][12] 
        v8 = 0 if int(col) < 2019 else self.chartingbxAA[col][7]*self.basedata[col][12] 
        self.chartingbxBx.at[self.indx6[0],col] = v1
        self.chartingbxBx.at[self.indx6[1],col] = v2
        self.chartingbxBx.at[self.indx6[2],col] = v3
        self.chartingbxBx.at[self.indx6[3],col] = v4
        self.chartingbxBx.at[self.indx6[4],col] = v5
        self.chartingbxBx.at[self.indx6[5],col] = v6
        self.chartingbxBx.at[self.indx6[6],col] = v7
        self.chartingbxBx.at[self.indx6[7],col] = v8
        pass


    def calc_all(self):
        for i in self.years:
            watermelondatamodel.basedata_cbix65(self,i)
            watermelondatamodel.basedata_cbix55(self,i)
            watermelondatamodel.basedata_cbix45(self,i)
            watermelondatamodel.meidatacalc1(self,i)
            watermelondatamodel.meidatacalc2(self,i)
            watermelondatamodel.cbix65calc1(self,i)
            watermelondatamodel.cbix65calc2(self,i)
            watermelondatamodel.cbix55calc1(self,i)
            watermelondatamodel.cbix55calc2(self,i)
            watermelondatamodel.cbix45calc1(self,i)
            watermelondatamodel.cbix45calc2(self,i)
            watermelondatamodel.blendbar(self,i)
            #watermelondatamodel.basedata_estimates(self,i)


            watermelondatamodel.basedata_planned(self,i)
            watermelondatamodel.alumina_DemandL(self,i)

            watermelondatamodel.alumina_ImportedAlumina(self,i)
            watermelondatamodel.alumina_ExistingDemandImports(self,i)
            watermelondatamodel.alumina_PlannedAdditionalImports(self,i)
            watermelondatamodel.alumina_Depletion(self,i)
            watermelondatamodel.alumina_CBIXUS65(self,i)
            watermelondatamodel.alumina_CBIXUS55(self,i)
            watermelondatamodel.alumina_CBIXUS45(self,i)
            watermelondatamodel.alumina_Domestic(self,i)
            watermelondatamodel.alumina_domesticL(self,i)
            watermelondatamodel.alumina_CBIXUS45L(self,i)
            watermelondatamodel.alumina_CBIXUS55L(self,i)
            watermelondatamodel.alumina_CBIXUS65L(self,i)

            watermelondatamodel.bauxite_Domestic(self,i)
            watermelondatamodel.bauxite_CBIXUS45(self,i)
            watermelondatamodel.bauxite_CBIXUS55(self,i)
            watermelondatamodel.bauxite_CBIXUS65(self,i)
            watermelondatamodel.bauxite_Depletion(self,i)
            watermelondatamodel.bauxite_PlannedAdditionalImports(self,i)
            watermelondatamodel.bauxite_ExistingDemandImports(self,i)
            watermelondatamodel.bauxite_Importedbauxite(self,i)
            watermelondatamodel.bauxite_domesticL(self,i)
            watermelondatamodel.bauxite_CBIXUS45L(self,i)
            watermelondatamodel.bauxite_CBIXUS55L(self,i)
            watermelondatamodel.bauxite_CBIXUS65L(self,i)
            watermelondatamodel.bauxite_DemandL(self,i)

            watermelondatamodel.demandcalc1(self,i)
            watermelondatamodel.demandcalc2(self,i)
            watermelondatamodel.demandcalc3(self,i)
            watermelondatamodel.demandcalc4(self,i)
            watermelondatamodel.demandcalc5(self,i)

            watermelondatamodel.allcbix(self,i)
            watermelondatamodel.cbix55(self,i)
            watermelondatamodel.allcbix2(self,i)
            watermelondatamodel.cbix552(self,i)
            watermelondatamodel.baux_aa(self,i)
            watermelondatamodel.baux_bx(self,i)
            for j in range(5):
                watermelondatamodel.meiprop_calc(self,j,i)
                watermelondatamodel.bauxitedata2_calc(self,j,i)
        watermelondatamodel.baux_charting(self)

    def name_all(self):
        self.aluminadata.loc[0,"df_info"] = 'aluminadata'
        self.bauxitedata.loc[0,"df_info"] = 'bauxitedata'
        self.meipropdata.loc[0,"df_info"] = 'meipropdata'
        self.bauxitedata2.loc[0,"df_info"] = 'bauxitedata2'
        self.allcbixdata.loc[0,"df_info"] = 'allcbixdata'
        self.cbix55data.loc[0,"df_info"] = 'cbix55data'



t.end_time()
class watermelongriffinmodel():
    def __init__(self,data):
        self.griffinmeidata = pd.read_csv('griffinmeidata.csv')
        self.griffincbix55data = pd.read_csv('griffincbix55data.csv')
        self.basedata = data.basedata
        years = list(map(str,range(2005,2032)))
        self.years = years
        indx1 = ['AA made from import bauxite','AA from domestic bauxite','Planned production from imports']
        indx2 = ['Griffin Planned AA from Import Bx','April Planned AA from Import Bx','April - Griffin','April - Griffin as Bx']
        indx3 = ['Griffin Planned AA from Import Bx','April Planned AA from Import Bx','April - Griffin']
        self.indx1 = indx1
        self.indx2 = indx2
        self.indx3 = indx3
        self.meidata = pd.DataFrame(columns=years,index=indx1)
        self.cbixus55data = pd.DataFrame(columns=years,index=indx1)
        self.plannedimportprod = pd.DataFrame(columns=years,index=indx2)
        self.importsmeidata = pd.DataFrame(columns=years,index=indx2)
        self.importscbix55data = pd.DataFrame(columns=years,index=indx2)
        self.plannedimportneeddata = pd.DataFrame(columns=years,index=indx3)
        self.importsbxmeidata = pd.DataFrame(columns=years,index=indx3)
        self.importsbxcbix55data = pd.DataFrame(columns=years,index=indx3)

        self.griffinmeidata.name = 'griffinmeidata'
        self.griffincbix55data.name = 'griffincbix55data'
        self.meidata.name = 'meidata'
        self.cbixus55data.name = 'cbixus55data'
        self.plannedimportprod.name = 'plannedimportprod'
        self.importsmeidata.name = 'importsmeidata'
        self.importscbix55data.name = 'importscbix55data'
        self.plannedimportneeddata.name = 'plannedimportneeddata'
        self.importsbxmeidata.name = 'importsbxmeidata'
        self.importsbxcbix55data.name = 'importsbxcbix55data'

    def meidata_calc(self,col):
        value1 = self.basedata[col][3]
        value2 = self.basedata[col][4]
        value3 = self.basedata[col][11]
        self.meidata.at[self.indx1[0],col] = value1
        self.meidata.at[self.indx1[1],col] = value2
        self.meidata.at[self.indx1[2],col] = value3

    def cbixus55data_calc(self,col):
        value1 = self.basedata[col][7]
        value2 = self.basedata[col][8]
        self.cbixus55data.at[self.indx1[0],col] = value1
        self.cbixus55data.at[self.indx1[1],col] = value2


    def plannedimportprod_calc(self,col):
        value1 = self.griffinmeidata[col][2]
        value2 = self.meidata[col][2]
        value3 = value2-value1
        value4 = self.basedata[col][0]*value3
        self.plannedimportprod.at[self.indx2[0],col] = value1
        self.plannedimportprod.at[self.indx2[1],col] = value2
        self.plannedimportprod.at[self.indx2[2],col] = value3
        self.plannedimportprod.at[self.indx2[3],col] = value4

    def importsmeidata_calc(self,col):
        value1 = self.griffinmeidata[col][0]
        value2 = self.meidata[col][0]
        value3 = value2-value1
        value4 = value3*self.basedata[col][0]
        self.importsmeidata.at[self.indx2[0],col] = value1
        self.importsmeidata.at[self.indx2[1],col] = value2
        self.importsmeidata.at[self.indx2[2],col] = value3
        self.importsmeidata.at[self.indx2[3],col] = value4

    def importscbix55data_calc(self,col):
        value1 = self.griffincbix55data[col][0]
        value2 = self.cbixus55data[col][0]
        value3 = value2-value1
        value4 = value3*self.basedata[col][0]
        self.importscbix55data.at[self.indx2[0],col] = value1
        self.importscbix55data.at[self.indx2[1],col] = value2
        self.importscbix55data.at[self.indx2[2],col] = value3
        self.importscbix55data.at[self.indx2[3],col] = value4


    def plannedimportneeddata_calc(self,col):
        value1 = self.basedata[col][0]*self.plannedimportprod[col][0]
        value2 = self.basedata[col][0]*self.plannedimportprod[col][1]
        value3 = value2-value1
        self.plannedimportneeddata.at[self.indx3[0],col] = value1
        self.plannedimportneeddata.at[self.indx3[1],col] = value2
        self.plannedimportneeddata.at[self.indx3[2],col] = value3

    def importsbxmeidata_calc(self,col):
        value1 = self.basedata[col][0]*self.importsmeidata[col][0]
        value2 = self.basedata[col][0]*self.importsmeidata[col][1]
        value3 = value2-value1
        self.importsbxmeidata.at[self.indx3[0],col] = value1
        self.importsbxmeidata.at[self.indx3[1],col] = value2
        self.importsbxmeidata.at[self.indx3[2],col] = value3

    def importsbxcbix55data_calc(self,col):
        value1 = self.basedata[col][0]*self.importscbix55data[col][0]
        value2 = self.basedata[col][0]*self.importscbix55data[col][1]
        value3 = value2-value1
        self.importsbxcbix55data.at[self.indx3[0],col] = value1
        self.importsbxcbix55data.at[self.indx3[1],col] = value2
        self.importsbxcbix55data.at[self.indx3[2],col] = value3

    def calc_all(self):
        for i in self.years:
            watermelongriffinmodel.meidata_calc(self,i)
            watermelongriffinmodel.cbixus55data_calc(self,i)
            watermelongriffinmodel.plannedimportprod_calc(self,i)
            watermelongriffinmodel.importsmeidata_calc(self,i)
            watermelongriffinmodel.importscbix55data_calc(self,i)
            watermelongriffinmodel.plannedimportneeddata_calc(self,i)
            watermelongriffinmodel.importsbxmeidata_calc(self,i)
            watermelongriffinmodel.importsbxcbix55data_calc(self,i)



w = watermelondatamodel()
w.calc_all()
#x = watermelongriffinmodel(w)
#x.calc_all()

w.aluminadata.to_csv('outputdata/aluminadata1.csv')
w.bauxitedata.to_csv('outputdata/bauxitedata.csv')
w.meipropdata.to_csv('outputdata/meipropdata.csv')
w.bauxitedata2.to_csv('outputdata/bauxitedata2.csv')
w.allcbixdata.to_csv('outputdata/allcbixdata.csv')
w.cbix55data.to_csv('outputdata/cbix55data.csv')

w.allcbixdatav2.to_csv('outputdata/allcbixdatav2.csv')
w.cbix55datav2.to_csv('outputdata/cbix55datav2.csv')
w.meidata.to_csv('outputdata/meidatav2.csv')
w.cbixus65.to_csv('outputdata/cbixus65.csv')
w.cbixus55.to_csv('outputdata/cbixus55.csv')
w.cbixus45.to_csv('outputdata/cbixus45.csv')
w.bxdemandcbix55.to_csv('outputdata/bxdemandcbix55.csv')
w.basedata.to_csv('outputdata/basedata.csv', index=False)
w.bauxite_charting.to_csv("outputdata/bauxitecharting.csv", index=False)
w.chartingbxAA.to_csv("outputdata/chartingbxAA.csv")
w.chartingbxBx.to_csv("outputdata/chartingbxBx.csv")


w_aluminadata = db_conv.multi_year_multi_out(w.aluminadata, "alumina data", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_bauxitedata = db_conv.multi_year_multi_out(w.bauxitedata, "bauxite data", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_meipropdata = db_conv.multi_year_multi_out(w.meipropdata, "mei prop data", col_params=[(0, "category")], make_multi="Table", not_num_indexed=True)
w_bauxitedata2 = db_conv.multi_year_multi_out(w.bauxitedata2, "bauxite data2", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_allcbixdata = db_conv.multi_year_multi_out(w.allcbixdata, "all cbix data", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_cbix55data = db_conv.multi_year_multi_out(w.cbix55data, "cbix55 data", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_allcbixdatav2 = db_conv.multi_year_multi_out(w.allcbixdatav2, "all cbix datav2", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_cbix55datav2 = db_conv.multi_year_multi_out(w.cbix55datav2, "cbix 55 data v2", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_meidata = db_conv.multi_year_multi_out(w.meidata, "mei data", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_cbixus65 = db_conv.multi_year_multi_out(w.cbixus65, "cbix us65", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_cbixus55 = db_conv.multi_year_multi_out(w.cbixus55, "cbix us55", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_cbixus45 = db_conv.multi_year_multi_out(w.cbixus45, "cbix us45", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_bxdemandcbix55 = db_conv.multi_year_multi_out(w.bxdemandcbix55, "bx demand cbix55", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_basedata = db_conv.mult_year_single_output(w.basedata, "base data")
w_bauxite = db_conv.multi_year_multi_out(w.bauxite_charting, "Charting - Bauxite Demand by Domestic Capacity", make_multi="Table")
w_chartingbxAA = db_conv.multi_year_multi_out(w.chartingbxAA, "chartingbxAA", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)
w_chartingbxBx = db_conv.multi_year_multi_out(w.chartingbxBx, "chartingbxBx", col_params=[(0, "Field")], make_multi="Table", not_num_indexed=True)

dblist = [
    w_aluminadata,
    w_bauxitedata,
    w_meipropdata,
    w_bauxitedata2,
    w_allcbixdata,
    w_cbix55data,
    w_allcbixdatav2,
    w_cbix55datav2,
    w_meidata,
    w_cbixus65,
    w_cbixus55,
    w_cbixus45,
    w_bxdemandcbix55,
    w_basedata,
    w_bauxite,
    w_chartingbxAA,
    w_chartingbxBx
]

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
snapshot_output_data = snapshot_output_data
uploadtodb.upload(snapshot_output_data)

# reverse snapshot
# reversed_data = db_conv.reverse(snapshot_output_data)
# for a in reversed_data['Watermelon charts'].keys():
#     reversed_data['Watermelon charts'][a].to_csv(f"reversed_watermelon/{a}.csv", index=False)

'''
x.meidata.to_csv('outputdata/meidata.csv')
x.cbixus55data.to_csv('outputdata/cbixus55data.csv')
x.plannedimportprod.to_csv('outputdata/plannedimportprod.csv')
x.importsmeidata.to_csv('outputdata/importsmeidata.csv')
x.importscbix55data.to_csv('outputdata/importscbix55data.csv')
x.plannedimportneeddata.to_csv('outputdata/plannedimportneeddata.csv')
x.importsbxmeidata.to_csv('outputdata/importsbxmeidata.csv')
x.importsbxcbix55data.to_csv('outputdata/importsbxcbix55data.csv')'''
