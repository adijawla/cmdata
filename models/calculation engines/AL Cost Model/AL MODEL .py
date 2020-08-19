



import pandas as pd
import numpy as np
import datetime 
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb

ai_flat = Flatdbconverter("Al Cost Model")
class AlCostModel():
    def __init__ (self):
        	
        Quarters=['2013 Q1',	'2013 Q2',	'2013 Q3',	'2013 Q4'	,'2014 Q1',	'2014 Q2',	'2014 Q3',	'2014 Q4',	'2015 Q1',	'2015 Q2',	'2015 Q3',	'2015 Q4',	'2016 Q1',	'2016 Q2',	'2016 Q3',	'2016 Q4'	'2017 Q1'	,'2017 Q2'	,'2017 Q3',	'2017 Q4',	'2018 Q1'	,'2018 Q2',	'2018 Q3',	'2018 Q4',	'2019 Q1',	'2019 Q2',	'2019 Q3',	'2019 Q4',	'2020 Q1']

        column = ['Ref','Smelter','Ch_Smelter','Smelter Short Name',
                          'Group','Continent','Country','Technoloy','State/Province','Average Cell Life,day',
                          'Headcount (workshop)','Ownership','Latitude','Longitude','Energy Source',
                          'Currency','Exchange Rate', 'Capacity','Production',
                          'Production,annualized','Aa Consumption, Kt','Self-supplied','3-rd Party','3-rd Party Average rice, VAT excl',
                          'Unit Ele Consumption, kW.h/t.Al','Grid','Captive','AlF3 Self-supply','Below 300'
                          ,'300','350','400','420','500','600','Below 300','300','350','400',	'420',	'500'	'600']

        self.DataEngine=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Data Engine')
        self.ElectricitySource=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Electricity Source')
        self.Quarter=Quarters
        
        self.DataEngineCal = pd.DataFrame(columns=column)
        
        self.Currency=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Currency')
        self.Smeltercapacity=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Smelter Capacity')
        self.Smelterproduction=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Smelter Production')
        self.AaSupplied=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Aa SelfSupply Ratio')

        self.Aasource=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Aa Source')
        self.CaptivePowerCost=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Captive power cost')
        
        self.ElectricityGrid=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Electricity Grid')
        self.ElectricityConsumption=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Electricity Consumption')

        
        Reference = list(map(str,range(1,194)))
        
        
        self.Summary = pd.DataFrame(index=Reference)
        self.Salary=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Salary')

        self.NaturalGasPrice=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='NaturalGasPrice')
        
        self.CapitalCost=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Capital Cost')
        self.CapitalCost1=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='CapitalCost')
        self.Carbon_Price=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='carbon price')

        self.AlF3_Price=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='AlF3_Price')
        self.CarbonSource=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='Carbon Source')

        self.Aasource1=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='AaSource')
        self.web=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='web3')
    
        self.SelfsuppliedAacost1=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='SelfsuppliedAacost')
        
        self.SelfSuppliedRefinary=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='SelfSuppliedRefinary')

        self.AaselfsupplyRatio=pd.read_excel (r'AL Cost Model.xlsx',sheet_name='AaselfsupplyRatio')

        
    
        
    def CalcAll(self):
        AlCostModel.EnergySource(self)
        AlCostModel.Currency(self)
        AlCostModel.AnnualProduction(self)
        AlCostModel.ExchangeRate(self)
        AlCostModel.SmelterCapacity(self)
        AlCostModel.SmelterProduction(self)
        AlCostModel.AASource(self)
        AlCostModel.SelfSupplied(self)
        AlCostModel.General_Information(self)
        AlCostModel.Yearofcommencement(self)
        
        AlCostModel.Numberofcells(self)
        AlCostModel.Operatingcells(self)
        AlCostModel.Sumnumberofcells(self)
        AlCostModel.Sumoperatingcells(self)
        AlCostModel.ElectricityPriceCaptive(self)
        AlCostModel.ElectricityPriceGrid(self)
        AlCostModel.ModelAnnualProduction(self)
        AlCostModel.CM_Adjustment_Parameter_for_Grid(self)
        AlCostModel.ElectricityConsumed(self)
        AlCostModel.ElectricityReductionCells(self)
        AlCostModel.ElectricityAuxiliaries(self)
        AlCostModel.ElectricityCaptive(self)
        AlCostModel.Electricitysourcegrid(self)
        AlCostModel.Compositeelectricitylocal(self)
        AlCostModel.CompositeelectricityUSD(self)
        AlCostModel.ElectricityCost(self)
        AlCostModel.NaturalGasPrice(self)
        AlCostModel.NaturalGasOperations(self)
        AlCostModel.NaturalGasNG(self)
        AlCostModel.NaturalGasOil(self)
        AlCostModel.NaturalGasOilPrice(self)
        AlCostModel.NaturalGasPricebyGJ(self)
        AlCostModel.DieselPricebyGJ(self)
        AlCostModel.NonElectricityCostNaturalGas(self)
        AlCostModel.NonElectricityCostDiesel(self)
        AlCostModel.NonElectricityCostNonElectricity(self)
        AlCostModel.LabourNBD(self)
        AlCostModel.LabourCMAdjustment(self)
        AlCostModel.LabourPensionsbenefitssocialsecurity(self)
        AlCostModel.BasicWorkingYear(self)
        AlCostModel.HourlyPay(self)
        AlCostModel.HeadCount(self)
        AlCostModel.LabourProductivity(self)
        AlCostModel.LabourCostLocal(self)
        AlCostModel.LabourCostUSD(self)
        AlCostModel.AdministrationCost(self)
        AlCostModel.TotalLaborCost(self)
        
        AlCostModel.OperatingCostAvgCellLife(self)
        AlCostModel.Productionperday(self)
        AlCostModel.Productionpercellperday(self)
        AlCostModel.CellsRebuilt(self)
        AlCostModel.RebuiltCostUS(self)
        AlCostModel.RebuiltCostLocal(self)
        AlCostModel.RebuiltCostUSt(self)
        
        
        AlCostModel.CapitalCostLocal(self)
        AlCostModel.CapitalCostUSD(self)
        AlCostModel.LifeOfPlant(self)
        AlCostModel.YearsOfOperation(self)
        AlCostModel.CapitalkValue(self)
        AlCostModel.OtherMaterialCostUSD(self)
        AlCostModel.OtherMaterialCostLocal(self)
        AlCostModel.AluminiumFlourideInput(self)
        AlCostModel.AluminiumFlourideprice(self)
        AlCostModel.AluminiumFlourideCostsLocal(self)
        AlCostModel.AluminiumFlourideCostsUSD(self)
        AlCostModel.CarbonFreightProcurement(self)
        AlCostModel.CarbonFreightSelfSupplied(self)
        AlCostModel.CarbonPrice3partyProcurement(self)
        AlCostModel.CarbonPriceSelfSupplied(self)
        AlCostModel.CarbonSourceSelfSupplied(self)
        
        '''AlCostModel.CarbonSource3PartyProcurement(self)'''
        
        AlCostModel.FreightSelf(self)
        AlCostModel.AluminaFreightSelf(self)
        AlCostModel.web(self)
        AlCostModel.Aluminapriceselfsupplied(self)
        AlCostModel.Aluminapriceselfsupplied1(self)
        AlCostModel.Aluminasourceselfupplied(self)
        AlCostModel.Aluminasource3Party(self)
        AlCostModel.AluminaInput(self)
        AlCostModel.PMT(self)
        AlCostModel.CapitalkValue(self)
        
        
        AlCostModel.carboninputgross(self)
        AlCostModel.carboninputrecycled(self)
        AlCostModel.carbonNet(self)
        AlCostModel.carboncostlocal(self)
        AlCostModel.carboncostUSD(self)
        AlCostModel.AluminaCost(self)
        AlCostModel.RawMaterials(self)
        
        AlCostModel.Energy(self)
        AlCostModel. OperatingCostLocal(self)
        AlCostModel.Others(self)
        AlCostModel.OperatingCostLocal70(self)
        AlCostModel.OperatingCostLocal30(self)
        AlCostModel.OtherCost(self)
        AlCostModel.CashCost(self)
        AlCostModel.Local(self)
        AlCostModel.LeagueTable(self)
    def EnergySource(self):
        
        Dp_dataengine = self.DataEngine.copy()
        
        Dp_ElecSource=self.ElectricitySource.copy()
        
        Dp_ElecSource=Dp_ElecSource[['Ref','Energy Source']]
        Dp_dataengine=Dp_dataengine.merge(Dp_ElecSource, on='Ref', how='left')
        
        

        self.Dp_DataEngine = Dp_dataengine
    
    def AnnualProduction(self,year='2020'):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_SmelterProduction=self.Smelterproduction.copy()
        Dp_SmelterProduction1=Dp_SmelterProduction['Ref']
        
        Dp_SmelterProduction = Dp_SmelterProduction.filter(regex=year)
        Columns=Dp_SmelterProduction.shape[1]
        
        Dp_SmelterProduction['Annual Producton'] = Dp_SmelterProduction.mean(axis=1)
        
        Dp_SmelterProduction['Annual Producton'] =(Dp_SmelterProduction ['Annual Producton']*4)
        Dp_SmelterProduction['Annual Producton']=Dp_SmelterProduction['Annual Producton'].round(0)
        
        Dp_SmelterProduction=pd.concat([Dp_SmelterProduction1,Dp_SmelterProduction],axis=1)
        
        Dp_SmelterProduction=Dp_SmelterProduction[['Ref','Annual Producton']]
        Dp_dataengine=Dp_dataengine.merge(Dp_SmelterProduction, on='Ref', how='left')
        Dp_dataengine=Dp_dataengine.replace(np.NaN,0)
        
        self.Dp_DataEngine = Dp_dataengine
        
    def Currency(self):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_currency=self.Currency.copy()
       
        Dp_currency=Dp_currency[['Country','Currency']]
        Dp_dataengine=Dp_dataengine.merge(Dp_currency, on='Country', how='left')
        
        

        self.Dp_DataEngine = Dp_dataengine
    
    
    
    def ExchangeRate(self,Quarter='2020 Q1'):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_currency=self.Currency.copy()
       
        Dp_currency=Dp_currency[['Currency',Quarter]]
        Dp_currency = Dp_currency.rename(columns={Quarter: 'Exchange Rate'})

       
        Dp_dataengine=Dp_dataengine.merge(Dp_currency, on='Currency', how='left')
        
        

        self.Dp_DataEngine = Dp_dataengine
        
    def SmelterCapacity(self,Quarter='2020 Q1'):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_SmelterCapacity=self.Smeltercapacity.copy()
       
        Dp_Capacity=Dp_SmelterCapacity[['Ref',Quarter]]
        Dp_Capacity = Dp_Capacity.rename(columns={Quarter: 'Capacity'})

        
        Dp_dataengine=Dp_dataengine.merge(Dp_Capacity, on='Ref', how='left')
        
        

        self.Dp_DataEngine = Dp_dataengine
    def SmelterProduction(self,Quarter='2020 Q1'):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_SmelterProduction=self.Smelterproduction.copy()
       
        Dp_Production=Dp_SmelterProduction[['Ref',Quarter]]
        Dp_Production = Dp_Production.rename(columns={Quarter: 'Production'})

        
        Dp_dataengine=Dp_dataengine.merge(Dp_Production, on='Ref', how='left')
        Dp_dataengine=Dp_dataengine.replace(np.NaN,0)
        

        self.Dp_DataEngine = Dp_dataengine
    def AASource(self,Quarter='2020 Q1'):
        
        
        Dp_Aasource=self.Aasource.copy()
        Dp_Aasupplied=self.AaSupplied.copy()
        
        Dp_Aasupplied=Dp_Aasupplied[['Ref',Quarter]]
        Dp_Aasupplied = Dp_Aasupplied.rename(columns={Quarter: 'Self Supplied'})

        
        Dp_Aasource=Dp_Aasource.merge(Dp_Aasupplied, on='Ref', how='left')
        Dp_Aasource=Dp_Aasource.replace(np.NaN,0)
        

        self.Aasource = Dp_Aasource
    def SelfSupplied(self):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_Aasource=self.Aasource.copy()
        
        Dp_Aasource=Dp_Aasource[['Ref','Self Supplied']]
        

        
        Dp_dataengine=Dp_dataengine.merge(Dp_Aasource, on='Ref', how='left')
        Dp_dataengine=Dp_dataengine.replace(np.NaN,0)
        Dp_dataengine['3-rd Party']=1-Dp_dataengine['Self Supplied']
        
        Dp_dataengine=Dp_dataengine.drop_duplicates()
        self.Dp_DataEngine = Dp_dataengine
    def General_Information(self):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        

        Dp_Summary=self.Summary.copy()
        Dp_Summary['Ref']=Dp_Summary.index
        Dp_dataengine=Dp_dataengine[['Ref','Capacity','Technoloy','Production','Smelter','Latitude','Longitude','Smelter Short Name','Continent','Country','Currency','Exchange Rate','Group','State/Province','Ownership']]
        Dp_Summary['Ref']=Dp_Summary['Ref'].astype(int)
        
        Dp_Summary=Dp_Summary.merge(Dp_dataengine, on='Ref', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)

        
        self.Summary = Dp_Summary
    def Yearofcommencement(self):
        Dp_Smeltercapacity=self.Smeltercapacity.copy()
        Dp_Summary=self.Summary.copy()
        Dp_Smeltercapacity=Dp_Smeltercapacity[['Ref','Year of commence']]
        Dp_Summary=Dp_Summary.merge(Dp_Smeltercapacity, on='Ref', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)

        self.Summary = Dp_Summary

    def ModelAnnualProduction(self):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        
        Dp_Summary=self.Summary.copy()
        Dp_dataengine=Dp_dataengine[['Ref','Annual Producton']]
        
        Dp_Summary=Dp_Summary.merge(Dp_dataengine, on='Ref', how='left') 
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def Numberofcells(self):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        

        Dp_Summary=self.Summary.copy()
        
        Dp_dataengine=Dp_dataengine[['Ref','Cells Below 300', 'Cells 300','Cells 350','Cells 400','Cells 420','Cells 500','Cells 600']]
        Dp_Summary=Dp_Summary.merge(Dp_dataengine, on='Ref', how='left') 
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def Operatingcells(self):
        
        Dp_dataengine = self.Dp_DataEngine.copy()
        

        Dp_Summary=self.Summary.copy()
        
        Dp_dataengine=Dp_dataengine[['Ref','Operating cells Below 300','Operating cells 300','Operating cells 350','Operating cells 400','Operating cells 420','Operating cells 500','Operating cells 600']]
        Dp_Summary=Dp_Summary.merge(Dp_dataengine, on='Ref', how='left') 
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def Sumoperatingcells(self):
        
        Dp_Summary=self.Summary.copy()
        Dp_Summary['Number of Operating Cells']=Dp_Summary['Operating cells Below 300']+Dp_Summary['Operating cells 300']+Dp_Summary['Operating cells 350']+Dp_Summary['Operating cells 400']+Dp_Summary['Operating cells 420']+Dp_Summary['Operating cells 500']+Dp_Summary['Operating cells 600']
       
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    
    def Sumnumberofcells(self):
        
        Dp_Summary=self.Summary.copy()
        Dp_Summary['Number of Cells']=Dp_Summary['Cells Below 300']+Dp_Summary['Cells 300']+Dp_Summary['Cells 350']+Dp_Summary['Cells 400']+Dp_Summary['Cells 420']+Dp_Summary['Cells 500']+Dp_Summary['Cells 600']
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)

        self.Summary = Dp_Summary
    def ElectricityPriceCaptive(self,Quarter='2020 Q1'):
        Dp_Summary=self.Summary.copy()
        Dp_captivepowercost=self.CaptivePowerCost
        
        Dp_captivepowercost=Dp_captivepowercost[['Ref',Quarter]]
        Dp_captivepowercostowercost=Dp_captivepowercost[['Ref',Quarter]]
       
        
        Dp_Summary=Dp_Summary.merge(Dp_captivepowercost, on='Ref', how='left')
        Dp_Summary = Dp_Summary.rename(columns={Quarter: 'Electricity Price Captive'})
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def ElectricityPriceGrid(self,Quarter='2020 Q1'):
        Dp_Summary=self.Summary.copy()
        Dp_ElectricityGrid=self.ElectricityGrid
        Dp_ElectricityGrid=Dp_ElectricityGrid[['Grid',Quarter]]
        Dp_Summary=Dp_Summary.merge(Dp_ElectricityGrid, left_on='State/Province',right_on='Grid', how='left')
        Dp_Summary = Dp_Summary.rename(columns={Quarter: 'Electricity Price Grid'})
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
       
        self.Summary = Dp_Summary
    def CM_Adjustment_Parameter_for_Grid(self):
        Dp_Summary=self.Summary.copy()
        Dp_captivepowercost=self.CaptivePowerCost
        Dp_captivepowercost=Dp_captivepowercost[['Ref','CM Adjustment for Grid']]
        Dp_Summary=Dp_Summary.merge(Dp_captivepowercost, on='Ref', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)

       
        self.Summary = Dp_Summary
    def ElectricityConsumed(self,Quarter='2020 Q1'):
        Dp_Summary=self.Summary.copy()
        Dp_Electricityconsumption=self.ElectricityConsumption
        Dp_Electricityconsumption=Dp_Electricityconsumption[['Ref',Quarter]]
        Dp_Summary=Dp_Summary.merge(Dp_Electricityconsumption, on='Ref', how='left')
        Dp_Summary = Dp_Summary.rename(columns={Quarter: 'Electricity Consumed'})
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        
        
        self.Summary = Dp_Summary
    def ElectricityReductionCells(self):
        Dp_Summary=self.Summary.copy()
        Dp_Summary['Reduction Cells Only (98% of total)']= 0.98*Dp_Summary['Electricity Consumed']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def ElectricityAuxiliaries(self):
    
        Dp_Summary=self.Summary.copy()
        Dp_Summary['Auxiliaries (2% of total)']= 0.02*Dp_Summary['Electricity Consumed']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary

    def ElectricityCaptive(self,Quarter='2020 Q1'):
        
        Dp_Summary = self.Summary.copy()
        
        Dp_Electricitysource=self.ElectricitySource.copy()
       
        Dp_Electricitysource=Dp_Electricitysource[['Ref',Quarter]]
        Dp_Electricitysource = Dp_Electricitysource.rename(columns={Quarter: 'Electricity Source Captive'})

        
        Dp_Summary=Dp_Summary.merge(Dp_Electricitysource, on='Ref', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def Electricitysourcegrid(self):
        
        Dp_Summary = self.Summary.copy()
        
        Dp_Summary=self.Summary.copy()
        Dp_Summary['Electricity Source grid']=1- Dp_Summary['Electricity Source Captive']
       
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def Compositeelectricitylocal(self):
        
        Dp_Summary = self.Summary.copy()
        
       
        Dp_Summary['Composite Electricity Price Local']=(Dp_Summary['Electricity Source Captive']*Dp_Summary['Electricity Price Captive'])+((Dp_Summary['Electricity Source grid']*Dp_Summary['Electricity Price Grid'])*(1-Dp_Summary['CM Adjustment for Grid']))
       
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CompositeelectricityUSD(self):
        
        Dp_Summary = self.Summary.copy()
        
        
        Dp_Summary['Composite Electricity Price USD']=Dp_Summary['Composite Electricity Price Local']/Dp_Summary['Exchange Rate']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        
        self.Summary = Dp_Summary
    def ElectricityCost(self):
        
        Dp_Summary = self.Summary.copy()
        
        
        Dp_Summary['Electricity Cost']=Dp_Summary['Composite Electricity Price USD']*Dp_Summary['Electricity Consumed']
       
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
          
        
    def NaturalGasPrice(self,Quarter='2020 Q1'):
        
        Dp_NaturalGasPrice = self.NaturalGasPrice.copy()
        
        Dp_Summary = self.Summary.copy()
        
       
       
        Dp_NaturalGasPrice=Dp_NaturalGasPrice[['State/Province',Quarter]]
        Dp_NaturalGasPrice = Dp_NaturalGasPrice.rename(columns={Quarter: 'Natural Gas Price'})

        
        Dp_Summary=Dp_Summary.merge(Dp_NaturalGasPrice, on='State/Province', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary  
    def NaturalGasOperations(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Non Electricity Other Operations']=0.19
        self.Summary = Dp_Summary  
    def NaturalGasNG(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Non Electricity Natural Gas']=1
        self.Summary = Dp_Summary  
    
    def NaturalGasOil(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Non Electricity Oil']=0
        self.Summary = Dp_Summary
    def NaturalGasOilPrice(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Non Electricity Oil Price']=0
        self.Summary = Dp_Summary
    def NaturalGasPricebyGJ(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Natural Gas, 0.035 GJ/m3']=(Dp_Summary['Natural Gas Price']/Dp_Summary['Exchange Rate']/0.035)
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def DieselPricebyGJ(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Diesel, 44.43 GJ/L']=(Dp_Summary['Non Electricity Oil Price']/Dp_Summary['Exchange Rate']/0.035)
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def NonElectricityCostNaturalGas(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['NonElectricity Natural Gas']= (Dp_Summary['Non Electricity Natural Gas']*Dp_Summary['Non Electricity Other Operations']* Dp_Summary['Natural Gas, 0.035 GJ/m3'])
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def NonElectricityCostDiesel(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['NonElectricity Diesel']=(Dp_Summary['Non Electricity Other Operations']*Dp_Summary['Non Electricity Oil']*Dp_Summary['Diesel, 44.43 GJ/L'])
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def NonElectricityCostNonElectricity(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['NonElectricity NonElectricity']=(Dp_Summary['NonElectricity Diesel']+Dp_Summary['NonElectricity Natural Gas'])
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def LabourNBD(self,Quarter='2020 Q1'):
        Dp_Summary = self.Summary.copy()
        Dp_Salary= self.Salary.copy()
         
       
        Dp_Salary=Dp_Salary[['NBS',Quarter]]
        Dp_Salary = Dp_Salary.rename(columns={Quarter: 'Yearly Salary (NBS)'})

        
        Dp_Summary=Dp_Summary.merge(Dp_Salary, left_on='State/Province',right_on='NBS', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def LabourCMAdjustment(self):
        Dp_Summary = self.Summary.copy()
        Dp_Salary= self.Salary.copy()
         
       
        Dp_Salary=Dp_Salary[['NBS','Adjustment']]
        Dp_Salary = Dp_Salary.rename(columns={'Adjustment': 'CM Adjustment Parameter'})

        
        Dp_Summary=Dp_Summary.merge(Dp_Salary, left_on='State/Province',right_on='NBS', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def LabourPensionsbenefitssocialsecurity(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Pensions, benefits, social security']=Dp_Summary['Yearly Salary (NBS)']*((1+Dp_Summary['CM Adjustment Parameter'])/3)*0.35
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def BasicWorkingYear(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Full basic working year']=2000
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def HourlyPay(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Hourly Pay']=(Dp_Summary['Yearly Salary (NBS)']+Dp_Summary['Pensions, benefits, social security'])/Dp_Summary['Full basic working year']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def HeadCount(self):
        Dp_Summary = self.Summary.copy()
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_dataengine=Dp_dataengine[['Ref','Headcount (workshop)']]
        Dp_dataengine= Dp_dataengine.rename(columns={'Headcount (workshop)':'Employees Head Count'})

        
        Dp_Summary=Dp_Summary.merge(Dp_dataengine,on='Ref', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def LabourProductivity(self):
        Dp_Summary = self.Summary.copy()
       
        Dp_Summary['Labor Productivity']=Dp_Summary['Employees Head Count']*(Dp_Summary['Full basic working year']/(1000*Dp_Summary['Annual Producton']))
        Dp_Summary =Dp_Summary.replace([np.inf, -np.inf], np.nan)

        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def LabourCostLocal(self):
        Dp_Summary = self.Summary.copy()
       
        Dp_Summary['Labor Cost local']=Dp_Summary['Hourly Pay']*Dp_Summary['Labor Productivity']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def LabourCostUSD(self):
        Dp_Summary = self.Summary.copy()
       
        Dp_Summary['Labor Cost USD']=Dp_Summary['Labor Cost local']/Dp_Summary['Exchange Rate']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def AdministrationCost(self):
        Dp_Summary = self.Summary.copy()
        for i in range(Dp_Summary.shape[0]): 
            if Dp_Summary.loc[i,'Ownership']=='Central SOE':
                
                Dp_Summary.loc[i,'Administration Cost']=0.2
            if Dp_Summary.loc[i,'Ownership']=='Provincial SOE':
            
                Dp_Summary.loc[i,'Administration Cost']=0.15
        
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0.1)
        self.Summary = Dp_Summary
    def TotalLaborCost(self):
        Dp_Summary = self.Summary.copy()
       
        Dp_Summary['Total Labor Cost']=(Dp_Summary['Labor Cost USD']*(1+Dp_Summary['Administration Cost']))
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def OperatingCostAvgCellLife(self):
        Dp_Summary = self.Summary.copy()
        Dp_dataengine = self.Dp_DataEngine.copy()
        Dp_dataengine =Dp_dataengine [['Ref','Average Cell Life,day']]
        Dp_Summary=Dp_Summary.merge(Dp_dataengine, on='Ref', how='left')
        
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def Productionperday(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Production per day']=(Dp_Summary['Annual Producton']/365)
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    
    
    def Productionpercellperday(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Production per cell per day']=(Dp_Summary['Production per day']/Dp_Summary['Number of Operating Cells'])
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CellsRebuilt(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Cells Rebuilt']=(Dp_Summary['Number of Operating Cells'])/(Dp_Summary['Average Cell Life,day']/365)
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def RebuiltCostUS(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Rebuilt Cost USD per cell']=92000
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    
    def RebuiltCostLocal(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Rebuilt Cost Local per cell']=Dp_Summary['Rebuilt Cost USD per cell']*Dp_Summary['Exchange Rate']
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    
    def RebuiltCostUSt(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Rebuilt Cost US/T']=(Dp_Summary['Rebuilt Cost USD per cell']*(Dp_Summary['Cells Rebuilt']/(Dp_Summary['Annual Producton']*1000)))
        Dp_Summary =Dp_Summary.replace([np.inf, -np.inf], np.nan)
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary

    
    def OperatingCostUS(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Operating cost USD']=Dp_Summary['Operating cost local']/Dp_Summary['Exchange Rate']
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def OperatingCostLocal(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Operating cost local']=(((Dp_Summary['Raw Materials Local']+Dp_Summary['Energy Local'])/0.85)*0.13)
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def OperatingCostLocal70(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Maintenance (70%)']=Dp_Summary['Operating cost local']*0.7
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def OperatingCostLocal30(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Other (30%)']=Dp_Summary['Operating cost local']*0.3
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def OtherCost(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Operating cost local']=Dp_Summary['Operating cost local']
        Dp_Summary['Other Cost (Operating)']=Dp_Summary['Operating cost local']+Dp_Summary['Rebuilt Cost US/T']
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CapitalCostLocal(self):
        Dp_Summary = self.Summary.copy()
        Dp_CapitalCost =self.CapitalCost.copy()
        Dp_CapitalCost=Dp_CapitalCost.transpose()
        Dp_CapitalCost=Dp_CapitalCost.reset_index() 
        new_header = Dp_CapitalCost.iloc[0] 
        Dp_CapitalCost = Dp_CapitalCost[1:] 
        Dp_CapitalCost.columns = new_header 
        print(Dp_CapitalCost)
        Dp_Summary=Dp_Summary.merge(Dp_CapitalCost, on='Ref', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CapitalCostUSD(self):
        Dp_Summary = self.Summary.copy()
        
        Dp_CapitalCost1 =self.CapitalCost1.copy()
        Dp_Summary=Dp_Summary.merge(Dp_CapitalCost1, on='State/Province', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        Dp_Summary['Capital Cost 1']=Dp_Summary['Capital Cost']/Dp_Summary['Exchange Rate']
        Dp_Summary['Capital Cost 2']=Dp_Summary['Value']/Dp_Summary['Exchange Rate']
        df=Dp_Summary['Ref']
       
        Dp_Summary['val']=0
        for i in range(df.shape[0]):
            if Dp_Summary.loc[i,'Capital Cost']>Dp_Summary.loc[i,'val']:
                Dp_Summary.loc[i,'Capital Cost USD']=Dp_Summary.loc[i,'Capital Cost 1']
            else:
                Dp_Summary.loc[i,'Capital Cost USD']=Dp_Summary.loc[i,'Capital Cost 2']
           
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def LifeOfPlant(self):
        Dp_Summary = self.Summary.copy()
        
        for i in range(Dp_Summary.shape[0]):
            if Dp_Summary.loc[i,'Greenfield/Brownfield']=='B':
                Dp_Summary.loc[i,'Life of Plant']=10
            else:
                Dp_Summary.loc[i,'Life of Plant']=15
           
        
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def YearsOfOperation(self):
        Dp_Summary = self.Summary.copy()
        current_time = datetime.datetime.now() 
        Dp_Summary['Years of Operation']=current_time.year-Dp_Summary['Year of commence']
         

        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    
    def OtherMaterialCostUSD(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Other Material Cost USD']=24
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def OtherMaterialCostLocal(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Other Material Cost Local']=Dp_Summary['Other Material Cost USD']*Dp_Summary['Exchange Rate']
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def AluminiumFlourideInput(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Aluminium fluoride input']= 0.02 

        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    
    def AluminiumFlourideprice(self,Quarter='2020 Q1'):
        Dp_Summary=self.Summary.copy()
        Dp_ALF3=self.AlF3_Price.copy()
        Dp_ALF3=Dp_ALF3[['State/Province',Quarter]]
        Dp_Summary=Dp_Summary.merge(Dp_ALF3, on='State/Province', how='left')
        Dp_Summary = Dp_Summary.rename(columns={Quarter: 'Aluminium fluoride price'})
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def AluminiumFlourideCostsLocal(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Aluminium fluoride costs Local']= Dp_Summary['Aluminium fluoride input']*Dp_Summary['Aluminium fluoride price']
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def AluminiumFlourideCostsUSD(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Aluminium fluoride costs USD']= Dp_Summary['Aluminium fluoride costs Local']/Dp_Summary['Exchange Rate']
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CarbonFreightProcurement(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Carbon Freight Procurement']=100
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CarbonFreightSelfSupplied(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Carbon Freight Self Supplied']=0
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CarbonPrice3partyProcurement(self,Quarter='2020 Q1'):
        Dp_Summary = self.Summary.copy()
        Dp_Carbon_Price=self.Carbon_Price.copy()
       
       
        Dp_Carbon_Price=Dp_Carbon_Price[['State/Province',Quarter]]
        Dp_Summary=Dp_Summary.merge(Dp_Carbon_Price, on='State/Province', how='left')
        Dp_Summary = Dp_Summary.rename(columns={Quarter: 'Carbon Price 3-rd Party procurement'})
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def CarbonPriceSelfSupplied(self):
        Dp_Summary = self.Summary.copy()
       
        Dp_Summary['Carbon Price Self-supplied']=Dp_Summary['Carbon Price 3-rd Party procurement']*0.8
       
        
        self.Summary = Dp_Summary
    def CarbonSourceSelfSupplied(self,Quarter='2020 Q1'):
        Dp_Summary = self.Summary.copy()
        Dp_CarbonSource=self.CarbonSource.copy()
       
       
        Dp_CarbonSource=Dp_CarbonSource[['Ref',Quarter]]
        Dp_Summary=Dp_Summary.merge(Dp_CarbonSource, on='Ref', how='left')
        Dp_Summary = Dp_Summary.rename(columns={Quarter: 'Carbon Source Self Supplied'})
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary 
    
    def FreightSelf(self):
        Dp_Aasource1 = self.Aasource1.copy()
        Dp_Aasource = self.Aasource.copy()
        Dp_dataengine = self.DataEngine.copy()
        Dp_dataengine=Dp_dataengine.merge(Dp_Aasource1, on='State/Province', how='left')
        Dp_dataengine=Dp_dataengine.replace(np.NaN,0)
        
        Dp_dataengine['FreightSelf']=(Dp_dataengine['Guangxi']*Dp_Aasource['Guangxi self'])+(Dp_dataengine['Shandong']*Dp_Aasource['Shandong self'])+(Dp_dataengine['Henan']*Dp_Aasource['Henan self'])+(Dp_dataengine['Shanxi']*Dp_Aasource['Shanxi self'])+(Dp_dataengine['Inner Mongolia']*Dp_Aasource['Inner Mongolia self'])+(Dp_dataengine['Guizhou']*Dp_Aasource['Guizhou self '])+(Dp_dataengine['Chongqing']*Dp_Aasource['Chongqing self '])
        Dp_dataengine['Freight3rd']=(Dp_dataengine['Guangxi']*Dp_Aasource['Guangxi'])+(Dp_dataengine['Shandong']*Dp_Aasource['Shandong'])+(Dp_dataengine['Henan']*Dp_Aasource['Henan'])+(Dp_dataengine['Shanxi']*Dp_Aasource['Shanxi'])+(Dp_dataengine['Inner Mongolia']*Dp_Aasource['Inner Mongolia'])+(Dp_dataengine['Guizhou']*Dp_Aasource['Guizhou'])+(Dp_dataengine['Chongqing']*Dp_Aasource['Chongqing'])
        Dp_dataengine=Dp_dataengine[['Ref','FreightSelf','Freight3rd']]
        self.Dp_DataEngine = Dp_dataengine 
    def AluminaFreightSelf(self):
        Dp_Summary = self.Summary.copy()
        Dp_dataengine = self.Dp_DataEngine.copy()
        
        Dp_Summary=Dp_Summary.merge(Dp_dataengine, on='Ref', how='left')
        print('Al')
        Dp_Summary = Dp_Summary.rename(columns={'FreightSelf': 'Alumina Freight Self','Freight3rd': 'Alumina Freight 3-rd Party procurement'})
        Dp_Summary['Alumina Freight Self']=Dp_Summary['Alumina Freight Self']/1.05
        Dp_Summary['Alumina Freight 3-rd Party procurement']=Dp_Summary['Alumina Freight 3-rd Party procurement']/1.05
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        print(Dp_Summary)
        self.Summary = Dp_Summary 
    def web(self,Quarter='2020 Q1'):
        Dp_Aasource = self.Aasource.copy()
        Dp_web3 = self.web.copy()
        Dp_web3=Dp_web3[['State/Province',Quarter]]
        Dp_web3 = Dp_web3.rename(columns={Quarter: 'Aa web Value'})
        Dp_web3=Dp_web3.transpose()
        new_header =Dp_web3.iloc[0] 
        Dp_web3 = Dp_web3[1:] 
        Dp_web3.columns = new_header
        Dp_web3=pd.DataFrame(Dp_web3)
        Dp_Aasource['Outsource Price,RMB/t']=((Dp_web3["Guangxi"].iloc[0]*Dp_Aasource["Guangxi"])+(Dp_web3["Shandong"].iloc[0]*Dp_Aasource["Shandong"])+(Dp_web3["Henan"].iloc[0]*Dp_Aasource["Henan"])+(Dp_web3["Shanxi"].iloc[0]*Dp_Aasource["Shanxi"])+(Dp_web3["Guizhou"].iloc[0]*Dp_Aasource["Guizhou"])+(Dp_web3["Yunnan"].iloc[0]*Dp_Aasource["Yunnan"])+(Dp_web3["Chongqing"].iloc[0]*Dp_Aasource["Chongqing"]))/1.13
        
       
        Dp_Aasource=Dp_Aasource.replace(np.NaN,0)
        self.Dp_web3=Dp_web3
        self.Aasource = Dp_Aasource
    def Aluminapriceselfsupplied(self):
        Dp_Summary = self.Summary.copy()
        Dp_Aasource = self.Aasource.copy()
        Dp_Aasource=Dp_Aasource[['Ref','Outsource Price,RMB/t']]
        Dp_Aasource = Dp_Aasource.rename(columns={'Outsource Price,RMB/t': 'Alumina Price Self Supplied'})
        Dp_Summary=Dp_Summary.merge(Dp_Aasource,on='Ref', how='left')
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def Aluminapriceselfsupplied1(self,Quarter='2020 Q1'):
        SelfsuppliedAacost1 = self.SelfsuppliedAacost1.copy()
        SelfSuppliedRefinary = self.SelfSuppliedRefinary.copy()
        
        SelfSuppliedRefinary=SelfSuppliedRefinary[['Refinary',Quarter]]
        
        SelfsuppliedAacost1=SelfsuppliedAacost1.merge(SelfSuppliedRefinary,left_on='Refinery1',right_on='Refinary', how='left')
        SelfsuppliedAacost1 = SelfsuppliedAacost1.rename(columns={Quarter: 'Cost'})
        for i in range(SelfsuppliedAacost1.shape[0]):
            if SelfsuppliedAacost1.loc[i,'Self-supply']==0:
                SelfsuppliedAacost1.loc[i,'Cost']=0
       
        
        SelfsuppliedAacost1['Aa_cost']= ((SelfsuppliedAacost1['Cost']*SelfsuppliedAacost1['Ratio1'])+(SelfsuppliedAacost1['Cost']*SelfsuppliedAacost1['Ratio2'])  )
        SelfsuppliedAacost1=SelfsuppliedAacost1.replace(np.NaN,0)
        Dp_Summary = self.Summary.copy()
        SelfsuppliedAacost1=SelfsuppliedAacost1[['Ref','Aa_cost']]
        Dp_Summary=Dp_Summary.merge(SelfsuppliedAacost1,on='Ref', how='left')
        
        self.Summary=Dp_Summary
    def Aluminasourceselfupplied(self,Quarter='2020 Q1'):
        AaselfsupplyRatio = self.AaselfsupplyRatio.copy()
        Dp_Summary = self.Summary.copy()
        
        AaselfsupplyRatio=AaselfsupplyRatio[['Ref',Quarter]]
        AaselfsupplyRatio = AaselfsupplyRatio.rename(columns={Quarter: 'Alumina Source Self Supplied'})
        
        Dp_Summary=Dp_Summary.merge(AaselfsupplyRatio , on='Ref', how='left')

        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        
        self.Summary =Dp_Summary
    def Aluminasource3Party(self,Quarter='2020 Q1'):
        
        Dp_Summary = self.Summary.copy()
        
        Dp_Summary['Alumina Third Party Procurement']=1-Dp_Summary['Alumina Source Self Supplied']
        

        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        
        self.Summary =Dp_Summary
    def AluminaInput(self):
        
        Dp_Summary = self.Summary.copy()
        
        Dp_Summary['Alumina Input']=1.93

        

        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        
        self.Summary =Dp_Summary
    def PMT(self):
        
        Dp_Summary = self.Summary.copy()
        
        Dp_Summary['PMT1']=(-1*np.pmt(Dp_Summary['Finance charge (Intererest rate)'],Dp_Summary['Life of Plant'],Dp_Summary['Capital Cost USD']))
        Dp_Summary['PMT']=((Dp_Summary['PMT1'])*(((Dp_Summary['Life of Plant']+(Dp_Summary['Construction period']/2))/Dp_Summary['Life of Plant'])))

        self.Summary =Dp_Summary
    
    def CapitalkValue(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Capital K Value']=1
        Dp_Summary['Temp']=Dp_Summary['Life of Plant']-Dp_Summary['Years of Operation']
        for i in range(Dp_Summary.shape[0]):
            if Dp_Summary.loc[i,'Greenfield/Brownfield']!='B' and Dp_Summary.loc[i,'Temp']>=0 :
                Dp_Summary['Capital K Value']=0.25
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def carboncostlocal(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['carbon Cost Local']=(((Dp_Summary['Carbon Freight Self Supplied']+Dp_Summary['Carbon Price Self-supplied'])*Dp_Summary['Carbon Source Self Supplied'])+((Dp_Summary['Carbon Freight Procurement']+Dp_Summary['Carbon Price 3-rd Party procurement'])*(1-Dp_Summary['Carbon Source Self Supplied']))*(Dp_Summary['Carbon Net']))
        
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def carboncostUSD(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['carbon Cost USD']=Dp_Summary['carbon Cost Local']/Dp_Summary['Exchange Rate']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def carboninputgross(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Capacity']=Dp_Summary['Capacity'].astype(float)
        t1=0.5
        t2=0.48
        t3=0.45
        t4=0.4
        Dp_Summary['Carbon Gross']=t4
        for i in range(Dp_Summary.shape[0]):
            if Dp_Summary.loc[i,'Capacity']<=200 :
                print('Sup1')
                Dp_Summary.loc[i,'Carbon Gross']=t1
            if Dp_Summary.loc[i,'Capacity']>200 and Dp_Summary.loc[i,'Capacity']<=500 :
                print('Sup2')
                Dp_Summary.loc[i,'Carbon Gross']=t2
            if Dp_Summary.loc[i,'Capacity']>500 and Dp_Summary.loc[i,'Capacity']<=1000:
                print('Sup3')
                
                Dp_Summary.loc[i,'Carbon Gross']=t3
            
                
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def carboninputrecycled(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Carbon Recycled']= Dp_Summary['Carbon Gross']*0.19
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def carbonNet(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Carbon Net']= Dp_Summary['Carbon Gross']-Dp_Summary['Carbon Recycled']
        Dp_Summary=Dp_Summary.replace(np.NaN,0)
        self.Summary = Dp_Summary
    def AluminaCost(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Alumina Local']=(((( Dp_Summary['Aa_cost']+Dp_Summary['Alumina Freight Self'])*Dp_Summary['Alumina Source Self Supplied'])+((Dp_Summary['Alumina Freight 3-rd Party procurement']+Dp_Summary['Alumina Price Self Supplied'])*Dp_Summary['Alumina Third Party Procurement']))*Dp_Summary['Alumina Input'])
        Dp_Summary['Alumina USD']=Dp_Summary['Alumina Local']/Dp_Summary['Exchange Rate']
        self.Summary = Dp_Summary
    def RawMaterials(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Raw Materials']=Dp_Summary['Alumina USD']+Dp_Summary['carbon Cost USD']+Dp_Summary['Aluminium fluoride costs USD']+Dp_Summary['Other Material Cost USD']
        Dp_Summary['Raw Materials Local']=Dp_Summary['Raw Materials']*Dp_Summary['Exchange Rate']
      
        self.Summary = Dp_Summary
    def Others(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Others']=Dp_Summary['Total Labor Cost']+Dp_Summary['Rebuilt Cost US/T']+Dp_Summary['Operating cost local']
        Dp_Summary['Others Local']=Dp_Summary['Others']*Dp_Summary['Exchange Rate']
      


        self.Summary = Dp_Summary
    def Energy(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Energy']=Dp_Summary['Electricity Cost']+Dp_Summary['NonElectricity NonElectricity']
        Dp_Summary['Energy Local']=Dp_Summary['Energy']*Dp_Summary['Exchange Rate']
      


        self.Summary = Dp_Summary
    def CashCost(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Cash Cost']=Dp_Summary['Energy']++Dp_Summary['Raw Materials']+Dp_Summary['Others']
        Dp_Summary['Cash Cost Local']=Dp_Summary['Cash Cost']*Dp_Summary['Exchange Rate']
      



        self.Summary = Dp_Summary
    def Local(self):
        Dp_Summary = self.Summary.copy()
        Dp_Summary['Electricity Cost Local ']=Dp_Summary['Electricity Cost']*Dp_Summary['Exchange Rate']
        Dp_Summary[' Non Electricity Cost Local ']=Dp_Summary['NonElectricity NonElectricity']*Dp_Summary['Exchange Rate']
        Dp_Summary[' Labor Cost Local ']=Dp_Summary['Total Labor Cost']*Dp_Summary['Exchange Rate']
        Dp_Summary[' Rebuilt Cost Local ']=Dp_Summary['Rebuilt Cost US/T']*Dp_Summary['Exchange Rate']
        Dp_Summary[' Operating cost local ']=Dp_Summary['Operating cost local']*Dp_Summary['Exchange Rate']



        self.Summary = Dp_Summary
    def LeagueTable(self):
        Dp_Summary1 = self.Summary.copy()

        Dp_Summary1=Dp_Summary1[['Ref','Smelter','Country','State/Province',
                                 'Annual Producton','Raw Materials Local','Others Local',
                                 'Energy Local']]
        


        self.Summary1 = Dp_Summary1


 
y=AlCostModel()       
y.CalcAll()
snapshot_output_data = pd.DataFrame(columns=ai_flat.out_col)

y.Summary1.to_excel("OP test.xlsx",sheet_name='Sheet2',encoding='utf-8',index=False)
y.Summary.to_excel("OP test1.xlsx",sheet_name='Sheet2',encoding='utf-8',index=False)

db_list = [
    ai_flat.single_year_mult_out(y.Summary1, "AL model output"),
    ai_flat.single_year_mult_out(y.Summary, "AL model output 1")
]
 
snapshot_output_data = pd.concat(db_list, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, ai_flat.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv")
uploadtodb.upload(snapshot_output_data)
        
        