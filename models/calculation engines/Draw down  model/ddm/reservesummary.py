import pandas as pd
import numpy as np

#from ddm.economic import Inventory
import warnings
import time
from ddm.codetimer.timer import Timer
warnings.filterwarnings("ignore")
#calc15,16,17 after bf6
#

summary_timer = Timer("Reserve Summary", txt=True)
class summary():
    def __init__(self):
        summary_timer.start()
        self.db = pd.read_csv('ddm/output.csv',encoding = "utf-8")
        self.reservedb = pd.read_csv('ddm/reserve.csv',encoding = "utf-8")
        self.lookupdb = pd.read_csv('ddm/DomesticBARsCumulative.csv',encoding = "utf-8")
        self.staticdb = pd.read_csv('ddm/bauxite_allocations.csv',encoding = "utf-8")
        self.hdb = pd.read_csv('ddm/hhdb.csv',encoding = "utf-8")
        self.gradeAlO3 = 0.7
        self.gradeAS = 9
        self.extractable_to_SGA_input = 0.7
        self.reserve_data_to_use = "current"
        self.totaldb = pd.read_csv("ddm/totaldb.csv",encoding = "utf-8")
        self.provt = pd.DataFrame(columns = ["Province","Inventory"])
        col = pd.read_csv('ddm/caustic.csv',encoding = "utf-8")
        self.col = list(col['refinery'][::-1])
        self.bfdb = pd.DataFrame(columns = ["Name","Province","year","estimated_reserves_consumed_per_year","cumulative_factored_up_production" ],index=range(len(self.col)+70))
        self.l = self.reservedb.shape[0]-1
        self.pr = self.reservedb["Province"]
        self.pr= list(self.pr)
        idx = pd.IndexSlice
        mxidd = pd.MultiIndex.from_product([self.col,self.pr])
        self.newallocdb = pd.read_csv("ddm/newcalallo.csv",encoding = "utf-8")
        self.db["deep bauxite"] = self.reservedb["deep bauxite"]
        self.db['Include Inventory in Allocations Further Processed?'] = self.reservedb["Include Inventory in Allocations Further Processed?"]
        self.bfdb["cumulative_factored_up_production"] = self.bfdb["cumulative_factored_up_production"].astype(float)
        self.bfdb["estimated_reserves_consumed_per_year"] = self.bfdb["estimated_reserves_consumed_per_year"].astype(float)
        summary_timer.stop()

    def calc0(self,i):
        summary_timer.start()
        v = 1/(1-self.staticdb['Refractory Allocation'][i]) if self.staticdb['Refractory Allocation'][i] > 0 else 1
        self.db.at[i,'correction_factor']= v
        summary_timer.stop()
    def calc1(self,i): #minable_shallow_inventory
        summary_timer.start()
        v = self.reservedb['Inventory'][i]-self.reservedb['deep bauxite'][i]
        self.db.at[i,"minable_shallow_inventory"] = v
        summary_timer.stop()
    def calc2(self,i):#refractory_allocation
        summary_timer.start()
        v = self.reservedb['Inventory'][i]*self.staticdb['Refractory Allocation'][i] 
        self.db.at[i,"refractory_allocation"] = v
        summary_timer.stop()
    def calc3(self,i):#remaining_inventory_available_to_mining_for_SGA000t
        summary_timer.start()
        v = self.db["minable_shallow_inventory"][i] - self.db["refractory_allocation"][i]
        
        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA000t"] = v
        
        summary_timer.stop()
    def calc4(self,i):#remaining_inventory_available_to_mining_for_SGA_AA and working_stock_prior_to_AA_and_AS_de_rating_AA
        summary_timer.start()
        v = (self.db["minable_shallow_inventory"][i]*self.reservedb["AI203"][i] -  self.db["refractory_allocation"][i]*self.gradeAlO3)/self.db["remaining_inventory_available_to_mining_for_SGA000t"][i] # column need to be added "bauxite gradeAI203"
        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA_AA"] = v
        self.db.at[i,"working_stock_prior_to_AA_and_AS_de_rating_AA"] = v
        summary_timer.stop()
    def calc5(self,i):#working_stock_prior_to_AA_and_AS_de_rating_AS
        summary_timer.start()
        v = self.db["remaining_inventory_available_to_mining_for_SGA_AA"][i]/((self.db["minable_shallow_inventory"][i]*self.reservedb["AI203"][i]/self.reservedb["AS"][i] - self.db["refractory_allocation"][i]*self.gradeAlO3/self.gradeAS)/self.db["remaining_inventory_available_to_mining_for_SGA000t"][i])
        
        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA_AS"] = v
        self.db.at[i,"working_stock_prior_to_AA_and_AS_de_rating_AS"] = v
        summary_timer.stop()
    def calc6(self,i):#remaining_inventory_available_to_mining_for_SGA_SiO2
        summary_timer.start()
        v =  self.db["remaining_inventory_available_to_mining_for_SGA_AA"][i]/self.db["remaining_inventory_available_to_mining_for_SGA_AS"][i]
        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA_SiO2"] = v
        summary_timer.stop()
    def calc7(self,i):#extractable_to_SGA
        summary_timer.start()
        v = 0.7 if self.reservedb["Province"][i] == "Guangxi" else 0.6 if self.reservedb["Province"][i] == "Guizhou" else 1.0 if self.reservedb["Province"][i] == "Shandong" and self.reservedb["County"][i] != "Zouwu" else self.extractable_to_SGA_input # column needed to be added "extractable_to_SGA_input"
        self.db.at[i,"extractable_to_SGA"] = v
        summary_timer.stop()
    def calc8(self,i):#working_stock_prior_to_AA_and_AS_de_rating000t
        summary_timer.start()
        v = (self.db["minable_shallow_inventory"][i] - self.db["refractory_allocation"][i])*self.db["extractable_to_SGA"][i] if self.reserve_data_to_use == "current" else 0
        
        self.db.at[i,"working_stock_prior_to_AA_and_AS_de_rating000t"] = v
        summary_timer.stop()
    def calc9(self,i):#de_rated_Al2O3
        summary_timer.start()
        v = self.db["working_stock_prior_to_AA_and_AS_de_rating_AA"][i]*self.reservedb["de-rating factor Al2O3"][i] if self.reserve_data_to_use == "current" else 0
        self.db.at[i,"de_rated_Al2O3"] = v
        summary_timer.stop()
    def calc10(self,i):#de_rated_AS
        summary_timer.start()
        v = self.db["working_stock_prior_to_AA_and_AS_de_rating_AS"][i]*self.reservedb["de-rating factor AS"][i] if self.reserve_data_to_use == "current" else 0
        self.db.at[i,"de_rated_AS"] = v
        summary_timer.stop()
    def calc11(self,i):#de_rated_SiO2
        summary_timer.start()
        v = self.db["de_rated_Al2O3"][i]/self.db["de_rated_AS"][i] if self.db["de_rated_AS"][i] > 0 else 0
        self.db.at[i,"de_rated_SiO2"] = v
        summary_timer.stop()
    def calc12(self,i):#province_county_000twstockprior
        summary_timer.start()
        v = str(self.reservedb["Province"][i])+"-"+str(self.reservedb["County"][i])+"-"+str(float(self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]))
        self.db.loc[i,"province_county_000twstockprior"] = v
        summary_timer.stop()
    def calc13(self,i):#refractory_depletion_end_2009
        summary_timer.start()
        v = self.db["refractory_allocation"].sum()/self.db["refractory_depletion_end_2009_000t"].sum() if self.db["refractory_depletion_end_2009_000t"].sum() > 0 else 0 #AD25
        self.db.at[i,"refractory_depletion_end_2009"] = v
        summary_timer.stop()
    def calc14(self,i):#refractory_depletion_end_2009_000t
        summary_timer.start()
        v = self.db["refractory_depletion_end_2009"][i]*self.db["working_stock_prior_to_AA_and_AS_de_rating000t"].sum()
        self.db.at[i,"refractory_depletion_end_2009_000t"] = v
        summary_timer.stop()
    def calc15(self,i):#SGA_depletion_end_2009
        summary_timer.start()
        x = self.staticdb.iloc[i,2:10000:8].reset_index(drop=True)
        y = self.bfdb["estimated_reserves_consumed_per_year"].reset_index(drop=True)
        z = x*y
        z = z.sum()
        v = min(1,z)/(1-self.staticdb['Refractory Allocation'][i])
        
        self.db.at[i,"SGA_depletion_end_2009"] = v
        if self.reservedb["Province"][i] == "Shandong" and self.reservedb["County"][i] != "Zouwu":
            self.db.at[i,"SGA_depletion_end_2009"] = 1.00
        if self.reservedb["Include in Endowment in Further Processing? (Provinces only)"][i] == "N1":
            self.db.at[i,"SGA_depletion_end_2009"] = 0.0
        summary_timer.stop()
    def calc16(self,i):#SGA_depletion_end_2009_000t
        summary_timer.start()
        v = self.db["SGA_depletion_end_2009"][i]*self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]
        self.db.at[i,"SGA_depletion_end_2009_000t"] = v
        summary_timer.stop()
    def calc17(self,i):#endowment_end_2009_prov_totals
        summary_timer.start()
        v = max(0,self.reservedb["GeologicalEndowment"][i]-self.db["refractory_depletion_end_2009_000t"][i]-self.db["SGA_depletion_end_2009_000t"][i])
        v0 = self.reservedb["GeologicalEndowment"][i]
        v1 = 0 if type(self.db["refractory_depletion_end_2009_000t"][i]) != float else self.db["refractory_depletion_end_2009_000t"][i]
        v2 = 0 if pd.isnull(self.db["SGA_depletion_end_2009_000t"][i]) else self.db["SGA_depletion_end_2009_000t"][i]
        v3 = max(0,v0-v1-v2)
        self.db.at[i,"endowment_end_2009_prov_totals"] = v3
        summary_timer.stop()
    def calc18(self,i):#geol_endowment_end_2009
        summary_timer.start()
        v = self.reservedb["GeologicalEndowment"][i] - (self.db.loc[self.db.Province==self.db["Province"][i]]["refractory_depletion_end_2009_000t"].sum()+self.db.loc[self.db.Province==self.db["Province"][i]]["SGA_depletion_end_2009_000t"].sum()) # sum if
        self.db.at[i,"geol_endowment_end_2009"] = v
        summary_timer.stop()
    def calc19(self,i):#endowment_end_2009_all
        summary_timer.start()
        v = 0 if self.reservedb["County"][i] == np.nan or self.reservedb["County"][i] == self.reservedb["County"][i+1] else "something"
        self.db.loc[i,"endowment_end_2009_all"] = v
        summary_timer.stop()
    def calc20(self,i):#max_extr_inventory_end_2009_1
        summary_timer.start()
        v = self.db["max_extr_inventory_end_2009_2"][i]+self.db["max_extr_inventory_end_2009_3"][i]
        self.db.at[i,"max_extr_inventory_end_2009_1"] = v
        summary_timer.stop()
    def calc21(self,i):#AA_end_2009_1
        summary_timer.start()
        v = (self.db["max_extr_inventory_end_2009_2"][i]*self.db["AA_end_2009_2"][i]+self.db["max_extr_inventory_end_2009_3"][i]*self.db["AA_end_2009_3"][i])/self.db["max_extr_inventory_end_2009_1"][i] if self.db["max_extr_inventory_end_2009_1"][i] > 0 else 0
        self.db.at[i,"AA_end_2009_1"] = v
        summary_timer.stop()
    def calc22(self,i):#SiO2_end_2009_1
        summary_timer.start()
        v = (self.db["max_extr_inventory_end_2009_2"][i]*self.db["SiO2_end_2009_2"][i]+self.db["max_extr_inventory_end_2009_3"][i]*self.db["SiO2_end_2009_3"][i])/self.db["max_extr_inventory_end_2009_1"][i] if self.db["max_extr_inventory_end_2009_1"][i] > 0 else 0
        self.db.at[i,"SiO2_end_2009_1"] = v
        summary_timer.stop()
    def calc23(self,i):#AS_end_2009_1
        summary_timer.start()
        v = self.db["AA_end_2009_1"][i]/self.db["SiO2_end_2009_1"][i] if self.db["SiO2_end_2009_1"][i] > 0 else 0
        self.db.at[i,"AS_end_2009_1"] = v
        summary_timer.stop()
    def calc24(self,i):#max_extr_inventory_end_2009_2
        summary_timer.start()
        v = self.db["refractory_allocation"][i]-self.db["refractory_depletion_end_2009_000t"][i]
        self.db.at[i,"max_extr_inventory_end_2009_2"] = v    
        summary_timer.stop()
    def calc25(self,i):#AA_end_2009_2
        summary_timer.start()
        v = self.gradeAlO3
        self.db.at[i,"AA_end_2009_2"] = v
        summary_timer.stop()
    def calc26(self,i):#AS_end_2009_2
        summary_timer.start()
        v = self.gradeAS
        self.db.at[i,"AS_end_2009_2"] = v
        summary_timer.stop()
    def calc27(self,i):#SiO2_end_2009_2
        summary_timer.start()
        v = self.db["AA_end_2009_2"][i]/self.db["AS_end_2009_2"][i] if self.db["AS_end_2009_2"][i] > 0 else 0
        self.db.at[i,"SiO2_end_2009_2"] = v
        summary_timer.stop()
    def calc28(self,i):#max_extr_inventory_end_2009_3
        summary_timer.start()
        v = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i] - self.db["SGA_depletion_end_2009_000t"][i]
        self.db.at[i,"max_extr_inventory_end_2009_3"] = v    
        summary_timer.stop()
    def calc29(self,i):#AA_end_2009_3
        summary_timer.start()
        v0 = self.db["SGA_depletion_end_2009"][i]#af
        v1 = self.db["de_rated_Al2O3"][i]#z
        v2 = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]#u
        v = ((v1*v2)-(((v1+0.02)+((v1+0.02)-v0*0.04))/2)*v0*v2)/((1-v0)*v2) if 1-v0 > 0 else 0
        self.db.at[i,"AA_end_2009_3"] = v
        summary_timer.stop()
    def calc30(self,i):#SiO2_end_2009_3
        summary_timer.start()
        v0 = self.db["SGA_depletion_end_2009"][i]
        v1 = self.db["de_rated_SiO2"][i]
        v2 = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]
        v = ((v1*v2)-(((v1-0.03)+((v1-0.03)+v0*0.06))/2)*v0*v2)/((1-v0)*v2) if 1-v0 > 0 else 0
        self.db.at[i,"SiO2_end_2009_3"] = v
        summary_timer.stop()
    def calc31(self,i):#AS_end_2009_3
        summary_timer.start()
        v = self.db["AA_end_2009_3"][i]/self.db["SiO2_end_2009_3"][i] if self.db["SiO2_end_2009_3"][i] > 0 else 0
        self.db.at[i,"AS_end_2009_3"] = v
        summary_timer.stop()
    def calc32(self,i):#unallocated
        summary_timer.start()
        v = 0 if self.staticdb.iloc[i,2:834:8].sum() > 0 else self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]
        self.db.at[i,"unallocated"] = v
        if self.reservedb["Province"][i] == "Shandong":
            self.db.at[i,"unallocated"] = 0
        summary_timer.stop()
    def calc33(self,i):#check_sum
        summary_timer.start()
        v = self.staticdb.iloc[i,2:834:8].sum() + self.staticdb['Refractory Allocation'][i]
        self.db.at[i,"check_sum"] = v
    
    # horizontal table starts here
        summary_timer.stop()
    def calc34(self,ref):# Allocation
        summary_timer.start()
        self.hdb[ref+" Allocation"] = self.hdb[ref+" Allocation"].astype(float)
        v0=self.db.loc[self.db.Province=="Guangxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guangxi"]["correction_factor"]*self.staticdb[self.staticdb.province=="Guangxi"][ref+" Allocation"]
        v0 = v0.sum() if type(v0) != int else 0
        v1=self.db.loc[self.db.Province=="Guizhou"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guizhou"]["correction_factor"]*self.staticdb[self.staticdb.province=="Guizhou"][ref+" Allocation"] 
        v1 = v1.sum() if type(v1) != int else 0
        v2=self.db.loc[self.db.Province=="Henan"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Henan"]["correction_factor"]*self.staticdb[self.staticdb.province=="Henan"][ref+" Allocation"]
        v2 = v2.sum() if type(v2) != int else 0
        v3=self.db.loc[self.db.Province=="Shanxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Shanxi"]["correction_factor"]*self.staticdb[self.staticdb.province=="Shanxi"][ref+" Allocation"]
        v3 = v3.sum() if type(v3) != int else 0
        v5=self.db["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db["correction_factor"]*self.staticdb[ref+" Allocation"]
        v5 = v5.sum() if type(v5) != int else 0
        self.hdb.at[0,ref+" Allocation"] = v0
        self.hdb.at[1,ref+" Allocation"] = v1
        self.hdb.at[2,ref+" Allocation"] = v2
        self.hdb.at[3,ref+" Allocation"] = v3
        self.hdb.at[5,ref+" Allocation"] = v5
        v4=self.hdb[ref+" Allocation"][5]-(self.hdb[ref+" Allocation"][0]+self.hdb[ref+" Allocation"][1]+self.hdb[ref+" Allocation"][2]+self.hdb[ref+" Allocation"][3])
        v4 = 0 if v4 < 0.000001 else v4
        self.hdb.at[4,ref+" Allocation"] = v4
        
        summary_timer.stop()
    def calc35(self,ref):#Closed
        summary_timer.start()
        self.hdb[ref+" Closed"] = self.hdb[ref+" Closed"].astype(float)
        v0=0 if self.hdb[ref+" Allocation"][0] == 0 else self.db.loc[self.db.Province=="Guangxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guangxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Closed"]/self.hdb[ref+" Allocation"][0]
        v0 = v0.sum() if type(v0) != int else 0
        v1=0 if self.hdb[ref+" Allocation"][1] == 0 else self.db.loc[self.db.Province=="Guizhou"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guizhou"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Closed"]/self.hdb[ref+" Allocation"][1]
        v1 = v1.sum() if type(v1) != int else 0
        v2=0 if self.hdb[ref+" Allocation"][2] == 0 else self.db.loc[self.db.Province=="Henan"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Henan"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Closed"]/self.hdb[ref+" Allocation"][2]
        v2 = v2.sum() if type(v2) != int else 0
        v3=0 if self.hdb[ref+" Allocation"][3] == 0 else self.db.loc[self.db.Province=="Shanxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Shanxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Closed"]/self.hdb[ref+" Allocation"][3]
        v3 = v3.sum() if type(v3) != int else 0
        v5= 0 if self.hdb[ref+" Allocation"][5] == 0 else self.db["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Closed"]/self.hdb[ref+" Allocation"][5]
        v5 = v5.sum() if type(v5) != int else 0
        self.hdb.at[0,ref+" Closed"] = v0
        self.hdb.at[1,ref+" Closed"] = v1
        self.hdb.at[2,ref+" Closed"] = v2
        self.hdb.at[3,ref+" Closed"] = v3
        self.hdb.at[5,ref+" Closed"] = v5
        x = self.hdb[ref+" Allocation"][:4]*self.hdb[ref+" Closed"][:4]
        v4= 0 if self.hdb[ref+" Allocation"][4] == 0 else (v5*self.hdb[ref+" Allocation"][5]-x.sum())/self.hdb[ref+" Allocation"][4]
        self.hdb.at[4,ref+" Closed"] = v4
        
        summary_timer.stop()
    def calc36(self,ref):#Purchased
        summary_timer.start()
        self.hdb[ref+" Purchased"] = self.hdb[ref+" Purchased"].astype(float)
        v0=0 if self.hdb[ref+" Allocation"][0] == 0 else 1-self.hdb[ref+" Closed"][0]
        v0 = v0.sum() if type(v0) != int else 0
        v1=0 if self.hdb[ref+" Allocation"][1] == 0 else 1-self.hdb[ref+" Closed"][1] 
        v1 = v1.sum() if type(v1) != int else 0
        v2=0 if self.hdb[ref+" Allocation"][2] == 0 else 1-self.hdb[ref+" Closed"][2]
        v2 = v2.sum() if type(v2) != int else 0
        v3=0 if self.hdb[ref+" Allocation"][3] == 0 else 1-self.hdb[ref+" Closed"][3]
        v3 = v3.sum() if type(v3) != int else 0
        v4=0 if self.hdb[ref+" Allocation"][4] == 0 else 1-self.hdb[ref+" Closed"][4]
        v4 = v4.sum() if type(v4) != int else 0
        v5=0 if self.hdb[ref+" Allocation"][5] == 0 else 1-self.hdb[ref+" Closed"][5]
        v5 = v5.sum() if type(v5) != int else 0
        self.hdb.at[0,ref+" Purchased"] = v0
        self.hdb.at[1,ref+" Purchased"] = v1
        self.hdb.at[2,ref+" Purchased"] = v2
        self.hdb.at[3,ref+" Purchased"] = v3
        self.hdb.at[4,ref+" Purchased"] = v4
        self.hdb.at[5,ref+" Purchased"] = v5
        summary_timer.stop()
    def calc37(self,ref):#avg distance
        summary_timer.start()
        self.hdb[ref+" Avg Distance"] = self.hdb[ref+" Avg Distance"].astype(float)
        v0=0 if self.hdb[ref+" Allocation"][0] == 0 else self.db.loc[self.db.Province=="Guangxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guangxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Avg Distance"]/self.hdb[ref+" Allocation"][0]
        v0 = v0.sum() if type(v0) != int else 0
        v1=0 if self.hdb[ref+" Allocation"][1] == 0 else self.db.loc[self.db.Province=="Guizhou"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guizhou"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Avg Distance"]/self.hdb[ref+" Allocation"][1]
        v1 = v1.sum() if type(v1) != int else 0
        v2=0 if self.hdb[ref+" Allocation"][2] == 0 else self.db.loc[self.db.Province=="Henan"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Henan"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Avg Distance"]/self.hdb[ref+" Allocation"][2]
        v2 = v2.sum() if type(v2) != int else 0
        v3=0 if self.hdb[ref+" Allocation"][3] == 0 else self.db.loc[self.db.Province=="Shanxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Shanxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Avg Distance"]/self.hdb[ref+" Allocation"][3]
        v3=v3.sum() if type(v3) != int else 0
        v5=0 if self.hdb[ref+" Allocation"][5] == 0 else self.db["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db["correction_factor"]*self.staticdb[ref+" Allocation"]*self.staticdb[ref+" Avg Distance"]/self.hdb[ref+" Allocation"][5]
        v5 = v5.sum() if type(v5) != int else 0
        self.hdb.at[0,ref+" Avg Distance"] = v0
        self.hdb.at[1,ref+" Avg Distance"] = v1
        self.hdb.at[2,ref+" Avg Distance"] = v2
        self.hdb.at[3,ref+" Avg Distance"] = v3
        self.hdb.at[5,ref+" Avg Distance"] = v5
        x = self.hdb[ref+" Allocation"][:4]*self.hdb[ref+" Avg Distance"][:4]
        v4=0 if self.hdb[ref+" Allocation"][4] == 0 else (v5*self.hdb[ref+" Allocation"][5] - x.sum())/self.hdb[ref+" Allocation"][4]
        v4 = v4.sum() if type(v4) != int else 0
        self.hdb.at[4,ref+" Avg Distance"] = v4
        
        summary_timer.stop()
    def calc38(self,ref):#avg Al2O3
        summary_timer.start()
        self.hdb[ref+" Avg %Al2O3"] = self.hdb[ref+" Avg %Al2O3"].astype(float)
        v0=0 if self.hdb[ref+" Allocation"][0] == 0 else self.db.loc[self.db.Province=="Guangxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guangxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_Al2O3"]/self.hdb[ref+" Allocation"][0]
        v0 = v0.sum() if type(v0) != int else 0
        v1=0 if self.hdb[ref+" Allocation"][1] == 0 else self.db.loc[self.db.Province=="Guizhou"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guizhou"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_Al2O3"]/self.hdb[ref+" Allocation"][1] 
        v1 = v1.sum() if type(v1) != int else 0
        v2=0 if self.hdb[ref+" Allocation"][2] == 0 else self.db.loc[self.db.Province=="Henan"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Henan"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_Al2O3"]/self.hdb[ref+" Allocation"][2]
        v2 = v2.sum() if type(v2) != int else 0
        v3=0 if self.hdb[ref+" Allocation"][3] == 0 else self.db.loc[self.db.Province=="Shanxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Shanxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_Al2O3"]/self.hdb[ref+" Allocation"][3]
        v3 = v3.sum() if type(v3) != int else 0
        v5=0 if self.hdb[ref+" Allocation"][5] == 0 else self.db["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_Al2O3"]/self.hdb[ref+" Allocation"][5]
        v5 = v5.sum() if type(v5) != int else 0
        self.hdb.at[0,ref+" Avg %Al2O3"] = v0
        self.hdb.at[1,ref+" Avg %Al2O3"] = v1
        self.hdb.at[2,ref+" Avg %Al2O3"] = v2
        self.hdb.at[3,ref+" Avg %Al2O3"] = v3
        self.hdb.at[5,ref+" Avg %Al2O3"] = v5
        x = self.hdb[ref+" Allocation"][:4]*self.hdb[ref+" Avg %Al2O3"][:4]
        v4=0 if self.hdb[ref+" Allocation"][4] == 0 else (v5*self.hdb[ref+" Allocation"][5] - x.sum())/self.hdb[ref+" Allocation"][4]
        v4 = v4.sum() if type(v4) != int else 0
        self.hdb.at[4,ref+" Avg %Al2O3"] = v4
        
        summary_timer.stop()
    def calc39(self,ref):#avg SiO2
        summary_timer.start()
        self.hdb[ref+" Avg %SiO2"] = self.hdb[ref+" Avg %SiO2"].astype(float)
        v0=0 if self.hdb[ref+" Allocation"][0] == 0 else self.db.loc[self.db.Province=="Guangxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guangxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_SiO2"]/self.hdb[ref+" Allocation"][0]
        v0 = v0.sum() if type(v0) != int else 0
        v1=0 if self.hdb[ref+" Allocation"][1] == 0 else self.db.loc[self.db.Province=="Guizhou"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Guizhou"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_SiO2"]/self.hdb[ref+" Allocation"][1] 
        v1=v1.sum() if type(v1) != int else 0
        v2=0 if self.hdb[ref+" Allocation"][2] == 0 else self.db.loc[self.db.Province=="Henan"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Henan"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_SiO2"]/self.hdb[ref+" Allocation"][2]
        v2=v2.sum() if type(v2) != int else 0
        v3=0 if self.hdb[ref+" Allocation"][3] == 0 else self.db.loc[self.db.Province=="Shanxi"]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province=="Shanxi"]["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_SiO2"]/self.hdb[ref+" Allocation"][3]
        v3=v3.sum() if type(v3) != int else 0
        v5=0 if self.hdb[ref+" Allocation"][5] == 0 else self.db["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db["correction_factor"]*self.staticdb[ref+" Allocation"]*self.db["de_rated_SiO2"]/self.hdb[ref+" Allocation"][5]
        v5 = v5.sum() if type(v5) != int else 0
        self.hdb.at[0,ref+" Avg %SiO2"] = v0
        self.hdb.at[1,ref+" Avg %SiO2"] = v1
        self.hdb.at[2,ref+" Avg %SiO2"] = v2
        self.hdb.at[3,ref+" Avg %SiO2"] = v3
        self.hdb.at[5,ref+" Avg %SiO2"] = v5
        x =self.hdb[ref+" Allocation"][:4]*self.hdb[ref+" Avg %SiO2"][:4]
        v4=0 if self.hdb[ref+" Allocation"][4] == 0 else (v5*self.hdb[ref+" Allocation"][5] - x.sum())/self.hdb[ref+" Allocation"][4]
        v4 = v4.sum() if type(v4) != int else 0
        self.hdb.at[4,ref+" Avg %SiO2"] = v4 

        summary_timer.stop()
    def calc40(self,ref):#avg AS
        summary_timer.start()
        self.hdb[ref+" Avg A/S"] = self.hdb[ref+" Avg A/S"].astype(float)
        v0=0 if self.hdb[ref+" Allocation"][0] == 0 else self.hdb[ref+" Avg %Al2O3"][0]/self.hdb[ref+" Avg %SiO2"][0]
        v0 = v0.sum() if type(v0) != int else 0
        v1=0 if self.hdb[ref+" Allocation"][1] == 0 else self.hdb[ref+" Avg %Al2O3"][1]/self.hdb[ref+" Avg %SiO2"][1]  
        v1 = v1.sum() if type(v1) != int else 0
        v2=0 if self.hdb[ref+" Allocation"][2] == 0 else self.hdb[ref+" Avg %Al2O3"][2]/self.hdb[ref+" Avg %SiO2"][2] 
        v2 = v2.sum() if type(v2) != int else 0
        v3=0 if self.hdb[ref+" Allocation"][3] == 0 else self.hdb[ref+" Avg %Al2O3"][3]/self.hdb[ref+" Avg %SiO2"][3] 
        v3 = v3.sum() if type(v3) != int else 0
        v4=0 if self.hdb[ref+" Allocation"][4] == 0 else self.hdb[ref+" Avg %Al2O3"][4]/self.hdb[ref+" Avg %SiO2"][4] 
        v4 = v4.sum() if type(v4) != int else 0
        v5=0 if self.hdb[ref+" Allocation"][5] == 0 else self.hdb[ref+" Avg %Al2O3"][5]/self.hdb[ref+" Avg %SiO2"][5] 
        v5 = v5.sum() if type(v5) != int else 0
        self.hdb.at[0,ref+" Avg A/S"] = v0
        self.hdb.at[1,ref+" Avg A/S"] = v1
        self.hdb.at[2,ref+" Avg A/S"] = v2
        self.hdb.at[3,ref+" Avg A/S"] = v3
        self.hdb.at[4,ref+" Avg A/S"] = v4
        self.hdb.at[5,ref+" Avg A/S"] = v5
    # horizontal table ends here
    

    #total of each column sarts here
        summary_timer.stop()
    def calc410(self):
        summary_timer.start()
        self.totaldb["deep bauxite"] = self.totaldb["deep bauxite"].astype(float)
        v = self.db["deep bauxite"].sum()
        self.totaldb.at[0,"deep bauxite"] = v
        summary_timer.stop()
    def calc41(self):
        summary_timer.start()
        self.totaldb["minable_shallow_inventory"] = self.totaldb["minable_shallow_inventory"].astype(float)
        v = self.db["minable_shallow_inventory"].sum()
        self.totaldb.at[0,"minable_shallow_inventory"] = v
        summary_timer.stop()
    def calc42(self):
        summary_timer.start()
        self.totaldb["refractory_allocation"] = self.totaldb["refractory_allocation"].astype(float)
        v = self.db["refractory_allocation"].sum()
        self.totaldb.at[0,"refractory_allocation"] = v
        
        summary_timer.stop()
    def calc43(self):
        summary_timer.start()
        self.totaldb["remaining_inventory_available_to_mining_for_SGA000t"] = self.totaldb["remaining_inventory_available_to_mining_for_SGA000t"].astype(float)
        v = self.db["remaining_inventory_available_to_mining_for_SGA000t"].sum()
        self.totaldb.at[0,"remaining_inventory_available_to_mining_for_SGA000t"] = v
        
        summary_timer.stop()
    def calc44(self):
        summary_timer.start()
        self.totaldb["remaining_inventory_available_to_mining_for_SGA_AA"] = self.totaldb["remaining_inventory_available_to_mining_for_SGA_AA"].astype(float)
        self.totaldb["working_stock_prior_to_AA_and_AS_de_rating_AA"] = self.totaldb["working_stock_prior_to_AA_and_AS_de_rating_AA"].astype(float)
        v = self.db["remaining_inventory_available_to_mining_for_SGA_AA"]*self.db["remaining_inventory_available_to_mining_for_SGA000t"]
        v = v.sum()/self.totaldb["remaining_inventory_available_to_mining_for_SGA000t"][0]
        self.totaldb.at[0,"remaining_inventory_available_to_mining_for_SGA_AA"] = v
        self.totaldb.at[0,"working_stock_prior_to_AA_and_AS_de_rating_AA"] = v
        
        summary_timer.stop()
    def calc45(self):
        summary_timer.start()
        self.totaldb["remaining_inventory_available_to_mining_for_SGA_SiO2"] = self.totaldb["remaining_inventory_available_to_mining_for_SGA_SiO2"].astype(float)
        v = self.db["remaining_inventory_available_to_mining_for_SGA_SiO2"]*self.db["remaining_inventory_available_to_mining_for_SGA000t"]
        v = v.sum()/self.totaldb["remaining_inventory_available_to_mining_for_SGA000t"][0]
        self.totaldb.at[0,"remaining_inventory_available_to_mining_for_SGA_SiO2"] = v
        
        summary_timer.stop()
    def calc46(self):
        summary_timer.start()
        self.totaldb["remaining_inventory_available_to_mining_for_SGA_AS"] = self.totaldb["remaining_inventory_available_to_mining_for_SGA_AS"].astype(float)
        self.totaldb["working_stock_prior_to_AA_and_AS_de_rating_AS"] = self.totaldb["working_stock_prior_to_AA_and_AS_de_rating_AS"].astype(float)
        v = self.totaldb["remaining_inventory_available_to_mining_for_SGA_AA"][0]/self.totaldb["remaining_inventory_available_to_mining_for_SGA_SiO2"][0]
        self.totaldb.at[0,"remaining_inventory_available_to_mining_for_SGA_AS"] = v
        self.totaldb.at[0,"working_stock_prior_to_AA_and_AS_de_rating_AS"] = v
        
        summary_timer.stop()
    def calc47(self):
        summary_timer.start()
        self.totaldb["working_stock_prior_to_AA_and_AS_de_rating000t"] = self.totaldb["working_stock_prior_to_AA_and_AS_de_rating000t"].astype(float)
        v = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"].iloc[:161].sum()
        self.totaldb.at[0,"working_stock_prior_to_AA_and_AS_de_rating000t"] = v

        
        summary_timer.stop()
    def calc48(self):
        summary_timer.start()
        self.totaldb["extractable_to_SGA"] = self.totaldb["extractable_to_SGA"].astype(float)
        v = self.totaldb["working_stock_prior_to_AA_and_AS_de_rating000t"][0]/self.totaldb["remaining_inventory_available_to_mining_for_SGA000t"][0]
        self.totaldb.at[0,"extractable_to_SGA"] = v
        
        summary_timer.stop()
    def calc49(self):
        summary_timer.start()
        self.totaldb["de_rated_Al2O3"] = self.totaldb["de_rated_Al2O3"].astype(float)
        v = self.db["de_rated_Al2O3"].iloc[:161]*self.db["working_stock_prior_to_AA_and_AS_de_rating000t"].iloc[:161]
        v = v.sum()/self.totaldb["working_stock_prior_to_AA_and_AS_de_rating000t"][0]
         
        self.totaldb.at[0,"de_rated_Al2O3"] = v
        
        summary_timer.stop()
    def calc50(self):
        summary_timer.start()
        self.totaldb["de_rated_SiO2"] = self.totaldb["de_rated_SiO2"].astype(float)
        v = self.db["de_rated_SiO2"].iloc[:161]*self.db["working_stock_prior_to_AA_and_AS_de_rating000t"].iloc[:161]
        v = v.sum()/self.totaldb["working_stock_prior_to_AA_and_AS_de_rating000t"][0]
        self.totaldb.at[0,"de_rated_SiO2"] = v
        summary_timer.stop()
    def calc51(self):
        summary_timer.start()
        self.totaldb["de_rated_AS"] = self.totaldb["de_rated_AS"].astype(float)
        v = self.totaldb["de_rated_Al2O3"][0]/self.totaldb["de_rated_SiO2"][0]
        self.totaldb.at[0,"de_rated_AS"] = v
        
        summary_timer.stop()
    def calc52(self):
        summary_timer.start()
        v = self.db["province_county_000twstockprior"].sum()
        self.totaldb.at[0,"province_county_000twstockprior"] = v
        
        summary_timer.stop()
    def calc53(self):
        summary_timer.start()
        self.totaldb["refractory_depletion_end_2009"] = self.totaldb["refractory_depletion_end_2009"].astype(float)
        v = self.db["refractory_depletion_end_2009"].sum()
        self.totaldb.at[0,"refractory_depletion_end_2009"] = v
        
        summary_timer.stop()
    def calc54(self):
        summary_timer.start()
        self.totaldb["refractory_depletion_end_2009_000t"] = self.totaldb["refractory_depletion_end_2009_000t"].astype(float)
        v = self.db["refractory_depletion_end_2009_000t"].sum()
        self.totaldb.at[0,"refractory_depletion_end_2009_000t"] = v
        
        summary_timer.stop()
    def calc55(self):
        summary_timer.start()
        self.totaldb["SGA_depletion_end_2009"] = self.totaldb["SGA_depletion_end_2009"].astype(float)
        v = self.db["SGA_depletion_end_2009"].iloc[:161]*self.db["working_stock_prior_to_AA_and_AS_de_rating000t"].iloc[:161]
        v = v.sum()/self.totaldb["working_stock_prior_to_AA_and_AS_de_rating000t"][0]
        self.totaldb.at[0,"SGA_depletion_end_2009"] = v
        
        summary_timer.stop()
    def calc56(self):
        summary_timer.start()
        self.totaldb["SGA_depletion_end_2009_000t"] = self.totaldb["SGA_depletion_end_2009_000t"].astype(float)
        v = self.totaldb["working_stock_prior_to_AA_and_AS_de_rating000t"][0]*self.totaldb["SGA_depletion_end_2009"][0]
        self.totaldb.at[0,"SGA_depletion_end_2009_000t"] = v
        
        summary_timer.stop()
    def calc57(self):
        summary_timer.start()
        self.totaldb["endowment_end_2009_prov_totals"] = self.totaldb["endowment_end_2009_prov_totals"].astype(float)
        v = self.db["endowment_end_2009_prov_totals"].sum()
        self.totaldb.at[0,"endowment_end_2009_prov_totals"] = v
        
        summary_timer.stop()
    def calc58(self):
        summary_timer.start()
        self.totaldb["geol_endowment_end_2009"] = self.totaldb["geol_endowment_end_2009"].astype(float)
        v = self.db["geol_endowment_end_2009"].sum()
        self.totaldb.at[0,"geol_endowment_end_2009"] = v
        
        summary_timer.stop()
    def calc59(self):
        summary_timer.start()
        self.totaldb["max_extr_inventory_end_2009_1"] = self.totaldb["max_extr_inventory_end_2009_1"].astype(float)
        v = self.db["max_extr_inventory_end_2009_1"].sum()
        self.totaldb.at[0,"max_extr_inventory_end_2009_1"] = v
        
        summary_timer.stop()
    def calc60(self):
        summary_timer.start()
        self.totaldb["endowment_end_2009_all"] = self.totaldb["endowment_end_2009_all"].astype(float)
        v = self.totaldb["max_extr_inventory_end_2009_1"][0]/1000
        self.totaldb.at[0,"endowment_end_2009_all"] = v
        
        summary_timer.stop()
    def calc61(self):
        summary_timer.start()
        self.totaldb["AA_end_2009_1"] = self.totaldb["AA_end_2009_1"].astype(float)
        v = self.db["AA_end_2009_1"]*self.db["max_extr_inventory_end_2009_1"]
        v = v.sum()/self.totaldb["max_extr_inventory_end_2009_1"][0]
        self.totaldb.at[0,"AA_end_2009_1"] = v
        
        summary_timer.stop()
    def calc62(self):
        summary_timer.start()
        self.totaldb["SiO2_end_2009_1"] = self.totaldb["SiO2_end_2009_1"].astype(float)
        v = self.db["SiO2_end_2009_1"]*self.db["max_extr_inventory_end_2009_1"]
        v = v.sum()/self.totaldb["max_extr_inventory_end_2009_1"][0]
        
        self.totaldb.at[0,"SiO2_end_2009_1"] = v
        
        summary_timer.stop()
    def calc63(self):
        summary_timer.start()
        self.totaldb["AS_end_2009_1"] = self.totaldb["AS_end_2009_1"].astype(float)
        v = self.totaldb["AA_end_2009_1"][0]/self.totaldb["SiO2_end_2009_1"][0]
        self.totaldb.at[0,"AS_end_2009_1"] = v
        
        summary_timer.stop()
    def calc64(self):
        summary_timer.start()
        self.totaldb["max_extr_inventory_end_2009_2"] = self.totaldb["max_extr_inventory_end_2009_2"].astype(float)
        v = self.db["max_extr_inventory_end_2009_2"].sum()
        self.totaldb.at[0,"max_extr_inventory_end_2009_2"] = v
        
        summary_timer.stop()
    def calc65(self):
        summary_timer.start()
        self.totaldb["AA_end_2009_2"] = self.totaldb["AA_end_2009_2"].astype(float)
        v = self.gradeAlO3
        self.totaldb.at[0,"AA_end_2009_2"] = v
        
        summary_timer.stop()
    def calc66(self):
        summary_timer.start()
        self.totaldb["AS_end_2009_2"] = self.totaldb["AS_end_2009_2"].astype(float)
        v = self.gradeAS
        self.totaldb.at[0,"AS_end_2009_2"] = v
        
        summary_timer.stop()
    def calc67(self):
        summary_timer.start()
        self.totaldb["SiO2_end_2009_2"] = self.totaldb["SiO2_end_2009_2"].astype(float)
        v = self.totaldb["AA_end_2009_2"][0]/self.totaldb["AS_end_2009_2"][0] if self.totaldb["AS_end_2009_2"][0] > 0 else 0
        self.totaldb.at[0,"SiO2_end_2009_2"] = v
        
        summary_timer.stop()
    def calc68(self):
        summary_timer.start()
        self.totaldb["max_extr_inventory_end_2009_3"] = self.totaldb["max_extr_inventory_end_2009_3"].astype(float)
        v = self.db["max_extr_inventory_end_2009_3"].sum()
        self.totaldb.at[0,"max_extr_inventory_end_2009_3"] = v
        
        summary_timer.stop()
    def calc69(self):
        summary_timer.start()
        self.totaldb["AA_end_2009_3"] = self.totaldb["AA_end_2009_3"].astype(float)
        v = self.db["AA_end_2009_3"]*self.db["max_extr_inventory_end_2009_3"]
        v = v.sum()/self.totaldb["max_extr_inventory_end_2009_3"][0]
        self.totaldb.at[0,"AA_end_2009_3"] = v
        summary_timer.stop()

    def calc70(self):
        summary_timer.start()
        self.totaldb["SiO2_end_2009_3"] = self.totaldb["SiO2_end_2009_3"].astype(float)
        v = self.db["SiO2_end_2009_3"]*self.db["max_extr_inventory_end_2009_3"]
        v = v.sum()/self.totaldb["max_extr_inventory_end_2009_3"][0]
        self.totaldb.at[0,"SiO2_end_2009_3"] = v
        summary_timer.stop()

    def calc71(self):
        summary_timer.start()
        self.totaldb["AS_end_2009_3"] = self.totaldb["AS_end_2009_3"].astype(float)
        v = self.totaldb["AA_end_2009_3"][0]/self.totaldb["SiO2_end_2009_3"][0]
        self.totaldb.at[0,"AS_end_2009_3"] = v
        summary_timer.stop()

    def calc72(self):
        summary_timer.start()
        self.totaldb["unallocated"] = self.totaldb["unallocated"].astype(float)
        v = self.db["unallocated"].sum()
        self.totaldb.at[0,"unallocated"] = v
        summary_timer.stop(end=True)
    #totalends here

    # provincial starts here
        
    def provincialtotalcalc(self,i):
        summary_timer.start()
        v0 = self.reservedb["Province"][i]
        v1 = self.reservedb['Inventory'][i]
        v2 = self.reservedb['GeologicalEndowment'][i]
        v4 = self.db.loc[self.db.Province == v0]["deep bauxite"].sum()
        v5 = self.db.loc[self.db.Province == v0]["minable_shallow_inventory"].sum()
        v3 = v1-v5-v4
        v6 = self.db.loc[self.db.Province == v0]["refractory_allocation"].sum()
        v7 = self.db.loc[self.db.Province == v0]["remaining_inventory_available_to_mining_for_SGA000t"].sum()
        v9 = self.db.loc[self.db.Province == v0]["working_stock_prior_to_AA_and_AS_de_rating000t"].sum()
        v8 = v7-v9
        v10 = 0 if v9 == 0 else self.db.loc[self.db.Province == v0]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province == v0]["remaining_inventory_available_to_mining_for_SGA_AA"]
        v10 = 0 if v9 == 0 else v10.sum()/v9
        v11 = 0 if v9 == 0 else self.db.loc[self.db.Province == v0]["working_stock_prior_to_AA_and_AS_de_rating000t"]*self.db.loc[self.db.Province == v0]["remaining_inventory_available_to_mining_for_SGA_SiO2"]
        v11 = 0 if v9 == 0 else v10/(v11.sum()/v9)
        v12 = self.db.loc[self.db.Province == v0]["de_rated_Al2O3"]*self.db.loc[self.db.Province == v0]["working_stock_prior_to_AA_and_AS_de_rating000t"]
        v12 = v12.sum()/v9 if v9 > 0 else 0
        v14 = self.db.loc[self.db.Province == v0]["de_rated_SiO2"]*self.db.loc[self.db.Province == v0]["working_stock_prior_to_AA_and_AS_de_rating000t"]
        v14 = v14.sum()/v9 if v9 > 0 else 0
        v13 = v12/v14 if v14 > 0 and v9 >= 0 else 0
        v15 = self.db.loc[self.db.Province == v0]["refractory_depletion_end_2009"].sum()
        v16 = self.db.loc[self.db.Province == v0]["SGA_depletion_end_2009_000t"].sum()
        v17 = self.db.loc[self.db.Province == v0]["geol_endowment_end_2009"].sum()
        v18 = self.db.loc[self.db.Province == v0]["max_extr_inventory_end_2009_1"].sum()
        v19 = self.db.loc[self.db.Province == v0]["max_extr_inventory_end_2009_1"]*self.db.loc[self.db.Province == v0]["AA_end_2009_1"]
        v19 = v19.sum()/v18 if v18 > 0 else 0
        v20 = self.db.loc[self.db.Province == v0]["max_extr_inventory_end_2009_1"]*self.db.loc[self.db.Province == v0]["SiO2_end_2009_1"]
        v20 = v20.sum()/v18 if v18 > 0 else 0
        v21 = v19/v20 if v20 > 0 else 0
        v22 = self.db.loc[self.db.Province == v0]["max_extr_inventory_end_2009_2"].sum()
        v23 = self.totaldb["AA_end_2009_2"][0] if v22 > 0 else 0 
        v24 = self.totaldb["AS_end_2009_2"][0] if v22 > 0 else 0 
        v25 = self.totaldb["SiO2_end_2009_2"][0] if v22 > 0 else 0 
        v26 = self.db.loc[self.db.Province == v0]["max_extr_inventory_end_2009_3"].sum()
        v27 = self.db.loc[self.db.Province == v0]["max_extr_inventory_end_2009_3"]*self.db.loc[self.db.Province == v0]["AA_end_2009_3"]/v26 if v26 > 0 else 0
        if type(v27) != int:
            v27 = v27.sum()
        v28 = self.db.loc[self.db.Province == v0]["max_extr_inventory_end_2009_3"]*self.db.loc[self.db.Province == v0]["SiO2_end_2009_3"]/v26 if v26 > 0 else 0
        if type(v28) != int:
            v28 = v28.sum()
        v29 = v27/v28 if v28 > 0 else 0
        v30 = self.db.loc[self.db.Province == v0]["unallocated"].sum()

        # the one inside double quotes is the column name of provincial and 'v' followed by a number is the variable which has that value
        # for folmula you can refer above from v0 to v30
        self.provt.at[i,"Province"] = v0
        self.provt.at[i,"Inventory"] = v1
        self.provt.at[i,"Geological Endowment"] = v2
        self.provt.at[i,"Excluded"] = v3
        self.provt.at[i,"deep Bauxite"] = v4
        self.provt.at[i,"minable shallow inventory"] = v5
        self.provt.at[i,"refractory allocation"] = v6
        self.provt.at[i,"remaining_inventory_available_to_mining_for_SGA000t"] = v7
        self.provt.at[i,"mining losses"] = v8
        self.provt.at[i,"working_stock_prior_to_AA_and_AS_de_rating000t"] = v9
        self.provt.at[i,"working_stock_prior_to_AA_and_AS_de_rating_AA"] = v10
        self.provt.at[i,"working_stock_prior_to_AA_and_AS_de_rating_AS"] = v11
        self.provt.at[i,"de_rated_Al2O3"] = v12
        self.provt.at[i,"de_rated_AS"] = v13
        self.provt.at[i,"de_rated_SiO2"] = v14
        self.provt.at[i,"refractory_depletion_end_2009"] = v15
        self.provt.at[i,"SGA_depletion_end_2009_000t"] = v16
        self.provt.at[i,"geol_endowment_end_2009"] = v17
        self.provt.at[i,"max_extr_inventory_end_2009_1"] = v18
        self.provt.at[i,"AA_end_2009_1"] = v19
        self.provt.at[i,"SiO2_end_2009_1"] = v20
        self.provt.at[i,"AS_end_2009_1"] = v21
        self.provt.at[i,"max_extr_inventory_end_2009_2"] = v22
        self.provt.at[i,"AA_end_2009_2"] = v23
        self.provt.at[i,"AS_end_2009_2"] = v24
        self.provt.at[i,"SiO2_end_2009_2"] = v25
        self.provt.at[i,"max_extr_inventory_end_2009_3"] = v26
        self.provt.at[i,"AA_end_2009_3"] = v27
        self.provt.at[i,"SiO2_end_2009_3"] = v28
        self.provt.at[i,"AS_end_2009_3"] = v29
        self.provt.at[i,"unallocated"] = v30
        summary_timer.stop()

    def calcbfdb(self,i,ref,proddata10):# This is the top horizontal bf1-bf10 table
        summary_timer.start()
        self.bfdb["estimated_reserves_consumed_per_year"] = self.bfdb["estimated_reserves_consumed_per_year"].astype(float)
        self.bfdb["cumulative_factored_up_production"] = self.bfdb["cumulative_factored_up_production"].astype(float)
        y = "2009"
        ref = proddata10["refinery"][i]
        self.bfdb.at[i,"Name"] = ref
        self.bfdb.at[i,"Province"] = proddata10["province"][i]
        self.bfdb.at[i,"year"] = y
        c126 = proddata10.loc[proddata10.refinery==ref][y].sum()*self.lookupdb.loc[self.lookupdb.province==proddata10["province"][i]][y].sum()
        c93 = self.hdb[ref+" Allocation"][5]
        v=min(1,c126*1000/c93) if self.hdb[ref+" Allocation"][5] > 0 else 0 
        self.bfdb.at[i,"estimated_reserves_consumed_per_year"] = v# bf6 "estimated_reserves_consumed_per_year"
        w=proddata10.loc[proddata10.refinery==ref][y].sum()
        self.bfdb.at[i,"cumulative_factored_up_production"] = w
        summary_timer.stop()

    def calcextra(self):
        summary_timer.start()
        v = 99000000.000
        s = [9.0,3.0,3.0,3.0,2.0,4.5,1.2,1.5,2.0,4.0,4.7,5.0]
        al = [0.54,0.39,0.50,0.395,0.51,0.545,0.42,0.483,0.39,0.43,0.46,0.50]
        for i in range(self.l,self.l+11):
            self.db.at[i,"working_stock_prior_to_AA_and_AS_de_rating000t"] = v
            self.db.at[i,"de_rated_SiO2"] = s[i-self.l]/100
            self.db.at[i,'correction_factor'] = 1
            self.db.at[i,"de_rated_Al2O3"] = al[i-self.l]
        summary_timer.stop()
    def calcnewalloccalc(self,i,ref):
        summary_timer.start()
        v0 = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]*(1-self.bfdb.loc[self.bfdb.Name==ref]["estimated_reserves_consumed_per_year"].sum())*self.staticdb[ref+" Allocation"][i]/(1-self.staticdb['Refractory Allocation'][i])
        v1 = self.staticdb[ref+" Closed"][i]
        self.newallocdb.at[i,ref+" NewCal"] = v0
        self.newallocdb.at[i,ref+" Closed"] = v1
        summary_timer.stop()
    def calcall1(self):
        # summary_timer.start()
        '''
        x = Inventory()
        start = time.process_time()
        x.calcall()
        end = time.process_time() - start
        print(f"Total runtime -- {round(end/60, 2)} mins.")
        if self.reservedb['cbx'][0] == 1:
            cbxdb = x.cbxdb
            self.db["working_stock_prior_to_AA_and_AS_de_rating000t"] = cbxdb["Max Economic Tonnes - blending in country"]
            n = 1
        else:
            n = 0
            # calculation sequence '''
        for i in range(self.l):
            if self.reservedb["Include Inventory in Allocations Further Processed?"][i] == "Yes":
                summary.calc0(self,i)
                summary.calc1(self,i)
                summary.calc2(self,i)
                summary.calc3(self,i)
                summary.calc4(self,i)
                summary.calc5(self,i)
                summary.calc6(self,i)
                summary.calc7(self,i)
                # if n == 0:
                summary.calc8(self,i)   
                summary.calc9(self,i)
                summary.calc10(self,i)
                summary.calc11(self,i)
                summary.calc12(self,i)
                summary.calc13(self,i)
                summary.calc14(self,i)
            elif not pd.isnull(self.db["County"][i])  and (self.db["Province"][i] == "Shaanxi" or self.db["Province"][i] == "Shanxi"):
                summary.calc7(self,i)

                

        summary.calcextra(self)    
        for i in range(len(self.col)):
            summary.calc34(self,self.col[i])
            summary.calc35(self,self.col[i])
            summary.calc36(self,self.col[i])
            summary.calc37(self,self.col[i])
            summary.calc38(self,self.col[i])
            summary.calc39(self,self.col[i])
            summary.calc40(self,self.col[i])
        # summary_timer.stop()
        
    def calcall2(self,proddata10):
        # summary_timer.start()
        for i in range(proddata10.shape[0]):
            summary.calcbfdb(self,i,self.col[i],proddata10)
                
        for i in range(self.l):
            if self.reservedb["Include Inventory in Allocations Further Processed?"][i] == "Yes":
                summary.calc15(self,i)
                summary.calc16(self,i)
            if self.reservedb["Include in Endowment in Further Processing? (Provinces only)"][i] ==  "Y1":
                summary.calc17(self,i)
            
            if self.reservedb["Include Inventory in Allocations Further Processed?"][i] == "NO" and pd.isnull(self.db["County"][i]):
                summary.calc18(self,i)
                
            if self.reservedb["Include Inventory in Allocations Further Processed?"][i] == "Yes":
                
                summary.calc19(self,i)
                
                summary.calc28(self,i)
                summary.calc29(self,i)
                summary.calc30(self,i)
                summary.calc31(self,i)
                summary.calc32(self,i)
                summary.calc33(self,i)
                summary.calc24(self,i)
                summary.calc25(self,i)
                summary.calc26(self,i)
                summary.calc27(self,i)
                summary.calc20(self,i)
                summary.calc21(self,i)
                summary.calc22(self,i)
                summary.calc23(self,i)
                
        summary.calc410(self)
        summary.calc41(self)
        summary.calc42(self)
        summary.calc43(self)
        summary.calc44(self)
        summary.calc45(self)
        summary.calc46(self)
        summary.calc47(self)
        summary.calc48(self)
        summary.calc49(self)
        summary.calc50(self)
        summary.calc51(self)
        #summary.calc52(self)
        summary.calc53(self)
        summary.calc54(self)
        summary.calc55(self)
        summary.calc56(self)
        #summary.calc57(self)
        summary.calc58(self)
        summary.calc59(self)
        summary.calc60(self)
        summary.calc61(self)
        summary.calc62(self)
        summary.calc63(self)
        summary.calc64(self)
        summary.calc65(self)
        summary.calc66(self)
        summary.calc67(self)
        summary.calc68(self)
        summary.calc69(self)
        summary.calc70(self)
        summary.calc71(self)
        summary.calc72(self)
        for i in range(self.l):
            if self.reservedb["Include Inventory in Allocations Further Processed?"][i] == "NO" and pd.isnull(self.db["County"][i]):
                summary.provincialtotalcalc(self,i)
        for i in range(len(self.col)):
            for j in range(self.l):
                if self.reservedb["Include Inventory in Allocations Further Processed?"][j] == "Yes":
                    summary.calcnewalloccalc(self,j,self.col[i])
        # summary_timer.stop()
        


