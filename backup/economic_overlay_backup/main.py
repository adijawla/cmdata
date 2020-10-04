import time, os
import numpy as np
import pandas as pd
from flatdb.flatdbconverter import Flatdbconverter
from extension import DB_TO_FILE
from scipy.stats import beta
from outputdb import uploadtodb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

dblist = []

db_conv = Flatdbconverter("Economic overlay")

# Start of Rahul's Code
class Summary:
    def __init__(self):
        self.db = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/output.csv'),encoding = "utf-8")
        self.reservedb = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/reserve.csv'))
        self.lookupdb = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/datalookup.csv'))
        self.moddb = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/reserve.csv'))
        self.staticdb = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/reservestatic.csv'))
        self.hdb = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/hhdb.csv'))
        self.gradeAlO3 = 0.7
        self.gradeAS = 9
        self.extractable_to_SGA_input = 0.7
        self.reserve_data_to_use = "current"
        self.totaldb = pd.read_csv(os.path.join(BASE_DIR, "reserve_summary_inputs/totaldb.csv"),encoding = "utf-8")
        self.provt = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/provt.csv'),encoding = "utf-8")
        col = pd.read_csv(os.path.join(BASE_DIR, 'reserve_summary_inputs/caustic.csv'))
        self.col = list(col['sheetname'][::-1])
        self.bfdb = pd.DataFrame(columns = ["Name","Province","year","estimated_reserves_consumed_per_year","cumulative_factored_up_production" ],index=range(len(self.col)+70))
        self.l = self.reservedb.shape[0]-1
        self.pr = self.reservedb["Province"]
        self.pr= list(self.pr)
        idx = pd.IndexSlice
        mxidd = pd.MultiIndex.from_product([self.col,self.pr])
        self.newallocdb = pd.read_csv(os.path.join(BASE_DIR, "reserve_summary_inputs/newcalallo.csv"))

        self.db["deep bauxite"] = self.moddb["deep bauxite"]
        self.db['deposit'] = self.reservedb["Deposit"]
        self.bfdb["cumulative_factored_up_production"] = self.bfdb["cumulative_factored_up_production"].astype(float)
        self.bfdb["estimated_reserves_consumed_per_year"] = self.bfdb["estimated_reserves_consumed_per_year"].astype(float)
    def calc0(self,i):
        v = 1/(1-self.moddb['unique case'][i]) if self.moddb['unique case'][i] > 0 else 1
        self.db.at[i,'correction_factor'] = v
    def calc1(self,i): #minable_shallow_inventory
        v = self.reservedb['Inventory'][i]-self.moddb['deep bauxite'][i]
        self.db.at[i,"minable_shallow_inventory"] = v
    def calc2(self,i):#refractory_allocation
        v = self.reservedb['Inventory'][i]*self.moddb['unique case'][i]
        self.db.at[i,"refractory_allocation"] = v
    def calc3(self,i):#remaining_inventory_available_to_mining_for_SGA000t
        v = self.db["minable_shallow_inventory"][i] - self.db["refractory_allocation"][i]
        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA000t"] = v
    def calc4(self,i):#remaining_inventory_available_to_mining_for_SGA_AA and working_stock_prior_to_AA_and_AS_de_rating_AA
        v = (self.db["minable_shallow_inventory"][i]*self.reservedb["AI203"][i] -  self.db["refractory_allocation"][i]*self.gradeAlO3)/self.db["remaining_inventory_available_to_mining_for_SGA000t"][i] # column need to be added "bauxite gradeAI203"
        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA_AA"] = v
        self.db.at[i,"working_stock_prior_to_AA_and_AS_de_rating_AA"] = v
    def calc5(self,i):#working_stock_prior_to_AA_and_AS_de_rating_AS
        v = self.db["remaining_inventory_available_to_mining_for_SGA_AA"][i]/((self.db["minable_shallow_inventory"][i]*self.reservedb["AI203"][i]/self.reservedb["AS"][i] - self.db["refractory_allocation"][i]*self.gradeAlO3/self.gradeAS)/self.db["remaining_inventory_available_to_mining_for_SGA000t"][i])

        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA_AS"] = v
        self.db.at[i,"working_stock_prior_to_AA_and_AS_de_rating_AS"] = v
    def calc6(self,i):#remaining_inventory_available_to_mining_for_SGA_SiO2
        v =  self.db["remaining_inventory_available_to_mining_for_SGA_AA"][i]/self.db["remaining_inventory_available_to_mining_for_SGA_AS"][i]
        self.db.at[i,"remaining_inventory_available_to_mining_for_SGA_SiO2"] = v
    def calc7(self,i):#extractable_to_SGA
        v = 0.7 if self.reservedb["Province"][i] == "Guangxi" else 0.6 if self.reservedb["Province"][i] == "Guizhou" else 1.0 if self.reservedb["Province"][i] == "Shandong" and self.reservedb["County"][i] != "Zouwu" else self.extractable_to_SGA_input # column needed to be added "extractable_to_SGA_input"
        self.db.at[i,"extractable_to_SGA"] = v
    def calc8(self,i):#working_stock_prior_to_AA_and_AS_de_rating000t
        v = (self.db["minable_shallow_inventory"][i] - self.db["refractory_allocation"][i])*self.db["extractable_to_SGA"][i] if self.reserve_data_to_use == "current" else 0
        self.db.at[i,"working_stock_prior_to_AA_and_AS_de_rating000t"] = v
    def calc9(self,i):#de_rated_Al2O3
        v = self.db["working_stock_prior_to_AA_and_AS_de_rating_AA"][i]*self.moddb["de-rating factor Al2O3"][i] if self.reserve_data_to_use == "current" else 0
        self.db.at[i,"de_rated_Al2O3"] = v
    def calc10(self,i):#de_rated_AS
        v = self.db["working_stock_prior_to_AA_and_AS_de_rating_AS"][i]*self.moddb["de-rating factor AS"][i] if self.reserve_data_to_use == "current" else 0
        self.db.at[i,"de_rated_AS"] = v
    def calc11(self,i):#de_rated_SiO2
        v = self.db["de_rated_Al2O3"][i]/self.db["de_rated_AS"][i] if self.db["de_rated_AS"][i] > 0 else 0
        self.db.at[i,"de_rated_SiO2"] = v
    def calc12(self,i):#province_county_000twstockprior
        v = str(self.reservedb["Province"][i])+"-"+str(self.reservedb["County"][i])+"-"+str(int(self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]))
        self.db.loc[i,"province_county_000twstockprior"] = v
    def calc13(self,i):#refractory_depletion_end_2009
        v = self.db["refractory_allocation"].sum()/self.db["refractory_depletion_end_2009_000t"].sum() if self.db["refractory_depletion_end_2009_000t"].sum() > 0 else 0 #AD25
        self.db.at[i,"refractory_depletion_end_2009"] = v
    def calc14(self,i):#refractory_depletion_end_2009_000t
        v = self.db["refractory_depletion_end_2009"][i]*self.db["working_stock_prior_to_AA_and_AS_de_rating000t"].sum()
        self.db.at[i,"refractory_depletion_end_2009_000t"] = v
    def calc15(self,i):#SGA_depletion_end_2009
        x = self.staticdb.iloc[i,2:10000:8].reset_index(drop=True)
        y = self.bfdb["estimated_reserves_consumed_per_year"].reset_index(drop=True)
        z = x*y
        z = z.sum()
        v = min(1,z)/(1-self.moddb["unique case"][i])

        self.db.at[i,"SGA_depletion_end_2009"] = v
        if self.reservedb["Province"][i] == "Shandong" and self.reservedb["County"][i] != "Zouwu":
            self.db.at[i,"SGA_depletion_end_2009"] = 1.0
        if self.reservedb["Switch"][i] == "N1":
            self.db.at[i,"SGA_depletion_end_2009"] = 0.0
    def calc16(self,i):#SGA_depletion_end_2009_000t
        v = self.db["SGA_depletion_end_2009"][i]*self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]
        self.db.at[i,"SGA_depletion_end_2009_000t"] = v
    def calc17(self,i):#endowment_end_2009_prov_totals
        v = max(0,self.reservedb["GeologicalEndowment"][i]-self.db["refractory_depletion_end_2009_000t"][i]-self.db["SGA_depletion_end_2009_000t"][i])
        v0 = self.reservedb["GeologicalEndowment"][i]
        v1 = 0 if type(self.db["refractory_depletion_end_2009_000t"][i]) != float else self.db["refractory_depletion_end_2009_000t"][i]
        v2 = 0 if pd.isnull(self.db["SGA_depletion_end_2009_000t"][i]) else self.db["SGA_depletion_end_2009_000t"][i]
        v3 = max(0,v0-v1-v2)
        self.db.at[i,"endowment_end_2009_prov_totals",] = 3
    def calc18(self,i):#geol_endowment_end_2009
        v = self.reservedb["GeologicalEndowment"][i] - (self.db.loc[self.db.Province==self.db["Province"][i]]["refractory_depletion_end_2009_000t"].sum()+self.db.loc[self.db.Province==self.db["Province"][i]]["SGA_depletion_end_2009_000t"].sum()) # sum if
        self.db.at[i,"geol_endowment_end_2009"] = v
    def calc19(self,i):#endowment_end_2009_all
        v = 0 if self.reservedb["County"][i] == np.nan or self.reservedb["County"][i] == self.reservedb["County"][i+1] else "something"
        self.db.loc[i,"endowment_end_2009_all"] = v
    def calc20(self,i):#max_extr_inventory_end_2009_1
        v = self.db["max_extr_inventory_end_2009_2"][i]+self.db["max_extr_inventory_end_2009_3"][i]
        self.db.at[i,"max_extr_inventory_end_2009_1"] = v
    def calc21(self,i):#AA_end_2009_1
        v = (self.db["max_extr_inventory_end_2009_2"][i]*self.db["AA_end_2009_2"][i]+self.db["max_extr_inventory_end_2009_3"][i]*self.db["AA_end_2009_3"][i])/self.db["max_extr_inventory_end_2009_1"][i] if self.db["max_extr_inventory_end_2009_1"][i] > 0 else 0
        self.db.at[i,"AA_end_2009_1"] = v
    def calc22(self,i):#SiO2_end_2009_1
        v = (self.db["max_extr_inventory_end_2009_2"][i]*self.db["SiO2_end_2009_2"][i]+self.db["max_extr_inventory_end_2009_3"][i]*self.db["SiO2_end_2009_3"][i])/self.db["max_extr_inventory_end_2009_1"][i] if self.db["max_extr_inventory_end_2009_1"][i] > 0 else 0
        self.db.at[i,"SiO2_end_2009_1"] = v
    def calc23(self,i):#AS_end_2009_1
        v = self.db["AA_end_2009_1"][i]/self.db["SiO2_end_2009_1"][i] if self.db["SiO2_end_2009_1"][i] > 0 else 0
        self.db.at[i,"AS_end_2009_1"] = v
    def calc24(self,i):#max_extr_inventory_end_2009_2
        v = self.db["refractory_allocation"][i]-self.db["refractory_depletion_end_2009_000t"][i]
        self.db.at[i,"max_extr_inventory_end_2009_2"] = v
    def calc25(self,i):#AA_end_2009_2
        v = self.gradeAlO3
        self.db.at[i,"AA_end_2009_2"] = v
    def calc26(self,i):#AS_end_2009_2
        v = self.gradeAS
        self.db.at[i,"AS_end_2009_2"] = v
    def calc27(self,i):#SiO2_end_2009_2
        v = self.db["AA_end_2009_2"][i]/self.db["AS_end_2009_2"][i] if self.db["AS_end_2009_2"][i] > 0 else 0
        self.db.at[i,"SiO2_end_2009_2"] = v
    def calc28(self,i):#max_extr_inventory_end_2009_3
        v = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i] - self.db["SGA_depletion_end_2009_000t"][i]
        self.db.at[i,"max_extr_inventory_end_2009_3"] = v
    def calc29(self,i):#AA_end_2009_3
        v0 = self.db["SGA_depletion_end_2009"][i]#af
        v1 = self.db["de_rated_Al2O3"][i]#z
        v2 = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]#u
        v = ((v1*v2)-(((v1+0.02)+((v1+0.02)-v0*0.04))/2)*v0*v2)/((1-v0)*v2) if 1-v0 > 0 else 0
        self.db.at[i,"AA_end_2009_3"] = v
    def calc30(self,i):#SiO2_end_2009_3
        v0 = self.db["SGA_depletion_end_2009"][i]
        v1 = self.db["de_rated_SiO2"][i]
        v2 = self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]
        v = ((v1*v2)-(((v1-0.03)+((v1-0.03)+v0*0.06))/2)*v0*v2)/((1-v0)*v2) if 1-v0 > 0 else 0
        self.db.at[i,"SiO2_end_2009_3"] = v
    def calc31(self,i):#AS_end_2009_3
        v = self.db["AA_end_2009_3"][i]/self.db["SiO2_end_2009_3"][i] if self.db["SiO2_end_2009_3"][i] > 0 else 0
        self.db.at[i,"AS_end_2009_3"] = v
    def calc32(self,i):#unallocated
        v = 0 if self.staticdb.iloc[i,2:834:8].sum() > 0 else self.db["working_stock_prior_to_AA_and_AS_de_rating000t"][i]
        self.db.at[i,"unallocated"] = v
        if self.reservedb["Province"][i] == "Shandong":
            self.db.at[i,"unallocated"] = 0
    def calc33(self,i):#check_sum
        v = self.staticdb.iloc[i,2:834:8].sum() + self.moddb['unique case'][i]
        self.db.at[i,"check_sum"] = v

    def calcall1(self): # calculation sequence
        for i in range(self.l):
            if self.reservedb["Deposit"][i] == "Yes":
                self.calc0(i)
                self.calc1(i)
                self.calc2(i)
                self.calc3(i)
                self.calc4(i)
                self.calc5(i)
                self.calc6(i)
                self.calc7(i)
                self.calc8(i)
                self.calc9(i)
                self.calc10(i)
                self.calc11(i)
                self.calc12(i)
                self.calc13(i)
                self.calc14(i)
            elif not pd.isnull(self.db["County"][i])  and (self.db["Province"][i] == "Shaanxi" or self.db["Province"][i] == "Shanxi"):
                self.calc7(i)
    def calcall2(self):
        for i in range(self.l):
            if self.reservedb["Deposit"][i] == "Yes":
                self.calc15(i)
                self.calc16(i)
            if self.reservedb["Switch"][i] ==  "Y1":
                self.calc17(i)

            if self.reservedb["Deposit"][i] == "NO" and pd.isnull(self.db["County"][i]):
                self.calc18(i)

            if self.reservedb["Deposit"][i] == "Yes":
                self.calc19(i)
                self.calc28(i)
                self.calc29(i)
                self.calc30(i)
                self.calc31(i)
                self.calc32(i)
                self.calc33(i)
                self.calc24(i)
                self.calc25(i)
                self.calc26(i)
                self.calc27(i)
                self.calc20(i)
                self.calc21(i)
                self.calc22(i)
                self.calc23(i)

    def calc_all_funcs(self):
        self.calcall1()
        self.calcall2()

        if self.db.iloc[-1, 0] == "China Total":
            self.db = self.db.loc[:len(self.db)-2, :] # To Remove the last un-required row

        dblist.append(db_conv.single_year_mult_out(self.db, "Reserve Summary DB"))
        self.db.to_csv("db.csv", index=False)

# End of Rahul's code



class Inventory:
    def __init__(self):
        Summary().calc_all_funcs()
        ext = DB_TO_FILE()

        self.output_db          = pd.read_csv(os.path.join(BASE_DIR, "db.csv"))
        self.inputs             = ext.inputs()
        self.inputs.drop(['creation_date', 'updation_date', 'inputs_id'], axis=1, inplace=True)
        self.lookuptable        = ext.lookuptable()
        self.other_controls     = ext.other_controls()
        self.range_max          = ext.depth_buckets()
        self.perct_AA_df_cols = ["Starting grade","Depletion grade","Scaled mean","Scaled variance","Alpha value","Beta value"]
        [self.perct_AA_df_cols.append(f"percentile mined {i/100}")  for i in range(5,101,5)]

        self.perct_AA_df_data0 = ["","","","","",""]
        [self.perct_AA_df_data0.append(f"{i}th") for i in range(5,101,5)]

        self.perct_SiO2_df_cols =["errors","A/S approx max","A/S approx min","Silica average	","Silica total (kt)","Starting grade","Depletion grade","Scaled mean","Scaled variance","Alpha value","Beta value"]
        [self.perct_SiO2_df_cols.append(f"percentile mined {i/100}")  for i in range(5,101,5)]

        self.perct_SiO2_df_data0 = ["","","","","","","","","","",""]
        [self.perct_SiO2_df_data0.append(f"{i}th") for i in range(5,101,5)]

        self.perct_SiO2_cab_df_cols = ["errors","A/S approx max","A/S approx min","Silica average","Silica total (kt)","Starting grade","Depletion grade","Scaled mean","Scaled variance","Alpha value","Beta value"]
        [self.perct_SiO2_cab_df_cols.append(f"percentile mined {i/100}")  for i in range(5,101,5)]

        self.perct_SiO2_cab_df_data0 = ["","","","","","","","","","",""]
        [self.perct_SiO2_cab_df_data0.append(f"{i}th") for i in range(5,101,5)]

        self.op_ug_cols =  ["open_pit1","open_pit2","open_pit3","underground1","underground2","underground3","underground4","underground5","underground6","underground7"]

        self.max1 =  self.range_max.loc[1, "open_pit1"]   # 125.0/3.0
        self.max2 =  self.range_max.loc[1, "open_pit2"]   # 125.0*2.0/3.0
        self.max3 =  self.range_max.loc[1, "open_pit3"]   # 125.0
        self.max4 =  self.range_max.loc[1, "underground1"]   # (500.0-125.0)*1.0/7.0 + self.max3
        self.max5 =  self.range_max.loc[1, "underground2"]   # (500.0-125.0)*1.0/7.0 + self.max4
        self.max6 =  self.range_max.loc[1, "underground3"]   # (500.0-125.0)*1.0/7.0 + self.max5
        self.max7 =  self.range_max.loc[1, "underground4"]   # (500.0-125.0)*1.0/7.0 + self.max6
        self.max8 =  self.range_max.loc[1, "underground5"]   # (500.0-125.0)*1.0/7.0 + self.max7
        self.max9 =  self.range_max.loc[1, "underground6"]   # (500.0-125.0)*1.0/7.0 + self.max8
        self.max10 = self.range_max.loc[1, "underground7"]   #  (500.0-125.0)*1.0/7.0 + self.max9

        self.min1 =  self.range_max.loc[0, "open_pit1"]
        self.min2 =  self.range_max.loc[0, "open_pit2"]
        self.min3 =  self.range_max.loc[0, "open_pit3"]
        self.min4 =  self.range_max.loc[0, "underground1"]
        self.min5 =  self.range_max.loc[0, "underground2"]
        self.min6 =  self.range_max.loc[0, "underground3"]
        self.min7 =  self.range_max.loc[0, "underground4"]
        self.min8 =  self.range_max.loc[0, "underground5"]
        self.min9 =  self.range_max.loc[0, "underground6"]
        self.min10 = self.range_max.loc[0, "underground7"]

        self.avg1 = (self.min1 + self.max1)/2
        self.avg2 = (self.min2 + self.max2)/2
        self.avg3 = (self.min3 + self.max3)/2
        self.avg4 = (self.min4 + self.max4)/2
        self.avg5 = (self.min5 + self.max5)/2
        self.avg6 = (self.min6 + self.max6)/2
        self.avg7 = (self.min7 + self.max7)/2
        self.avg8 = (self.min8 + self.max8)/2
        self.avg9 = (self.min9 + self.max9)/2
        self.avg10 = (self.min10 +self.max10)/2

        self.range_max.at[2, "mine_type"] = 'Avg for range (m)'
        self.range_max.at[2, "open_pit1":"underground7"] = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]
        self.range_max.drop(['depthbuckets_percentoftonnage_id'], axis=1, inplace=True)
        print(self.range_max.columns)
        print(self.range_max)

        # Dataframes
        self.linear_eqn_sb_df = 0
        self.depth_splits_sb_df = 0
        self.depth_split_cab_df = 0
        self.sedimentary_bauxite_td_df = 0 #total
        self.collapse_accm_bauxite_df = 0   #total
        self.linear_eqn_sb_only_df = 0
        self.depth_multiplier_sulphur_cntm_df = 0
        self.perct_inventory_sulphur_contaminated_db_df = 0
        self.perct_AA_percentile_mined_sb_df = 0
        self.perct_SiO2_percentile_mined_sb_df = 0
        self.perct_AA_percentile_mined_cab_df = 0
        self.perct_SiO2_percentile_mined_cab_df = 0
        self.stripping_ratio_sb_df = 0
        self.stripping_ratio_ca_bx_df = 0
        self.electricity_costs_msb_RMBt_ROM_ore_df = 0
        self.electricity_costs_cab_RMB_t_ROM_ore_df = 0 #AVG5
        self.diesel_costs_msb_RMB_t_ROM_ore_df = 0
        self.diesel_costs_mca_RMB_t_ROM_ore_df = 0 #AVG5
        self.labour_costs_msb_RMB_t_ROM_ore_df = 0
        self.labour_costs_mcab_RMB_t_ROM_ore_df = 0 #AVG5
        self.mine_transport_costs_sedimentary_df = 0 #AVG
        self.washing_factor_df = 0
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df = 0
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df = 0 #AVG5
        self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df = 0
        self.total_mining_costs_RMB_t_ore_sb_df = 0
        self.total_mining_costs_RMB_t_ore_cab_df = 0 #AVG5
        self.delivery_costs_RG_RT_df = 0
        self.t_div_t_BX_AA_percentile_mined_sb_df = 0
        self.t_div_t_BX_AA_percentile_mined_cab_df = 0
        self.caustic_use_percentile_mined_sb_t_div_t_AA_df = 0
        self.caustic_use_percentile_mined_cab_df = 0
        self.CBIX_BX_AA_production_cost_df = 0
        self.final_costs_df = 0
        self.tonnages_in_categories_df = 0
        self.cost_with_dummy_for_ranking_df = 0
        self.ranks_by_costs_df = 0
        self.cell_columns_by_cost_rank_df = 0
        self.costs_by_cost_rank_df = 0
        self.tonages_by_cost_rank_df = 0
        self.max_blended_tonnes_entity_cost_limit_df = 0
        self.max_economic_tonnes_df = 0

        self.Costs_RMBtAA  = 0
        self.Tonnages_kt = 0
        self.Rank_AA_proc_cost  = 0


    # Depth Splits - sedimentary bauxite
    def linear_eqn_sb(self):
        df = self.inputs
        print(df.columns)
        self.linear_eqn_sb_df = pd.DataFrame(columns=["m","c"])

        for i in range(len(self.output_db)):
            m = 1.0 / float(df.loc[:, "maximum_burial"][i] - df.loc[:,"minimum_burial"][i] )
            c = 0.0 - float(df.loc[:,"minimum_burial"][i] * m)

            self.linear_eqn_sb_df.at[i,"m"] = m
            self.linear_eqn_sb_df.at[i,"c"] = c
            self.linear_eqn_sb_df.at[i, "province"] = df.at[i, "province"]
            self.linear_eqn_sb_df.at[i, "county"]   = df.at[i, "county"]

        print(self.linear_eqn_sb_df)
        print(self.linear_eqn_sb_df.sort_values(["province"], axis=0, ascending=True).reset_index().drop(['index'], axis=1))


    def main_vlookup(self, search, province, target):
        v = province.map(lambda x: x.lower() == search.lower())
        try:
            return float(target[v].tolist()[0])
        except Exception:
            return np.nan

    # --Depth buckets - percent of tonnage per depth range
    def depth_splits_sb(self):
        # df = pd.read_csv('outputs/depth_splits_sedimentary_bauxite/linear_eqn-depth_buckets_percent_tonnage_per_depth_range.csv')
        df = self.linear_eqn_sb_df
        self.depth_splits_sb_df = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(df)):
            to_be_summed = [] # all max values are to be stored in here for further calculations

            #open pit 1 (open_pit)
            if ((float(df.loc[i, "m"]) * self.max1) + float(df.loc[i, "c"])) <= 0:
                open_pit_1 = 0
            elif ((float(df.loc[i, "m"]) * self.max1) + float(df.loc[i, "c"])) > 1:
                open_pit_1 = 1
            else:
                open_pit_1 = (float(df.loc[i, "m"]) * self.max1) + float(df.loc[i, "c"])
            self.depth_splits_sb_df.at[i, "open_pit1"] = open_pit_1
            to_be_summed.append(open_pit_1)

            #open pit 2 (open_pit)
            if ((float(df.loc[i, "m"]) * self.max2) + float(df.loc[i, "c"])) <= 0:
                open_pit_2 = 0
            elif ((float(df.loc[i, "m"]) * self.max2) + float(df.loc[i, "c"])) > 1:
                open_pit_2 = 1
            else:
                open_pit_2 = (float(df.loc[i, "m"]) * self.max2) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "open_pit2"] = open_pit_2
            to_be_summed.append(open_pit_2)

            #open pit 3 (open_pit)
            if ((float(df.loc[i, "m"]) * self.max3) + float(df.loc[i, "c"])) <= 0:
                open_pit_3 = 0
            elif ((float(df.loc[i, "m"]) * self.max3) + float(df.loc[i, "c"])) > 1:
                open_pit_3 = 1
            else:
                open_pit_3 = (float(df.loc[i, "m"]) * self.max3) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "open_pit3"] = open_pit_3
            to_be_summed.append(open_pit_3)

            #underground 1 (underground)
            if ((float(df.loc[i, "m"]) * self.max4) + float(df.loc[i, "c"])) <= 0:
                underground_1 = 0 - sum(to_be_summed)
            elif ((float(df.loc[i, "m"]) * self.max4) + float(df.loc[i, "c"])) > 1:
                underground_1 = 1 - sum(to_be_summed)
            else:
                underground_1 = (float(df.loc[i, "m"]) * self.max4) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "underground1"] = underground_1
            to_be_summed.append(underground_1)

            #underground 2 (underground)
            if ((float(df.loc[i, "m"]) * self.max5) + float(df.loc[i, "c"])) <= 0:
                underground_2 = 0 - sum(to_be_summed)
            elif ((float(df.loc[i, "m"]) * self.max5) + float(df.loc[i, "c"])) > 1:
                underground_2 = 1 - sum(to_be_summed)
            else:
                underground_2 = (float(df.loc[i, "m"]) * self.max5) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "underground2"] = underground_2
            to_be_summed.append(underground_2)

            #underground 3 (underground)
            if ((float(df.loc[i, "m"]) * self.max6) + float(df.loc[i, "c"])) <= 0:
                underground_3 = 0 - sum(to_be_summed)
            elif ((float(df.loc[i, "m"]) * self.max6) + float(df.loc[i, "c"])) > 1:
                underground_3 = 1 - sum(to_be_summed)
            else:
                underground_3 = (float(df.loc[i, "m"]) * self.max6) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "underground3"] = underground_3
            to_be_summed.append(underground_3)

            #underground 4 (underground)
            if ((float(df.loc[i, "m"]) * self.max7) + float(df.loc[i, "c"])) <= 0:
                underground_4 = 0 - sum(to_be_summed)
            elif ((float(df.loc[i, "m"]) * self.max7) + float(df.loc[i, "c"])) > 1:
                underground_4 = 1 - sum(to_be_summed)
            else:
                underground_4 = (float(df.loc[i, "m"]) * self.max7) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "underground4"] = underground_4
            to_be_summed.append(underground_4)

            #underground 5 (underground)
            if ((float(df.loc[i, "m"]) * self.max8) + float(df.loc[i, "c"])) <= 0:
                underground_5 = 0 - sum(to_be_summed)
            elif ((float(df.loc[i, "m"]) * self.max8) + float(df.loc[i, "c"])) > 1:
                underground_5 = 1 - sum(to_be_summed)
            else:
                underground_5 = (float(df.loc[i, "m"]) * self.max8) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "underground5"] = underground_5
            to_be_summed.append(underground_5)

            #underground 6 (underground)
            if ((float(df.loc[i, "m"]) * self.max9) + float(df.loc[i, "c"])) <= 0:
                underground_6 = 0 - sum(to_be_summed)
            elif ((float(df.loc[i, "m"]) * self.max9) + float(df.loc[i, "c"])) > 1:
                underground_6 = 1 - sum(to_be_summed)
            else:
                underground_6 = (float(df.loc[i, "m"]) * self.max9) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "underground6"] = underground_6
            to_be_summed.append(underground_6)

            #underground 7 (underground)
            if ((float(df.loc[i, "m"]) * self.max10) + float(df.loc[i, "c"])) <= 0:
                underground_7 = 0 - sum(to_be_summed)
            elif ((float(df.loc[i, "m"]) * self.max10) + float(df.loc[i, "c"])) > 1:
                underground_7 = 1 - sum(to_be_summed)
            else:
                underground_7 = (float(df.loc[i, "m"]) * self.max10) + float(df.loc[i, "c"]) - sum(to_be_summed)
            self.depth_splits_sb_df.at[i, "underground7"] = underground_7
            to_be_summed.append(underground_7)

    # Depth Splits - collapse accumulated bauxite
    def depth_split_cab(self):
        # df = pd.read_csv('outputs/depth_splits_sedimentary_bauxite/depth_buckets-percent_tonnage_per_depth_range.csv')
        df = self.depth_splits_sb_df
        self.depth_split_cab_df = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(df)):
            self.depth_split_cab_df.at[i, "open_pit1"] = 1
            self.depth_split_cab_df.at[i, "open_pit2":"underground7"] = 0


    # Sedimentary Bauxite - tonnages per depth range
    def sedimentary_bauxite_td(self):
        working_stock = self.output_db.loc[:, "working_stock_prior_to_AA_and_AS_de_rating000t"]
        collapse_acc  = self.inputs.loc[:, "proportion_collapse_guanxi"]
        # df = pd.read_csv("outputs/depth_splits_sedimentary_bauxite/depth_buckets-percent_tonnage_per_depth_range.csv")
        df = self.depth_splits_sb_df
        self.sedimentary_bauxite_td_df = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(col, i):
            v = (1.0 -float(collapse_acc[i])) * float(df.loc[i, col]) * float(working_stock[i])
            return 0 if pd.isna(v) else v

        for i in range(len(working_stock)):
            self.sedimentary_bauxite_td_df.at[i, "open_pit1"] = main_func("open_pit1", i)
            self.sedimentary_bauxite_td_df.at[i, "open_pit2"] = main_func("open_pit2", i)
            self.sedimentary_bauxite_td_df.at[i, "open_pit3"] = main_func("open_pit3", i)
            self.sedimentary_bauxite_td_df.at[i, "underground1"] = main_func("underground1", i)
            self.sedimentary_bauxite_td_df.at[i, "underground2"] = main_func("underground2", i)
            self.sedimentary_bauxite_td_df.at[i, "underground3"] = main_func("underground3", i)
            self.sedimentary_bauxite_td_df.at[i, "underground4"] = main_func("underground4", i)
            self.sedimentary_bauxite_td_df.at[i, "underground5"] = main_func("underground5", i)
            self.sedimentary_bauxite_td_df.at[i, "underground6"] = main_func("underground6", i)
            self.sedimentary_bauxite_td_df.at[i, "underground7"] = main_func("underground7", i)



    # Collapse Accumulated Bauxite - tonnages per depth range
    def collapse_accm_bauxite(self):
        working_stock = self.output_db.loc[:, "working_stock_prior_to_AA_and_AS_de_rating000t"]
        collapse_acc  = self.inputs.loc[:, "proportion_collapse_guanxi"]
        # df = pd.read_csv("outputs/depth_splits-collapse_accumulated_bauxite.csv")
        df = self.depth_split_cab_df
        self.collapse_accm_bauxite_df = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(col, i):
            v = float(collapse_acc[i]) * float(df.loc[i, col]) * float(working_stock[i])
            return 0 if pd.isna(v) else v

        for i in range(len(working_stock)):
            self.collapse_accm_bauxite_df.at[i, "open_pit1"] = main_func("open_pit1", i)
            self.collapse_accm_bauxite_df.at[i, "open_pit2"] = main_func("open_pit2", i)
            self.collapse_accm_bauxite_df.at[i, "open_pit3"] = main_func("open_pit3", i)
            self.collapse_accm_bauxite_df.at[i, "underground1"] = main_func("underground1", i)
            self.collapse_accm_bauxite_df.at[i, "underground2"] = main_func("underground2", i)
            self.collapse_accm_bauxite_df.at[i, "underground3"] = main_func("underground3", i)
            self.collapse_accm_bauxite_df.at[i, "underground4"] = main_func("underground4", i)
            self.collapse_accm_bauxite_df.at[i, "underground5"] = main_func("underground5", i)
            self.collapse_accm_bauxite_df.at[i, "underground6"] = main_func("underground6", i)
            self.collapse_accm_bauxite_df.at[i, "underground7"] = main_func("underground7", i)



    def linear_eqn_sb_only(self):
        max_burial = self.inputs.loc[:, "maximumburial_plataeu"]
        min_burial = self.inputs.loc[:,"minimumburial_contamination_start"]
        self.linear_eqn_sb_only_df = pd.DataFrame(columns=["m","c"])

        for i in range(len(max_burial)):
            m = 1.0 / float(max_burial[i] - min_burial[i] )
            c = 0.0 - float(min_burial[i] * m)

            self.linear_eqn_sb_only_df.at[i,"m"] = m
            self.linear_eqn_sb_only_df.at[i,"c"] = c



    # Suplhur contamination - sedimentary bauxite only
    # --depth multiplier for sulphur contamination
    def depth_multiplier_sulphur_cntm(self):
        # df = pd.read_csv("outputs/suplhur_contamination_sedimentary_bauxite_only/linear_equation-Suplhur_contamination-sedimentary_bauxite_only.csv")
        df = self.linear_eqn_sb_only_df
        min_burial = self.inputs.loc[: , "minimumburial_contamination_start"]
        self.depth_multiplier_sulphur_cntm_df = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(i, min_val, max_val):
            calc1 = min([0 if float(min_burial[i]) > max_val else (max_val-float(min_burial[i]))/(max_val-min_val) ], [1])[0]

            x = min(max((float(df.loc[i,"m"])*min_val+float(df.loc[i,"c"])), 0.0),1.0)
            y = min(max((float(df.loc[i,"m"])*max_val+float(df.loc[i,"c"])), 0.0),1.0)
            calc2 = (x+y)/2

            return calc1*calc2

        for i in range(len(df)):
            #open pit 1 (open_pit)
            self.depth_multiplier_sulphur_cntm_df.at[i, "open_pit1"] = main_func(i, 0.0, self.max1)
            #open pit 2 (open_pit)
            self.depth_multiplier_sulphur_cntm_df.at[i, "open_pit2"] = main_func(i, self.max1, self.max2)
            #open pit 3 (open_pit)
            self.depth_multiplier_sulphur_cntm_df.at[i, "open_pit3"] = main_func(i, self.max2, self.max3)
            #underground 1 (underground)
            self.depth_multiplier_sulphur_cntm_df.at[i, "underground1"] = main_func(i, self.max3, self.max4)
            #underground 2 (underground)
            self.depth_multiplier_sulphur_cntm_df.at[i, "underground2"] = main_func(i, self.max4, self.max5)
            #underground 3 (underground)
            self.depth_multiplier_sulphur_cntm_df.at[i, "underground3"] = main_func(i, self.max5, self.max6)
            #underground 4 (underground)
            self.depth_multiplier_sulphur_cntm_df.at[i, "underground4"] = main_func(i, self.max6, self.max7)
            #underground 5 (underground)
            self.depth_multiplier_sulphur_cntm_df.at[i, "underground5"] = main_func(i, self.max7, self.max8)
            #underground 6 (underground)
            self.depth_multiplier_sulphur_cntm_df.at[i, "underground6"] = main_func(i, self.max8, self.max9)
            #underground 7 (underground)
            self.depth_multiplier_sulphur_cntm_df.at[i, "underground7"] = main_func(i, self.max9, self.max10)


    # Suplhur contamination - sedimentary bauxite only
    # --% Inventory Sulphur contaminated by depth bucket
    def perct_inventory_sulphur_contaminated_db(self):
        gnr_percnt = self.inputs.loc[:, "general_county_province"]
        max_contm  = self.inputs.loc[:, "maximumcontamination_county_province"]
        # df         = pd.read_csv("outputs/suplhur_contamination_sedimentary_bauxite_only/depth_multiplier_for_sulphur_contamination.csv")
        df = self.depth_multiplier_sulphur_cntm_df
        self.perct_inventory_sulphur_contaminated_db_df     = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(col, i):
            return float(max_contm[i])*float(df.loc[i, col]) if np.isnan(gnr_percnt[i]) else float(gnr_percnt[i])

        for i in range(len(df)):
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "open_pit1"] = main_func("open_pit1", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "open_pit2"] = main_func("open_pit2", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "open_pit3"] = main_func("open_pit3", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "underground1"] = main_func("underground1", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "underground2"] = main_func("underground2", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "underground3"] = main_func("underground3", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "underground4"] = main_func("underground4", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "underground5"] = main_func("underground5", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "underground6"] = main_func("underground6", i)
            self.perct_inventory_sulphur_contaminated_db_df.at[i, "underground7"] = main_func("underground7", i)



    # A/S workings for sedimentary bauxite
    def perct_AA_percentile_mined_sb(self):
        dr_ai203 = self.output_db.loc[:, "de_rated_Al2O3"] # sub-column %Al2O3
        pf_factr = self.inputs.loc[:, "factor_x"]
        factr_x_flag = 1
        self.perct_AA_percentile_mined_sb_df = pd.DataFrame(columns=  self.perct_AA_df_cols)   #creating sub-columns
        self.perct_AA_percentile_mined_sb_df.at[0, :] = self.perct_AA_df_data0

        for i in range(len(dr_ai203)):
            str_gd  = min((1.0+0.164/2.0)*float(dr_ai203[i]), 0.85) if float(dr_ai203[i])>0 else np.nan       # Starting grade
            dpl_gd  = max((1.0+(0.76-1.0)/2.0)*float(dr_ai203[i]), 0.40) if float(dr_ai203[i])>0 else np.nan   # Depletion grade
            sc_mn   = (float(dr_ai203[i])-(min([str_gd,dpl_gd])))/(max([str_gd,dpl_gd])-min([str_gd,dpl_gd])) if float(dr_ai203[i])>0 else np.nan   # Scaled mean
            sc_vr   = ((((0.12*float(dr_ai203[i]))**2.0/16.0)/ (float(pf_factr[i]) if factr_x_flag == 1 else 1)))/(max([str_gd,dpl_gd])-min([str_gd,dpl_gd]))**2 if float(dr_ai203[i])>0 else np.nan    # Scaled variance
            alp_v   = (sc_mn*(sc_mn/sc_vr*(1.0-sc_mn)-1.0)) if float(dr_ai203[i])>0 else np.nan   # Alpha value
            bt_vl   = (1.0-sc_mn)*(sc_mn/sc_vr*(1.0-sc_mn)-1.0) if float(dr_ai203[i])>0 else np.nan   # Beta value
            perct_5th = (beta.ppf(1-0.05,alp_v, bt_vl, dpl_gd, str_gd - dpl_gd) + str_gd)/2 if float(dr_ai203[i])>0 else np.nan # percentile mined 5th

            data = [str_gd,dpl_gd,sc_mn,sc_vr,alp_v,bt_vl,perct_5th]

            # percentile mined 10th - 100th
            prev_pect = 0.05
            for j in range(10,101,5):
                v = (beta.ppf(1-prev_pect,alp_v, bt_vl, dpl_gd, str_gd - dpl_gd) + beta.ppf(1-j/100.0,alp_v, bt_vl, dpl_gd, str_gd - dpl_gd))/2 if float(dr_ai203[i])>0 else np.nan
                prev_pect = j/100.0
                data.append(v)

            self.perct_AA_percentile_mined_sb_df.at[i+1, :] = data


    # %SiO2 by percentile mined - sedimentary bauxite
    def perct_SiO2_percentile_mined_sb(self):
        dr_ai203 = self.output_db.loc[:, "de_rated_Al2O3"] # sub-column %Al2O3
        dr_as    = self.output_db.loc[:, "de_rated_AS"]  # sub-column A/S
        pf_factr = self.inputs.loc[:, "factor_x"]
        working_stock = self.output_db.loc[:, "working_stock_prior_to_AA_and_AS_de_rating000t"]
        # df       = pd.read_csv("outputs/as_workings_sedimentary_bauxite/perct_AA_percentile_mined-sedimentary_bauxite.csv")
        df = self.perct_AA_percentile_mined_sb_df
        factr_x_flag = 1
        self.perct_SiO2_percentile_mined_sb_df = pd.DataFrame(columns= self.perct_SiO2_df_cols)#creating sub-columns

        self.perct_SiO2_percentile_mined_sb_df.at[0, :] = self.perct_SiO2_df_data0

        for i in range(len(dr_ai203)):
            if np.isnan(float(dr_ai203[i])):
                dr_ai203.at[i] = 0
            if np.isnan(float(dr_as[i])):
                dr_as.at[i] = 0

            errors        = 1 if (float(dr_ai203[i])*float(dr_as[i])) == 0 else 0
            as_approx_max = np.nan if errors == 1 else 3.645/2*float(dr_as[i])
            as_approx_min = np.nan if errors == 1 else max([0, ((1+(0.453-1)/2)*float(dr_as[i]))])
            silica_avg    = np.nan if errors == 1 else float(dr_ai203[i])/float(dr_as[i])
            silica_total  = np.nan if errors == 1 else float(working_stock[i])*silica_avg
            start_grade   = np.nan if errors == 1 else float(df.loc[i, "Starting grade"])/as_approx_max
            dpl_grade     = np.nan if errors == 1 else float(df.loc[i, "Depletion grade"])/as_approx_min
            scaled_mean   = np.nan if errors == 1 else (silica_avg - min([start_grade,dpl_grade]))/(max([start_grade,dpl_grade])-min([start_grade,dpl_grade]))
            scaled_var    = np.nan if errors == 1 else ((0.215*silica_avg)**2/(float(pf_factr[i]) if factr_x_flag==1 else 1)/(max([start_grade,dpl_grade])-min([start_grade,dpl_grade]))**2)
            aplha_val     = np.nan if errors == 1 else scaled_mean*(scaled_mean*(1-scaled_mean)/scaled_var-1)
            beta_val      = np.nan if errors == 1 else (1-scaled_mean)*(scaled_mean*(1-scaled_mean)/scaled_var-1)
            perct_5th = np.nan if errors == 1 else (beta.ppf(0.05,aplha_val, beta_val, start_grade ,dpl_grade-start_grade)+ start_grade)/2    # percentile mined 5th

            data = [errors,as_approx_max,as_approx_min,silica_avg,silica_total,start_grade,dpl_grade,scaled_mean,scaled_var,aplha_val,beta_val,perct_5th]

            # percentile mined 10th - 100th
            prev_pect = 0.05
            for j in range(10,101,5):
                v = np.nan if errors == 1 else (beta.ppf(prev_pect,aplha_val, beta_val, start_grade ,dpl_grade-start_grade) + beta.ppf(j/100.0,aplha_val, beta_val,start_grade ,dpl_grade-start_grade))/2
                prev_pect = j/100.0
                data.append(v)

            self.perct_SiO2_percentile_mined_sb_df.at[i+1, :] = data


    # A/S workings for collapse accumulated bauxite
    def perct_AA_percentile_mined_cab(self):
        dr_ai203 = self.output_db.loc[:, "de_rated_Al2O3"] # sub-column %Al2O3
        pf_factr = self.inputs.loc[:, "factor_x"]
        factr_x_flag = 1
        self.perct_AA_percentile_mined_cab_df = pd.DataFrame(columns=self.perct_AA_df_cols) #creating sub-columns

        self.perct_AA_percentile_mined_cab_df.at[0, :] = self.perct_AA_df_data0

        for i in range(len(dr_ai203)):
            str_gd  = min((1.0+0.164/2.0)*float(dr_ai203[i]), 0.85) if float(dr_ai203[i])>0 else np.nan        # Starting grade
            dpl_gd  = max((1.0+(0.76-1.0)/2.0)*float(dr_ai203[i]), 0.40) if float(dr_ai203[i])>0 else np.nan   # Depletion grade
            sc_mn   = (float(dr_ai203[i])-(min([str_gd,dpl_gd])))/(max([str_gd,dpl_gd])-min([str_gd,dpl_gd])) if float(dr_ai203[i])>0 else np.nan   # Scaled mean
            sc_vr   = ((((0.12*float(dr_ai203[i]))**2.0/16.0)/ (float(pf_factr[i]) if factr_x_flag == 1 else 1)))/(max([str_gd,dpl_gd])-min([str_gd,dpl_gd]))**2 if float(dr_ai203[i])>0 else np.nan    # Scaled variance
            alp_v   = (sc_mn*(sc_mn/sc_vr*(1.0-sc_mn)-1.0)) if float(dr_ai203[i])>0 else np.nan   # Alpha value
            bt_vl   = (1.0-sc_mn)*(sc_mn/sc_vr*(1.0-sc_mn)-1.0) if float(dr_ai203[i])>0 else np.nan   # Beta value
            perct_5th = (beta.ppf(1-0.05,alp_v, bt_vl, dpl_gd, str_gd - dpl_gd) + str_gd)/2 if float(dr_ai203[i])>0 else np.nan # percentile mined 5th

            data = [str_gd,dpl_gd,sc_mn,sc_vr,alp_v,bt_vl,perct_5th]

            # percentile mined 10th - 100th
            prev_pect = 0.05
            for j in range(10,101,5):
                v = (beta.ppf(1-prev_pect,alp_v, bt_vl, dpl_gd, str_gd - dpl_gd) + beta.ppf(1-j/100.0,alp_v, bt_vl, dpl_gd, str_gd - dpl_gd))/2 if float(dr_ai203[i])>0 else np.nan
                prev_pect = j/100.0
                data.append(v)

            self.perct_AA_percentile_mined_cab_df.at[i+1, :] = data


    def perct_SiO2_percentile_mined_cab(self):
        dr_ai203 = self.output_db.loc[:, "de_rated_Al2O3"] # sub-column %Al2O3
        dr_as    = self.output_db.loc[:, "de_rated_AS"]  # sub-column A/S
        pf_factr = self.inputs.loc[:, "factor_x"]
        working_stock = self.output_db.loc[:, "working_stock_prior_to_AA_and_AS_de_rating000t"]
        # df       = pd.read_csv("outputs/as_workings_sedimentary_bauxite/perct_AA_percentile_mined-sedimentary_bauxite.csv")
        df = self.perct_AA_percentile_mined_sb_df
        factr_x_flag = 1
        self.perct_SiO2_percentile_mined_cab_df = pd.DataFrame(columns= self.perct_SiO2_cab_df_cols) #creating sub-columns

        self.perct_SiO2_percentile_mined_cab_df.at[0, :] = self.perct_SiO2_cab_df_data0

        for i in range(len(dr_ai203)):
            if np.isnan(float(dr_ai203[i])):
                dr_ai203.at[i] = 0
            if np.isnan(float(dr_as[i])):
                dr_as.at[i] = 0

            errors        = 1 if (float(dr_ai203[i])*float(dr_as[i])) == 0 else 0
            as_approx_max = np.nan if errors == 1 else 3.645/2*float(dr_as[i])
            as_approx_min = np.nan if errors == 1 else max([0, ((1+(0.453-1)/2)*float(dr_as[i]))])
            silica_avg    = np.nan if errors == 1 else float(dr_ai203[i])/float(dr_as[i])
            silica_total  = np.nan if errors == 1 else float(working_stock[i])*silica_avg
            start_grade   = np.nan if errors == 1 else float(df.loc[i, "Starting grade"])/as_approx_max
            dpl_grade     = np.nan if errors == 1 else float(df.loc[i, "Depletion grade"])/as_approx_min
            scaled_mean   = np.nan if errors == 1 else (silica_avg - min([start_grade,dpl_grade]))/(max([start_grade,dpl_grade])-min([start_grade,dpl_grade]))
            scaled_var    = np.nan if errors == 1 else ((0.215*silica_avg)**2/(float(pf_factr[i]) if factr_x_flag==1 else 1)/(max([start_grade,dpl_grade])-min([start_grade,dpl_grade]))**2)
            aplha_val     = np.nan if errors == 1 else scaled_mean*(scaled_mean*(1-scaled_mean)/scaled_var-1)
            beta_val      = np.nan if errors == 1 else (1-scaled_mean)*(scaled_mean*(1-scaled_mean)/scaled_var-1)
            perct_5th = np.nan if errors == 1 else (beta.ppf(0.05,aplha_val, beta_val, start_grade ,dpl_grade-start_grade)+ start_grade)/2    # percentile mined 5th

            data = [errors,as_approx_max,as_approx_min,silica_avg,silica_total,start_grade,dpl_grade,scaled_mean,scaled_var,aplha_val,beta_val,perct_5th]

            # percentile mined 10th - 100th
            prev_pect = 0.05
            for j in range(10,101,5):
                v = np.nan if errors == 1 else (beta.ppf(prev_pect,aplha_val, beta_val, start_grade ,dpl_grade-start_grade) + beta.ppf(j/100.0,aplha_val, beta_val,start_grade ,dpl_grade-start_grade))/2
                prev_pect = j/100.0
                data.append(v)

            self.perct_SiO2_percentile_mined_cab_df.at[i+1, :] = data


    # Mining costs
    def stripping_ratio_sb(self):
        deposit_tick = self.inputs.loc[:, "deposit_thickness"]
        self.stripping_ratio_sb_df = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(avg, ind):
            return avg/float(deposit_tick[ind])

        for i in range(len(deposit_tick)):
            self.avg1 = (0 + (125.0/3.0))/2
            self.avg2 = ((125.0/3.0) + (125.0*2.0/3.0))/2
            self.avg3 = ((125.0*2.0/3.0) + 125.0)/2
            self.stripping_ratio_sb_df.at[i, "open_pit1":"open_pit3"] = [main_func(self.avg1, i), main_func(self.avg2, i), main_func(self.avg3, i)]
            self.stripping_ratio_sb_df.at[i, "underground1":"underground7"] = 0.5



    def stripping_ratio_ca_bx(self):
        df = self.inputs
        self.stripping_ratio_ca_bx_df = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(df)):
            self.stripping_ratio_ca_bx_df.at[i, "open_pit1"] = 0.33


    def electricity_costs_msb_RMBt_ROM_ore(self):
        province   = self.lookuptable.loc[:, "province"] # Lookup table province column
        elc_use    = self.lookuptable.loc[:, "electricity_use"] # open_pit specific rates sub-column -- Electricity Use
        bs_elc_use = self.lookuptable.loc[:, "base_electricy_use"] # Under Ground specific rates -- Base electricy use
        elctr      = self.lookuptable.loc[:, "electricity"] # General rates and costs sub-column -- Electricity

        df         = self.inputs.loc[:, "province"] # Inputs province table
        # mc = pd.read_csv("outputs/mining_costs/stripping_ratio-sedimentary_bauxite.csv")
        mc = self.stripping_ratio_sb_df
        self.electricity_costs_msb_RMBt_ROM_ore_df      = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(mc)):
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "open_pit1"] = self.main_vlookup(df[i], province, elc_use) * (self.main_vlookup(df[i], province, elctr)*(1+float(mc.loc[i, "open_pit1"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "open_pit2"] = self.main_vlookup(df[i], province, elc_use) * (self.main_vlookup(df[i], province, elctr)*(1+float(mc.loc[i, "open_pit2"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "open_pit3"] = self.main_vlookup(df[i], province, elc_use) * (self.main_vlookup(df[i], province, elctr)*(1+float(mc.loc[i, "open_pit3"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "underground1"] = (self.main_vlookup(df[i], province, bs_elc_use)*(0.002*self.avg4+0.796)) * (self.main_vlookup(df[i], province, elctr) * (1+float(mc.loc[i, "underground1"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "underground2"] = (self.main_vlookup(df[i], province, bs_elc_use)*(0.002*self.avg5+0.796)) * (self.main_vlookup(df[i], province, elctr) * (1+float(mc.loc[i, "underground2"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "underground3"] = (self.main_vlookup(df[i], province, bs_elc_use)*(0.002*self.avg6+0.796)) * (self.main_vlookup(df[i], province, elctr) * (1+float(mc.loc[i, "underground3"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "underground4"] = (self.main_vlookup(df[i], province, bs_elc_use)*(0.002*self.avg7+0.796)) * (self.main_vlookup(df[i], province, elctr) * (1+float(mc.loc[i, "underground4"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "underground5"] = (self.main_vlookup(df[i], province, bs_elc_use)*(0.002*self.avg8+0.796)) * (self.main_vlookup(df[i], province, elctr) * (1+float(mc.loc[i, "underground5"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "underground6"] = (self.main_vlookup(df[i], province, bs_elc_use)*(0.002*self.avg9+0.796)) * (self.main_vlookup(df[i], province, elctr) * (1+float(mc.loc[i, "underground6"])))
            self.electricity_costs_msb_RMBt_ROM_ore_df.at[i, "underground7"] = (self.main_vlookup(df[i], province, bs_elc_use)*(0.002*self.avg10+0.796)) * (self.main_vlookup(df[i], province, elctr) * (1+float(mc.loc[i, "underground7"])))



    def electricity_costs_cab_RMB_t_ROM_ore(self):
        province = self.lookuptable.loc[:, "province"] # Lookup table province column
        elc_use  = self.lookuptable.loc[:, "electricity_use"] # open_pit specific rates sub-column -- Electricity Use
        elctr    = self.lookuptable.loc[:, "electricity"] # General rates and costs sub-column -- Electricity

        df       = self.inputs.loc[:, "province"] # Inputs province table
        # mc = pd.read_csv("outputs/mining_costs/stripping_ratio-sedimentary_bauxite.csv")
        mc = self.stripping_ratio_sb_df
        self.electricity_costs_cab_RMB_t_ROM_ore_df      = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(mc)):
            self.electricity_costs_cab_RMB_t_ROM_ore_df.at[i, "open_pit1"] = self.main_vlookup(df[i], province, elc_use) * self.main_vlookup(df[i], province, elctr)


    def diesel_costs_msb_RMB_t_ROM_ore(self):
        # Scale of open_pit mining (ktpy)
        province = self.lookuptable.loc[:, "province"] # Lookup table province columngnr_sr
        elctr    = self.lookuptable.loc[:, "diesel"] # General rates and costs sub-column -- Electricity

        df          = self.inputs.loc[:, "province"] # Inputs province table
        scale_op    = self.inputs.loc[:, "scale_openpit_mining"]
        scale_ug    = self.inputs.loc[:, "scale_underground_mining"]

        # mc = pd.read_csv("outputs/mining_costs/stripping_ratio-sedimentary_bauxite.csv")
        mc = self.stripping_ratio_sb_df
        self.diesel_costs_msb_RMB_t_ROM_ore_df      = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(mc)):
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "open_pit1"] =  (((0.005*self.avg1+1+(1*scale_op[i]**-0.129))/4.75) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "open_pit1"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "open_pit2"] =  (((0.005*self.avg2+1+(1*scale_op[i]**-0.129))/4.75) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "open_pit2"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "open_pit3"] =  (((0.005*self.avg3+1+(1*scale_op[i]**-0.129))/4.75) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "open_pit3"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "underground1"] =  (((1.5*(scale_ug[i]**-0.129))*3.5*1.2*2) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "underground1"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "underground2"] =  (((1.5*(scale_ug[i]**-0.129))*3.5*1.2*2) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "underground2"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "underground3"] =  (((1.5*(scale_ug[i]**-0.129))*3.5*1.2*2) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "underground3"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "underground4"] =  (((1.5*(scale_ug[i]**-0.129))*3.5*1.2*2) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "underground4"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "underground5"] =  (((1.5*(scale_ug[i]**-0.129))*3.5*1.2*2) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "underground5"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "underground6"] =  (((1.5*(scale_ug[i]**-0.129))*3.5*1.2*2) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "underground6"]))))
            self.diesel_costs_msb_RMB_t_ROM_ore_df.at[i, "underground7"] =  (((1.5*(scale_ug[i]**-0.129))*3.5*1.2*2) * ((self.main_vlookup(df[i], province, elctr) / 1000) * (1+float(mc.loc[i, "underground7"]))))



    def diesel_costs_mca_RMB_t_ROM_ore(self):
        province = self.lookuptable.loc[:, "province"] # Lookup table province column
        diesel   = self.lookuptable.loc[:, "diesel"] # General rates and costs sub-column -- Diesel
        scale_op    = self.inputs.loc[:, "scale_openpit_mining"]

        df          = self.inputs.loc[:, "province"] # Inputs province table
        # mc = pd.read_csv("outputs/mining_costs/stripping_ratio-collapse_accumulated_bx.csv")
        mc = self.stripping_ratio_ca_bx_df
        self.diesel_costs_mca_RMB_t_ROM_ore_df      = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(mc)):
            avg = 5
            self.diesel_costs_mca_RMB_t_ROM_ore_df.at[i, "open_pit1"] =  ((0.005*avg+1+(1*scale_op[i]**-0.129))/2.7) * (self.main_vlookup(df[i], province, diesel) / 1000) * (1+float(mc.loc[i, "open_pit1"]))


    def labour_costs_msb_RMB_t_ROM_ore(self):
        province = self.lookuptable.loc[:, "province"] # Lookup table province column
        lab1    = self.lookuptable.loc[:, "labourrmb"] # General rates and costs sub-column -- Labour RMB/year
        lab2    = self.lookuptable.loc[:, "labourhours"] # General rates and costs sub-column -- Labour Hours/year
        df      = self.inputs.loc[:, "province"] # Inputs province table
        # mc = pd.read_csv("outputs/mining_costs/stripping_ratio-sedimentary_bauxite.csv")
        mc = self.stripping_ratio_sb_df
        self.labour_costs_msb_RMB_t_ROM_ore_df      = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(mc)):
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "open_pit1"] =  ((0.0022*self.avg1+0.44)/4.75)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "open_pit1"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "open_pit2"] =  ((0.0022*self.avg2+0.44)/4.75)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "open_pit2"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "open_pit3"] =  ((0.0022*self.avg3+0.44)/4.75)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "open_pit3"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "underground1"] =  (((0.7 if self.avg4<125.0 else (0.01*self.avg4-0.5 if self.avg4<450.0 else 4)) *(0.66)+0.2) * 0.7*1.2*2)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "underground1"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "underground2"] =  (((0.7 if self.avg5<125.0 else (0.01*self.avg5-0.5 if self.avg5<450.0 else 4)) *(0.66)+0.2) * 0.7*1.2*2)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "underground1"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "underground3"] =  (((0.7 if self.avg6<125.0 else (0.01*self.avg6-0.5 if self.avg6<450.0 else 4)) *(0.66)+0.2) * 0.7*1.2*2)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "underground1"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "underground4"] =  (((0.7 if self.avg7<125.0 else (0.01*self.avg7-0.5 if self.avg7<450.0 else 4)) *(0.66)+0.2) * 0.7*1.2*2)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "underground1"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "underground5"] =  (((0.7 if self.avg8<125.0 else (0.01*self.avg8-0.5 if self.avg8<450.0 else 4)) *(0.66)+0.2) * 0.7*1.2*2)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "underground1"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "underground6"] =  (((0.7 if self.avg9<125.0 else (0.01*self.avg9-0.5 if self.avg9<450.0 else 4)) *(0.66)+0.2) * 0.7*1.2*2)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "underground1"]))
            self.labour_costs_msb_RMB_t_ROM_ore_df.at[i, "underground7"] =  (((0.7 if self.avg10<125.0 else (0.01*self.avg10-0.5 if self.avg10<450.0 else 4)) *(0.66)+0.2) * 0.7*1.2*2)*(self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "underground1"]))



    def labour_costs_mcab_RMB_t_ROM_ore(self):
        province = self.lookuptable.loc[:, "province"] # Lookup table province column
        lab1    = self.lookuptable.loc[:, "labourrmb"] # General rates and costs sub-column -- Labour RMB/year
        lab2    = self.lookuptable.loc[:, "labourhours"] # General rates and costs sub-column -- Labour Hours/year

        df      = self.inputs.loc[:, "province"] # Inputs province table
        # mc = pd.read_csv("outputs/mining_costs/stripping_ratio-collapse_accumulated_bx.csv")
        mc = self.stripping_ratio_ca_bx_df
        self.labour_costs_mcab_RMB_t_ROM_ore_df      = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(mc)):
            avg = 5
            self.labour_costs_mcab_RMB_t_ROM_ore_df.at[i, "open_pit1"] =  ((0.0022*avg+0.44)/2.7) * (self.main_vlookup(df[i], province, lab1)/self.main_vlookup(df[i], province, lab2)) * (1+float(mc.loc[i, "open_pit1"]))



    def mine_transport_costs_sedimentary(self):
        # df = pd.read_csv('outputs/mining_costs/labour_costs-mining_collapse_accumulated_bauxite-RMB-tROM_ore.csv')
        df = self.labour_costs_mcab_RMB_t_ROM_ore_df
        self.mine_transport_costs_sedimentary_df = pd.DataFrame(columns=self.op_ug_cols)

        for i in range(len(df)):
            self.mine_transport_costs_sedimentary_df.at[i, "open_pit1":"underground7"] = 0



    def washing_factor(self):
        self.washing_factor_df = pd.DataFrame(columns=["For Collapse Accumulated bauxite … to dry ore assume 17.5% H2O"])

        for i in range(len(self.inputs)):
            self.washing_factor_df.at[i,"For Collapse Accumulated bauxite … to dry ore assume 17.5% H2O"] = 2/(1-17.5/100)


    def costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb(self):
        ec_msb  = self.electricity_costs_msb_RMBt_ROM_ore_df # pd.read_csv("outputs/mining_costs/electricity_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore.csv")
        dc_msb  = self.diesel_costs_msb_RMB_t_ROM_ore_df     # pd.read_csv("outputs/mining_costs/diesel_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore.csv")
        ls_msb  = self.labour_costs_msb_RMB_t_ROM_ore_df     # pd.read_csv("outputs/mining_costs/labour_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore.csv")
        im_cost = self.mine_transport_costs_sedimentary_df   # pd.read_csv("outputs/mining_costs/In_Mine_Transport_costs-sedimentary.csv")
        dc_sb   = self.inputs.loc[:, "dressingcost_sedimentarybauxite"]
        ec_op   = self.inputs.loc[:, "explosivecost_openpit"]
        ec_ug   = self.inputs.loc[:, "explosivecost_underground"]

        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df  = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(col, i, exp_df):
            return (ec_msb.loc[i,col] + dc_msb.loc[i,col] + ls_msb.loc[i,col] + im_cost.loc[i,col] + dc_sb[i] + exp_df[i])

        for i in range(len(ec_msb)):
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "open_pit1"] = main_func("open_pit1", i, ec_op)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "open_pit2"] = main_func("open_pit2", i, ec_op)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "open_pit3"] = main_func("open_pit3", i, ec_op)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "underground1"] = main_func("underground1", i, ec_ug)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "underground2"] = main_func("underground2", i, ec_ug)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "underground3"] = main_func("underground3", i, ec_ug)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "underground4"] = main_func("underground4", i, ec_ug)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "underground5"] = main_func("underground5", i, ec_ug)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "underground6"] = main_func("underground6", i, ec_ug)
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.at[i, "underground7"] = main_func("underground7", i, ec_ug)


    def costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx(self):
        ec_ecab  = self.electricity_costs_cab_RMB_t_ROM_ore_df  # pd.read_csv("outputs/mining_costs/electricity_costs-collapse_accumulated_bauxite-RMB-t_ROM_ore.csv")
        dc_mcab  = self.diesel_costs_mca_RMB_t_ROM_ore_df       # pd.read_csv("outputs/mining_costs/diesel_costs-mining_collapse_accumulated_bauxite-RMB-t_ROM_ore.csv")
        ls_mcab  = self.labour_costs_mcab_RMB_t_ROM_ore_df      # pd.read_csv("outputs/mining_costs/labour_costs-mining_collapse_accumulated_bauxite-RMB-tROM_ore.csv")
        im_cost = self.mine_transport_costs_sedimentary_df      # pd.read_csv("outputs/mining_costs/In_Mine_Transport_costs-sedimentary.csv")
        ws_fac  = self.washing_factor_df       # pd.read_csv("outputs/mining_costs/washing_factor.csv")
        dc_cab  = self.inputs.loc[:, "dressingcost_collapseaccumulatedbauxite"]
        ec_op   = self.inputs.loc[:, "explosivecost_openpit"]

        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df  = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(col, ind):
            return (((ec_ecab.loc[i,col]*2) + (dc_mcab.loc[i,col]*2) + (ls_mcab.loc[i,col]*2) + (im_cost.loc[i,col]*2)) + float(dc_cab[i]) + float(ec_op[i])) * float(ws_fac.iloc[i, 0])

        for i in range(len(ec_ecab)):
            self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df.at[i, "open_pit1"] = main_func("open_pit1", i)


    def taxes_royalties_Other_allowance_Capital_up_front_costs_charge(self):
        province    = self.lookuptable.loc[:, "province"] # Lookup table province column
        tax_royalty = self.lookuptable.loc[:, "tax_-_royalty"] # General rates and costs sub-column -- Tax - Royalty
        tax_other   = self.lookuptable.loc[:, "tax_-_other"] # General rates and costs sub-column -- Tax - Other

        df          = self.inputs.loc[:, "province"]

        self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df  = pd.DataFrame(
            columns=["Taxes and Royalties","Other Allowance","Capital  / Up Front Costs Charge","Capital  / Up Front Costs Charge.1"]
        )

        self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df.at[0,:] = ["","","open pit - RMB/t ROM ore","underground - RMB/t ROM ore"]
        self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df.at[1,:] = ["","","all depths","all depths"]

        for i in range(len(df)):
            self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df.at[i+2, "Taxes and Royalties"] = self.main_vlookup(df[i], province, tax_royalty) + self.main_vlookup(df[i], province, tax_other)
            self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df.at[i+2, "Other Allowance"] = 0
            self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df.at[i+2, "Capital  / Up Front Costs Charge"] = 15
            self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df.at[i+2, "Capital  / Up Front Costs Charge.1"] = 60



    def total_mining_costs_RMB_t_ore_sb(self):
        cost_cb    = self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df    # pd.read_csv("outputs/mining_costs/costs_RMB-t_ROM_ore_before_per-t-ore_charges-sedimentary_bauxite.csv")
        tax_others = self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df  # pd.read_csv("outputs/mining_costs/taxes_royalties_Other_allowance_Capital_up_front_costs_charge.csv")

        self.total_mining_costs_RMB_t_ore_sb_df     = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(col, a, op_ug):
            return ((float(cost_cb.loc[a,col]) + float(tax_others.loc[a+2,"Taxes and Royalties"])) * (1 + float(tax_others.loc[a+2,"Other Allowance"])) + float(op_ug.iloc[a+2]))

        for i in range(len(cost_cb)):
            op = tax_others.loc[:, "Capital  / Up Front Costs Charge"]
            ug = tax_others.loc[:, "Capital  / Up Front Costs Charge.1"]

            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "open_pit1"] = main_func("open_pit1", i, op)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "open_pit2"] = main_func("open_pit2", i, op)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "open_pit3"] = main_func("open_pit3", i, op)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "underground1"] = main_func("underground1", i, ug)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "underground2"] = main_func("underground2", i, ug)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "underground3"] = main_func("underground3", i, ug)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "underground4"] = main_func("underground4", i, ug)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "underground5"] = main_func("underground5", i, ug)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "underground6"] = main_func("underground6", i, ug)
            self.total_mining_costs_RMB_t_ore_sb_df.at[i, "underground7"] = main_func("underground7", i, ug)



    def total_mining_costs_RMB_t_ore_cab(self):
        cost_cabx    = self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df   # pd.read_csv("outputs/mining_costs/costs_RMB-t_ROM_ore_before_per-t-ore_charges-collapse_accumulated_bx.csv")
        tax_others = self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df  # pd.read_csv("outputs/mining_costs/taxes_royalties_Other_allowance_Capital_up_front_costs_charge.csv")
        ws_fac  = self.washing_factor_df       # pd.read_csv("outputs/mining_costs/washing_factor.csv")

        self.total_mining_costs_RMB_t_ore_cab_df     = pd.DataFrame(columns=self.op_ug_cols)

        def main_func(col, a, op_ug):
            return ((float(cost_cabx.loc[a,col]) + float(tax_others.loc[a+2,"Taxes and Royalties"])) * (1 + float(tax_others.loc[a+2,"Other Allowance"])) + (float(op_ug.iloc[a+2]) * float(ws_fac.iloc[a])))

        for i in range(len(cost_cabx)):
            op = tax_others.loc[:, "Capital  / Up Front Costs Charge"]

            self.total_mining_costs_RMB_t_ore_cab_df.at[i, "open_pit1"] = main_func("open_pit1", i, op)



    # Delivery costs
    def delivery_costs_RG_RT(self):
        province  = self.lookuptable.loc[:, "province"] # Lookup table province column
        transp    = self.lookuptable.loc[:, "transport"].astype(float)
        avd_dist  = self.inputs.loc[:, "average_distance_refinery"].astype(float)

        df = self.inputs.loc[:,"province"]
        lime      = self.lookuptable.loc[:, "lime"].astype(float)
        ter_rmb   = self.lookuptable.loc[:, "thermal_coalrmb"].astype(float)
        ter_kcal  = self.lookuptable.loc[:, "thermal_coalkcal"].astype(float)
        anth_rmb  = self.lookuptable.loc[:, "anthratcite_coalrmb"].astype(float)
        anth_kcal = self.lookuptable.loc[:, "anthratcite_coalkcal"].astype(float)

        self.delivery_costs_RG_RT_df = pd.DataFrame(columns=[
            "Delivery Costs",
            "High sulphur treatmenty charge",
            "Alumina DSP Factor t.aa/t.R.Si",
            "Recovery of AA after losses",
            "Caustic Use Factor t.NAOH/t.R.Si",
            "Energy Costs",
            "Lime Costs",
            "Other Costs (labour, maintenance, other consumables)"
        ])

        self.delivery_costs_RG_RT_df.at[0, :] = ["RMB/t ore", "RMB/t_AA", "", "", "", "RMB/t_AA", "RMB/t_AA", "RMB/t_AA"]

        def mvl(search, target):
            v = province.map(lambda x: x.lower() == search.lower())
            try:
                return float(target[v].tolist()[0])
            except Exception:
                pass

        for i in range(len(df)):
            self.delivery_costs_RG_RT_df.at[i+1,"Delivery Costs"] = mvl(df[i], transp) * float(avd_dist.iloc[i])
            self.delivery_costs_RG_RT_df.at[i+1,"High sulphur treatmenty charge"] = 180
            self.delivery_costs_RG_RT_df.at[i+1,"Alumina DSP Factor t.aa/t.R.Si"] = 1.23
            self.delivery_costs_RG_RT_df.at[i+1,"Recovery of AA after losses"] = 0.9
            self.delivery_costs_RG_RT_df.at[i+1,"Caustic Use Factor t.NAOH/t.R.Si"] = 0.6
            self.delivery_costs_RG_RT_df.at[i+1,"Energy Costs"] = (13.5/(4.18* mvl(df[i],ter_kcal) /1000) * mvl(df[i],ter_rmb)) + (5 / (4.18*mvl(df[i],anth_kcal) / 1000) * mvl(df[i],anth_rmb))
            self.delivery_costs_RG_RT_df.at[i+1,"Lime Costs"] = 0.22 * mvl(df[i],lime)
            self.delivery_costs_RG_RT_df.at[i+1,"Other Costs (labour, maintenance, other consumables)"] = self.other_controls.loc[0,"other_costs"]


    def t_div_t_BX_AA_percentile_mined_sb(self):
        rg_rr   = self.delivery_costs_RG_RT_df
        df_sio2 = self.perct_SiO2_percentile_mined_sb_df
        df_aa   = self.perct_AA_percentile_mined_sb_df

        self.t_div_t_BX_AA_percentile_mined_sb_df = pd.DataFrame(
            columns= [f"percentile mined {i/100}"  for i in range(5,101,5)]
        )
        self.t_div_t_BX_AA_percentile_mined_sb_df.at[0,:] = [f"{i}th" for i in range(5,101,5)]

        for i in range(1, len(df_aa)):
            for a in range(5,101,5):
                self.t_div_t_BX_AA_percentile_mined_sb_df.at[i, f"percentile mined {a/100}"] = 0 if (np.isnan(float(df_aa.loc[i, f"percentile mined {a/100}"])) or np.isnan(float(df_sio2.loc[i, f"percentile mined {a/100}"]))) else 1/((float(df_aa.loc[i, f"percentile mined {a/100}"]) - float(rg_rr.loc[i, "Alumina DSP Factor t.aa/t.R.Si"]) * float(df_sio2.loc[i, f"percentile mined {a/100}"])) * float(rg_rr.loc[i, "Recovery of AA after losses"]))


    def t_div_t_BX_AA_percentile_mined_cab(self):
        rg_rr   = self.delivery_costs_RG_RT_df      # pd.read_csv("outputs/delivery_costs/RGtoRR_Diesel_cost.csv")
        df_sio2 = self.perct_SiO2_percentile_mined_cab_df       # pd.read_csv("outputs/as_workings_collapse_accumulated_bauxite/perct_SiO2_percentile_mined-collapse_sedimentary_bauxite.csv")
        df_aa   = self.perct_AA_percentile_mined_cab_df     # pd.read_csv("outputs/as_workings_collapse_accumulated_bauxite/perct_AA_percentile_mined-collapse_accumulated_bauxite.csv")

        self.t_div_t_BX_AA_percentile_mined_cab_df = pd.DataFrame(
            columns= [f"percentile mined {i/100}"  for i in range(5,101,5)]
        )
        self.t_div_t_BX_AA_percentile_mined_cab_df.at[0,:] = [f"{i}th" for i in range(5,101,5)]

        for i in range(1, len(df_aa)):
            for a in range(5,101,5):
                self.t_div_t_BX_AA_percentile_mined_cab_df.at[i, f"percentile mined {a/100}"] = 0 if (np.isnan(float(df_aa.loc[i, f"percentile mined {a/100}"])) or np.isnan(float(df_sio2.loc[i, f"percentile mined {a/100}"]))) else 1/((float(df_aa.loc[i, f"percentile mined {a/100}"]) - float(rg_rr.loc[i, "Alumina DSP Factor t.aa/t.R.Si"]) * float(df_sio2.loc[i, f"percentile mined {a/100}"])) * float(rg_rr.loc[i, "Recovery of AA after losses"]))



    def caustic_use_percentile_mined_sb_t_div_t_AA(self):
        rg_rr   = self.delivery_costs_RG_RT_df      # pd.read_csv("outputs/delivery_costs/RGtoRR_Diesel_cost.csv")
        df_sio2 = self.perct_SiO2_percentile_mined_sb_df     # pd.read_csv("outputs/as_workings_sedimentary_bauxite/perct_SiO2_percentile_mined-sedimentary_bauxite.csv")
        tt_sb   = self.t_div_t_BX_AA_percentile_mined_sb_df  # pd.read_csv("outputs/delivery_costs/t-t_BX:AA_by_percentile_mined-sedimentary_bauxite.csv")

        self.caustic_use_percentile_mined_sb_t_div_t_AA_df = pd.DataFrame(
            columns= [f"percentile mined {i/100}"  for i in range(5,101,5)]
        )
        self.caustic_use_percentile_mined_sb_t_div_t_AA_df.at[0,:] = [f"{i}th" for i in range(5,101,5)]

        for i in range(1, len(tt_sb)):
            for a in range(5,101,5):
                self.caustic_use_percentile_mined_sb_t_div_t_AA_df.at[i,f"percentile mined {a/100}"] = 0 if np.isnan(float(df_sio2.loc[i, f"percentile mined {a/100}"])) else float(df_sio2.loc[i, f"percentile mined {a/100}"]) * float(rg_rr.loc[i, "Caustic Use Factor t.NAOH/t.R.Si"]) * float(tt_sb.loc[i, f"percentile mined {a/100}"]) + 0.035



    def caustic_use_percentile_mined_cab(self):
        rg_rr   = self.delivery_costs_RG_RT_df      # pd.read_csv("outputs/delivery_costs/RGtoRR_Diesel_cost.csv")
        df_sio2 = self.perct_SiO2_percentile_mined_cab_df       # pd.read_csv("outputs/as_workings_collapse_accumulated_bauxite/perct_SiO2_percentile_mined-collapse_sedimentary_bauxite.csv")
        tt_sb   = self.t_div_t_BX_AA_percentile_mined_cab_df    # pd.read_csv("outputs/delivery_costs/t-t_BX:AA_by_percentile_mined-collapse_accumlulated_bx.csv")

        self.caustic_use_percentile_mined_cab_df = pd.DataFrame(
            columns= [f"percentile mined {i/100}"  for i in range(5,101,5)]
        )
        self.caustic_use_percentile_mined_cab_df.at[0,:] = [f"{i}th" for i in range(5,101,5)]

        for i in range(1, len(tt_sb)):
            for a in range(5,101,5):
                self.caustic_use_percentile_mined_cab_df.at[i,f"percentile mined {a/100}"] = 0 if np.isnan(float(df_sio2.loc[i, f"percentile mined {a/100}"])) else float(df_sio2.loc[i, f"percentile mined {a/100}"]) * float(rg_rr.loc[i, "Caustic Use Factor t.NAOH/t.R.Si"]) * float(tt_sb.loc[i, f"percentile mined {a/100}"]) + 0.035



    def CBIX_BX_AA_production_cost(self):
        index = -1 * int(self.lookuptable.shape[0] - self.lookuptable[self.lookuptable.loc[:, "province"].map(lambda x: x.lower() == 'shaanxi')].index.to_numpy()[-1])

        oc_df = self.other_controls
        lime       = float(self.lookuptable.loc[:, "lime"].iloc[index])
        caustic_sd = float(self.lookuptable.loc[:, "caustic_soda"].iloc[index])
        tc_rmb     = float(self.lookuptable.loc[:, "thermal_coalrmb"].iloc[index])
        tc_kcal    = float(self.lookuptable.loc[:, "thermal_coalkcal"].iloc[index])
        ant_rmb    = float(self.lookuptable.loc[:, "anthratcite_coalrmb"].iloc[index])
        ant_kcal   = float(self.lookuptable.loc[:, "anthratcite_coalkcal"].iloc[index])
        mud_d      = float(self.lookuptable.loc[:, "mud_disposal"].iloc[index])

        self.CBIX_BX_AA_production_cost_df     = pd.DataFrame(
            columns= [
                "Bx delivery cost (RMB/dmt)",
                "Bx Delivered Cost",
                "Bx (t/t_AA)",
                "NaOH use (t/t_AA)",
                "Bx cost (RMB/t_AA)",
                "Caustic cost (RMB/t_AA)",
                "Energy Cost (RMB/t_AA)",
                "Lime Cost (RMB/t_AA)",
                "Residue disposal cost (RMB/t_AA)",
                "Other Costs (labour, maintenance, other consumables)",
                "Total Cost - CBIX BX"
            ]
        )
        self.CBIX_BX_AA_production_cost_df.at[0,:] = ["RMB/dmt","RMB/dmt","","","","","","","","RMB/dmt",""]


        self.CBIX_BX_AA_production_cost_df.at[1, "Bx delivery cost (RMB/dmt)"] = 60
        self.CBIX_BX_AA_production_cost_df.at[1, "Bx Delivered Cost"] = oc_df.loc[0, "cbix_Bx_price"] * oc_df.loc[0, "fxrate"] + self.CBIX_BX_AA_production_cost_df.loc[1, "Bx delivery cost (RMB/dmt)"]
        self.CBIX_BX_AA_production_cost_df.at[1, "Bx (t/t_AA)"]  = 1/((50/100-5/100*1)*90/100)
        self.CBIX_BX_AA_production_cost_df.at[1, "NaOH use (t/t_AA)"] = 5/100* self.CBIX_BX_AA_production_cost_df.loc[1, "Bx (t/t_AA)"] *0.9+0.035
        self.CBIX_BX_AA_production_cost_df.at[1, "Bx cost (RMB/t_AA)"] = self.CBIX_BX_AA_production_cost_df.loc[1, "Bx Delivered Cost"] * self.CBIX_BX_AA_production_cost_df.at[1, "Bx (t/t_AA)"]
        self.CBIX_BX_AA_production_cost_df.at[1, "Caustic cost (RMB/t_AA)"] = self.CBIX_BX_AA_production_cost_df.loc[1, "NaOH use (t/t_AA)"] * caustic_sd
        self.CBIX_BX_AA_production_cost_df.at[1, "Energy Cost (RMB/t_AA)"] = 12.15/(4.18*tc_kcal/1000) * tc_rmb + 5/(4.18*ant_kcal/1000) * ant_rmb
        self.CBIX_BX_AA_production_cost_df.at[1, "Lime Cost (RMB/t_AA)"] = 0.036 * lime
        self.CBIX_BX_AA_production_cost_df.at[1, "Residue disposal cost (RMB/t_AA)"] =  (self.CBIX_BX_AA_production_cost_df.loc[1, "Bx (t/t_AA)"] -1 + self.CBIX_BX_AA_production_cost_df.loc[1, "NaOH use (t/t_AA)"] + +0.036) * mud_d
        self.CBIX_BX_AA_production_cost_df.at[1, "Other Costs (labour, maintenance, other consumables)"] = oc_df.loc[0, "other_costs"]
        self.CBIX_BX_AA_production_cost_df.at[1, "Total Cost - CBIX BX"] = float(self.CBIX_BX_AA_production_cost_df.loc[1, "Bx cost (RMB/t_AA)"]) + float(self.CBIX_BX_AA_production_cost_df.loc[1, "Caustic cost (RMB/t_AA)"]) + float(self.CBIX_BX_AA_production_cost_df.loc[1, "Energy Cost (RMB/t_AA)"]) + float(self.CBIX_BX_AA_production_cost_df.loc[1, "Lime Cost (RMB/t_AA)"]) + float(self.CBIX_BX_AA_production_cost_df.loc[1, "Residue disposal cost (RMB/t_AA)"]) + float(self.CBIX_BX_AA_production_cost_df.loc[1, "Other Costs (labour, maintenance, other consumables)"])

        for i in range(len(self.inputs)):
            self.CBIX_BX_AA_production_cost_df.at[i+2, "Total Cost - CBIX BX"] = self.CBIX_BX_AA_production_cost_df.loc[1, "Total Cost - CBIX BX"]



    def final_costs(self):
        province = self.lookuptable.loc[:, "province"] # Lookup table province column
        caustic_sd = self.lookuptable.loc[:, "caustic_soda"]
        mud_d      = self.lookuptable.loc[:,  "mud_disposal"]

        df      = self.inputs.loc[:, "province"] # Inputs province table
        rg_rr   = self.delivery_costs_RG_RT_df      # pd.read_csv("outputs/delivery_costs/RGtoRR_Diesel_cost.csv")
        aa_sb   = self.t_div_t_BX_AA_percentile_mined_sb_df     # pd.read_csv("outputs/delivery_costs/t-t_BX:AA_by_percentile_mined-sedimentary_bauxite.csv")
        cu_sb   = self.caustic_use_percentile_mined_sb_t_div_t_AA_df     # pd.read_csv("outputs/delivery_costs/caustic_use_by_percentile_mined-sedimentary_bauxite_t-t_AA.csv")
        aa_cab  = self.t_div_t_BX_AA_percentile_mined_cab_df        # pd.read_csv("outputs/delivery_costs/t-t_BX:AA_by_percentile_mined-collapse_accumlulated_bx.csv")
        cu_cab  = self.caustic_use_percentile_mined_cab_df      # pd.read_csv("outputs/delivery_costs/caustic_use_by_percentile_mined-collapse_accumlulated_bx.csv")
        tm_sb   = self.total_mining_costs_RMB_t_ore_sb_df       # pd.read_csv("outputs/mining_costs/total_mining_costs_RMB-t_ore-sedimentary_bauxite.csv")
        tm_cab  = self.total_mining_costs_RMB_t_ore_cab_df      # pd.read_csv("outputs/mining_costs/total_mining_costs_RMB-t_ore-collapse_accumulated_bauxite.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" for i in range(1,201)]
        row2_sl = ["Low" for i in range(1,201)]
        row3_sl = [avgs[i] for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(10) for k in range(5,101,5)]

        new_df1 = pd.DataFrame(
            columns=row1_sl
        )
        new_df1.at[0,:] = row2_sl
        new_df1.at[1,:] = row3_sl
        new_df1.at[2,:] = row4_sl

        for i in range(1, len(df)):
            data = []

            for j in range(len(avgs)):
                for k in range(5,101,5):
                    v = (float(aa_sb.loc[i, f"percentile mined {k/100}"])*(float(tm_sb.iloc[i, j])+float(rg_rr.loc[i, "Delivery Costs"]))) + (float(cu_sb.loc[i, f"percentile mined {k/100}"])*self.main_vlookup(df.iloc[i-1], province, caustic_sd)) + float(rg_rr.loc[i,"Energy Costs"]) + float(rg_rr.loc[i,"Lime Costs"]) + ((float(aa_sb.loc[i, f"percentile mined {k/100}"])-1 + float(cu_sb.loc[i, f"percentile mined {k/100}"])+0.22) * self.main_vlookup(df.iloc[-1], province, mud_d)) + float(rg_rr.loc[i, "Other Costs (labour, maintenance, other consumables)"]) + 0
                    data.append(v)

            new_df1.at[i+2,:] = data

        # Sed'try high
        row1_sh = [f"Sed'try {i}" for i in range(1,201)]
        row2_sh = ["High" for i in range(1,201)]
        row3_sh = [avgs[i] for i in range(10) for j in range(20)]
        row4_sh = [f"{k}th" for i in range(10) for k in range(5,101,5)]

        new_df2 = pd.DataFrame(
            columns=row1_sh
        )
        new_df2.at[0,:] = row2_sh
        new_df2.at[1,:] = row3_sh
        new_df2.at[2,:] = row4_sh


        for i in range(1, len(df)):
            data = []

            for j in range(len(avgs)):
                for k in range(5,101,5):
                    v = (float(aa_sb.loc[i, f"percentile mined {k/100}"])*(float(tm_sb.iloc[i, j])+float(rg_rr.loc[i, "Delivery Costs"]))) + (float(cu_sb.loc[i, f"percentile mined {k/100}"])*self.main_vlookup(df.iloc[i-1], province, caustic_sd)) + float(rg_rr.loc[i,"Energy Costs"]) + float(rg_rr.loc[i,"Lime Costs"]) + ((float(aa_sb.loc[i, f"percentile mined {k/100}"])-1 + float(cu_sb.loc[i, f"percentile mined {k/100}"])+0.22) * self.main_vlookup(df.iloc[i-1], province, mud_d)) + float(rg_rr.loc[i, "Other Costs (labour, maintenance, other consumables)"]) + float(rg_rr.loc[i,"High sulphur treatmenty charge"])
                    data.append(v)

            new_df2.at[i+2,:] = data

        # Coll. Acc. low
        row1_cl = [f"Coll. Acc. {i}" for i in range(1,21)]
        row2_cl = ["Low" for i in range(1,21)]
        row3_cl = [avgs[0] for i in range(20)]
        row4_cl = [f"{k}th" for k in range(5,101,5)]

        new_df3 = pd.DataFrame(
            columns=row1_cl
        )
        new_df3.at[0,:] = row2_cl
        new_df3.at[1,:] = row3_cl
        new_df3.at[2,:] = row4_cl


        for i in range(1, len(df)):
            data = []
            for k in range(5,101,5):
                v = (float(aa_cab.loc[i, f"percentile mined {k/100}"])*(float(tm_cab.iloc[i, 0])+float(rg_rr.loc[i, "Delivery Costs"]))) + (float(cu_cab.loc[i, f"percentile mined {k/100}"])*self.main_vlookup(df.iloc[i-1], province, caustic_sd)) + float(rg_rr.loc[i,"Energy Costs"]) + float(rg_rr.loc[i,"Lime Costs"]) + ((float(aa_cab.loc[i, f"percentile mined {k/100}"])-1 + float(cu_cab.loc[i, f"percentile mined {k/100}"])+0.22) * self.main_vlookup(df.iloc[i-1], province, mud_d)) + float(rg_rr.loc[i, "Other Costs (labour, maintenance, other consumables)"]) + 0
                data.append(v)

            new_df3.at[i+2,:] = data

        # concat(objs, axis=0, join="outer", join_axes=None, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, sort=None, copy=True)
        self.final_costs_df = pd.concat([new_df1, new_df2, new_df3], axis=1, join="outer", sort=True)


    def tonnages_in_categories(self):
        perct_sulpher_sb  = self.perct_inventory_sulphur_contaminated_db_df     # pd.read_csv("outputs/suplhur_contamination_sedimentary_bauxite_only/perct_inventory_sulphur_contaminated_by_depth_bucket.csv")
        depth_range_sb    = self.sedimentary_bauxite_td_df        # pd.read_csv("outputs/sedimentary_bauxite-tonnages_per_depth_range.csv")
        depth_range_cab   = self.collapse_accm_bauxite_df        # pd.read_csv("outputs/collapse_accumulated_bauxite-tonnages_per_depth_range.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" for i in range(1,201)]
        row2_sl = ["Low" for i in range(1,201)]
        row3_sl = [avgs[i] for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(10) for k in range(5,101,5)]

        new_df1 = pd.DataFrame(
            columns=row1_sl
        )
        new_df1.at[0,:] = row2_sl
        new_df1.at[1,:] = row3_sl
        new_df1.at[2,:] = row4_sl


        for i in range(len(self.inputs)):
            data = []
            for k in range(len(avgs)):
                for j in range(20):
                    v = float(depth_range_sb.iloc[i, k])/20 * (1-float(perct_sulpher_sb.iloc[i, k]))
                    data.append(v)

            new_df1.at[i+3,:] = data

        # Sed'try high
        row1_sh = [f"Sed'try {i}" for i in range(1,201)]
        row2_sh = ["High" for i in range(1,201)]
        row3_sh = [avgs[i] for i in range(10) for j in range(20)]
        row4_sh = [f"{k}th" for i in range(10) for k in range(5,101,5)]

        new_df2 = pd.DataFrame(
            columns=row1_sh
        )
        new_df2.at[0,:] = row2_sh
        new_df2.at[1,:] = row3_sh
        new_df2.at[2,:] = row4_sh


        for i in range(len(self.inputs)):
            data = []
            for k in range(len(avgs)):
                for j in range(20):
                    v = float(depth_range_sb.iloc[i, k])/20 * float(perct_sulpher_sb.iloc[i, k])
                    data.append(v)

            new_df2.at[i+3,:] = data

        # Coll. Acc. low
        row1_cl = [f"Coll. Acc. {i}" for i in range(1,21)]
        row2_cl = ["Low" for i in range(1,21)]
        row3_cl = [avgs[0] for i in range(20)]
        row4_cl = [f"{k}th" for k in range(5,101,5)]

        new_df3 = pd.DataFrame(
            columns=row1_cl
        )
        new_df3.at[0,:] = row2_cl
        new_df3.at[1,:] = row3_cl
        new_df3.at[2,:] = row4_cl


        for i in range(len(self.inputs)):
            data = []
            for k in range(len(avgs)):
                for j in range(2):
                    v = float(depth_range_cab.iloc[i, 0])/20
                    data.append(v)
            new_df3.at[i+3,:] = data

        # concat(objs, axis=0, join="outer", join_axes=None, ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, sort=None, copy=True)
        self.tonnages_in_categories_df = pd.concat([new_df1, new_df2, new_df3], axis=1, join="outer", sort=True)


    def cost_with_dummy_for_ranking(self):
        df = self.final_costs_df        # pd.read_csv("outputs/final_costs/final_costs.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" if i <= 400 else f"Coll. Acc. {i}" for i in range(1,421)]
        row2_sl = ["Low" if i <= 200 else "High" for i in range(1,401)]
        row3_sl = [avgs[i] for h in range(2) for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(21) for k in range(5,101,5)]

        [row2_sl.append("Low") for i in range(20)]
        [row3_sl.append(avgs[0]) for i in range(20)]

        self.cost_with_dummy_for_ranking_df = pd.DataFrame(
            columns=row1_sl
        )
        self.cost_with_dummy_for_ranking_df.at[0,:] = row2_sl
        self.cost_with_dummy_for_ranking_df.at[1,:] = row3_sl
        self.cost_with_dummy_for_ranking_df.at[2,:] = row4_sl


        for i in range(3, len(df)):
            data = []
            col_val = 1432
            for j in range(420):
                v = float(df.iloc[i, j]) + col_val / 1000000000
                data.append(v)
                col_val += 1

            self.cost_with_dummy_for_ranking_df.at[i,:] = data




    def ranks_by_costs(self):
        df = self.cost_with_dummy_for_ranking_df    # pd.read_csv("outputs/cost_with_dummy_for_ranking_reasons.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" if i <= 400 else f"Coll. Acc. {i}" for i in range(1,421)]
        row2_sl = ["Low" if i <= 200 else "High" for i in range(1,401)]
        row3_sl = [avgs[i] for h in range(2) for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(21) for k in range(5,101,5)]

        [row2_sl.append("Low") for i in range(20)]
        [row3_sl.append(avgs[0]) for i in range(20)]

        self.ranks_by_costs_df = pd.DataFrame(
            columns=row1_sl
        )
        self.ranks_by_costs_df.at[0,:] = row2_sl
        self.ranks_by_costs_df.at[1,:] = row3_sl
        self.ranks_by_costs_df.at[2,:] = row4_sl
        self.ranks_by_costs_df.at[3,:] = [f"rank {i}" for i in range(1, 421)]

        for i in range(3, len(df)):
            self.ranks_by_costs_df.at[i+1,:] = df.loc[i,:].rank(method="min", ascending=True)

    def cell_columns_by_cost_rank(self):
        df = self.ranks_by_costs_df     # pd.read_csv("outputs/ranks_by_costs.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" if i <= 400 else f"Coll. Acc. {i}" for i in range(1,421)]
        row2_sl = ["Low" if i <= 200 else "High" for i in range(1,401)]
        row3_sl = [avgs[i] for h in range(2) for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(21) for k in range(5,101,5)]

        [row2_sl.append("Low") for i in range(20)]
        [row3_sl.append(avgs[0]) for i in range(20)]

        self.cell_columns_by_cost_rank_df = pd.DataFrame(
            columns=row1_sl
        )
        self.cell_columns_by_cost_rank_df.at[0,:] = row2_sl
        self.cell_columns_by_cost_rank_df.at[1,:] = row3_sl
        self.cell_columns_by_cost_rank_df.at[2,:] = row4_sl
        self.cell_columns_by_cost_rank_df.at[3,:] = [f"rank {i}" for i in range(1, 421)]

        for i in range(4, len(df)):
            data = []
            col_val = 1854.0
            for j in range(1,421):
                for k in range(420):
                    if float(df.iloc[i,k]) == float(j):
                        data.append(k+col_val)
                        break

            self.cell_columns_by_cost_rank_df.at[i,:] = data



    def costs_by_cost_rank(self):
        final_costs  = self.final_costs_df      #  pd.read_csv("outputs/final_costs/final_costs.csv")
        cc_cost_rank = self.cell_columns_by_cost_rank_df        # pd.read_csv("outputs/cell_columns_by_cost_rank.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" if i <= 400 else f"Coll. Acc. {i}" for i in range(1,421)]
        row2_sl = ["Low" if i <= 200 else "High" for i in range(1,401)]
        row3_sl = [avgs[i] for h in range(2) for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(21) for k in range(5,101,5)]

        [row2_sl.append("Low") for i in range(20)]
        [row3_sl.append(avgs[0]) for i in range(20)]

        self.costs_by_cost_rank_df = pd.DataFrame(
            columns=row1_sl
        )
        self.costs_by_cost_rank_df.at[0,:] = row2_sl
        self.costs_by_cost_rank_df.at[1,:] = row3_sl
        self.costs_by_cost_rank_df.at[2,:] = row4_sl
        self.costs_by_cost_rank_df.at[3,:] = [f"rank {i}" for i in range(1, 421)]

        for i in range(4, len(cc_cost_rank)):
            data = []
            for j in range(420):
                v = final_costs.iloc[i-1, int(float(cc_cost_rank.iloc[i, j])-1854.0)]
                data.append(v)

            self.costs_by_cost_rank_df.at[i,:] = data



    def tonages_by_cost_rank(self):
        tonnages  = self.tonnages_in_categories_df  # pd.read_csv("outputs/tonnages_in_categories.csv")
        cc_cost_rank = self.cell_columns_by_cost_rank_df    # pd.read_csv("outputs/cell_columns_by_cost_rank.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" if i <= 400 else f"Coll. Acc. {i}" for i in range(1,421)]
        row2_sl = ["Low" if i <= 200 else "High" for i in range(1,401)]
        row3_sl = [avgs[i] for h in range(2) for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(21) for k in range(5,101,5)]

        [row2_sl.append("Low") for i in range(20)]
        [row3_sl.append(avgs[0]) for i in range(20)]

        self.tonages_by_cost_rank_df = pd.DataFrame(
            columns=row1_sl
        )
        self.tonages_by_cost_rank_df.at[0,:] = row2_sl
        self.tonages_by_cost_rank_df.at[1,:] = row3_sl
        self.tonages_by_cost_rank_df.at[2,:] = row4_sl
        self.tonages_by_cost_rank_df.at[3,:] = [f"rank {i}" for i in range(1, 421)]

        for i in range(4, len(cc_cost_rank)):
            data = []
            for j in range(420):
                v = tonnages.iloc[i-1, int(float(cc_cost_rank.iloc[i, j])-1854.0)]
                data.append(v)

            self.tonages_by_cost_rank_df.at[i,:] = data



    def max_blended_tonnes_entity_cost_limit(self):
        tn_cr  = self.tonages_by_cost_rank_df   # pd.read_csv("outputs/tonnages_by_cost_rank.csv")
        c_cr   = self.costs_by_cost_rank_df   # pd.read_csv("outputs/costs_by_cost_rank.csv")
        cbix   = self.CBIX_BX_AA_production_cost_df       #pd.read_csv("outputs/CBIX_BX_AA_production_cost.csv")

        avgs = [self.avg1, self.avg2, self.avg3, self.avg4, self.avg5, self.avg6, self.avg7, self.avg8, self.avg9, self.avg10]

        # Sed'try low
        row1_sl = [f"Sed'try {i}" if i <= 400 else f"Coll. Acc. {i}" for i in range(1,421)]
        row2_sl = ["Low" if i <= 200 else "High" for i in range(1,401)]
        row3_sl = [avgs[i] for h in range(2) for i in range(10) for j in range(20)]
        row4_sl = [f"{k}th" for i in range(21) for k in range(5,101,5)]

        [row2_sl.append("Low") for i in range(20)]
        [row3_sl.append(avgs[0]) for i in range(20)]

        self.max_blended_tonnes_entity_cost_limit_df = pd.DataFrame(
            columns=row1_sl
        )
        self.max_blended_tonnes_entity_cost_limit_df.at[0,:] = row2_sl
        self.max_blended_tonnes_entity_cost_limit_df.at[1,:] = row3_sl
        self.max_blended_tonnes_entity_cost_limit_df.at[2,:] = row4_sl
        self.max_blended_tonnes_entity_cost_limit_df.at[3,:] = [f"rank {i}" for i in range(1, 421)]

        for i in range(4, len(tn_cr)):
            data = []
            for j in range(420):
                v = 0 if sum(tn_cr.iloc[i, 0:j+1].astype(float)) <= 0 else (sum(tn_cr.iloc[i, 0:j+1].astype(float)) if  sum(c_cr.iloc[i, 0:j+1].astype(float) * tn_cr.iloc[i, 0:j+1].astype(float))/sum(tn_cr.iloc[i, 0:j+1].astype(float)) <= float(cbix.loc[3, "Total Cost - CBIX BX"]) else 0)
                data.append(v)
            self.max_blended_tonnes_entity_cost_limit_df.at[i,:] = data


    def max_economic_tonnes(self):
        df       = self.max_blended_tonnes_entity_cost_limit_df # pd.read_csv("outputs/max_blended_tonnes_entity_cost_limit.csv")
        fn_cost  = self.final_costs_df      # pd.read_csv("outputs/final_costs/final_costs.csv")
        tn_cat   = self.tonnages_in_categories_df       # pd.read_csv("outputs/tonnages_in_categories.csv")
        cbix     = self.CBIX_BX_AA_production_cost_df       # pd.read_csv("outputs/CBIX_BX_AA_production_cost.csv")

        fn_cost.columns = [f"Sed'try {i}" if i <= 400 else f"Coll. Acc. {i}" for i in range(1,421)]

        self.max_economic_tonnes_df   = pd.DataFrame(columns=[
            "Province",
            "County",
            "Max Economic Tonnes - blending in country",
            "Max Economic Tonnes - NO blending in country",
            ])

        for i in range(4, len(df)):
            x = np.where(fn_cost.loc[i-1,:].astype(float) <= float(cbix.loc[i-3, "Total Cost - CBIX BX"]), 1, 0)
            y = tn_cat.loc[i-1, :].astype(float)

            self.max_economic_tonnes_df.at[i-4, "Province"] = self.inputs.at[i-4, "province"]
            self.max_economic_tonnes_df.at[i-4, "County"]   = self.inputs.at[i-4, "county"]
            self.max_economic_tonnes_df.at[i-4, "Max Economic Tonnes - blending in country"] = max(df.loc[i, :].astype(float))
            self.max_economic_tonnes_df.at[i-4, "Max Economic Tonnes - NO blending in country"] = sum(x*y)
        print(self.max_economic_tonnes_df)

    def high_sulphur_tablename_func(self):
        mine_switch = self.other_controls.loc[0, 'switch_control']
        _cols = ["Deposit Consumption (percentile)", "A/S"]

        [_cols.append(f"{int(self.range_max.loc[0, c])} - {int(self.range_max.loc[1, c])} - Low Sulphur") for c in self.range_max.columns[1:]]
        [_cols.append(f"{int(self.range_max.loc[0, c])} - {int(self.range_max.loc[1, c])} - High Sulphur") for c in self.range_max.columns[1:]]
        _cols.append(f"{int(self.range_max.iloc[0, 1])} - {int(self.range_max.iloc[1, 1])} - Collapse Acc. Deposits")
        final_costs_df_temp = self.final_costs_df.copy()
        tonnages_in_categories_df_temp = self.tonnages_in_categories_df.copy()
        ranks_by_costs_df_temp = self.ranks_by_costs_df.copy()

        final_costs_df_temp = final_costs_df_temp.loc[3:, :].reset_index()
        tonnages_in_categories_df_temp = tonnages_in_categories_df_temp.loc[3:, :].reset_index()
        ranks_by_costs_df_temp = ranks_by_costs_df_temp.loc[4:, :].reset_index()

        table1 = final_costs_df_temp[self.output_db.loc[:, "County"].astype(str).map(lambda x: x.lower() == mine_switch.lower())].drop(['index'], axis=1)
        table2 = tonnages_in_categories_df_temp[self.output_db.loc[:, "County"].astype(str).map(lambda x: x.lower() == mine_switch.lower())].drop(['index'], axis=1)
        table3 = ranks_by_costs_df_temp[self.output_db.loc[:, "County"].astype(str).map(lambda x: x.lower() == mine_switch.lower())].drop(['index'], axis=1)

        as_col = []
        for k in range(5,101,5):
            v1 = self.perct_AA_percentile_mined_cab_df.loc[1:, :].reset_index()[self.output_db.loc[:, "County"].astype(str).map(lambda x: x.lower() == mine_switch.lower())].drop(['index'], axis=1).reset_index()
            v2 = self.perct_SiO2_percentile_mined_cab_df.loc[1:, :].reset_index()[self.output_db.loc[:, "County"].astype(str).map(lambda x: x.lower() == mine_switch.lower())].drop(['index'], axis=1).reset_index()
            print(v1)
            print(v2)
            v1 = v1.loc[0, f"percentile mined {k/100}"]
            v2 = v2.loc[0, f"percentile mined {k/100}"]

            v = v1/v2 if v2 > 0 else 0
            as_col.append(v)

        Costs_RMBtAA       = pd.DataFrame(columns=_cols)
        Tonnages_kt        = pd.DataFrame(columns=_cols)
        Rank_AA_proc_cost  = pd.DataFrame(columns=_cols)

        Costs_RMBtAA.at[:, "Deposit Consumption (percentile)"] = [f"{k}th" for k in range(5,101,5)]
        Costs_RMBtAA.at[:, "A/S"] = as_col

        Tonnages_kt.at[:, "Deposit Consumption (percentile)"] = [f"{k}th" for k in range(5,101,5)]
        Tonnages_kt.at[:, "A/S"] = as_col

        Rank_AA_proc_cost.at[:, "Deposit Consumption (percentile)"] = [f"{k}th" for k in range(5,101,5)]
        Rank_AA_proc_cost.at[:, "A/S"] = as_col

        x = 0
        y = 0
        z = 0
        for col in Costs_RMBtAA.columns[2:]:
            Costs_RMBtAA.at[:, col] = table1.iloc[0, x:x+20].tolist()
            # print(table1.iloc[0, x:x+20])
            x +=20
        print(Costs_RMBtAA)

        for col in Tonnages_kt.columns[2:]:
            Tonnages_kt.at[:, col] = table2.iloc[0, y:y+20].tolist()
            # print(table1.iloc[0, x:x+20])
            y +=20
        print(Tonnages_kt)

        for col in Rank_AA_proc_cost.columns[2:]:
            Rank_AA_proc_cost.at[:, col] = table3.iloc[0, z:z+20].tolist()
            # print(table1.iloc[0, x:x+20])
            z +=20
        print(Rank_AA_proc_cost)

        self.Costs_RMBtAA  = Costs_RMBtAA
        self.Tonnages_kt = Tonnages_kt
        self.Rank_AA_proc_cost  = Rank_AA_proc_cost

    # All functions are called in calcall() function

    def calcall(self):
        #Run reserve summary codes
        self.linear_eqn_sb()
        print("Completed linear_eqn_sb")
        self.depth_splits_sb()
        print("Completed depth_splits_sb")
        self.depth_split_cab()
        print("Completed depth_split_cab")
        self.sedimentary_bauxite_td()
        print("Completed sedimentary_bauxite_td")
        self.collapse_accm_bauxite()
        print("Completed collapse_accm_bauxite")
        self.linear_eqn_sb_only()
        print("Completed linear_eqn_sb_only")
        self.depth_multiplier_sulphur_cntm()
        print("Completed depth_multiplier_sulphur_cntm")
        self.perct_inventory_sulphur_contaminated_db()
        print("Completed perct_inventory_sulphur_contaminated_db")
        self.perct_AA_percentile_mined_sb()
        print("Completed perct_AA_percentile_mined_sb")
        self.perct_SiO2_percentile_mined_sb()
        print("Completed perct_SiO2_percentile_mined_sb")
        self.perct_AA_percentile_mined_cab()
        print("Completed perct_AA_percentile_mined_cab")
        self.perct_SiO2_percentile_mined_cab()
        print("Completed perct_SiO2_percentile_mined_cab")
        self.stripping_ratio_sb()
        print("Completed stripping_ratio_sb")
        self.stripping_ratio_ca_bx()
        print("Completed stripping_ratio_ca_bx")
        self.electricity_costs_msb_RMBt_ROM_ore()
        print("Completed electricity_costs_msb_RMBt_ROM_ore")
        self.electricity_costs_cab_RMB_t_ROM_ore()
        print("Completed electricity_costs_cab_RMB_t_ROM_ore")
        self.diesel_costs_msb_RMB_t_ROM_ore()
        print("Completed diesel_costs_msb_RMB_t_ROM_ore")
        self.diesel_costs_mca_RMB_t_ROM_ore()
        print("Completed diesel_costs_mca_RMB_t_ROM_ore")
        self.labour_costs_msb_RMB_t_ROM_ore()
        print("Completed labour_costs_msb_RMB_t_ROM_ore")
        self.labour_costs_mcab_RMB_t_ROM_ore()
        print("Completed labour_costs_mcab_RMB_t_ROM_ore")
        self.mine_transport_costs_sedimentary()
        print("Completed mine_transport_costs_sedimentary")
        self.washing_factor()
        print("Completed washing_factor")
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb()
        print("Completed costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb")
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx()
        print("Completed costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx")
        self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge()
        print("Completed taxes_royalties_Other_allowance_Capital_up_front_costs_charge")
        self.total_mining_costs_RMB_t_ore_sb()
        print("Completed total_mining_costs_RMB_t_ore_sb")
        self.total_mining_costs_RMB_t_ore_cab()
        print("Completed total_mining_costs_RMB_t_ore_cab")
        self.delivery_costs_RG_RT()
        print("Completed delivery_costs_RG_RT")
        self.t_div_t_BX_AA_percentile_mined_sb()
        print("Completed t_div_t_BX_AA_percentile_mined_sb")
        self.t_div_t_BX_AA_percentile_mined_cab()
        print("Completed t_div_t_BX_AA_percentile_mined_cab")
        self.caustic_use_percentile_mined_sb_t_div_t_AA()
        print("Completed caustic_use_percentile_mined_sb_t_div_t_AA")
        self.caustic_use_percentile_mined_cab()
        print("Completed caustic_use_percentile_mined_cab")
        self.CBIX_BX_AA_production_cost()
        print("Completed CBIX_BX_AA_production_cost")
        self.final_costs()
        print("Completed final_costs")
        self.tonnages_in_categories()
        print("Completed tonnages_in_categories")
        self.cost_with_dummy_for_ranking()
        print("Completed cost_with_dummy_for_ranking")
        self.ranks_by_costs()
        print("Completed ranks_by_costs")
        self.cell_columns_by_cost_rank()
        print("Completed cell_columns_by_cost_rank")
        self.costs_by_cost_rank()
        print("Completed costs_by_cost_rank")
        self.tonages_by_cost_rank()
        print("Completed tonages_by_cost_rank")
        self.max_blended_tonnes_entity_cost_limit()
        print("Completed max_blended_tonnes_entity_cost_limit")
        self.max_economic_tonnes()
        print("Completed max_economic_tonnes")
        self.high_sulphur_tablename_func()
        print('Completed high_sulphur_table')

        #Add Min, Max, Avg and total
        metrics = self.range_max.copy()
        # add_avgs
        self.depth_splits_sb_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.depth_splits_sb_df], axis=0)
        self.depth_split_cab_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.depth_split_cab_df], axis=0)
        self.depth_multiplier_sulphur_cntm_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.depth_multiplier_sulphur_cntm_df], axis=0)
        self.perct_inventory_sulphur_contaminated_db_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.perct_inventory_sulphur_contaminated_db_df], axis=0)
        self.stripping_ratio_sb_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.stripping_ratio_sb_df], axis=0)
        self.stripping_ratio_ca_bx_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.stripping_ratio_ca_bx_df], axis=0)
        self.electricity_costs_msb_RMBt_ROM_ore_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.electricity_costs_msb_RMBt_ROM_ore_df], axis=0)
        self.diesel_costs_msb_RMB_t_ROM_ore_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.diesel_costs_msb_RMB_t_ROM_ore_df], axis=0)
        self.labour_costs_msb_RMB_t_ROM_ore_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.labour_costs_msb_RMB_t_ROM_ore_df], axis=0)
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df], axis=0)
        self.total_mining_costs_RMB_t_ore_sb_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.total_mining_costs_RMB_t_ore_sb_df], axis=0)


        # add_avg_5
        metrics.at[2, "open_pit1"] = 5
        self.electricity_costs_cab_RMB_t_ROM_ore_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.electricity_costs_cab_RMB_t_ROM_ore_df], axis=0)
        self.diesel_costs_mca_RMB_t_ROM_ore_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.diesel_costs_mca_RMB_t_ROM_ore_df], axis=0)
        self.labour_costs_mcab_RMB_t_ROM_ore_df  = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.labour_costs_mcab_RMB_t_ROM_ore_df], axis=0)
        self.mine_transport_costs_sedimentary_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.mine_transport_costs_sedimentary_df], axis=0)
        self.total_mining_costs_RMB_t_ore_cab_df  = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.total_mining_costs_RMB_t_ore_cab_df], axis=0)
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df = pd.concat([metrics.loc[:,  "open_pit1":"underground7"], self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df], axis=0)


        # add_totals
        totals = pd.DataFrame(columns=self.sedimentary_bauxite_td_df.columns)
        totals.at[0, :] = [self.sedimentary_bauxite_td_df.iloc[:, i].sum() for i in range(self.sedimentary_bauxite_td_df.shape[1])]

        totals1 = pd.DataFrame(columns=self.collapse_accm_bauxite_df.columns)
        totals1.at[0, :] = [self.collapse_accm_bauxite_df.iloc[:, i].sum() for i in range(self.collapse_accm_bauxite_df.shape[1])]

        self.sedimentary_bauxite_td_df = pd.concat([totals, self.sedimentary_bauxite_td_df], axis=0)
        self.collapse_accm_bauxite_df = pd.concat([totals1, self.collapse_accm_bauxite_df], axis=0)


        self.depth_splits_sb_df.reset_index()
        self.depth_split_cab_df.reset_index()
        self.depth_multiplier_sulphur_cntm_df.reset_index()
        self.perct_inventory_sulphur_contaminated_db_df.reset_index()
        self.stripping_ratio_sb_df.reset_index()
        self.stripping_ratio_ca_bx_df.reset_index()
        self.electricity_costs_msb_RMBt_ROM_ore_df.reset_index()
        self.diesel_costs_msb_RMB_t_ROM_ore_df.reset_index()
        self.labour_costs_msb_RMB_t_ROM_ore_df.reset_index()
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.reset_index()
        self.total_mining_costs_RMB_t_ore_sb_df.reset_index()
        self.electricity_costs_cab_RMB_t_ROM_ore_df.reset_index()
        self.diesel_costs_mca_RMB_t_ROM_ore_df.reset_index()
        self.labour_costs_mcab_RMB_t_ROM_ore_df.reset_index()
        self.mine_transport_costs_sedimentary_df.reset_index()
        self.total_mining_costs_RMB_t_ore_cab_df.reset_index()
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df.reset_index()
        self.sedimentary_bauxite_td_df.reset_index()
        self.collapse_accm_bauxite_df.reset_index()

        #SAVE all files
        dirs = [
            "outputs",
            "outputs/final_costs",
            "outputs/choosen_mine",
            "outputs/mining_costs",
            "outputs/delivery_costs",
            "outputs/max_economic_tonnes",
            "outputs/as_workings_sedimentary_bauxite",
            "outputs/depth_splits_sedimentary_bauxite",
            "outputs/as_workings_collapse_accumulated_bauxite",
            "outputs/suplhur_contamination_sedimentary_bauxite_only",
        ]

        for dirname in dirs:
            if os.path.exists(dirname) or dirname == "":
                pass
            else:
                os.mkdir(dirname)

        self.linear_eqn_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/depth_splits_sedimentary_bauxite/linear_eqn-depth_buckets_percent_tonnage_per_depth_range.xlsx"), index=False)
        self.depth_splits_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/depth_splits_sedimentary_bauxite/depth_buckets-percent_tonnage_per_depth_range.xlsx"), index=False)
        self.depth_split_cab_df.to_excel(os.path.join(BASE_DIR, "outputs/depth_splits-collapse_accumulated_bauxite.xlsx"), index=False)
        self.sedimentary_bauxite_td_df.to_excel(os.path.join(BASE_DIR, "outputs/sedimentary_bauxite-tonnages_per_depth_range.xlsx"), index=False)
        self.collapse_accm_bauxite_df.to_excel(os.path.join(BASE_DIR, "outputs/collapse_accumulated_bauxite-tonnages_per_depth_range.xlsx"), index=False)
        self.linear_eqn_sb_only_df.to_excel(os.path.join(BASE_DIR, "outputs/suplhur_contamination_sedimentary_bauxite_only/linear_equation-Suplhur_contamination-sedimentary_bauxite_only.xlsx"), index=False)
        self.depth_multiplier_sulphur_cntm_df.to_excel(os.path.join(BASE_DIR, "outputs/suplhur_contamination_sedimentary_bauxite_only/depth_multiplier_for_sulphur_contamination.xlsx"), index=False)
        self.perct_inventory_sulphur_contaminated_db_df.to_excel(os.path.join(BASE_DIR, "outputs/suplhur_contamination_sedimentary_bauxite_only/perct_inventory_sulphur_contaminated_by_depth_bucket.xlsx"), index=False)
        self.perct_AA_percentile_mined_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/as_workings_sedimentary_bauxite/perct_AA_percentile_mined-sedimentary_bauxite.xlsx"), index=False)
        self.perct_SiO2_percentile_mined_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/as_workings_sedimentary_bauxite/perct_SiO2_percentile_mined-sedimentary_bauxite.xlsx"), index=False)
        self.perct_AA_percentile_mined_cab_df.to_excel(os.path.join(BASE_DIR, "outputs/as_workings_collapse_accumulated_bauxite/perct_AA_percentile_mined-collapse_accumulated_bauxite.xlsx"), index=False)
        self.perct_SiO2_percentile_mined_cab_df.to_excel(os.path.join(BASE_DIR, "outputs/as_workings_collapse_accumulated_bauxite/perct_SiO2_percentile_mined-collapse_sedimentary_bauxite.xlsx"), index=False)
        self.stripping_ratio_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/stripping_ratio-sedimentary_bauxite.xlsx"), index=False)
        self.stripping_ratio_ca_bx_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/stripping_ratio-collapse_accumulated_bx.xlsx"), index=False)
        self.electricity_costs_msb_RMBt_ROM_ore_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/electricity_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore.xlsx"), index=False)
        self.electricity_costs_cab_RMB_t_ROM_ore_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/electricity_costs-collapse_accumulated_bauxite-RMB-t_ROM_ore.xlsx"), index=False)
        self.diesel_costs_msb_RMB_t_ROM_ore_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/diesel_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore.xlsx"), index=False)
        self.diesel_costs_mca_RMB_t_ROM_ore_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/diesel_costs-mining_collapse_accumulated_bauxite-RMB-t_ROM_ore.xlsx"), index=False)
        self.labour_costs_msb_RMB_t_ROM_ore_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/labour_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore.xlsx"), index=False)
        self.labour_costs_mcab_RMB_t_ROM_ore_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/labour_costs-mining_collapse_accumulated_bauxite-RMB-tROM_ore.xlsx"), index=False)
        self.mine_transport_costs_sedimentary_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/In_Mine_Transport_costs-sedimentary.xlsx"), index=False)
        self.washing_factor_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/washing_factor.xlsx"), index=False)
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/costs_RMB-t_ROM_ore_before_per-t-ore_charges-sedimentary_bauxite.xlsx"), index=False)
        self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/costs_RMB-t_ROM_ore_before_per-t-ore_charges-collapse_accumulated_bx.xlsx"), index=False)
        self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/taxes_royalties_Other_allowance_Capital_up_front_costs_charge.xlsx"), index=False)
        self.total_mining_costs_RMB_t_ore_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/total_mining_costs_RMB-t_ore-sedimentary_bauxite.xlsx"), index=False)
        self.total_mining_costs_RMB_t_ore_cab_df.to_excel(os.path.join(BASE_DIR, "outputs/mining_costs/total_mining_costs_RMB-t_ore-collapse_accumulated_bauxite.xlsx"), index=False)
        self.delivery_costs_RG_RT_df.to_excel(os.path.join(BASE_DIR, "outputs/delivery_costs/RGtoRR_Diesel_cost.xlsx"), index=False)
        self.t_div_t_BX_AA_percentile_mined_sb_df.to_excel(os.path.join(BASE_DIR, "outputs/delivery_costs/t-t_BX:AA_by_percentile_mined-sedimentary_bauxite.xlsx"), index=False)
        self.t_div_t_BX_AA_percentile_mined_cab_df.to_excel(os.path.join(BASE_DIR, "outputs/delivery_costs/t-t_BX:AA_by_percentile_mined-collapse_accumlulated_bx.xlsx"), index=False)
        self.caustic_use_percentile_mined_sb_t_div_t_AA_df.to_excel(os.path.join(BASE_DIR, "outputs/delivery_costs/caustic_use_by_percentile_mined-sedimentary_bauxite_t-t_AA.xlsx"), index=False)
        self.caustic_use_percentile_mined_cab_df.to_excel(os.path.join(BASE_DIR, "outputs/delivery_costs/caustic_use_by_percentile_mined-collapse_accumlulated_bx.xlsx"), index=False)
        self.CBIX_BX_AA_production_cost_df.to_excel(os.path.join(BASE_DIR, "outputs/CBIX_BX_AA_production_cost.xlsx"), index=False)
        self.final_costs_df.to_excel(os.path.join(BASE_DIR, "outputs/final_costs/final_costs.xlsx"), index=False)

        cols=pd.Series(self.tonnages_in_categories_df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        # rename the columns with the cols list.
        self.tonnages_in_categories_df.columns = cols
        print(cols)

        self.tonnages_in_categories_df.to_excel(os.path.join(BASE_DIR, "outputs/tonnages_in_categories.xlsx"), index=False)
        self.cost_with_dummy_for_ranking_df.to_excel(os.path.join(BASE_DIR, "outputs/cost_with_dummy_for_ranking_reasons.xlsx"), index=False)
        self.ranks_by_costs_df.to_excel(os.path.join(BASE_DIR, "outputs/ranks_by_costs.xlsx"), index=False)
        self.cell_columns_by_cost_rank_df.to_excel(os.path.join(BASE_DIR, "outputs/cell_columns_by_cost_rank.xlsx"), index=False)
        self.costs_by_cost_rank_df.to_excel(os.path.join(BASE_DIR, "outputs/costs_by_cost_rank.xlsx"), index=False)
        self.tonages_by_cost_rank_df.to_excel(os.path.join(BASE_DIR, "outputs/tonnages_by_cost_rank.xlsx"), index=False)
        self.max_blended_tonnes_entity_cost_limit_df.to_excel(os.path.join(BASE_DIR, "outputs/max_blended_tonnes_entity_cost_limit.xlsx"), index=False)
        self.max_economic_tonnes_df.to_excel(os.path.join(BASE_DIR, "outputs/max_economic_tonnes/max_economic_tonnes.xlsx"), index=False)
        self.Costs_RMBtAA.to_excel(os.path.join(BASE_DIR, "outputs/choosen_mine/Costs_RMBtAA.xlsx"), index=False)
        self.Tonnages_kt.to_excel(os.path.join(BASE_DIR, "outputs/choosen_mine/Tonnages_kt.xlsx"), index=False)
        self.Rank_AA_proc_cost.to_excel(os.path.join(BASE_DIR, "outputs/choosen_mine/Rank_by_AA_processing_cost.xlsx"), index=False)

        print(self.tonnages_in_categories_df)

        dblist.append(db_conv.single_year_mult_out(self.linear_eqn_sb_df, "depth_splits_sedimentary_bauxite linear_eqn-depth_buckets_percent_tonnage_per_depth_range"))
        dblist.append(db_conv.single_year_mult_out(self.depth_splits_sb_df, "depth_splits_sedimentary_bauxite depth_buckets-percent_tonnage_per_depth_range"))
        dblist.append(db_conv.single_year_mult_out(self.depth_split_cab_df, "depth_splits-collapse_accumulated_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.sedimentary_bauxite_td_df, "sedimentary_bauxite-tonnages_per_depth_range"))
        dblist.append(db_conv.single_year_mult_out(self.collapse_accm_bauxite_df, "collapse_accumulated_bauxite-tonnages_per_depth_range"))
        dblist.append(db_conv.single_year_mult_out(self.linear_eqn_sb_only_df, "suplhur_contamination_sedimentary_bauxite_only linear_equation-Suplhur_contamination-sedimentary_bauxite_only"))
        dblist.append(db_conv.single_year_mult_out(self.depth_multiplier_sulphur_cntm_df, "suplhur_contamination_sedimentary_bauxite_only depth_multiplier_for_sulphur_contamination"))
        dblist.append(db_conv.single_year_mult_out(self.perct_inventory_sulphur_contaminated_db_df, "suplhur_contamination_sedimentary_bauxite_only perct_inventory_sulphur_contaminated_by_depth_bucket"))
        dblist.append(db_conv.single_year_mult_out(self.perct_AA_percentile_mined_sb_df, "as_workings_sedimentary_bauxite perct_AA_percentile_mined-sedimentary_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.perct_SiO2_percentile_mined_sb_df, "as_workings_sedimentary_bauxite perct_SiO2_percentile_mined-sedimentary_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.perct_AA_percentile_mined_cab_df, "as_workings_collapse_accumulated_bauxite perct_AA_percentile_mined-collapse_accumulated_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.perct_SiO2_percentile_mined_cab_df, "as_workings_collapse_accumulated_bauxite perct_SiO2_percentile_mined-collapse_sedimentary_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.stripping_ratio_sb_df, "mining_costs stripping_ratio-sedimentary_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.stripping_ratio_ca_bx_df, "mining_costs stripping_ratio-collapse_accumulated_bx"))
        dblist.append(db_conv.single_year_mult_out(self.electricity_costs_msb_RMBt_ROM_ore_df, "mining_costs electricity_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore"))
        dblist.append(db_conv.single_year_mult_out(self.electricity_costs_cab_RMB_t_ROM_ore_df, "mining_costs electricity_costs-collapse_accumulated_bauxite-RMB-t_ROM_ore"))
        dblist.append(db_conv.single_year_mult_out(self.diesel_costs_msb_RMB_t_ROM_ore_df, "mining_costs diesel_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore"))
        dblist.append(db_conv.single_year_mult_out(self.diesel_costs_mca_RMB_t_ROM_ore_df, "mining_costs diesel_costs-mining_collapse_accumulated_bauxite-RMB-t_ROM_ore"))
        dblist.append(db_conv.single_year_mult_out(self.labour_costs_msb_RMB_t_ROM_ore_df, "mining_costs labour_costs-mining_sedimentary_bauxite-RMB-t_ROM_ore"))
        dblist.append(db_conv.single_year_mult_out(self.labour_costs_mcab_RMB_t_ROM_ore_df, "mining_costs labour_costs-mining_collapse_accumulated_bauxite-RMB-tROM_ore"))
        dblist.append(db_conv.single_year_mult_out(self.mine_transport_costs_sedimentary_df, "mining_costs In_Mine_Transport_costs-sedimentary"))
        dblist.append(db_conv.single_year_mult_out(self.washing_factor_df, "mining_costs washing_factor"))
        dblist.append(db_conv.single_year_mult_out(self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_sb_df, "mining_costs costs_RMB-t_ROM_ore_before_per-t-ore_charges-sedimentary_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.costs_RMB_t_ROM_ore_before_per_t_ore_charges_ca_bx_df, "mining_costs costs_RMB-t_ROM_ore_before_per-t-ore_charges-collapse_accumulated_bx"))
        dblist.append(db_conv.single_year_mult_out(self.taxes_royalties_Other_allowance_Capital_up_front_costs_charge_df, "mining_costs taxes_royalties_Other_allowance_Capital_up_front_costs_charge"))
        dblist.append(db_conv.single_year_mult_out(self.total_mining_costs_RMB_t_ore_sb_df, "mining_costs total_mining_costs_RMB-t_ore-sedimentary_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.total_mining_costs_RMB_t_ore_cab_df, "mining_costs total_mining_costs_RMB-t_ore-collapse_accumulated_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.delivery_costs_RG_RT_df, "delivery_costs RGtoRR_Diesel_cost"))
        dblist.append(db_conv.single_year_mult_out(self.t_div_t_BX_AA_percentile_mined_sb_df, "delivery_costs t-t_BX:AA_by_percentile_mined-sedimentary_bauxite"))
        dblist.append(db_conv.single_year_mult_out(self.t_div_t_BX_AA_percentile_mined_cab_df, "delivery_costs t-t_BX:AA_by_percentile_mined-collapse_accumlulated_bx"))
        dblist.append(db_conv.single_year_mult_out(self.caustic_use_percentile_mined_sb_t_div_t_AA_df, "delivery_costs caustic_use_by_percentile_mined-sedimentary_bauxite_t-t_AA"))
        dblist.append(db_conv.single_year_mult_out(self.caustic_use_percentile_mined_cab_df, "delivery_costs caustic_use_by_percentile_mined-collapse_accumlulated_bx"))
        dblist.append(db_conv.single_year_mult_out(self.CBIX_BX_AA_production_cost_df, "CBIX_BX_AA_production_cost"))
        dblist.append(db_conv.single_year_mult_out(self.final_costs_df, "final_costs final_costs"))
        dblist.append(db_conv.single_year_mult_out(self.tonnages_in_categories_df, "tonnages_in_categories"))
        dblist.append(db_conv.single_year_mult_out(self.cost_with_dummy_for_ranking_df, "cost_with_dummy_for_ranking_reasons"))
        dblist.append(db_conv.single_year_mult_out(self.ranks_by_costs_df, "ranks_by_costs"))
        dblist.append(db_conv.single_year_mult_out(self.cell_columns_by_cost_rank_df, "cell_columns_by_cost_rank"))
        dblist.append(db_conv.single_year_mult_out(self.costs_by_cost_rank_df, "costs_by_cost_rank"))
        dblist.append(db_conv.single_year_mult_out(self.tonages_by_cost_rank_df, "tonnages_by_cost_rank"))
        dblist.append(db_conv.single_year_mult_out(self.max_blended_tonnes_entity_cost_limit_df, "max_blended_tonnes_entity_cost_limit"))
        dblist.append(db_conv.single_year_mult_out(self.max_economic_tonnes_df, "max_economic_tonnes"))
        dblist.append(db_conv.single_year_mult_out(self.Costs_RMBtAA, "choosen_mine Costs_RMBtAA"))
        dblist.append(db_conv.single_year_mult_out(self.Tonnages_kt, "choosen_mine Tonnages_kt"))
        dblist.append(db_conv.single_year_mult_out(self.Rank_AA_proc_cost, "choosen_mine Rank_by_AA_processing_cost"))

if __name__ == "__main__":
    inventory = Inventory()

    start = time.process_time()         # start timer
    inventory.calcall()                 # call all functions
    end = time.process_time() - start   # end timer

    print(f"Total runtime -- {round(end/60, 2)} mins.")

# print(pd.concat([self.range_max.loc[:,  "open_pit1":"underground7"], self.depth_splits_sb_df], axis=0).reset_index())

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]
# snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)

uploadtodb.upload(snapshot_output_data)
