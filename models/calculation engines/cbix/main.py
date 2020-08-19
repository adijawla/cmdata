import time, os
import pandas as pd
import numpy as np
# import connect
# import uploadtodb
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb

db_conv =  Flatdbconverter("ViU/Freight(CBIX)")
dblist = []



class CBIX:
    def __init__(self):
        
        self.master_date_cell            = pd.read_excel("cbixinput.xlsx", sheet_name="Master date cell")
        self.cbix_co_efficients_inputs   = pd.read_excel("cbixinput.xlsx", sheet_name="CBIX Co-Efficients Determination Inputs")

        self.freights_trade_details      = pd.read_excel("cbixinput.xlsx", sheet_name="Freights US$ per wmt Trade Details")
        self.cbix_cf_trade_details       = pd.read_excel("cbixinput.xlsx", sheet_name="CBIX Co-Efficients Determination")
        self.target_cbix_price           = pd.read_excel("cbixinput.xlsx", sheet_name="Target CBIX Price")
        self.freights_trade_details.loc[:, "Date"] = self.master_date_cell.loc[0, "Date"]
        self.cbix_cf_trade_details.loc[:, "Date"]  = self.master_date_cell.loc[0, "Date"]

        for z, i in zip(range(0, self.cbix_cf_trade_details.shape[0], 2), range(int(self.cbix_cf_trade_details.shape[0]/2))):
            if i % 2 == 0:
                self.cbix_cf_trade_details.at[z:z+2, "LT R.Silica"]         = self.cbix_co_efficients_inputs.loc[0, "LT R.Silica"] + self.cbix_co_efficients_inputs.loc[1, "LT R.Silica"]
            else:
                self.cbix_cf_trade_details.at[z:z+2, "LT R.Silica"]         = self.cbix_co_efficients_inputs.loc[0, "LT R.Silica"] - self.cbix_co_efficients_inputs.loc[1, "LT R.Silica"]
        
        v1 = 4
        for z in range(self.cbix_cf_trade_details.shape[0]):
            if (z+1) <= int(self.cbix_cf_trade_details.shape[0] / 2):
                self.cbix_cf_trade_details.at[z, "LT Avail. Alumina"]   = self.cbix_co_efficients_inputs.loc[0, "LT Avail. Alumina"] + self.cbix_co_efficients_inputs.loc[1, "LT Avail. Alumina"] - self.cbix_cf_trade_details.loc[z, "LT R.Silica"]
            else:
                self.cbix_cf_trade_details.at[z, "LT Avail. Alumina"]   = self.cbix_co_efficients_inputs.loc[0, "LT Avail. Alumina"] - self.cbix_co_efficients_inputs.loc[1, "LT Avail. Alumina"] - self.cbix_cf_trade_details.loc[z, "LT R.Silica"]
            
            if (z) % 2 == 0:
                self.cbix_cf_trade_details.at[z, "Moisture"]            = self.cbix_co_efficients_inputs.loc[0, "Moisture"] + self.cbix_co_efficients_inputs.loc[1, "Moisture"]
            else:
                self.cbix_cf_trade_details.at[z, "Moisture"]            = self.cbix_co_efficients_inputs.loc[0, "Moisture"] - self.cbix_co_efficients_inputs.loc[1, "Moisture"]        


        self.indexes_mines               = pd.read_excel("cbixinput.xlsx", sheet_name="Indices Mines Exporting Ports")
        self.spc_leg_shp_table           = pd.read_excel("cbixinput.xlsx", sheet_name="Special 2 Leg Shipping Table")
        self.mj_max_cargo                = pd.read_excel("cbixinput.xlsx", sheet_name="MRN or Juruti max cargo")
        self.ship_time_cr                = pd.read_excel("cbixinput.xlsx", sheet_name="Ship Time Charter Rates")
        self.china_imp_prts              = pd.read_excel("cbixinput.xlsx", sheet_name="China n Importing Ports")
        self.china_ps                    = pd.read_excel("cbixinput.xlsx", sheet_name="China Price Series")
        self.processing_factors          = pd.read_excel("cbixinput.xlsx", sheet_name="processing factors")
        self.mud_disposal_cost           = pd.read_excel("cbixinput.xlsx", sheet_name="Mud disposal cost")
        self.ship_fuel_prices            = pd.read_excel("cbixinput.xlsx", sheet_name="Ship Fuel Prices")
        self.lignitious_coal             = pd.read_excel("cbixinput.xlsx", sheet_name="Lignitious Coal")
        self.sheetname_class             = pd.read_excel("cbixinput.xlsx", sheet_name="Sheetname_Class")
        self.global_factors              = pd.read_excel("cbixinput.xlsx", sheet_name="Global factors")
        self.trade_details               = pd.read_excel("cbixinput.xlsx", sheet_name="Trade Details")
        self.port_linkages               = pd.read_excel("cbixinput.xlsx", sheet_name="Port Linkages")
        self.canals_class                = pd.read_excel("cbixinput.xlsx", sheet_name="Canals_Class")
        self.caustic_soda                = pd.read_excel("cbixinput.xlsx", sheet_name="Caustic Soda")
        self.ship_speeds                 = pd.read_excel("cbixinput.xlsx", sheet_name="Ship Speeds")
        self.fx_rates                    = pd.read_excel("cbixinput.xlsx", sheet_name="FX Rates")
        self.canals                      = pd.read_excel("cbixinput.xlsx", sheet_name="Canals")
        self.lime                        = pd.read_excel("cbixinput.xlsx", sheet_name="Lime")
        self.db                          = {} #All generated outputs are temporarily stored here

        #CBIX
        co_efficient_df = pd.DataFrame(columns=["Coefficient", "Value"])
        co_efficient_df.at[0, "Coefficient"] = "C0"
        co_efficient_df.at[0, "Value"]       = self.cbix_cf_trade_details.loc[:, "Price"].mean()        
        
        co_efficient_df.at[1, "Coefficient"] = "C1"
        co_efficient_df.at[1, "Value"]       = (np.mean(self.cbix_cf_trade_details.loc[:3, "Price"]) - np.mean(self.cbix_cf_trade_details.loc[4:, "Price"])) / (2 * self.cbix_co_efficients_inputs.loc[1, "LT Avail. Alumina"]) / 100        

        co_efficient_df.at[2, "Coefficient"] = "C2"
        co_efficient_df.at[2, "Value"]       = (np.mean([self.cbix_cf_trade_details.loc[0:1, "Price"], self.cbix_cf_trade_details.loc[4:5, "Price"]]) - np.mean([self.cbix_cf_trade_details.loc[2:3, "Price"], self.cbix_cf_trade_details.loc[6:, "Price"]])) / (2 * self.cbix_co_efficients_inputs.loc[1, "LT R.Silica"]) / 100

        co_efficient_df.at[3, "Coefficient"] = "C3"
        co_efficient_df.at[3, "Value"]       = -0.1

        co_efficient_df.at[4, "Coefficient"] = "C3 actual"
        co_efficient_df.at[4, "Value"]       = (np.mean([self.cbix_cf_trade_details.loc[0, "Price"], self.cbix_cf_trade_details.loc[2, "Price"], self.cbix_cf_trade_details.loc[4, "Price"], self.cbix_cf_trade_details.loc[6, "Price"]]) - np.mean([self.cbix_cf_trade_details.loc[1, "Price"], self.cbix_cf_trade_details.loc[3, "Price"], self.cbix_cf_trade_details.loc[5, "Price"], self.cbix_cf_trade_details.loc[7, "Price"]])) / (2 * self.cbix_cf_trade_details.loc[1, "Moisture"])

        self.db["outputs/Coefficient_values.csv"] = co_efficient_df
        print(co_efficient_df)

        self.cdi_glb_factors_columns = [
            "kcal per GJ",
            "conversion kt to t",
            "hours per day",
            "legs per round trip",
            "shipping profit rate",
            "Alumina mol weight",
            "Silica Mol Wt",
            "Kaolinite (Al2O3.2SiO2.2H20) - Al2O3:SiO2 ratio",
            "Max % of vessel deadweight allowed for loading",
            "LOA estimate correlation multiplier",
            "LOA estimate correlation constant",
            "NRT estimate correlation multiplier",
            "NRT estimate correlation constant",
            "GRT estimate correlation multiplier",
            "GRT estimate correlation constant",
            "Minimum Lay days allowed",
            "Minimum Lay days allowed as % of sailing time",
            "Lay allowance Loading port",
            "Extra time tie up + untie at each port",
            "MDO/MGO burn vessel DWT denominator",
            "MDO/MGO burn vessel DWT slope",
            "MDO/MGO burn vessel DWT constant",
            "MDO/MGO burn vessel DWT ^2 coefficient",
            "Main engine burn vessel DWT denominator",
            "Main engine burn vessel DWT exponent",
            "Main engine burn vessel DWT base constant",
            "Main engine burn vessel DWT base slope",
            "Main engine burn speed linear coefficient",
            "Main engine burn speed ^2 coefficient",
            "Main engine burn speed ^3 coefficient",
            "Main engine burn overall correction factor",
            "Reference Port",
            "Freight Insurance Rate",
            "Freight Commission",
        ]


        self.nominal_mine_col = [
            "Mine",
            "Date",
            "Total Alumina",
            "LT Avail. Alumina",
            "Total Silica",
            "LT R.Silica",
            "Quartz / HT Silica",
            "Mono-hydrate / HT Alumina",
            "Moisture",
            "Processing",
            "Processing Penalties to be applied",
            "Tonnage typical vessel",
            "Exporting Port",
            "Specific Index",
            "General Index"
        ]

        self.final_specs_columns = [
            "Mine",
            "Total Alumina",
            "LT Avail. Alumina",
            "Total Silica",
            "LT R.Silica",
            "Quartz / HT Silica",
            "Mono-hydrate / HT Extble Alumina",
            "Moisture",
            "Processing",
            "Cargo Tonnage",
            "Double Leg Shipping? (South America special transload leg)",
            "Exporting Port",
            "South America special transloading Port",
            "South america special transloading cargo tonnage",
            "Importing Port",
            "Specific Index",
            "General Index",
            "Price Type",
            "Price Basis",
            "Price FOB",
            "Freight (adjusted to Qingdao)",
            "Price CIF (Port adjustyed to Qingdao)",
            "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)",
            "EXPLANITORY NOTES RECORD (on changes made to final spec etc)",
            "Old CBIX type Calc",
            "INDEX VALUE  Guinea 45/3 LT",
            "INDEX VALUE  Indonesian LT",
            "INDEX VALUE  Australia HT",
            "CBIX LT",
            "CBIX HT",
            "Old CBIX",
        ]

        self.bauxite_details_columns = [
            "Price",
            "Total Alumina",
            "LT Avail. Alumina",
            "Total Silica",
            "LT R.Silica",
            "Quartz / HT Silica",
            "Mono-hydrate / HT Alumina",
            "Moisture",
            "Processing",
            "Processing Penalties",
        ]

        self.china_prcs_factrs_columns = [
            "HT Alumina Dissolution",
            "DSP Al2O3:SiO2 (wt/wt)",
            "DSP NaOH:SiO2 (wt/wt)",
            "Lime rate (wt/wt_AA)",
            "Lig Coal (GJ/t)",
            "Quartz Attack",
            "Extraction Efficiency %",
            "Caustic wash loss (t.NaOH/t.aa)",
        ]

        self.alumina_prod_costs_columns = [
            "Reactive Alumina",
            "Reactive Silica",
            "Available Alumina",
            "Tonnes per Tonne",
            "Bauxite Cost Incl. Processing Penalties",
            "Caustic Use t.NAOH / t.AA",
            "Caustic cost",
            "Thermal Energy Cost",
            "Lime Cost",
            "Mud make",
            "Mud Disposal Cost",
            "Total Cost",
        ]

        self.frieght_calcs_columns = [
            "Vessel dwt",
            "Vessel Class for timecharter & fuel burn rates",
            "Vessel Class for distance, canals, loading and unloading rates, fuel prices",
            "Estimated LOA (length over all) (m)",
            "Estimated NRT (net register tons)",
            "Estimated GRT (gross register tons)",
        ]

        self.exporting_port_dts_cols = [
            "Handysize",
            "Supramax loading rates",
            "Panamax loading rates",
            "NeoPanamax loading rate",
            "Suezmax loading rate",
            "Capesize loading rates",
            "VLOC loading rates",

        ]

        self.port_linkages_cols = [
            "VLOC – Distance using Main Engine Fuel",
            "VLOC – Distance using Auxiliary Fuel",
            "VLOC – Applicable Time Charter Index",
            "VLOC – Applicable Fuel Region",
            "VLOC – Main Engine Fuel",
            "VLOC – Auxiliary Fuel",
            "VLOC – Canals used",
            "Capesize – Distance using Main Engine Fuel",
            "Capesize – Distance using Auxiliary Fuel",
            "Capesize – Applicable Time Charter Index",
            "Capesize – Applicable Fuel Region",
            "Capesize – Main Engine Fuel",
            "Capesize – Auxiliary Fuel",
            "Capesize – Canals used",
            "Suezmax – Distance using Main Engine Fuel",
            "Suezmax – Distance using Auxiliary Fuel",
            "Suezmax – Applicable Time Charter Index",
            "Suezmax – Applicable Fuel Region",
            "Suezmax – Main Engine Fuel",
            "Suezmax – Auxiliary Fuel",
            "Suezmax – Canals used",
            "NeoPanamax – Distance using Main Engine Fuel",
            "NeoPanamax – Distance using Auxiliary Fuel",
            "NeoPanamax – Applicable Time Charter Index",
            "NeoPanamax – Applicable Fuel Region",
            "NeoPanamax – Main Engine Fuel",
            "NeoPanamax – Auxiliary Fuel",
            "NeoPanamax – Canals used",
            "Panamax – Distance using Main Engine Fuel",
            "Panamax – Distance using Auxiliary Fuel",
            "Panamax – Applicable Time Charter Index",
            "Panamax – Applicable Fuel Region",
            "Panamax – Main Engine Fuel",
            "Panamax – Auxiliary Fuel",
            "Panamax – Canals used",
            "Supramax – Distance using Main Engine Fuel",
            "Supramax – Distance using Auxiliary Fuel",
            "Supramax – Applicable Time Charter Index",
            "Supramax – Applicable Fuel Region",
            "Supramax – Main Engine Fuel",
            "Supramax – Auxiliary Fuel",
            "Supramax – Canals used",
            "Handysize – Distance using Main Engine Fuel",
            "Handysize – Distance using Auxiliary Fuel",
            "Handysize – Applicable Time Charter Index",
            "Handysize – Applicable Fuel Region",
            "Handysize – Main Engine Fuel",
            "Handysize – Auxiliary Fuel",
            "Handysize – Canals used",
        ]

        self.fuel_prices_cols = [
            "HSFO(180 / 380)",
            "VLSFO",
            "LNG",
            "MDO/MGO Regular",
            "MDO/MGO Low Sulphur",
        ]

        self.importing_port_det_cols = [
            "Unloading rate Handysize",
            "Unloading rate Supramax",
            "Unloading rate Panamax",
            "Unloading rate NeoPanamax",
            "Unloading rate Suezmax",
            "Unloading rate Capesize",
            "Unloading rate VLOC",
            "Currency",
            "RMB Fixed fee",
            "RMB per day berthed",
            "RMB per day anchored",
            "RMB/T_Cargo (wet)",
            "RMB/T_Cargo (wet)/day berthed",
            "RMB/T_Cargo (wet)/day anchored",
            "RMB/NRT",
            "RMB/NRT/day berthed",
            "RMB/NRT/day anchored",
            "RMB/GRT",
            "RMB/GRT/day berthed",
            "RMB/GRT/day anchored",
            "RMB/LOA",
            "RMB/LOA/day berthed",
            "RMB/LOA/day anchored",
        ]

        self.cost_details_cols = [
            "Canal",
            "Currency",
            "Tonnage Reference",
            "Capacity Tarrif Graduations - 1st",
            "Capacity Tarrif Graduations - 2nd",
            "Capacity Tarrif Graduations - 3rd",
            "Capacity Tarrif Graduations - 4th",
            "Capacity Tarrif Graduations - 5th",
            "Capacity Tarrif Graduations - 6th",
            "Capacity Tarrif Graduations - 7th",
            "Capacity Tarrif Graduations - 8th",
            "Capacity Tarrif Graduations - 9th",
            "Capacity Tarrifs - 1st",
            "Capacity Tarrifs - 2nd",
            "Capacity Tarrifs - 3rd",
            "Capacity Tarrifs - 4th",
            "Capacity Tarrifs - 5th",
            "Capacity Tarrifs - 6th",
            "Capacity Tarrifs - 7th",
            "Capacity Tarrifs - 8th",
            "Capacity Tarrifs - 9th",
            "Capacity Tarrifs – rem",
            "Cargo Tarrif Graduations - 1st",
            "Cargo Tarrif Graduations - 2nd",
            "Cargo Tarrif Graduations - 3rd",
            "Cargo Tarrif Graduations - 4th",
            "Cargo Tarrif Graduations - 5th",
            "Cargo Tarrif Graduations - 6th",
            "Cargo Tarrif Graduations - 7th",
            "Cargo Tarrif Graduations - 8th",
            "Cargo Tarrif Graduations - 9th",
            "Cargo Tarrifs - 1st",
            "Cargo Tarrifs - 2nd",
            "Cargo Tarrifs - 3rd",
            "Cargo Tarrifs - 4th",
            "Cargo Tarrifs - 5th",
            "Cargo Tarrifs - 6th",
            "Cargo Tarrifs - 7th",
            "Cargo Tarrifs - 8th",
            "Cargo Tarrifs - 9th",
            "Cargo Tarrifs – rem",
            "Days delay",

        ]

        self.canal_cost_workings_cols = [
            "Amount per Capacity Tarrif Graduations - Tonnage Reference",
            "Amount per Capacity Tarrif Graduations - 1st",
            "Amount per Capacity Tarrif Graduations - 2nd",
            "Amount per Capacity Tarrif Graduations - 3rd",
            "Amount per Capacity Tarrif Graduations - 4th",
            "Amount per Capacity Tarrif Graduations - 5th",
            "Amount per Capacity Tarrif Graduations - 6th",
            "Amount per Capacity Tarrif Graduations - 7th",
            "Amount per Capacity Tarrif Graduations - 8th",
            "Amount per Capacity Tarrif Graduations - 9th",
            "Amounts per Cargo Tarrif Graduations - Tonnage Reference",
            "Amounts per Cargo Tarrif Graduations - 1st",
            "Amounts per Cargo Tarrif Graduations - 2nd",
            "Amounts per Cargo Tarrif Graduations - 3rd",
            "Amounts per Cargo Tarrif Graduations - 4th",
            "Amounts per Cargo Tarrif Graduations - 5th",
            "Amounts per Cargo Tarrif Graduations - 6th",
            "Amounts per Cargo Tarrif Graduations - 7th",
            "Amounts per Cargo Tarrif Graduations - 8th",
            "Amounts per Cargo Tarrif Graduations - 9th",
        ]

        self.final_costing_up_for_leg_cols = [
            "Round trip distance Main fuel",
            "Round Trip Distance on Auxiliary fuel",
            "Vessel Loading rate",
            "Vessel Un-Loading rate",
            "Vessel Speed",
            "Main Engine Fuel burn rate",
            "MDO / MGO burn rate",
            "Days at Avg Speed Main Fuel",
            "Days at Avg Speed Auxiliary Fuel",
            "Days Loading",
            "Days unloading",
            "Extra Days due to Canal use",
            "Lay days allowance (50% each port)",
            "Main Fuel HFO / VLSFO Fuel Price",
            "Auxiliary Fuel (MDO / MGO / LNG) Price",
            "Main Fuel Cost",
            "Auxiliary Fuel Cost",
            "FX rate per USD",
            "Fixed fee",
            "Berthing Charge",
            "Anchorage Charge",
            "Per T_Cargo (wet) Charge",
            "RMB/T_Cargo (wet)/day berthed",
            "RMB/T_Cargo (wet)/day anchored",
            "RMB/NRT",
            "RMB/NRT/day berthed",
            "RMB/NRT/day anchored",
            "RMB/GRT",
            "RMB/GRT/day berthed",
            "RMB/GRT/day anchored",
            "RMB/LOA",
            "RMB/LOA/day berthed",
            "RMB/LOA/day anchored",
            "Time Charter Rate",
            "Time Charter Cost",
            "Canal Costs",
            "Total Cost for Leg - before Insurance & Commission",
            "Total Cost for Leg - with Insurance & Commission",
            "Per tonne cargo incl. Insurance & Commission",
        ]

        self.bauxite_details_inputs = [
            "Price",
            "Total Alumina",
            "LT Avail. Alumina",
            "Total Silica",
            "LT R.Silica",
            "Quartz / HT Silica",
            "Mono-hydrate / HT Alumina",
            "Moisture",
            "Processing",
            "Processing Penalties",
        ]

        self.china_processing_factors = [
            "HT Alumina Dissolution",
            "DSP Al2O3:SiO2 (wt/wt)",
            "DSP NaOH:SiO2 (wt/wt)",
            "Lime rate (wt/wt_AA)",
            "Lig Coal (GJ/t)",
            "Quartz Attack",
            "Extraction Efficiency %",
            "Caustic wash loss (t.NaOH/t.aa)",
        ]

    def nominal_mine_div_index_specifications(self):
        new_df = pd.DataFrame(columns=self.nominal_mine_col)
        freights_df = pd.DataFrame(columns=self.nominal_mine_col)
        cbix_coe_df = pd.DataFrame(columns=self.nominal_mine_col)

        new_df.at[:, "Mine"] = self.trade_details.loc[:, "Mine"]
        new_df.at[:, "Date"] = self.trade_details.loc[:, "Date"].map(lambda x: x.date())

        freights_df.at[:, "Mine"] = self.freights_trade_details.loc[:, "Mine"]
        freights_df.at[:, "Date"] = self.freights_trade_details.loc[:, "Date"].map(lambda x: x.date())

        cbix_coe_df.at[:, "Mine"] = self.cbix_cf_trade_details.loc[:, "Mine"]
        cbix_coe_df.at[:, "Date"] = self.cbix_cf_trade_details.loc[:, "Date"].map(lambda x: x.date())


        def lookup(search1, search2, target):
            for ind in range(self.indexes_mines.shape[0]):
                if self.indexes_mines.loc[ind, "Mine"] == search1 and self.indexes_mines.loc[ind, "Date"].date() <= search2:
                    return target.iloc[ind]
            return 0
        #trade details
        for col in self.nominal_mine_col[2:]:
            for i in range(self.trade_details.shape[0]):
                new_df.at[i, col] = lookup(new_df.loc[i, "Mine"], new_df.loc[i, "Date"], self.indexes_mines.loc[:, "DWT" if col == 'Tonnage typical vessel' else col])
        #freights trade details
        for col in self.nominal_mine_col[2:]:
            for i in range(self.freights_trade_details.shape[0]):
                freights_df.at[i, col] = lookup(freights_df.loc[i, "Mine"], freights_df.loc[i, "Date"], self.indexes_mines.loc[:, "DWT" if col == 'Tonnage typical vessel' else col])
        #cbix co-efficients trade details
        for col in self.nominal_mine_col[2:]:
            for i in range(self.cbix_cf_trade_details.shape[0]):
                cbix_coe_df.at[i, col] = lookup(cbix_coe_df.loc[i, "Mine"], cbix_coe_df.loc[i, "Date"], self.indexes_mines.loc[:, "DWT" if col == 'Tonnage typical vessel' else col])


        self.db["outputs/nominal_mine_div_index_specifications.csv"] = new_df
        self.db["outputs/freights/nominal_mine_div_index_specifications.csv"] = freights_df
        self.db["outputs/cbix_co_efficients_determination/nominal_mine_div_index_specifications.csv"] = cbix_coe_df

    def global_factors_func(self):
        cols = self.global_factors.columns.tolist()
        cols.insert(23, "MDO/MGO burn vessel DWT ^2 coefficient")
        cols.insert(26, "Main engine burn vessel DWT base constant")
        cols.insert(27, "Main engine burn vessel DWT base slope")

        new_df       = pd.DataFrame(columns=cols)
        freights_df  = pd.DataFrame(columns=cols)
        cbix_coe_df  = pd.DataFrame(columns=cols)

        for i in range(len(self.trade_details)):
            for j in range(len(new_df.columns)):
                if new_df.columns[j] in self.global_factors.columns:
                    new_df.at[i, new_df.columns[j]] = self.global_factors.loc[0, new_df.columns[j]]
                else:
                    new_df.at[i, new_df.columns[j]] = 0
        
        for i in range(len(self.freights_trade_details)):
            for j in range(len(freights_df.columns)):
                if freights_df.columns[j] in self.global_factors.columns:
                    freights_df.at[i, freights_df.columns[j]] = self.global_factors.loc[0, freights_df.columns[j]]
                else:
                    freights_df.at[i, freights_df.columns[j]] = 0

        for i in range(len(self.cbix_cf_trade_details)):
            for j in range(len(cbix_coe_df.columns)):
                if cbix_coe_df.columns[j] in self.global_factors.columns:
                    cbix_coe_df.at[i, cbix_coe_df.columns[j]] = self.global_factors.loc[0, cbix_coe_df.columns[j]]
                else:
                    cbix_coe_df.at[i, cbix_coe_df.columns[j]] = 0

        new_df.drop("Date", axis=1, inplace=True)
        freights_df.drop("Date", axis=1, inplace=True)
        cbix_coe_df.drop("Date", axis=1, inplace=True)
        
        self.db["outputs/global_factors.csv"] = new_df
        self.db["outputs/freights/global_factors.csv"] = freights_df
        self.db["outputs/cbix_co_efficients_determination/global_factors.csv"] = cbix_coe_df
    
    def final_specifications_to_viu_adjustment(self):
        glb_factors  = self.db["outputs/global_factors.csv"]
        freights_glb_factors  = self.db["outputs/freights/global_factors.csv"]
        cbix_coe_glb_factors  = self.db["outputs/cbix_co_efficients_determination/global_factors.csv"]
        
        nominal_mine = self.db["outputs/nominal_mine_div_index_specifications.csv"]
        freights_nominal_mine = self.db["outputs/freights/nominal_mine_div_index_specifications.csv"]
        cbix_coe_nominal_mine = self.db["outputs/cbix_co_efficients_determination/nominal_mine_div_index_specifications.csv"]        
        
        new_df       = pd.DataFrame(columns=self.final_specs_columns)
        freights_df  = pd.DataFrame(columns=self.final_specs_columns)
        cbix_coe_df  = pd.DataFrame(columns=self.final_specs_columns)

        def bool_lookup(search1, search2):
            for ind in range(self.spc_leg_shp_table.shape[0]):
                if self.spc_leg_shp_table.loc[ind, "Mine"] == search1 and self.spc_leg_shp_table.loc[ind, "Date"].date() <= search2:
                    return True
            return False

        def lookup(search1, search2, target):
            for ind in range(self.spc_leg_shp_table.shape[0]):

                if self.spc_leg_shp_table.loc[ind, "Mine"] == search1 and self.spc_leg_shp_table.loc[ind, "Date"].date() <= search2:
                    return target.iloc[ind]
            return 0


        for i in range(len(self.trade_details)):
            new_df.at[i, "Mine"]                             = self.trade_details.loc[i, "Mine"]
            new_df.at[i, "Total Alumina"]                    = nominal_mine.loc[i, "Total Alumina"] if pd.isna(self.trade_details.loc[i, "Total Alumina"]) else self.trade_details.loc[i, "Total Alumina"]
            new_df.at[i, "LT Avail. Alumina"]                = nominal_mine.loc[i, "LT Avail. Alumina"] if pd.isna(self.trade_details.loc[i, "LT Avail. Alumina"]) else self.trade_details.loc[i, "LT Avail. Alumina"]
            new_df.at[i, "Total Silica"]                     = nominal_mine.loc[i, "Total Silica"] if pd.isna(self.trade_details.loc[i, "Total Silica"]) else self.trade_details.loc[i, "Total Silica"]
            new_df.at[i, "LT R.Silica"]                      = nominal_mine.loc[i, "LT R.Silica"] if pd.isna(self.trade_details.loc[i, "LT R.Silica"]) else self.trade_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Quartz / HT Silica"]               = nominal_mine.loc[i, "Quartz / HT Silica"] if pd.isna(self.trade_details.loc[i, "Quartz / HT Silica"]) else self.trade_details.loc[i, "Quartz / HT Silica"]
            new_df.at[i, "Mono-hydrate / HT Extble Alumina"] = nominal_mine.loc[i, "Mono-hydrate / HT Alumina"] if pd.isna(self.trade_details.loc[i, "Mono-hydrate / HT Extble Alumina"]) else self.trade_details.loc[i, "Mono-hydrate / HT Extble Alumina"]
            new_df.at[i, "Moisture"]                         = nominal_mine.loc[i, "Moisture"] if pd.isna(self.trade_details.loc[i, "Moisture"]) else self.trade_details.loc[i, "Moisture"]
            new_df.at[i, "Processing"]                       = nominal_mine.loc[i, "Processing"]
            new_df.at[i, "Cargo Tonnage"]                    = float(nominal_mine.loc[i, "Tonnage typical vessel"]) * float(glb_factors.loc[i, "Max % of vessel deadweight allowed for loading"]) * (1 - float(new_df.loc[i, "Moisture"])) if pd.isna(self.trade_details.loc[i, "Tonnage"]) else float(self.trade_details.loc[i, "Tonnage"]) * (1 if self.trade_details.loc[i, "Price Basis"] == "dmt" else (1 - float(new_df.loc[i, "Moisture"])))
            new_df.at[i, "Double Leg Shipping? (South America special transload leg)"] = "Yes" if (new_df.loc[i, "Cargo Tonnage"] > self.mj_max_cargo.loc[0, "MRN/Juruti max cargo single leg shipping"] and bool_lookup(self.trade_details.loc[i, "Mine"], self.trade_details.loc[i, "Date"])) else "No"
            new_df.at[i, "Exporting Port"]                   = lookup(new_df.loc[i, "Mine"], self.trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 1"]) if new_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else (nominal_mine.loc[i, "Exporting Port"] if pd.isna(self.trade_details.loc[i, "Exporting Port"]) else self.trade_details.loc[i, "Exporting Port"])
            new_df.at[i, "South America special transloading Port"] = 0 if new_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else lookup(new_df.loc[i, "Mine"], self.trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 2"])
            new_df.at[i, "South america special transloading cargo tonnage"] = 0 if new_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else float(new_df.loc[i, "Cargo Tonnage"])/round(float(new_df.loc[i, "Cargo Tonnage"])/float(lookup(new_df.loc[i, "Mine"], self.trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "Typical River Capable Cargo (dmt)"])), 0)
            new_df.at[i, "Importing Port"]                   = "Qingdao" if pd.isna(self.trade_details.loc[i, "Importing Port"]) else self.trade_details.loc[i, "Importing Port"]
            new_df.at[i, "Specific Index"]                   = nominal_mine.loc[i, "Specific Index"]
            new_df.at[i, "General Index"]                    = nominal_mine.loc[i, "General Index"]
            new_df.at[i, "Price Type"]                       = self.trade_details.loc[i, "Final Specs Price Type"]
            new_df.at[i, "Price Basis"]                      = self.trade_details.loc[i, "Final Specs Price Basis"]
            new_df.at[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"] = nominal_mine.loc[i, "Processing Penalties to be applied"]
            new_df.at[i, "EXPLANITORY NOTES RECORD (on changes made to final spec etc)"] = np.nan
            new_df.at[i, "Old CBIX type Calc"] = self.trade_details.loc[i, "Old CBIX type Calc"]

            #freights
        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Mine"]                             = self.freights_trade_details.loc[i, "Mine"]
            freights_df.at[i, "Total Alumina"]                    = freights_nominal_mine.loc[i, "Total Alumina"] if pd.isna(self.freights_trade_details.loc[i, "Total Alumina"]) else self.freights_trade_details.loc[i, "Total Alumina"]
            freights_df.at[i, "LT Avail. Alumina"]                = freights_nominal_mine.loc[i, "LT Avail. Alumina"] if pd.isna(self.freights_trade_details.loc[i, "LT Avail. Alumina"]) else self.freights_trade_details.loc[i, "LT Avail. Alumina"]
            freights_df.at[i, "Total Silica"]                     = freights_nominal_mine.loc[i, "Total Silica"] if pd.isna(self.freights_trade_details.loc[i, "Total Silica"]) else self.freights_trade_details.loc[i, "Total Silica"]
            freights_df.at[i, "LT R.Silica"]                      = freights_nominal_mine.loc[i, "LT R.Silica"] if pd.isna(self.freights_trade_details.loc[i, "LT R.Silica"]) else self.freights_trade_details.loc[i, "LT R.Silica"]
            freights_df.at[i, "Quartz / HT Silica"]               = freights_nominal_mine.loc[i, "Quartz / HT Silica"] if pd.isna(self.freights_trade_details.loc[i, "Quartz / HT Silica"]) else self.freights_trade_details.loc[i, "Quartz / HT Silica"]
            freights_df.at[i, "Mono-hydrate / HT Extble Alumina"] = freights_nominal_mine.loc[i, "Mono-hydrate / HT Alumina"] if pd.isna(self.freights_trade_details.loc[i, "Mono-hydrate / HT Extble Alumina"]) else self.freights_trade_details.loc[i, "Mono-hydrate / HT Extble Alumina"]
            freights_df.at[i, "Moisture"]                         = freights_nominal_mine.loc[i, "Moisture"] if pd.isna(self.freights_trade_details.loc[i, "Moisture"]) else self.freights_trade_details.loc[i, "Moisture"]
            freights_df.at[i, "Processing"]                       = freights_nominal_mine.loc[i, "Processing"]
            freights_df.at[i, "Cargo Tonnage"]                    = float(freights_nominal_mine.loc[i, "Tonnage typical vessel"]) * float(freights_glb_factors.loc[i, "Max % of vessel deadweight allowed for loading"]) * (1 - float(freights_df.loc[i, "Moisture"])) if pd.isna(self.freights_trade_details.loc[i, "Tonnage"]) else float(self.freights_trade_details.loc[i, "Tonnage"]) * (1 if self.freights_trade_details.loc[i, "Price Basis"] == "dmt" else (1 - float(freights_df.loc[i, "Moisture"])))
            freights_df.at[i, "Double Leg Shipping? (South America special transload leg)"] = "Yes" if (freights_df.loc[i, "Cargo Tonnage"] > self.mj_max_cargo.loc[0, "MRN/Juruti max cargo single leg shipping"] and bool_lookup(self.freights_trade_details.loc[i, "Mine"], self.freights_trade_details.loc[i, "Date"])) else "No"
            freights_df.at[i, "Exporting Port"]                   = lookup(freights_df.loc[i, "Mine"], self.freights_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 1"]) if freights_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else (freights_nominal_mine.loc[i, "Exporting Port"] if pd.isna(self.freights_trade_details.loc[i, "Exporting Port"]) else self.freights_trade_details.loc[i, "Exporting Port"])
            freights_df.at[i, "South America special transloading Port"] = 0 if freights_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else lookup(freights_df.loc[i, "Mine"], self.freights_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 2"])
            freights_df.at[i, "South america special transloading cargo tonnage"] = 0 if freights_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else float(freights_df.loc[i, "Cargo Tonnage"])/round(float(freights_df.loc[i, "Cargo Tonnage"])/float(lookup(freights_df.loc[i, "Mine"], self.freights_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "Typical River Capable Cargo (dmt)"])), 0)
            freights_df.at[i, "Importing Port"]                   = "Qingdao" if pd.isna(self.freights_trade_details.loc[i, "Importing Port"]) else self.freights_trade_details.loc[i, "Importing Port"]
            freights_df.at[i, "Specific Index"]                   = freights_nominal_mine.loc[i, "Specific Index"]
            freights_df.at[i, "General Index"]                    = freights_nominal_mine.loc[i, "General Index"]
            freights_df.at[i, "Price Type"]                       = self.freights_trade_details.loc[i, "Final Specs Price Type"]
            freights_df.at[i, "Price Basis"]                      = self.freights_trade_details.loc[i, "Final Specs Price Basis"]
            freights_df.at[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"] = freights_nominal_mine.loc[i, "Processing Penalties to be applied"]
            freights_df.at[i, "EXPLANITORY NOTES RECORD (on changes made to final spec etc)"] = np.nan
            freights_df.at[i, "Old CBIX type Calc"] = self.freights_trade_details.loc[i, "Old CBIX type Calc"]

        #cbix co-efficients
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Mine"]                             = self.cbix_cf_trade_details.loc[i, "Mine"]
            cbix_coe_df.at[i, "Total Alumina"]                    = cbix_coe_nominal_mine.loc[i, "Total Alumina"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Total Alumina"]) else self.cbix_cf_trade_details.loc[i, "Total Alumina"]
            cbix_coe_df.at[i, "LT Avail. Alumina"]                = cbix_coe_nominal_mine.loc[i, "LT Avail. Alumina"] if pd.isna(self.cbix_cf_trade_details.loc[i, "LT Avail. Alumina"]) else self.cbix_cf_trade_details.loc[i, "LT Avail. Alumina"]
            cbix_coe_df.at[i, "Total Silica"]                     = cbix_coe_nominal_mine.loc[i, "Total Silica"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Total Silica"]) else self.cbix_cf_trade_details.loc[i, "Total Silica"]
            cbix_coe_df.at[i, "LT R.Silica"]                      = cbix_coe_nominal_mine.loc[i, "LT R.Silica"] if pd.isna(self.cbix_cf_trade_details.loc[i, "LT R.Silica"]) else self.cbix_cf_trade_details.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Quartz / HT Silica"]               = cbix_coe_nominal_mine.loc[i, "Quartz / HT Silica"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Quartz / HT Silica"]) else self.cbix_cf_trade_details.loc[i, "Quartz / HT Silica"]
            cbix_coe_df.at[i, "Mono-hydrate / HT Extble Alumina"] = cbix_coe_nominal_mine.loc[i, "Mono-hydrate / HT Alumina"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Mono-hydrate / HT Extble Alumina"]) else self.cbix_cf_trade_details.loc[i, "Mono-hydrate / HT Extble Alumina"]
            cbix_coe_df.at[i, "Moisture"]                         = cbix_coe_nominal_mine.loc[i, "Moisture"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Moisture"]) else self.cbix_cf_trade_details.loc[i, "Moisture"]
            cbix_coe_df.at[i, "Processing"]                       = cbix_coe_nominal_mine.loc[i, "Processing"]
            cbix_coe_df.at[i, "Cargo Tonnage"]                    = float(cbix_coe_nominal_mine.loc[i, "Tonnage typical vessel"]) * float(cbix_coe_glb_factors.loc[i, "Max % of vessel deadweight allowed for loading"]) * (1 - float(cbix_coe_df.loc[i, "Moisture"])) if pd.isna(self.cbix_cf_trade_details.loc[i, "Tonnage"]) else float(self.cbix_cf_trade_details.loc[i, "Tonnage"]) * (1 if self.cbix_cf_trade_details.loc[i, "Price Basis"] == "dmt" else (1 - float(cbix_coe_df.loc[i, "Moisture"])))
            cbix_coe_df.at[i, "Double Leg Shipping? (South America special transload leg)"] = "Yes" if (cbix_coe_df.loc[i, "Cargo Tonnage"] > self.mj_max_cargo.loc[0, "MRN/Juruti max cargo single leg shipping"] and bool_lookup(self.cbix_cf_trade_details.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"])) else "No"
            cbix_coe_df.at[i, "Exporting Port"]                   = lookup(cbix_coe_df.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 1"]) if cbix_coe_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else (cbix_coe_nominal_mine.loc[i, "Exporting Port"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Exporting Port"]) else self.cbix_cf_trade_details.loc[i, "Exporting Port"])
            cbix_coe_df.at[i, "South America special transloading Port"] = 0 if cbix_coe_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else lookup(cbix_coe_df.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 2"])
            cbix_coe_df.at[i, "South america special transloading cargo tonnage"] = 0 if cbix_coe_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else float(cbix_coe_df.loc[i, "Cargo Tonnage"])/round(float(cbix_coe_df.loc[i, "Cargo Tonnage"])/float(lookup(cbix_coe_df.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "Typical River Capable Cargo (dmt)"])), 0)
            cbix_coe_df.at[i, "Importing Port"]                   = "Qingdao" if pd.isna(self.cbix_cf_trade_details.loc[i, "Importing Port"]) else self.cbix_cf_trade_details.loc[i, "Importing Port"]
            cbix_coe_df.at[i, "Specific Index"]                   = cbix_coe_nominal_mine.loc[i, "Specific Index"]
            cbix_coe_df.at[i, "General Index"]                    = cbix_coe_nominal_mine.loc[i, "General Index"]
            cbix_coe_df.at[i, "Price Type"]                       = self.cbix_cf_trade_details.loc[i, "Final Specs Price Type"]
            cbix_coe_df.at[i, "Price Basis"]                      = self.cbix_cf_trade_details.loc[i, "Final Specs Price Basis"]
            cbix_coe_df.at[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"] = cbix_coe_nominal_mine.loc[i, "Processing Penalties to be applied"]
            cbix_coe_df.at[i, "EXPLANITORY NOTES RECORD (on changes made to final spec etc)"] = np.nan
            cbix_coe_df.at[i, "Old CBIX type Calc"] = self.cbix_cf_trade_details.loc[i, "Old CBIX type Calc"]

        self.db["outputs/final_specifications_to_viu_adjustment.csv"] = new_df
        self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"] = freights_df
        self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"] = cbix_coe_df


    def bauxite_details_input_func(self):
        fin_spec_df = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fin_spec_df = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec_df = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame(columns=self.bauxite_details_columns)
        freights_df = pd.DataFrame(columns=self.bauxite_details_columns)
        cbix_coe_df = pd.DataFrame(columns=self.bauxite_details_columns)

        for i in range(len(self.trade_details)):
            # new_df.at[i, "Price"]   = 
            new_df.at[i, "Total Alumina"]           = fin_spec_df.loc[i, "Total Alumina"]
            new_df.at[i, "LT Avail. Alumina"]       = fin_spec_df.loc[i, "LT Avail. Alumina"]
            new_df.at[i, "Total Silica"]            = fin_spec_df.loc[i, "Total Silica"]
            new_df.at[i, "LT R.Silica"]             = fin_spec_df.loc[i, "LT R.Silica"]
            new_df.at[i, "Quartz / HT Silica"]      = fin_spec_df.loc[i, "Quartz / HT Silica"]
            new_df.at[i, "Mono-hydrate / HT Alumina"] = fin_spec_df.loc[i, "Mono-hydrate / HT Extble Alumina"]
            new_df.at[i, "Moisture"]                = fin_spec_df.loc[i, "Moisture"]
            new_df.at[i, "Processing"]              = fin_spec_df.loc[i, "Processing"]
            new_df.at[i, "Processing Penalties"]    = fin_spec_df.loc[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"]
        
        for i in range(len(self.freights_trade_details)):
            # freights_df.at[i, "Price"]   = 
            freights_df.at[i, "Total Alumina"]           = freights_fin_spec_df.loc[i, "Total Alumina"]
            freights_df.at[i, "LT Avail. Alumina"]       = freights_fin_spec_df.loc[i, "LT Avail. Alumina"]
            freights_df.at[i, "Total Silica"]            = freights_fin_spec_df.loc[i, "Total Silica"]
            freights_df.at[i, "LT R.Silica"]             = freights_fin_spec_df.loc[i, "LT R.Silica"]
            freights_df.at[i, "Quartz / HT Silica"]      = freights_fin_spec_df.loc[i, "Quartz / HT Silica"]
            freights_df.at[i, "Mono-hydrate / HT Alumina"] = freights_fin_spec_df.loc[i, "Mono-hydrate / HT Extble Alumina"]
            freights_df.at[i, "Moisture"]                = freights_fin_spec_df.loc[i, "Moisture"]
            freights_df.at[i, "Processing"]              = freights_fin_spec_df.loc[i, "Processing"]
            freights_df.at[i, "Processing Penalties"]    = freights_fin_spec_df.loc[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            # cbix_coe_df.at[i, "Price"]   = 
            cbix_coe_df.at[i, "Total Alumina"]           = cbix_coe_fin_spec_df.loc[i, "Total Alumina"]
            cbix_coe_df.at[i, "LT Avail. Alumina"]       = cbix_coe_fin_spec_df.loc[i, "LT Avail. Alumina"]
            cbix_coe_df.at[i, "Total Silica"]            = cbix_coe_fin_spec_df.loc[i, "Total Silica"]
            cbix_coe_df.at[i, "LT R.Silica"]             = cbix_coe_fin_spec_df.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Quartz / HT Silica"]      = cbix_coe_fin_spec_df.loc[i, "Quartz / HT Silica"]
            cbix_coe_df.at[i, "Mono-hydrate / HT Alumina"] = cbix_coe_fin_spec_df.loc[i, "Mono-hydrate / HT Extble Alumina"]
            cbix_coe_df.at[i, "Moisture"]                = cbix_coe_fin_spec_df.loc[i, "Moisture"]
            cbix_coe_df.at[i, "Processing"]              = cbix_coe_fin_spec_df.loc[i, "Processing"]
            cbix_coe_df.at[i, "Processing Penalties"]    = cbix_coe_fin_spec_df.loc[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"]
        
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = cbix_coe_df

    def china_processing_factors_func(self):
        baux_dets_inputs          = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        freights_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        cbix_coe_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        
        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        freights_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

        def lookup(search1, search2, target):
            ind = (self.processing_factors.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.processing_factors.loc[:, "Processing Regime"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])
        
        #frieghts
        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        #cbix co-efficients
        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = cbix_coe_df

    def alumina_production_cost_calcs_ece(self):
        bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]     # pd.read_excel("outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv")
        freights_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        cbix_coe_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        

        china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]  # pd.read_excel("outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv")
        freights_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        cbix_coe_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        
        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        freights_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Reactive Alumina"]     =  (bx_details.loc[i, "Total Alumina"] - bx_details.loc[i, "LT Avail. Alumina"] - bx_details.loc[i, "LT R.Silica"]) * china_prc.loc[i, "HT Alumina Dissolution"] + bx_details.loc[i, "LT Avail. Alumina"] + bx_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Reactive Silica"]      =  bx_details.loc[i, "LT R.Silica"] + china_prc.loc[i, "Quartz Attack"] * (bx_details.loc[i, "Total Silica"] - bx_details.loc[i, "LT R.Silica"])
            new_df.at[i, "Available Alumina"]    =  new_df.loc[i, "Reactive Alumina"] - china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * new_df.loc[i, "Reactive Silica"]
            new_df.at[i, "Tonnes per Tonne"]     =  1 / new_df.loc[i, "Available Alumina"] / china_prc.loc[i, "Extraction Efficiency %"]
            new_df.at[i, "Caustic Use t.NAOH / t.AA"] = new_df.loc[i, "Reactive Silica"] * china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * new_df.loc[i, "Tonnes per Tonne"] + china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Reactive Alumina"]     =  (freights_baux_dets_inputs.loc[i, "Total Alumina"] - freights_baux_dets_inputs.loc[i, "LT Avail. Alumina"] - freights_baux_dets_inputs.loc[i, "LT R.Silica"]) * freights_china_prc.loc[i, "HT Alumina Dissolution"] + freights_baux_dets_inputs.loc[i, "LT Avail. Alumina"] + freights_baux_dets_inputs.loc[i, "LT R.Silica"]
            freights_df.at[i, "Reactive Silica"]      =  freights_baux_dets_inputs.loc[i, "LT R.Silica"] + freights_china_prc.loc[i, "Quartz Attack"] * (freights_baux_dets_inputs.loc[i, "Total Silica"] - freights_baux_dets_inputs.loc[i, "LT R.Silica"])
            freights_df.at[i, "Available Alumina"]    =  freights_df.loc[i, "Reactive Alumina"] - freights_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * freights_df.loc[i, "Reactive Silica"]
            freights_df.at[i, "Tonnes per Tonne"]     =  1 / freights_df.loc[i, "Available Alumina"] / freights_china_prc.loc[i, "Extraction Efficiency %"]
            freights_df.at[i, "Caustic Use t.NAOH / t.AA"] = freights_df.loc[i, "Reactive Silica"] * freights_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * freights_df.loc[i, "Tonnes per Tonne"] + freights_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]


        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Reactive Alumina"]     =  (cbix_coe_baux_dets_inputs.loc[i, "Total Alumina"] - cbix_coe_baux_dets_inputs.loc[i, "LT Avail. Alumina"] - cbix_coe_baux_dets_inputs.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_baux_dets_inputs.loc[i, "LT Avail. Alumina"] + cbix_coe_baux_dets_inputs.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Reactive Silica"]      =  cbix_coe_baux_dets_inputs.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_baux_dets_inputs.loc[i, "Total Silica"] - cbix_coe_baux_dets_inputs.loc[i, "LT R.Silica"])
            cbix_coe_df.at[i, "Available Alumina"]    =  cbix_coe_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Reactive Silica"]
            cbix_coe_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_coe_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_coe_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_coe_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]

        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"] = cbix_coe_df


    def common_data_inputs_glb_facs(self):
        new_df      = pd.DataFrame(columns=self.cdi_glb_factors_columns)
        freights_df = pd.DataFrame(columns=self.cdi_glb_factors_columns)
        cbix_coe_df = pd.DataFrame(columns=self.cdi_glb_factors_columns)

        def lookup(search, target):           
            ind = (self.global_factors.loc[:, "Date"].map(lambda x: x.date()) <= search).map(lambda i: 1 if i == True else 0)
            idx = (target.map(lambda x: pd.notna(x))).map(lambda i: 1 if i == True else 0)            
            v = 1 / (ind * idx) 

            return target.iloc[max(v.value_counts())-1]


        hardcoded = [
            "MDO/MGO burn vessel DWT ^2 coefficient",
            "Main engine burn vessel DWT base constant",
            "Main engine burn vessel DWT base slope",
            "Main engine burn speed linear coefficient",
        ]

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if col in hardcoded:
                    v = 0
                else:
                    v = lookup(self.trade_details.loc[i, "Date"], self.global_factors.loc[:, col])

                new_df.at[i, col] = v

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if col in hardcoded:
                    v = 0
                else:
                    v = lookup(self.freights_trade_details.loc[i, "Date"], self.global_factors.loc[:, col])

                freights_df.at[i, col] = v

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if col in hardcoded:
                    v = 0
                else:
                    v = lookup(self.cbix_cf_trade_details.loc[i, "Date"], self.global_factors.loc[:, col])

                cbix_coe_df.at[i, col] = v
        
        self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"] = new_df
        self.db["outputs/common_data_inputs/freights/common_data_inputs_global_factors.csv"] = freights_df
        self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_global_factors.csv"] = cbix_coe_df


    def common_data_inputs_fx_rates(self):
        comm_cols = [
            "RMB per US$",
            "AU$ per US$",
            "US$ per US$",
            "IMF Special Drawing Rights to US Dollar",
        ]

        new_df      = pd.DataFrame(columns=comm_cols)
        freights_df = pd.DataFrame(columns=comm_cols)
        cbix_coe_df = pd.DataFrame(columns=comm_cols)

        def lookup(search, target):
            ind = (self.fx_rates.loc[:, "Date"].map(lambda x: x.date()) <= search) #.map(lambda i: 1 if i == True else 0)
            idx = (target.map(lambda x: not pd.isna(x))) #.map(lambda i: 1 if i == True else 0)            
            v = (ind & idx)
            try:
                return target[v].tolist()[-1]
            except Exception:
                return np.nan

        for i in range(len(self.trade_details)):
            new_df.at[i, "RMB per US$"] = lookup(self.trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "RMB per US$"])
            new_df.at[i, "AU$ per US$"] = lookup(self.trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "AU$ per US$"])
            new_df.at[i, "US$ per US$"] = lookup(self.trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "US$ per US$"])
            new_df.at[i, "IMF Special Drawing Rights to US Dollar"] = lookup(self.trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "IMF Special Drawing Rights to US Dollar"])            

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "RMB per US$"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "RMB per US$"])
            freights_df.at[i, "AU$ per US$"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "AU$ per US$"])
            freights_df.at[i, "US$ per US$"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "US$ per US$"])
            freights_df.at[i, "IMF Special Drawing Rights to US Dollar"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "IMF Special Drawing Rights to US Dollar"])            

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "RMB per US$"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "RMB per US$"])
            cbix_coe_df.at[i, "AU$ per US$"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "AU$ per US$"])
            cbix_coe_df.at[i, "US$ per US$"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "US$ per US$"])
            cbix_coe_df.at[i, "IMF Special Drawing Rights to US Dollar"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "IMF Special Drawing Rights to US Dollar"])            
                      
        self.db["outputs/common_data_inputs/common_data_inputs_fx_rates.csv"] = new_df
        self.db["outputs/common_data_inputs/freights/common_data_inputs_fx_rates.csv"] = freights_df
        self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_fx_rates.csv"] = cbix_coe_df

    def  china_input_prices(self):
        glb_facs = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]     # pd.read_excel("outputs/common_data_inputs/common_data_inputs_global_factors.csv")
        freights_glb_facs = self.db["outputs/common_data_inputs/freights/common_data_inputs_global_factors.csv"]
        cbix_coe_glb_facs = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_global_factors.csv"]

        fx_rates = self.db["outputs/common_data_inputs/common_data_inputs_fx_rates.csv"]       # pd.read_excel("outputs/common_data_inputs/common_data_inputs_fx_rates.csv")
        freights_fx_rates = self.db["outputs/common_data_inputs/freights/common_data_inputs_fx_rates.csv"]
        cbix_coe_fx_rates = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_fx_rates.csv"]

        cip_cols = [
            "Energy Price",
            "Caustic Price",
            "Lime Price",
            "Mud Disposal Cost",
        ]

        new_df      = pd.DataFrame(columns=cip_cols)
        freights_df = pd.DataFrame(columns=cip_cols)
        cbix_coe_df = pd.DataFrame(columns=cip_cols)

        def lookup(search, target_date, target):
            for ind in range(self.china_ps.shape[0]):
                filt = target[target_date.iloc[:].map(lambda x: x.date()) <= search]
                if filt is None:
                     filt = target[target_date.iloc[:].map(lambda x: x.date()) >= search]
                     return filt.iloc[0]
                else:
                    return filt.iloc[-1]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Energy Price"]  = lookup(self.trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Price"]) / fx_rates.loc[i, "RMB per US$"] / (lookup(self.trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Energy value"]) * glb_facs.loc[i, "conversion kt to t"] / glb_facs.loc[i, "kcal per GJ"])
            new_df.at[i, "Caustic Price"] = lookup(self.trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Price"]) / fx_rates.loc[i, "RMB per US$"] / (lookup(self.trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Grade"]))
            new_df.at[i, "Lime Price"]    = lookup(self.trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lime – Date"], self.china_ps.loc[:, "Lime – Price"]) / fx_rates.loc[i, "RMB per US$"]
            new_df.at[i, "Mud Disposal Cost"]    = lookup(self.trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Mud disposal cost - Date"], self.china_ps.loc[:, "Mud disposal cost – Price"]) / fx_rates.loc[i, "RMB per US$"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Energy Price"]  = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Price"]) / freights_fx_rates.loc[i, "RMB per US$"] / (lookup(self.freights_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Energy value"]) * freights_glb_facs.loc[i, "conversion kt to t"] / freights_glb_facs.loc[i, "kcal per GJ"])
            freights_df.at[i, "Caustic Price"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Price"]) / freights_fx_rates.loc[i, "RMB per US$"] / (lookup(self.freights_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Grade"]))
            freights_df.at[i, "Lime Price"]    = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lime – Date"], self.china_ps.loc[:, "Lime – Price"]) / freights_fx_rates.loc[i, "RMB per US$"]
            freights_df.at[i, "Mud Disposal Cost"]    = lookup(self.freights_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Mud disposal cost - Date"], self.china_ps.loc[:, "Mud disposal cost – Price"]) / freights_fx_rates.loc[i, "RMB per US$"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Energy Price"]  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"] / (lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Energy value"]) * cbix_coe_glb_facs.loc[i, "conversion kt to t"] / cbix_coe_glb_facs.loc[i, "kcal per GJ"])
            cbix_coe_df.at[i, "Caustic Price"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"] / (lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Grade"]))
            cbix_coe_df.at[i, "Lime Price"]    = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lime – Date"], self.china_ps.loc[:, "Lime – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"]
            cbix_coe_df.at[i, "Mud Disposal Cost"]    = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Mud disposal cost - Date"], self.china_ps.loc[:, "Mud disposal cost – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"]
            

        self.db["outputs/common_data_inputs/common_data_inputs_china_input_prices.csv"] = new_df
        self.db["outputs/common_data_inputs/freights/common_data_inputs_china_input_prices.csv"] = freights_df
        self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_china_input_prices.csv"] = cbix_coe_df

    def frieght_calculations(self, folder=None):
        glb_facs          = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]       # pd.read_excel("outputs/common_data_inputs/common_data_inputs_global_factors.csv")
        freights_glb_facs = self.db["outputs/common_data_inputs/freights/common_data_inputs_global_factors.csv"]
        cbix_coe_glb_facs = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_global_factors.csv"]

        final_spec           = self.db["outputs/final_specifications_to_viu_adjustment.csv"]     # pd.read_excel("outputs/final_specifications_to_viu_adjustment.csv")
        freights_fin_spec_df = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec_df = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]
        
        new_df      = pd.DataFrame(columns=self.frieght_calcs_columns)
        freights_df = pd.DataFrame(columns=self.frieght_calcs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.frieght_calcs_columns)

        def lookup(search, target_check, target):
            for ind in range(target_check.shape[0]):
                filt = target[target_check.iloc[:] <= search]
                if filt is None:
                     filt = target[target_check.iloc[:] >= search]
                     return filt.iloc[0]
                else:
                    return filt.iloc[-1]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Vessel dwt"] = ((final_spec.loc[i, "Cargo Tonnage"] if final_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else final_spec.loc[i, "South america special transloading cargo tonnage"]) / (1 - final_spec.loc[i, "Moisture"])) / glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            new_df.at[i, "Vessel Class for timecharter & fuel burn rates"] = lookup(new_df.loc[i, "Vessel dwt",], self.sheetname_class.loc[:, "DWT< tonnes"], self.sheetname_class.loc[:, "Class"])
            new_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(new_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            new_df.at[i, "Estimated LOA (length over all) (m)"]    = (new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "LOA estimate correlation multiplier"] + glb_facs.loc[i, "LOA estimate correlation constant"])
            new_df.at[i, "Estimated NRT (net register tons)"]  = (new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "NRT estimate correlation multiplier"] + glb_facs.loc[i, "NRT estimate correlation constant"])
            new_df.at[i, "Estimated GRT (gross register tons)"]    = (new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "GRT estimate correlation multiplier"] + glb_facs.loc[i, "GRT estimate correlation constant"])

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Vessel dwt"] = ((freights_fin_spec_df.loc[i, "Cargo Tonnage"] if freights_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else freights_fin_spec_df.loc[i, "South america special transloading cargo tonnage"]) / (1 - freights_fin_spec_df.loc[i, "Moisture"])) / freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            freights_df.at[i, "Vessel Class for timecharter & fuel burn rates"] = lookup(freights_df.loc[i, "Vessel dwt",], self.sheetname_class.loc[:, "DWT< tonnes"], self.sheetname_class.loc[:, "Class"])
            freights_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(freights_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            freights_df.at[i, "Estimated LOA (length over all) (m)"]    = (freights_df.loc[i, "Vessel dwt"] * freights_glb_facs.loc[i, "LOA estimate correlation multiplier"] + freights_glb_facs.loc[i, "LOA estimate correlation constant"])
            freights_df.at[i, "Estimated NRT (net register tons)"]  = (freights_df.loc[i, "Vessel dwt"] * freights_glb_facs.loc[i, "NRT estimate correlation multiplier"] + freights_glb_facs.loc[i, "NRT estimate correlation constant"])
            freights_df.at[i, "Estimated GRT (gross register tons)"]    = (freights_df.loc[i, "Vessel dwt"] * freights_glb_facs.loc[i, "GRT estimate correlation multiplier"] + freights_glb_facs.loc[i, "GRT estimate correlation constant"])


        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Vessel dwt"] = ((cbix_coe_fin_spec_df.loc[i, "Cargo Tonnage"] if cbix_coe_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fin_spec_df.loc[i, "South america special transloading cargo tonnage"]) / (1 - cbix_coe_fin_spec_df.loc[i, "Moisture"])) / cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_coe_df.at[i, "Vessel Class for timecharter & fuel burn rates"] = lookup(cbix_coe_df.loc[i, "Vessel dwt",], self.sheetname_class.loc[:, "DWT< tonnes"], self.sheetname_class.loc[:, "Class"])
            cbix_coe_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(cbix_coe_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            cbix_coe_df.at[i, "Estimated LOA (length over all) (m)"]    = (cbix_coe_df.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "LOA estimate correlation multiplier"] + cbix_coe_glb_facs.loc[i, "LOA estimate correlation constant"])
            cbix_coe_df.at[i, "Estimated NRT (net register tons)"]  = (cbix_coe_df.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "NRT estimate correlation multiplier"] + cbix_coe_glb_facs.loc[i, "NRT estimate correlation constant"])
            cbix_coe_df.at[i, "Estimated GRT (gross register tons)"]    = (cbix_coe_df.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "GRT estimate correlation multiplier"] + cbix_coe_glb_facs.loc[i, "GRT estimate correlation constant"])

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/frieght_calculations-actual_port-first_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/frieght_calculations-actual_port-first_leg.csv"] = cbix_coe_df

    def exporting_port_details_lr(self, folder=None):
        final_spec           = self.db["outputs/final_specifications_to_viu_adjustment.csv"]     # pd.read_excel("outputs/final_specifications_to_viu_adjustment.csv")
        freights_fin_spec_df = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec_df = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]


        new_df      = pd.DataFrame(columns=self.exporting_port_dts_cols)
        freights_df = pd.DataFrame(columns=self.exporting_port_dts_cols)
        cbix_coe_df = pd.DataFrame(columns=self.exporting_port_dts_cols)

        def lookup(search1, search2, target):            
            ind = (self.indexes_mines.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.indexes_mines.loc[:, "Exporting Port"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]

            return v.iloc[0]

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), final_spec.loc[i, "Exporting Port"], self.indexes_mines.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_fin_spec_df.loc[i, "Exporting Port"], self.indexes_mines.loc[:, col])


        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_fin_spec_df.loc[i, "Exporting Port"], self.indexes_mines.loc[:, col])

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details-loading_rates.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/exporting_port_details-loading_rates.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/exporting_port_details-loading_rates.csv"] = cbix_coe_df



    def port_linkages_funct(self, folder=None):
        fn_spec = self.db["outputs/final_specifications_to_viu_adjustment.csv"]    # pd.read_excel("outputs/final_specifications_to_viu_adjustment.csv")
        freights_fin_spec_df = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec_df = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]
        
        new_df = pd.DataFrame(columns=self.port_linkages_cols)
        freights_df = pd.DataFrame(columns=self.port_linkages_cols)
        cbix_coe_df = pd.DataFrame(columns=self.port_linkages_cols)
        
        def lookup(search1, search2, search3, target):
            for j in range(target.shape[0]):
                if (self.port_linkages.loc[j, "Exporting Port"] == search1) and (self.port_linkages.loc[j, "Importing Port"] == search2) and (self.port_linkages.loc[j, "Date"].date() <= search3):
                    return target.iloc[j]

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(
                    fn_spec.loc[i, "Exporting Port"], 
                    (fn_spec.loc[i, "South America special transloading Port"] if fn_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else fn_spec.loc[i, "Importing Port"]), 
                    self.trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                freights_df.at[i, col] = lookup(
                    freights_fin_spec_df.loc[i, "Exporting Port"], 
                    (freights_fin_spec_df.loc[i, "South America special transloading Port"] if freights_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else freights_fin_spec_df.loc[i, "Importing Port"]), 
                    self.freights_trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                cbix_coe_df.at[i, col] = lookup(
                    cbix_coe_fin_spec_df.loc[i, "Exporting Port"], 
                    (cbix_coe_fin_spec_df.loc[i, "South America special transloading Port"] if cbix_coe_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fin_spec_df.loc[i, "Importing Port"]), 
                    self.cbix_cf_trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])
        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages.csv"] = cbix_coe_df

    def fuel_prices_func(self, folder=None):
        port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.csv"]
        freights_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages.csv"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages.csv"]

        frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.csv"]
        freights_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/frieght_calculations-actual_port-first_leg.csv"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/frieght_calculations-actual_port-first_leg.csv"]

        new_df      = pd.DataFrame(columns=self.fuel_prices_cols)
        freights_df = pd.DataFrame(columns=self.fuel_prices_cols)
        cbix_coe_df = pd.DataFrame(columns=self.fuel_prices_cols)

        def lookup(search1, search2, target):
            for j in range(target.shape[0]):
                ind = (self.ship_fuel_prices.loc[:, "Fuel Region"] == search1) #.map(lambda i: 1 if i == True else 0)
                idx = (self.ship_fuel_prices.loc[:, "Date"].map(lambda x: x.date()) <= search2) #.map(lambda i: 1 if i == True else 0)            
                v = (ind & idx)

                return target[v].tolist()[-1]


        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = port_lkgs.loc[i, "Handysize – Applicable Fuel Region"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = port_lkgs.loc[i, "Supramax – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = port_lkgs.loc[i, "Panamax – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = port_lkgs.loc[i, "NeoPanamax – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = port_lkgs.loc[i, "Suezmax – Applicable Fuel Region"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = port_lkgs.loc[i, "Capesize – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = port_lkgs.loc[i, "VLOC – Applicable Fuel Region"]
                else:
                    v = None

                new_df.at[i, col] = lookup(v, self.trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )


        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = freights_port_lkgs.loc[i, "Handysize – Applicable Fuel Region"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = freights_port_lkgs.loc[i, "Supramax – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = freights_port_lkgs.loc[i, "Panamax – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = freights_port_lkgs.loc[i, "NeoPanamax – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = freights_port_lkgs.loc[i, "Suezmax – Applicable Fuel Region"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = freights_port_lkgs.loc[i, "Capesize – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = freights_port_lkgs.loc[i, "VLOC – Applicable Fuel Region"]
                else:
                    v = None

                freights_df.at[i, col] = lookup(v, self.freights_trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )


        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = cbix_coe_port_lkgs.loc[i, "Handysize – Applicable Fuel Region"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = cbix_coe_port_lkgs.loc[i, "Supramax – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = cbix_coe_port_lkgs.loc[i, "Panamax – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = cbix_coe_port_lkgs.loc[i, "Suezmax – Applicable Fuel Region"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = cbix_coe_port_lkgs.loc[i, "Capesize – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = cbix_coe_port_lkgs.loc[i, "VLOC – Applicable Fuel Region"]
                else:
                    v = None

                cbix_coe_df.at[i, col] = lookup(v, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/fuel_prices.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/fuel_prices.csv"] = cbix_coe_df

    def importing_port_details(self, folder=None):
        final_spec           = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fin_spec_df = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec_df = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame(columns=self.importing_port_det_cols)
        freights_df = pd.DataFrame(columns=self.importing_port_det_cols)
        cbix_coe_df = pd.DataFrame(columns=self.importing_port_det_cols)

        def lookup(search1, search2, target):
            idx = (self.china_imp_prts.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.china_imp_prts.loc[:, "Port"] == search2)
            v = (ind & idx)
            
            return target[v].iloc[0]

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if final_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes":
                    val = final_spec.loc[i, "South America special transloading Port"]
                else:
                    val = final_spec.loc[i, "Importing Port"]

                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if freights_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes":
                    val = freights_fin_spec_df.loc[i, "South America special transloading Port"]
                else:
                    val = freights_fin_spec_df.loc[i, "Importing Port"]

                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if cbix_coe_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes":
                    val = cbix_coe_fin_spec_df.loc[i, "South America special transloading Port"]
                else:
                    val = cbix_coe_fin_spec_df.loc[i, "Importing Port"]

                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        
        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/importing_port_details.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/importing_port_details.csv"] = cbix_coe_df


    def canals_details(self, folder=None):
        port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.csv"]
        freights_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages.csv"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages.csv"]

        frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.csv"]
        freights_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/frieght_calculations-actual_port-first_leg.csv"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/frieght_calculations-actual_port-first_leg.csv"]

        new_df      = pd.DataFrame(columns=self.cost_details_cols)
        freights_df = pd.DataFrame(columns=self.cost_details_cols)
        cbix_coe_df = pd.DataFrame(columns=self.cost_details_cols)

        def lookup(search1, search2, target):
            idx = (self.canals.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.canals.loc[:, "Canal"] == search2)
            v = (ind & idx)
            
            return target[v].tolist()[-1]

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = port_lkgs.loc[i, "Handysize – Canals used"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = port_lkgs.loc[i, "Supramax – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = port_lkgs.loc[i, "Panamax – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = port_lkgs.loc[i, "NeoPanamax – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = port_lkgs.loc[i, "Suezmax – Canals used"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = port_lkgs.loc[i, "Capesize – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = port_lkgs.loc[i, "VLOC – Canals used"]
                else:
                    v = None
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = freights_port_lkgs.loc[i, "Handysize – Canals used"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = freights_port_lkgs.loc[i, "Supramax – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = freights_port_lkgs.loc[i, "Panamax – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = freights_port_lkgs.loc[i, "NeoPanamax – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = freights_port_lkgs.loc[i, "Suezmax – Canals used"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = freights_port_lkgs.loc[i, "Capesize – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = freights_port_lkgs.loc[i, "VLOC – Canals used"]
                else:
                    v = None
                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = cbix_coe_port_lkgs.loc[i, "Handysize – Canals used"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = cbix_coe_port_lkgs.loc[i, "Supramax – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = cbix_coe_port_lkgs.loc[i, "Panamax – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = cbix_coe_port_lkgs.loc[i, "Suezmax – Canals used"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = cbix_coe_port_lkgs.loc[i, "Capesize – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = cbix_coe_port_lkgs.loc[i, "VLOC – Canals used"]
                else:
                    v = None
                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_details.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_details.csv"] = cbix_coe_df

    def canals_costs_workings(self, folder=None):
        canals_details             = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details.csv"]
        freights_df_canals_details = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_details.csv"]
        cbix_coe_df_canals_details = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_details.csv"]
        
        fc_actual_port             = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.csv"]
        freights_df_fc_actual_port = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/frieght_calculations-actual_port-first_leg.csv"]
        cbix_coe_df_fc_actual_port = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/frieght_calculations-actual_port-first_leg.csv"]
        
        glb_facs                   = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]
        freights_df_glb_facs       = self.db["outputs/common_data_inputs/freights/common_data_inputs_global_factors.csv"]
        cbix_coe_df_glb_facs       = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_global_factors.csv"]

        new_df      = pd.DataFrame(columns=self.canal_cost_workings_cols)
        freights_df = pd.DataFrame(columns=self.canal_cost_workings_cols)
        cbix_coe_df = pd.DataFrame(columns=self.canal_cost_workings_cols)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = fc_actual_port.loc[i, "Vessel dwt"] if canals_details.loc[i, "Tonnage Reference"] == "DWT" else fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if canals_details.loc[i, "Tonnage Reference"] == "NRT" else fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if canals_details.loc[i, "Tonnage Reference"] == "GRT" else None
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = fc_actual_port.loc[i, "Vessel dwt"] * glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = freights_df_fc_actual_port.loc[i, "Vessel dwt"] if freights_df_canals_details.loc[i, "Tonnage Reference"] == "DWT" else freights_df_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if freights_df_canals_details.loc[i, "Tonnage Reference"] == "NRT" else freights_df_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if freights_df_canals_details.loc[i, "Tonnage Reference"] == "GRT" else None
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = freights_df_fc_actual_port.loc[i, "Vessel dwt"] * freights_df_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = cbix_coe_df_fc_actual_port.loc[i, "Vessel dwt"] if cbix_coe_df_canals_details.loc[i, "Tonnage Reference"] == "DWT" else cbix_coe_df_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_df_canals_details.loc[i, "Tonnage Reference"] == "NRT" else cbix_coe_df_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_df_canals_details.loc[i, "Tonnage Reference"] == "GRT" else None
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = cbix_coe_df_fc_actual_port.loc[i, "Vessel dwt"] * cbix_coe_df_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_costs_workings.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_costs_workings.csv"] = cbix_coe_df

         
    def final_costing_up_for_leg(self, folder=None):
        fxrates                = self.db["outputs/common_data_inputs/common_data_inputs_fx_rates.csv"]
        freights_fxrates       = self.db["outputs/common_data_inputs/freights/common_data_inputs_fx_rates.csv"]
        cbix_coe_fxrates       = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_fx_rates.csv"]
        
        fuel_prices            = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices.csv"]
        freights_fuel_prices   = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/fuel_prices.csv"]
        cbix_coe_fuel_prices   = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/fuel_prices.csv"]
        
        port_lkgs              = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.csv"]
        freights_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages.csv"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages.csv"]
        
        canals_detls           = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details.csv"]
        freights_canals_detls  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_details.csv"]
        cbix_coe_canals_detls  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_details.csv"]
        
        glb_facs               = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]
        freights_glb_facs      = self.db["outputs/common_data_inputs/freights/common_data_inputs_global_factors.csv"]
        cbix_coe_glb_facs      = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_global_factors.csv"]
        
        canals_costs           = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings.csv"]
        freights_canals_costs  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_costs_workings.csv"]
        cbix_coe_canals_costs  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_costs_workings.csv"]
        
        imp_port_dets          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details.csv"]
        freights_imp_port_dets = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/importing_port_details.csv"]
        cbix_coe_imp_port_dets = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/importing_port_details.csv"]
        
        loading_rates          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details-loading_rates.csv"]
        freights_loading_rates = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/exporting_port_details-loading_rates.csv"]
        cbix_coe_loading_rates = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/exporting_port_details-loading_rates.csv"]
        
        frieght_calcs          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.csv"]
        freights_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/frieght_calculations-actual_port-first_leg.csv"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/frieght_calculations-actual_port-first_leg.csv"]
        
        fnl_specs              = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fnl_specs     = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fnl_specs     = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)
        freights_df = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)
        cbix_coe_df = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)

        fuel_prices_lookup = ["HSFO", "VLSFO", "LNG", "MGO_Regular", "MGO_Low_Sulph"]
        fx_rates_lookup    = ["RMB", "AUD", "USD", "SDR"]

        def lookup(search1, search2, target):
            idx = (self.ship_speeds.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.ship_speeds.loc[:, "Vesssel class"] == search2)
            v = (ind & idx)
            return target[v].tolist()[-1]

        def lookup1(search1, search2, target):
            x = self.ship_time_cr.loc[:, "Applicable Time Charter Index"] == search1
            y = self.ship_time_cr.loc[:, "Date"].map(lambda x: x.date()) <= search2 #.map(lambda i: 1 if i == True else 0)            
            z = self.ship_time_cr.loc[:, "Vessel Time Charter Rates"].map(lambda x: pd.notna(x))

            v =  (x & y & z)
            return target[v].tolist()[-1]
        

        for i in range(len(self.trade_details)):
            if frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                rt_dist_main_fuel = port_lkgs.loc[i, "Handysize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Handysize – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Handysize"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Handysize"]
                main_engine_fuel  = port_lkgs.loc[i, "Handysize – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Handysize – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Handysize – Applicable Time Charter Index"]
            
            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                rt_dist_main_fuel = port_lkgs.loc[i, "Supramax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Supramax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Supramax loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Supramax"]
                main_engine_fuel  = port_lkgs.loc[i, "Supramax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Supramax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Supramax – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                rt_dist_main_fuel = port_lkgs.loc[i, "Panamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Panamax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Panamax loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Panamax"]
                main_engine_fuel  = port_lkgs.loc[i, "Panamax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Panamax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Panamax – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                rt_dist_main_fuel = port_lkgs.loc[i, "NeoPanamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "NeoPanamax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "NeoPanamax loading rate"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate NeoPanamax"]
                main_engine_fuel  = port_lkgs.loc[i, "NeoPanamax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "NeoPanamax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "NeoPanamax – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                rt_dist_main_fuel = port_lkgs.loc[i, "Suezmax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Suezmax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Suezmax loading rate"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Suezmax"]
                main_engine_fuel  = port_lkgs.loc[i, "Suezmax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Suezmax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Suezmax – Applicable Time Charter Index"]
            
            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                rt_dist_main_fuel = port_lkgs.loc[i, "Capesize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Capesize – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Capesize loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Capesize"]
                main_engine_fuel  = port_lkgs.loc[i, "Capesize – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Capesize – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Capesize – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                rt_dist_main_fuel = port_lkgs.loc[i, "VLOC – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "VLOC – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "VLOC loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate VLOC"]
                main_engine_fuel  = port_lkgs.loc[i, "VLOC – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "VLOC – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "VLOC – Applicable Time Charter Index"]
            else:
                rt_dist_main_fuel = None

            for j in range(len(fuel_prices_lookup)):
                if main_engine_fuel == fuel_prices_lookup[j]:
                    fuel_lookup_col = j
                    break
                else:
                    fuel_lookup_col = None
            for j in range(len(fuel_prices_lookup)):
                if auxiliary_fuel == fuel_prices_lookup[j]:
                    aux_fuel_lookup = j
                    break
                else:
                    aux_fuel_lookup = None
            for j in range(len(fx_rates_lookup)):
                if imp_port_dets.loc[i, "Currency"] == fx_rates_lookup[j]:
                    fxrate_lookup = j
                

            new_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * glb_facs.loc[i, "legs per round trip"]
            new_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * glb_facs.loc[i, "legs per round trip"]
            new_df.at[i, "Vessel Loading rate"]                    = vl_loading
            new_df.at[i, "Vessel Un-Loading rate"]                 = vl_unloading
            new_df.at[i, "Vessel Speed"]                           = lookup(self.trade_details.loc[i, "Date"].date(), frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (new_df.loc[i, "Vessel Speed"] ** 2) + glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (new_df.loc[i, "Vessel Speed"] ** 3) + glb_facs.loc[i, "Main engine burn speed linear coefficient"] * new_df.loc[i, "Vessel Speed"])
            v2 = ((frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * glb_facs.loc[i, "Main engine burn overall correction factor"] + (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            new_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            new_df.at[i, "MDO / MGO burn rate"]                    = (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            new_df.at[i, "Days at Avg Speed Main Fuel"]            = new_df.loc[i, "Round trip distance Main fuel"] / new_df.loc[i, "Vessel Speed"] / glb_facs.loc[i, "hours per day"]
            new_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = new_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  new_df.loc[i, "Vessel Speed"] / glb_facs.loc[i, "hours per day"]
            new_df.at[i, "Days Loading"]                           = (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - fnl_specs.loc[i, "Moisture"]) / new_df.loc[i, "Vessel Loading rate"]
            new_df.at[i, "Days unloading"]                         = (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - fnl_specs.loc[i, "Moisture"]) / new_df.loc[i, "Vessel Un-Loading rate"]
            new_df.at[i, "Extra Days due to Canal use"]            = canals_detls.loc[i, "Days delay"] * glb_facs.loc[i, "legs per round trip"]
            new_df.at[i, "Lay days allowance (50% each port)"]     = max([(new_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),glb_facs.loc[i, "Minimum Lay days allowed"] ])
            new_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = fuel_prices.iloc[i, fuel_lookup_col]
            new_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = fuel_prices.iloc[i, aux_fuel_lookup]
            new_df.at[i, "Main Fuel Cost"]      = new_df.loc[i, "Days at Avg Speed Main Fuel"] * new_df.loc[i, "Main Engine Fuel burn rate"] * new_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            new_df.at[i, "Auxiliary Fuel Cost"] = new_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * new_df.loc[i, "MDO / MGO burn rate"] * new_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + new_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * new_df.loc[i, "Main Engine Fuel burn rate"] * new_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            new_df.at[i, "FX rate per USD"]     = fxrates.iloc[i, fxrate_lookup]
            new_df.at[i, "Fixed fee"]        = imp_port_dets.loc[i, "RMB Fixed fee"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Berthing Charge"]  = imp_port_dets.loc[i, "RMB per day berthed"] * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Anchorage Charge"] = imp_port_dets.loc[i, "RMB per day anchored"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Per T_Cargo (wet) Charge"]       = imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (frieght_calcs.loc[i, "Vessel dwt"] *  glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (frieght_calcs.loc[i, "Vessel dwt"] *  glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (frieght_calcs.loc[i, "Vessel dwt"] *  glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/NRT"] = imp_port_dets.loc[i, "RMB/NRT"] * frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/NRT/day berthed"]  = imp_port_dets.loc[i, "RMB/NRT/day berthed"] * frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/NRT/day anchored"] = imp_port_dets.loc[i, "RMB/NRT/day anchored"] * frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/GRT"] = imp_port_dets.loc[i, "RMB/GRT"] * frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/GRT/day berthed"]  = imp_port_dets.loc[i, "RMB/GRT/day berthed"] * frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/GRT/day anchored"] = imp_port_dets.loc[i, "RMB/GRT/day anchored"] * frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/LOA"] = imp_port_dets.loc[i, "RMB/LOA"] * frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/LOA/day berthed"]  = imp_port_dets.loc[i, "RMB/LOA/day berthed"] * frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/LOA/day anchored"] = imp_port_dets.loc[i, "RMB/LOA/day anchored"] * frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Time Charter Rate"] = lookup1(charter_ind, self.trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            new_df.at[i, "Time Charter Cost"] = sum([new_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), glb_facs.loc[i, "legs per round trip"]*glb_facs.loc[i, "Lay allowance Loading port"]]) * new_df.loc[i, "Time Charter Rate"]
            
            sum1 = np.array(canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            new_df.at[i, "Canal Costs"] = sum(sum1) + sum(sum2)
            new_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = new_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + new_df.loc[i, "Time Charter Cost"] + new_df.loc[i, "Canal Costs"] + new_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            new_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = new_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+glb_facs.loc[i, "Freight Insurance Rate"]) * (1+glb_facs.loc[i, "Freight Commission"])
            new_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(frieght_calcs.loc[i, "Vessel dwt"] * (1 - fnl_specs.loc[i, "Moisture"]) * glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        #freights
        for i in range(len(self.freights_trade_details)):
            if freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Handysize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Handysize – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Handysize"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Handysize"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Handysize – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Handysize – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Handysize – Applicable Time Charter Index"]
            
            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Supramax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Supramax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Supramax loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Supramax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Supramax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Supramax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Supramax – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Panamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Panamax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Panamax loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Panamax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Panamax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Panamax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Panamax – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "NeoPanamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "NeoPanamax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "NeoPanamax loading rate"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate NeoPanamax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "NeoPanamax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "NeoPanamax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "NeoPanamax – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Suezmax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Suezmax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Suezmax loading rate"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Suezmax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Suezmax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Suezmax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Suezmax – Applicable Time Charter Index"]
            
            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Capesize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Capesize – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Capesize loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Capesize"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Capesize – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Capesize – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Capesize – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "VLOC – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "VLOC – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "VLOC loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate VLOC"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "VLOC – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "VLOC – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "VLOC – Applicable Time Charter Index"]
            else:
                rt_dist_main_fuel = None

            for j in range(len(fuel_prices_lookup)):
                if main_engine_fuel == fuel_prices_lookup[j]:
                    fuel_lookup_col = j
                    break
                else:
                    fuel_lookup_col = None
            for j in range(len(fuel_prices_lookup)):
                if auxiliary_fuel == fuel_prices_lookup[j]:
                    aux_fuel_lookup = j
                    break
                else:
                    aux_fuel_lookup = None
            for j in range(len(fx_rates_lookup)):
                if freights_imp_port_dets.loc[i, "Currency"] == fx_rates_lookup[j]:
                    fxrate_lookup = j
                

            freights_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * freights_glb_facs.loc[i, "legs per round trip"]
            freights_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * freights_glb_facs.loc[i, "legs per round trip"]
            freights_df.at[i, "Vessel Loading rate"]                    = vl_loading
            freights_df.at[i, "Vessel Un-Loading rate"]                 = vl_unloading
            freights_df.at[i, "Vessel Speed"]                           = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (freights_glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (freights_df.loc[i, "Vessel Speed"] ** 2) + freights_glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (freights_df.loc[i, "Vessel Speed"] ** 3) + freights_glb_facs.loc[i, "Main engine burn speed linear coefficient"] * freights_df.loc[i, "Vessel Speed"])
            v2 = ((freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** freights_glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * freights_glb_facs.loc[i, "Main engine burn overall correction factor"] + (freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * freights_glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + freights_glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            freights_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            freights_df.at[i, "MDO / MGO burn rate"]                    = (freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            freights_df.at[i, "Days at Avg Speed Main Fuel"]            = freights_df.loc[i, "Round trip distance Main fuel"] / freights_df.loc[i, "Vessel Speed"] / freights_glb_facs.loc[i, "hours per day"]
            freights_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = freights_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  freights_df.loc[i, "Vessel Speed"] / freights_glb_facs.loc[i, "hours per day"]
            freights_df.at[i, "Days Loading"]                           = (freights_fnl_specs.loc[i, "Cargo Tonnage"] if freights_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else freights_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - freights_fnl_specs.loc[i, "Moisture"]) / freights_df.loc[i, "Vessel Loading rate"]
            freights_df.at[i, "Days unloading"]                         = (freights_fnl_specs.loc[i, "Cargo Tonnage"] if freights_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else freights_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - freights_fnl_specs.loc[i, "Moisture"]) / freights_df.loc[i, "Vessel Un-Loading rate"]
            freights_df.at[i, "Extra Days due to Canal use"]            = freights_canals_detls.loc[i, "Days delay"] * freights_glb_facs.loc[i, "legs per round trip"]
            freights_df.at[i, "Lay days allowance (50% each port)"]     = max([(freights_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * freights_glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),freights_glb_facs.loc[i, "Minimum Lay days allowed"] ])
            freights_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = freights_fuel_prices.iloc[i, fuel_lookup_col]
            freights_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = freights_fuel_prices.iloc[i, aux_fuel_lookup]
            freights_df.at[i, "Main Fuel Cost"]      = freights_df.loc[i, "Days at Avg Speed Main Fuel"] * freights_df.loc[i, "Main Engine Fuel burn rate"] * freights_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            freights_df.at[i, "Auxiliary Fuel Cost"] = freights_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * freights_df.loc[i, "MDO / MGO burn rate"] * freights_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + freights_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * freights_df.loc[i, "Main Engine Fuel burn rate"] * freights_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            freights_df.at[i, "FX rate per USD"]     = freights_fxrates.iloc[i, fxrate_lookup]
            freights_df.at[i, "Fixed fee"]        = freights_imp_port_dets.loc[i, "RMB Fixed fee"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Berthing Charge"]  = freights_imp_port_dets.loc[i, "RMB per day berthed"] * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Anchorage Charge"] = freights_imp_port_dets.loc[i, "RMB per day anchored"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Per T_Cargo (wet) Charge"]       = freights_imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] *  freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = freights_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] *  freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = freights_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] *  freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/NRT"] = freights_imp_port_dets.loc[i, "RMB/NRT"] * freights_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/NRT/day berthed"]  = freights_imp_port_dets.loc[i, "RMB/NRT/day berthed"] * freights_frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/NRT/day anchored"] = freights_imp_port_dets.loc[i, "RMB/NRT/day anchored"] * freights_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/GRT"] = freights_imp_port_dets.loc[i, "RMB/GRT"] * freights_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/GRT/day berthed"]  = freights_imp_port_dets.loc[i, "RMB/GRT/day berthed"] * freights_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/GRT/day anchored"] = freights_imp_port_dets.loc[i, "RMB/GRT/day anchored"] * freights_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/LOA"] = freights_imp_port_dets.loc[i, "RMB/LOA"] * freights_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/LOA/day berthed"]  = freights_imp_port_dets.loc[i, "RMB/LOA/day berthed"] * freights_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/LOA/day anchored"] = freights_imp_port_dets.loc[i, "RMB/LOA/day anchored"] * freights_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Time Charter Rate"] = lookup1(charter_ind, self.freights_trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            freights_df.at[i, "Time Charter Cost"] = sum([freights_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), freights_glb_facs.loc[i, "legs per round trip"]*freights_glb_facs.loc[i, "Lay allowance Loading port"]]) * freights_df.loc[i, "Time Charter Rate"]
            
            sum1 = np.array(freights_canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(freights_canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(freights_canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(freights_canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            freights_df.at[i, "Canal Costs"] = sum(sum1) + sum(sum2)
            freights_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = freights_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + freights_df.loc[i, "Time Charter Cost"] + freights_df.loc[i, "Canal Costs"] + freights_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            freights_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = freights_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+freights_glb_facs.loc[i, "Freight Insurance Rate"])
            freights_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if freights_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else freights_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(freights_frieght_calcs.loc[i, "Vessel dwt"] * (1 - freights_fnl_specs.loc[i, "Moisture"]) * freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])
            
        #cbix co-efficients
        for i in range(len(self.cbix_cf_trade_details)):
            if cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Handysize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Handysize – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Handysize"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Handysize"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Handysize – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Handysize – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Handysize – Applicable Time Charter Index"]
            
            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Supramax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Supramax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Supramax loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Supramax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Supramax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Supramax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Supramax – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Panamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Panamax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Panamax loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Panamax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Panamax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Panamax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Panamax – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "NeoPanamax loading rate"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate NeoPanamax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Suezmax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Suezmax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Suezmax loading rate"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Suezmax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Suezmax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Suezmax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Suezmax – Applicable Time Charter Index"]
            
            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Capesize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Capesize – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Capesize loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Capesize"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Capesize – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Capesize – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Capesize – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "VLOC – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "VLOC – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "VLOC loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate VLOC"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "VLOC – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "VLOC – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "VLOC – Applicable Time Charter Index"]
            else:
                rt_dist_main_fuel = None

            for j in range(len(fuel_prices_lookup)):
                if main_engine_fuel == fuel_prices_lookup[j]:
                    fuel_lookup_col = j
                    break
                else:
                    fuel_lookup_col = None
            for j in range(len(fuel_prices_lookup)):
                if auxiliary_fuel == fuel_prices_lookup[j]:
                    aux_fuel_lookup = j
                    break
                else:
                    aux_fuel_lookup = None
            for j in range(len(fx_rates_lookup)):
                if cbix_coe_imp_port_dets.loc[i, "Currency"] == fx_rates_lookup[j]:
                    fxrate_lookup = j
                

            cbix_coe_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_coe_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_coe_df.at[i, "Vessel Loading rate"]                    = vl_loading
            cbix_coe_df.at[i, "Vessel Un-Loading rate"]                 = vl_unloading
            cbix_coe_df.at[i, "Vessel Speed"]                           = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (cbix_coe_glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (cbix_coe_df.loc[i, "Vessel Speed"] ** 2) + cbix_coe_glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (cbix_coe_df.loc[i, "Vessel Speed"] ** 3) + cbix_coe_glb_facs.loc[i, "Main engine burn speed linear coefficient"] * cbix_coe_df.loc[i, "Vessel Speed"])
            v2 = ((cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * cbix_coe_glb_facs.loc[i, "Main engine burn overall correction factor"] + (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            cbix_coe_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            cbix_coe_df.at[i, "MDO / MGO burn rate"]                    = (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            cbix_coe_df.at[i, "Days at Avg Speed Main Fuel"]            = cbix_coe_df.loc[i, "Round trip distance Main fuel"] / cbix_coe_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_coe_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = cbix_coe_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  cbix_coe_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_coe_df.at[i, "Days Loading"]                           = (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_coe_df.loc[i, "Vessel Loading rate"]
            cbix_coe_df.at[i, "Days unloading"]                         = (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_coe_df.loc[i, "Vessel Un-Loading rate"]
            cbix_coe_df.at[i, "Extra Days due to Canal use"]            = cbix_coe_canals_detls.loc[i, "Days delay"] * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_coe_df.at[i, "Lay days allowance (50% each port)"]     = max([(cbix_coe_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed"] ])
            cbix_coe_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = cbix_coe_fuel_prices.iloc[i, fuel_lookup_col]
            cbix_coe_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = cbix_coe_fuel_prices.iloc[i, aux_fuel_lookup]
            cbix_coe_df.at[i, "Main Fuel Cost"]      = cbix_coe_df.loc[i, "Days at Avg Speed Main Fuel"] * cbix_coe_df.loc[i, "Main Engine Fuel burn rate"] * cbix_coe_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            cbix_coe_df.at[i, "Auxiliary Fuel Cost"] = cbix_coe_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * cbix_coe_df.loc[i, "MDO / MGO burn rate"] * cbix_coe_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + cbix_coe_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * cbix_coe_df.loc[i, "Main Engine Fuel burn rate"] * cbix_coe_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            cbix_coe_df.at[i, "FX rate per USD"]     = cbix_coe_fxrates.iloc[i, fxrate_lookup]
            cbix_coe_df.at[i, "Fixed fee"]        = cbix_coe_imp_port_dets.loc[i, "RMB Fixed fee"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Berthing Charge"]  = cbix_coe_imp_port_dets.loc[i, "RMB per day berthed"] * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Anchorage Charge"] = cbix_coe_imp_port_dets.loc[i, "RMB per day anchored"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Per T_Cargo (wet) Charge"]       = cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/NRT"] = cbix_coe_imp_port_dets.loc[i, "RMB/NRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/NRT/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/NRT/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/GRT"] = cbix_coe_imp_port_dets.loc[i, "RMB/GRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/GRT/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/GRT/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/LOA"] = cbix_coe_imp_port_dets.loc[i, "RMB/LOA"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/LOA/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/LOA/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Time Charter Rate"] = lookup1(charter_ind, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            cbix_coe_df.at[i, "Time Charter Cost"] = sum([cbix_coe_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), cbix_coe_glb_facs.loc[i, "legs per round trip"]*cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"]]) * cbix_coe_df.loc[i, "Time Charter Rate"]
            
            sum1 = np.array(cbix_coe_canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(cbix_coe_canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            cbix_coe_df.at[i, "Canal Costs"] = sum(sum1) + sum(sum2)
            cbix_coe_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = cbix_coe_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + cbix_coe_df.loc[i, "Time Charter Cost"] + cbix_coe_df.loc[i, "Canal Costs"] + cbix_coe_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            cbix_coe_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = cbix_coe_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+cbix_coe_glb_facs.loc[i, "Freight Insurance Rate"])
            cbix_coe_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if cbix_coe_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else cbix_coe_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] * (1 - cbix_coe_fnl_specs.loc[i, "Moisture"]) * cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/final_costing_up_for_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/final_costing_up_for_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/final_costing_up_for_leg.csv"] = cbix_coe_df
        

    def freight_calcualtions_actual_port_second_leg(self, folder=None):
        fnl_specs     = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fin_spec = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        glb_facs      = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]
        freights_glb_factors  = self.db["outputs/freights/global_factors.csv"]
        cbix_coe_glb_factors  = self.db["outputs/cbix_co_efficients_determination/global_factors.csv"]

        new_df      = pd.DataFrame(columns=self.frieght_calcs_columns)
        freights_df = pd.DataFrame(columns=self.frieght_calcs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.frieght_calcs_columns)

        def lookup(search, target_check, target):
            for ind in range(target_check.shape[0]):
                filt = target[target_check.iloc[:] <= search]
                if filt is None:
                     filt = target[target_check.iloc[:] >= search]
                     return filt.iloc[0]
                else:
                    return filt.iloc[-1]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Vessel dwt"]  = (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1 - fnl_specs.loc[i,"Moisture"])/ glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            new_df.at[i, "Vessel Class for timecharter & fuel burn rates"]  = lookup(new_df.loc[i, "Vessel dwt"], self.sheetname_class.loc[:, "DWT< tonnes"], self.sheetname_class.loc[:, "Class"])
            new_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(new_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            new_df.at[i, "Estimated LOA (length over all) (m)"] = new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "LOA estimate correlation multiplier"] + glb_facs.loc[i, "LOA estimate correlation constant"]
            new_df.at[i, "Estimated NRT (net register tons)"]   = new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "NRT estimate correlation multiplier"] + glb_facs.loc[i, "NRT estimate correlation constant"]
            new_df.at[i, "Estimated GRT (gross register tons)"] = new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "GRT estimate correlation multiplier"] + glb_facs.loc[i, "GRT estimate correlation constant"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Vessel dwt"]  = (freights_fin_spec.loc[i, "Cargo Tonnage"] if freights_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else freights_fin_spec.loc[i, "South america special transloading cargo tonnage"]) / (1 - freights_fin_spec.loc[i,"Moisture"])/ freights_glb_factors.loc[i, "Max % of vessel deadweight allowed for loading"]
            freights_df.at[i, "Vessel Class for timecharter & fuel burn rates"]  = lookup(freights_df.loc[i, "Vessel dwt"], self.sheetname_class.loc[:, "DWT< tonnes"], self.sheetname_class.loc[:, "Class"])
            freights_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(freights_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            freights_df.at[i, "Estimated LOA (length over all) (m)"] = freights_df.loc[i, "Vessel dwt"] * freights_glb_factors.loc[i, "LOA estimate correlation multiplier"] + freights_glb_factors.loc[i, "LOA estimate correlation constant"]
            freights_df.at[i, "Estimated NRT (net register tons)"]   = freights_df.loc[i, "Vessel dwt"] * freights_glb_factors.loc[i, "NRT estimate correlation multiplier"] + freights_glb_factors.loc[i, "NRT estimate correlation constant"]
            freights_df.at[i, "Estimated GRT (gross register tons)"] = freights_df.loc[i, "Vessel dwt"] * freights_glb_factors.loc[i, "GRT estimate correlation multiplier"] + freights_glb_factors.loc[i, "GRT estimate correlation constant"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Vessel dwt"]  = (cbix_coe_fin_spec.loc[i, "Cargo Tonnage"] if cbix_coe_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fin_spec.loc[i, "South america special transloading cargo tonnage"]) / (1 - cbix_coe_fin_spec.loc[i,"Moisture"])/ cbix_coe_glb_factors.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_coe_df.at[i, "Vessel Class for timecharter & fuel burn rates"]  = lookup(cbix_coe_df.loc[i, "Vessel dwt"], self.sheetname_class.loc[:, "DWT< tonnes"], self.sheetname_class.loc[:, "Class"])
            cbix_coe_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(cbix_coe_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            cbix_coe_df.at[i, "Estimated LOA (length over all) (m)"] = cbix_coe_df.loc[i, "Vessel dwt"] * cbix_coe_glb_factors.loc[i, "LOA estimate correlation multiplier"] + cbix_coe_glb_factors.loc[i, "LOA estimate correlation constant"]
            cbix_coe_df.at[i, "Estimated NRT (net register tons)"]   = cbix_coe_df.loc[i, "Vessel dwt"] * cbix_coe_glb_factors.loc[i, "NRT estimate correlation multiplier"] + cbix_coe_glb_factors.loc[i, "NRT estimate correlation constant"]
            cbix_coe_df.at[i, "Estimated GRT (gross register tons)"] = cbix_coe_df.loc[i, "Vessel dwt"] * cbix_coe_glb_factors.loc[i, "GRT estimate correlation multiplier"] + cbix_coe_glb_factors.loc[i, "GRT estimate correlation constant"]

        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/freight_calcualtions_actual_port_second_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/freight_calcualtions_actual_port_second_leg.csv"] = cbix_coe_df

    def exporting_port_details_loading_rate_second_leg(self, folder=None):
        final_spec        = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fin_spec = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame(columns=self.exporting_port_dts_cols)
        freights_df = pd.DataFrame(columns=self.exporting_port_dts_cols)
        cbix_coe_df = pd.DataFrame(columns=self.exporting_port_dts_cols)

        def lookup(search1, search2, target):            
            ind = (self.indexes_mines.loc[:, "Date"].map(lambda x: x.date()) <= search1)
            idx = (self.indexes_mines.loc[:, "Exporting Port"].map(lambda x: x == search2))
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), final_spec.loc[i, "South America special transloading Port"], self.indexes_mines.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_fin_spec.loc[i, "South America special transloading Port"], self.indexes_mines.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_fin_spec.loc[i, "South America special transloading Port"], self.indexes_mines.loc[:, col])

        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details_loading_rate_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/exporting_port_details_loading_rate_second_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/exporting_port_details_loading_rate_second_leg.csv"] = cbix_coe_df

    def port_linkages_funct_second_leg(self, folder=None):
        fn_spec           = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fin_spec = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]
        
        new_df      = pd.DataFrame(columns=self.port_linkages_cols)
        freights_df = pd.DataFrame(columns=self.port_linkages_cols)
        cbix_coe_df = pd.DataFrame(columns=self.port_linkages_cols)
        
        def lookup(search1, search2, search3, target):
            for j in range(target.shape[0]):
                if (self.port_linkages.loc[j, "Exporting Port"] == search1) and (self.port_linkages.loc[j, "Importing Port"] == search2) and (self.port_linkages.loc[j, "Date"].date() <= search3):
                    return target.iloc[j]
            return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(
                    fn_spec.loc[i, "South America special transloading Port"], 
                    (fn_spec.loc[i, "South America special transloading Port"] if fn_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else fn_spec.loc[i, "Importing Port"]), 
                    self.trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                freights_df.at[i, col] = lookup(
                    freights_fin_spec.loc[i, "South America special transloading Port"], 
                    (freights_fin_spec.loc[i, "South America special transloading Port"] if freights_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else freights_fin_spec.loc[i, "Importing Port"]), 
                    self.freights_trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                cbix_coe_df.at[i, col] = lookup(
                    cbix_coe_fin_spec.loc[i, "South America special transloading Port"], 
                    (cbix_coe_fin_spec.loc[i, "South America special transloading Port"] if cbix_coe_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fin_spec.loc[i, "Importing Port"]), 
                    self.cbix_cf_trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages_second_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages_second_leg.csv"] = cbix_coe_df
        
    def fuel_prices_func_second_leg(self, folder=None):
        port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.csv"]
        freights_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages_second_leg.csv"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages_second_leg.csv"]

        frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.csv"]
        freights_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/freight_calcualtions_actual_port_second_leg.csv"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/freight_calcualtions_actual_port_second_leg.csv"]

        new_df      = pd.DataFrame(columns=self.fuel_prices_cols)
        freights_df = pd.DataFrame(columns=self.fuel_prices_cols)
        cbix_coe_df = pd.DataFrame(columns=self.fuel_prices_cols)
        
        def lookup(search1, search2, target):
            ind = (self.ship_fuel_prices.loc[:, "Fuel Region"] == search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.ship_fuel_prices.loc[:, "Date"].map(lambda x: x.date()) <= search2) #.map(lambda i: 1 if i == True else 0)            
            v =  (ind & idx)
            try:
                return target[v].tolist()[-1]
            except Exception:
                return 0


        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = port_lkgs.loc[i, "Handysize – Applicable Fuel Region"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = port_lkgs.loc[i, "Supramax – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = port_lkgs.loc[i, "Panamax – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = port_lkgs.loc[i, "NeoPanamax – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = port_lkgs.loc[i, "Suezmax – Applicable Fuel Region"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = port_lkgs.loc[i, "Capesize – Applicable Fuel Region"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = port_lkgs.loc[i, "VLOC – Applicable Fuel Region"]
                else:
                    v = None

                new_df.at[i, col] = lookup(v, self.trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = freights_port_lkgs.loc[i, "Handysize – Applicable Fuel Region"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = freights_port_lkgs.loc[i, "Supramax – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = freights_port_lkgs.loc[i, "Panamax – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = freights_port_lkgs.loc[i, "NeoPanamax – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = freights_port_lkgs.loc[i, "Suezmax – Applicable Fuel Region"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = freights_port_lkgs.loc[i, "Capesize – Applicable Fuel Region"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = freights_port_lkgs.loc[i, "VLOC – Applicable Fuel Region"]
                else:
                    v = None

                freights_df.at[i, col] = lookup(v, self.freights_trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = cbix_coe_port_lkgs.loc[i, "Handysize – Applicable Fuel Region"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = cbix_coe_port_lkgs.loc[i, "Supramax – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = cbix_coe_port_lkgs.loc[i, "Panamax – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = cbix_coe_port_lkgs.loc[i, "Suezmax – Applicable Fuel Region"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = cbix_coe_port_lkgs.loc[i, "Capesize – Applicable Fuel Region"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = cbix_coe_port_lkgs.loc[i, "VLOC – Applicable Fuel Region"]
                else:
                    v = None

                cbix_coe_df.at[i, col] = lookup(v, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )

        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/fuel_prices_second_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/fuel_prices_second_leg.csv"] = cbix_coe_df
        
    def importing_port_details_second_leg(self, folder=None):
        final_spec = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fin_spec = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fin_spec = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame(columns=self.importing_port_det_cols)
        freights_df = pd.DataFrame(columns=self.importing_port_det_cols)
        cbix_coe_df = pd.DataFrame(columns=self.importing_port_det_cols)

        def lookup(search1, search2, target):
            idx = (self.china_imp_prts.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.china_imp_prts.loc[:, "Port"] == search2)
            v = (ind & idx)
            try:
                return target[v].iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if final_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No":
                    val = final_spec.loc[i, "South America special transloading Port"]
                else:
                    val = final_spec.loc[i, "Importing Port"]

                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if freights_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No":
                    val = freights_fin_spec.loc[i, "South America special transloading Port"]
                else:
                    val = freights_fin_spec.loc[i, "Importing Port"]

                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if cbix_coe_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No":
                    val = cbix_coe_fin_spec.loc[i, "South America special transloading Port"]
                else:
                    val = cbix_coe_fin_spec.loc[i, "Importing Port"]

                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/importing_port_details_second_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/importing_port_details_second_leg.csv"] = cbix_coe_df
        
    def canals_details_second_leg(self, folder=None):
        port_lkgs              = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.csv"]
        freights_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages_second_leg.csv"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages_second_leg.csv"]
        
        frieght_calcs          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.csv"]
        freights_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/freight_calcualtions_actual_port_second_leg.csv"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/freight_calcualtions_actual_port_second_leg.csv"]
        
        new_df      = pd.DataFrame(columns=self.cost_details_cols)
        freights_df = pd.DataFrame(columns=self.cost_details_cols)
        cbix_coe_df = pd.DataFrame(columns=self.cost_details_cols)

        def lookup(search1, search2, target):
            idx = (self.canals.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.canals.loc[:, "Canal"] == search2)
            v = (ind & idx)
            try:
                return target[v].tolist()[-1]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = port_lkgs.loc[i, "Handysize – Canals used"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = port_lkgs.loc[i, "Supramax – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = port_lkgs.loc[i, "Panamax – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = port_lkgs.loc[i, "NeoPanamax – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = port_lkgs.loc[i, "Suezmax – Canals used"]
                
                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = port_lkgs.loc[i, "Capesize – Canals used"]

                elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = port_lkgs.loc[i, "VLOC – Canals used"]
                else:
                    v = 0
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = freights_port_lkgs.loc[i, "Handysize – Canals used"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = freights_port_lkgs.loc[i, "Supramax – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = freights_port_lkgs.loc[i, "Panamax – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = freights_port_lkgs.loc[i, "NeoPanamax – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = freights_port_lkgs.loc[i, "Suezmax – Canals used"]
                
                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = freights_port_lkgs.loc[i, "Capesize – Canals used"]

                elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = freights_port_lkgs.loc[i, "VLOC – Canals used"]
                else:
                    v = 0
                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                    v = cbix_coe_port_lkgs.loc[i, "Handysize – Canals used"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                    v = cbix_coe_port_lkgs.loc[i, "Supramax – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                    v = cbix_coe_port_lkgs.loc[i, "Panamax – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                    v = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                    v = cbix_coe_port_lkgs.loc[i, "Suezmax – Canals used"]
                
                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                    v = cbix_coe_port_lkgs.loc[i, "Capesize – Canals used"]

                elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                    v = cbix_coe_port_lkgs.loc[i, "VLOC – Canals used"]
                else:
                    v = 0
                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_details_second_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_details_second_leg.csv"] = cbix_coe_df

    def canals_costs_workings_second_leg(self, folder=None):
        canals_details          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details_second_leg.csv"]
        freights_canals_details = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_details_second_leg.csv"]
        cbix_coe_canals_details = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_details_second_leg.csv"]
        
        fc_actual_port          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.csv"]
        freights_fc_actual_port = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/freight_calcualtions_actual_port_second_leg.csv"]
        cbix_coe_fc_actual_port = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/freight_calcualtions_actual_port_second_leg.csv"]
        
        glb_facs                = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]
        freights_glb_facs       = self.db["outputs/common_data_inputs/freights/common_data_inputs_global_factors.csv"]
        cbix_coe_glb_facs       = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_global_factors.csv"]

        new_df = pd.DataFrame(columns=self.canal_cost_workings_cols)
        freights_df = pd.DataFrame(columns=self.canal_cost_workings_cols)
        cbix_coe_df = pd.DataFrame(columns=self.canal_cost_workings_cols)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = fc_actual_port.loc[i, "Vessel dwt"] if canals_details.loc[i, "Tonnage Reference"] == "DWT" else fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if canals_details.loc[i, "Tonnage Reference"] == "NRT" else fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if canals_details.loc[i, "Tonnage Reference"] == "GRT" else 0
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            new_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (new_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = fc_actual_port.loc[i, "Vessel dwt"] * glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            new_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (new_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - new_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = freights_fc_actual_port.loc[i, "Vessel dwt"] if freights_canals_details.loc[i, "Tonnage Reference"] == "DWT" else freights_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if freights_canals_details.loc[i, "Tonnage Reference"] == "NRT" else freights_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if freights_canals_details.loc[i, "Tonnage Reference"] == "GRT" else 0
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            freights_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = freights_canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (freights_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = freights_fc_actual_port.loc[i, "Vessel dwt"] * freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            freights_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = freights_canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > freights_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - freights_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = cbix_coe_fc_actual_port.loc[i, "Vessel dwt"] if cbix_coe_canals_details.loc[i, "Tonnage Reference"] == "DWT" else cbix_coe_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_canals_details.loc[i, "Tonnage Reference"] == "NRT" else cbix_coe_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_canals_details.loc[i, "Tonnage Reference"] == "GRT" else 0
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            cbix_coe_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = cbix_coe_fc_actual_port.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            cbix_coe_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_coe_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_costs_workings_second_leg.csv"] = freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_costs_workings_second_leg.csv"] = cbix_coe_df

    def final_costing_up_for_second_leg(self, folder=None):
        fxrates                = self.db["outputs/common_data_inputs/common_data_inputs_fx_rates.csv"]
        freights_fxrates       = self.db["outputs/common_data_inputs/freights/common_data_inputs_fx_rates.csv"]
        cbix_coe_fxrates       = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_fx_rates.csv"]
        
        glb_facs               = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]
        freights_glb_facs      = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]
        cbix_coe_glb_facs      = self.db["outputs/common_data_inputs/common_data_inputs_global_factors.csv"]
        
        fuel_prices            = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices_second_leg.csv"]
        freights_fuel_prices   = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/fuel_prices_second_leg.csv"]
        cbix_coe_fuel_prices   = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/fuel_prices_second_leg.csv"]
        
        port_lkgs              = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.csv"]
        freights_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/port_linkages_second_leg.csv"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/port_linkages_second_leg.csv"]
        
        canals_detls           = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details_second_leg.csv"]
        freights_canals_detls  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_details_second_leg.csv"]
        cbix_coe_canals_detls  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_details_second_leg.csv"]
        
        canals_costs           = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings_second_leg.csv"]
        freights_canals_costs  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/canals_costs_workings_second_leg.csv"]
        cbix_coe_canals_costs  = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/canals_costs_workings_second_leg.csv"]
        
        imp_port_dets          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details_second_leg.csv"]
        freights_imp_port_dets = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/importing_port_details_second_leg.csv"]
        cbix_coe_imp_port_dets = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/importing_port_details_second_leg.csv"]
        
        loading_rates          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details_loading_rate_second_leg.csv"]
        freights_loading_rates = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/exporting_port_details_loading_rate_second_leg.csv"]
        cbix_coe_loading_rates = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/exporting_port_details_loading_rate_second_leg.csv"]
        
        frieght_calcs          = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.csv"]
        freights_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/freight_calcualtions_actual_port_second_leg.csv"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/freight_calcualtions_actual_port_second_leg.csv"]
        
        fnl_specs              = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_fnl_specs     = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_fnl_specs     = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)
        freights_df = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)
        cbix_coe_df = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)

        fuel_prices_lookup = ["HSFO", "VLSFO", "LNG", "MGO_Regular", "MGO_Low_Sulph"]
        fx_rates_lookup    = ["RMB", "AUD", "USD", "SDR"]

        def lookup(search1, search2, target):
            idx = (self.ship_speeds.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.ship_speeds.loc[:, "Vesssel class"] == search2)
            v = (ind & idx)
            return target[v].tolist()[-1]

        def lookup1(search1, search2, target):
            x = self.ship_time_cr.loc[:, "Applicable Time Charter Index"] == search1
            y = self.ship_time_cr.loc[:, "Date"].map(lambda x: x.date()) <= search2 #.map(lambda i: 1 if i == True else 0)            
            z = self.ship_time_cr.loc[:, "Vessel Time Charter Rates"].map(lambda x: pd.notna(x))

            v =  (x & y & z)
            try:
                return target[v].tolist()[-1]
            except Exception:
                return 0
        
        for i in range(len(self.trade_details)):
            if frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                rt_dist_main_fuel = port_lkgs.loc[i, "Handysize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Handysize – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Handysize"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Handysize"]
                main_engine_fuel  = port_lkgs.loc[i, "Handysize – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Handysize – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Handysize – Applicable Time Charter Index"]
            
            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                rt_dist_main_fuel = port_lkgs.loc[i, "Supramax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Supramax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Supramax loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Supramax"]
                main_engine_fuel  = port_lkgs.loc[i, "Supramax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Supramax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Supramax – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                rt_dist_main_fuel = port_lkgs.loc[i, "Panamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Panamax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Panamax loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Panamax"]
                main_engine_fuel  = port_lkgs.loc[i, "Panamax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Panamax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Panamax – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                rt_dist_main_fuel = port_lkgs.loc[i, "NeoPanamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "NeoPanamax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "NeoPanamax loading rate"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate NeoPanamax"]
                main_engine_fuel  = port_lkgs.loc[i, "NeoPanamax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "NeoPanamax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "NeoPanamax – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                rt_dist_main_fuel = port_lkgs.loc[i, "Suezmax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Suezmax – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Suezmax loading rate"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Suezmax"]
                main_engine_fuel  = port_lkgs.loc[i, "Suezmax – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Suezmax – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Suezmax – Applicable Time Charter Index"]
            
            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                rt_dist_main_fuel = port_lkgs.loc[i, "Capesize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "Capesize – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "Capesize loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate Capesize"]
                main_engine_fuel  = port_lkgs.loc[i, "Capesize – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "Capesize – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "Capesize – Applicable Time Charter Index"]

            elif frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                rt_dist_main_fuel = port_lkgs.loc[i, "VLOC – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = port_lkgs.loc[i, "VLOC – Distance using Auxiliary Fuel"]
                vl_loading        = loading_rates.loc[i, "VLOC loading rates"]
                vl_unloading      = imp_port_dets.loc[i, "Unloading rate VLOC"]
                main_engine_fuel  = port_lkgs.loc[i, "VLOC – Main Engine Fuel"]
                auxiliary_fuel    = port_lkgs.loc[i, "VLOC – Auxiliary Fuel"]
                charter_ind       = port_lkgs.loc[i, "VLOC – Applicable Time Charter Index"]
            else:
                rt_dist_main_fuel = None
                
            for j in range(len(fuel_prices_lookup)):
                if main_engine_fuel == fuel_prices_lookup[j]:
                    fuel_lookup_col = j
                    break
                else:
                    fuel_lookup_col = None

            for j in range(len(fuel_prices_lookup)):
                if auxiliary_fuel == fuel_prices_lookup[j]:
                    aux_fuel_lookup = j
                    break
                else:
                    aux_fuel_lookup = None

            for j in range(len(fx_rates_lookup)):
                if imp_port_dets.loc[i, "Currency"] == fx_rates_lookup[j]:
                    fxrate_lookup = j
                    break
                else:
                    fxrate_lookup = None
                    
            new_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * glb_facs.loc[i, "legs per round trip"]
            new_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * glb_facs.loc[i, "legs per round trip"]
            new_df.at[i, "Vessel Loading rate"]                    = vl_loading
            new_df.at[i, "Vessel Un-Loading rate"]                 = vl_unloading
            new_df.at[i, "Vessel Speed"]                           = lookup(self.trade_details.loc[i, "Date"].date(), frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (new_df.loc[i, "Vessel Speed"] ** 2) + glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (new_df.loc[i, "Vessel Speed"] ** 3) + glb_facs.loc[i, "Main engine burn speed linear coefficient"] * new_df.loc[i, "Vessel Speed"])
            v2 = ((frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * glb_facs.loc[i, "Main engine burn overall correction factor"] + (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            new_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            new_df.at[i, "MDO / MGO burn rate"]                    = (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            new_df.at[i, "Days at Avg Speed Main Fuel"]            = new_df.loc[i, "Round trip distance Main fuel"] / new_df.loc[i, "Vessel Speed"] / glb_facs.loc[i, "hours per day"]
            new_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = new_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  new_df.loc[i, "Vessel Speed"] / glb_facs.loc[i, "hours per day"]
            new_df.at[i, "Days Loading"]                           = 0 if new_df.loc[i, "Vessel Loading rate"] == 0 else (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - fnl_specs.loc[i, "Moisture"]) / new_df.loc[i, "Vessel Loading rate"]
            new_df.at[i, "Days unloading"]                         = 0 if new_df.loc[i, "Vessel Un-Loading rate"] == 0 else (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - fnl_specs.loc[i, "Moisture"]) / new_df.loc[i, "Vessel Un-Loading rate"]
            new_df.at[i, "Extra Days due to Canal use"]            = canals_detls.loc[i, "Days delay"] * glb_facs.loc[i, "legs per round trip"]
            new_df.at[i, "Lay days allowance (50% each port)"]     = 0 if new_df.loc[i, "Vessel Un-Loading rate"] == 0 else max([(new_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),glb_facs.loc[i, "Minimum Lay days allowed"] ])
            new_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = 0 if fuel_lookup_col == None else fuel_prices.iloc[i, fuel_lookup_col]
            new_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = 0 if aux_fuel_lookup == None else fuel_prices.iloc[i, aux_fuel_lookup]
            new_df.at[i, "Main Fuel Cost"]      = new_df.loc[i, "Days at Avg Speed Main Fuel"] * new_df.loc[i, "Main Engine Fuel burn rate"] * new_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            new_df.at[i, "Auxiliary Fuel Cost"] = new_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * new_df.loc[i, "MDO / MGO burn rate"] * new_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + new_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * new_df.loc[i, "Main Engine Fuel burn rate"] * new_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            new_df.at[i, "FX rate per USD"]     = 0 if (new_df.loc[i, "Round trip distance Main fuel"] == 0 or fxrate_lookup == None) else fxrates.iloc[i, fxrate_lookup]            
            new_df.at[i, "Fixed fee"]        = 0 if (imp_port_dets.loc[i, "RMB Fixed fee"] == 0 and new_df.loc[i, "FX rate per USD"] == 0) else imp_port_dets.loc[i, "RMB Fixed fee"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Berthing Charge"]  = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else  imp_port_dets.loc[i, "RMB per day berthed"] * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Anchorage Charge"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB per day anchored"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Per T_Cargo (wet) Charge"]       = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (frieght_calcs.loc[i, "Vessel dwt"] *  glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (frieght_calcs.loc[i, "Vessel dwt"] *  glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (frieght_calcs.loc[i, "Vessel dwt"] *  glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/NRT"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/NRT"] * frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/NRT/day berthed"]  = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/NRT/day berthed"] * frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/NRT/day anchored"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/NRT/day anchored"] * frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/GRT"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/GRT"] * frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/GRT/day berthed"]  = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/GRT/day berthed"] * frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/GRT/day anchored"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/GRT/day anchored"] * frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/LOA"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/LOA"] * frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/LOA/day berthed"]  = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/LOA/day berthed"] * frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([new_df.loc[i, "Days unloading"], glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "RMB/LOA/day anchored"] =0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else imp_port_dets.loc[i, "RMB/LOA/day anchored"] * frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (new_df.loc[i, "Lay days allowance (50% each port)"] * (1 - glb_facs.loc[i, "Lay allowance Loading port"])) / new_df.loc[i, "FX rate per USD"]
            new_df.at[i, "Time Charter Rate"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else lookup1(charter_ind, self.trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            new_df.at[i, "Time Charter Cost"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else sum([new_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), glb_facs.loc[i, "legs per round trip"]*glb_facs.loc[i, "Lay allowance Loading port"]]) * new_df.loc[i, "Time Charter Rate"]            
            sum1 = np.array(canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            new_df.at[i, "Canal Costs"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else sum(sum1) + sum(sum2)
            new_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = 0 if new_df.loc[i, "Round trip distance Main fuel"] == 0 else new_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + new_df.loc[i, "Time Charter Cost"] + new_df.loc[i, "Canal Costs"] + new_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            new_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = new_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+glb_facs.loc[i, "Freight Insurance Rate"]) * (1+glb_facs.loc[i, "Freight Commission"])
            new_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(frieght_calcs.loc[i, "Vessel dwt"] * (1 - fnl_specs.loc[i, "Moisture"]) * glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        for i in range(len(self.freights_trade_details)):
            if freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Handysize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Handysize – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Handysize"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Handysize"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Handysize – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Handysize – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Handysize – Applicable Time Charter Index"]
            
            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Supramax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Supramax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Supramax loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Supramax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Supramax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Supramax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Supramax – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Panamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Panamax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Panamax loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Panamax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Panamax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Panamax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Panamax – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "NeoPanamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "NeoPanamax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "NeoPanamax loading rate"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate NeoPanamax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "NeoPanamax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "NeoPanamax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "NeoPanamax – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Suezmax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Suezmax – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Suezmax loading rate"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Suezmax"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Suezmax – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Suezmax – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Suezmax – Applicable Time Charter Index"]
            
            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "Capesize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "Capesize – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "Capesize loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate Capesize"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "Capesize – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "Capesize – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "Capesize – Applicable Time Charter Index"]

            elif freights_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                rt_dist_main_fuel = freights_port_lkgs.loc[i, "VLOC – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = freights_port_lkgs.loc[i, "VLOC – Distance using Auxiliary Fuel"]
                vl_loading        = freights_loading_rates.loc[i, "VLOC loading rates"]
                vl_unloading      = freights_imp_port_dets.loc[i, "Unloading rate VLOC"]
                main_engine_fuel  = freights_port_lkgs.loc[i, "VLOC – Main Engine Fuel"]
                auxiliary_fuel    = freights_port_lkgs.loc[i, "VLOC – Auxiliary Fuel"]
                charter_ind       = freights_port_lkgs.loc[i, "VLOC – Applicable Time Charter Index"]
            else:
                rt_dist_main_fuel = None
                
            for j in range(len(fuel_prices_lookup)):
                if main_engine_fuel == fuel_prices_lookup[j]:
                    fuel_lookup_col = j
                    break
                else:
                    fuel_lookup_col = None

            for j in range(len(fuel_prices_lookup)):
                if auxiliary_fuel == fuel_prices_lookup[j]:
                    aux_fuel_lookup = j
                    break
                else:
                    aux_fuel_lookup = None

            for j in range(len(fx_rates_lookup)):
                if freights_imp_port_dets.loc[i, "Currency"] == fx_rates_lookup[j]:
                    fxrate_lookup = j
                    break
                else:
                    fxrate_lookup = None
                    
            freights_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * freights_glb_facs.loc[i, "legs per round trip"]
            freights_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * freights_glb_facs.loc[i, "legs per round trip"]
            freights_df.at[i, "Vessel Loading rate"]                    = vl_loading
            freights_df.at[i, "Vessel Un-Loading rate"]                 = vl_unloading
            freights_df.at[i, "Vessel Speed"]                           = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (freights_glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (freights_df.loc[i, "Vessel Speed"] ** 2) + freights_glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (freights_df.loc[i, "Vessel Speed"] ** 3) + freights_glb_facs.loc[i, "Main engine burn speed linear coefficient"] * freights_df.loc[i, "Vessel Speed"])
            v2 = ((freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** freights_glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * freights_glb_facs.loc[i, "Main engine burn overall correction factor"] + (freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * freights_glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + freights_glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            freights_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            freights_df.at[i, "MDO / MGO burn rate"]                    = (freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] / freights_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            freights_df.at[i, "Days at Avg Speed Main Fuel"]            = freights_df.loc[i, "Round trip distance Main fuel"] / freights_df.loc[i, "Vessel Speed"] / freights_glb_facs.loc[i, "hours per day"]
            freights_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = freights_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  freights_df.loc[i, "Vessel Speed"] / freights_glb_facs.loc[i, "hours per day"]
            freights_df.at[i, "Days Loading"]                           = 0 if freights_df.loc[i, "Vessel Loading rate"] == 0 else (freights_fnl_specs.loc[i, "Cargo Tonnage"] if freights_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else freights_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - freights_fnl_specs.loc[i, "Moisture"]) / freights_df.loc[i, "Vessel Loading rate"]
            freights_df.at[i, "Days unloading"]                         = 0 if freights_df.loc[i, "Vessel Un-Loading rate"] == 0 else (freights_fnl_specs.loc[i, "Cargo Tonnage"] if freights_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else freights_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - freights_fnl_specs.loc[i, "Moisture"]) / freights_df.loc[i, "Vessel Un-Loading rate"]
            freights_df.at[i, "Extra Days due to Canal use"]            = freights_canals_detls.loc[i, "Days delay"] * freights_glb_facs.loc[i, "legs per round trip"]
            freights_df.at[i, "Lay days allowance (50% each port)"]     = 0 if freights_df.loc[i, "Vessel Un-Loading rate"] == 0 else max([(freights_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * freights_glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),freights_glb_facs.loc[i, "Minimum Lay days allowed"] ])
            freights_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = 0 if fuel_lookup_col == None else freights_fuel_prices.iloc[i, fuel_lookup_col]
            freights_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = 0 if aux_fuel_lookup == None else freights_fuel_prices.iloc[i, aux_fuel_lookup]
            freights_df.at[i, "Main Fuel Cost"]      = freights_df.loc[i, "Days at Avg Speed Main Fuel"] * freights_df.loc[i, "Main Engine Fuel burn rate"] * freights_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            freights_df.at[i, "Auxiliary Fuel Cost"] = freights_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * freights_df.loc[i, "MDO / MGO burn rate"] * freights_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + freights_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * freights_df.loc[i, "Main Engine Fuel burn rate"] * freights_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            freights_df.at[i, "FX rate per USD"]     = 0 if (freights_df.loc[i, "Round trip distance Main fuel"] == 0 or fxrate_lookup == None) else freights_fxrates.iloc[i, fxrate_lookup]            
            freights_df.at[i, "Fixed fee"]        = 0 if (freights_imp_port_dets.loc[i, "RMB Fixed fee"] == 0 and freights_df.loc[i, "FX rate per USD"] == 0) else freights_imp_port_dets.loc[i, "RMB Fixed fee"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Berthing Charge"]  = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else  freights_imp_port_dets.loc[i, "RMB per day berthed"] * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Anchorage Charge"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB per day anchored"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Per T_Cargo (wet) Charge"]       = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] *  freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] *  freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (freights_frieght_calcs.loc[i, "Vessel dwt"] *  freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/NRT"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/NRT"] * freights_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/NRT/day berthed"]  = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/NRT/day berthed"] * freights_frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/NRT/day anchored"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/NRT/day anchored"] * freights_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/GRT"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/GRT"] * freights_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/GRT/day berthed"]  = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/GRT/day berthed"] * freights_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/GRT/day anchored"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/GRT/day anchored"] * freights_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/LOA"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/LOA"] * freights_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/LOA/day berthed"]  = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/LOA/day berthed"] * freights_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([freights_df.loc[i, "Days unloading"], freights_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "RMB/LOA/day anchored"] =0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_imp_port_dets.loc[i, "RMB/LOA/day anchored"] * freights_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (freights_df.loc[i, "Lay days allowance (50% each port)"] * (1 - freights_glb_facs.loc[i, "Lay allowance Loading port"])) / freights_df.loc[i, "FX rate per USD"]
            freights_df.at[i, "Time Charter Rate"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else lookup1(charter_ind, self.freights_trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            freights_df.at[i, "Time Charter Cost"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else sum([freights_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), freights_glb_facs.loc[i, "legs per round trip"]*freights_glb_facs.loc[i, "Lay allowance Loading port"]]) * freights_df.loc[i, "Time Charter Rate"]            
            sum1 = np.array(freights_canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(freights_canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(freights_canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(freights_canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            freights_df.at[i, "Canal Costs"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else sum(sum1) + sum(sum2)
            freights_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = 0 if freights_df.loc[i, "Round trip distance Main fuel"] == 0 else freights_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + freights_df.loc[i, "Time Charter Cost"] + freights_df.loc[i, "Canal Costs"] + freights_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            freights_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = freights_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+freights_glb_facs.loc[i, "Freight Insurance Rate"])
            freights_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if freights_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else freights_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/ (freights_frieght_calcs.loc[i, "Vessel dwt"] * (1 - freights_fnl_specs.loc[i, "Moisture"]) * freights_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        for i in range(len(self.cbix_cf_trade_details)):
            if cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Handysize":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Handysize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Handysize – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Handysize"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Handysize"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Handysize – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Handysize – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Handysize – Applicable Time Charter Index"]
            
            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Supramax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Supramax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Supramax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Supramax loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Supramax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Supramax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Supramax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Supramax – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Panamax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Panamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Panamax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Panamax loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Panamax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Panamax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Panamax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Panamax – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "NeoPanamax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "NeoPanamax loading rate"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate NeoPanamax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "NeoPanamax – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Suezmax":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Suezmax – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Suezmax – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Suezmax loading rate"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Suezmax"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Suezmax – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Suezmax – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Suezmax – Applicable Time Charter Index"]
            
            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "Capesize":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "Capesize – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "Capesize – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "Capesize loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate Capesize"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "Capesize – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "Capesize – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "Capesize – Applicable Time Charter Index"]

            elif cbix_coe_frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] == "VLOC":
                rt_dist_main_fuel = cbix_coe_port_lkgs.loc[i, "VLOC – Distance using Main Engine Fuel"]
                rt_dist_aux_fuel  = cbix_coe_port_lkgs.loc[i, "VLOC – Distance using Auxiliary Fuel"]
                vl_loading        = cbix_coe_loading_rates.loc[i, "VLOC loading rates"]
                vl_unloading      = cbix_coe_imp_port_dets.loc[i, "Unloading rate VLOC"]
                main_engine_fuel  = cbix_coe_port_lkgs.loc[i, "VLOC – Main Engine Fuel"]
                auxiliary_fuel    = cbix_coe_port_lkgs.loc[i, "VLOC – Auxiliary Fuel"]
                charter_ind       = cbix_coe_port_lkgs.loc[i, "VLOC – Applicable Time Charter Index"]
            else:
                rt_dist_main_fuel = None
                
            for j in range(len(fuel_prices_lookup)):
                if main_engine_fuel == fuel_prices_lookup[j]:
                    fuel_lookup_col = j
                    break
                else:
                    fuel_lookup_col = None

            for j in range(len(fuel_prices_lookup)):
                if auxiliary_fuel == fuel_prices_lookup[j]:
                    aux_fuel_lookup = j
                    break
                else:
                    aux_fuel_lookup = None

            for j in range(len(fx_rates_lookup)):
                if cbix_coe_imp_port_dets.loc[i, "Currency"] == fx_rates_lookup[j]:
                    fxrate_lookup = j
                    break
                else:
                    fxrate_lookup = None
                    
            cbix_coe_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_coe_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_coe_df.at[i, "Vessel Loading rate"]                    = vl_loading
            cbix_coe_df.at[i, "Vessel Un-Loading rate"]                 = vl_unloading
            cbix_coe_df.at[i, "Vessel Speed"]                           = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (cbix_coe_glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (cbix_coe_df.loc[i, "Vessel Speed"] ** 2) + cbix_coe_glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (cbix_coe_df.loc[i, "Vessel Speed"] ** 3) + cbix_coe_glb_facs.loc[i, "Main engine burn speed linear coefficient"] * cbix_coe_df.loc[i, "Vessel Speed"])
            v2 = ((cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * cbix_coe_glb_facs.loc[i, "Main engine burn overall correction factor"] + (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            cbix_coe_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            cbix_coe_df.at[i, "MDO / MGO burn rate"]                    = (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            cbix_coe_df.at[i, "Days at Avg Speed Main Fuel"]            = cbix_coe_df.loc[i, "Round trip distance Main fuel"] / cbix_coe_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_coe_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = cbix_coe_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  cbix_coe_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_coe_df.at[i, "Days Loading"]                           = 0 if cbix_coe_df.loc[i, "Vessel Loading rate"] == 0 else (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_coe_df.loc[i, "Vessel Loading rate"]
            cbix_coe_df.at[i, "Days unloading"]                         = 0 if cbix_coe_df.loc[i, "Vessel Un-Loading rate"] == 0 else (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_coe_df.loc[i, "Vessel Un-Loading rate"]
            cbix_coe_df.at[i, "Extra Days due to Canal use"]            = cbix_coe_canals_detls.loc[i, "Days delay"] * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_coe_df.at[i, "Lay days allowance (50% each port)"]     = 0 if cbix_coe_df.loc[i, "Vessel Un-Loading rate"] == 0 else max([(cbix_coe_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed"] ])
            cbix_coe_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = 0 if fuel_lookup_col == None else cbix_coe_fuel_prices.iloc[i, fuel_lookup_col]
            cbix_coe_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = 0 if aux_fuel_lookup == None else cbix_coe_fuel_prices.iloc[i, aux_fuel_lookup]
            cbix_coe_df.at[i, "Main Fuel Cost"]      = cbix_coe_df.loc[i, "Days at Avg Speed Main Fuel"] * cbix_coe_df.loc[i, "Main Engine Fuel burn rate"] * cbix_coe_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            cbix_coe_df.at[i, "Auxiliary Fuel Cost"] = cbix_coe_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * cbix_coe_df.loc[i, "MDO / MGO burn rate"] * cbix_coe_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + cbix_coe_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * cbix_coe_df.loc[i, "Main Engine Fuel burn rate"] * cbix_coe_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            cbix_coe_df.at[i, "FX rate per USD"]     = 0 if (cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 or fxrate_lookup == None) else cbix_coe_fxrates.iloc[i, fxrate_lookup]            
            cbix_coe_df.at[i, "Fixed fee"]        = 0 if (cbix_coe_imp_port_dets.loc[i, "RMB Fixed fee"] == 0 and cbix_coe_df.loc[i, "FX rate per USD"] == 0) else cbix_coe_imp_port_dets.loc[i, "RMB Fixed fee"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Berthing Charge"]  = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else  cbix_coe_imp_port_dets.loc[i, "RMB per day berthed"] * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Anchorage Charge"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB per day anchored"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Per T_Cargo (wet) Charge"]       = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/NRT"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/NRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/NRT/day berthed"]  = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/NRT/day anchored"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/GRT"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/GRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/GRT/day berthed"]  = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/GRT/day anchored"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/LOA"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/LOA"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/LOA/day berthed"]  = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([cbix_coe_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "RMB/LOA/day anchored"] =0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (cbix_coe_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_coe_df.loc[i, "FX rate per USD"]
            cbix_coe_df.at[i, "Time Charter Rate"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else lookup1(charter_ind, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            cbix_coe_df.at[i, "Time Charter Cost"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else sum([cbix_coe_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), cbix_coe_glb_facs.loc[i, "legs per round trip"]*cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"]]) * cbix_coe_df.loc[i, "Time Charter Rate"]            
            sum1 = np.array(cbix_coe_canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(cbix_coe_canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            cbix_coe_df.at[i, "Canal Costs"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else sum(sum1) + sum(sum2)
            cbix_coe_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = 0 if cbix_coe_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + cbix_coe_df.loc[i, "Time Charter Cost"] + cbix_coe_df.loc[i, "Canal Costs"] + cbix_coe_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            cbix_coe_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = cbix_coe_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+cbix_coe_glb_facs.loc[i, "Freight Insurance Rate"])
            cbix_coe_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if cbix_coe_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else cbix_coe_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] * (1 - cbix_coe_fnl_specs.loc[i, "Moisture"]) * cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/final_costing_up_for_second_leg.csv"] = new_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freights/final_costing_up_for_second_leg.csv"] =freights_df
        self.db[f"outputs/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/cbix_co_efficients_determination/final_costing_up_for_second_leg.csv"] =cbix_coe_df

    def final_freight_values_func(self):
        actual_nominated_port_final_costing_up_for_leg                 = self.db["outputs/frieght_calcs_to_actual_nominated_port/final_costing_up_for_leg.csv"]
        freights_actual_nominated_port_final_costing_up_for_leg        = self.db["outputs/frieght_calcs_to_actual_nominated_port/freights/final_costing_up_for_leg.csv"]
        cbix_coe_actual_nominated_port_final_costing_up_for_leg        = self.db["outputs/frieght_calcs_to_actual_nominated_port/cbix_co_efficients_determination/final_costing_up_for_leg.csv"]
        
        actual_nominated_port_final_costing_up_for_second_leg          = self.db["outputs/frieght_calcs_to_actual_nominated_port/final_costing_up_for_second_leg.csv"]
        freights_actual_nominated_port_final_costing_up_for_second_leg = self.db["outputs/frieght_calcs_to_actual_nominated_port/freights/final_costing_up_for_second_leg.csv"]
        cbix_coe_actual_nominated_port_final_costing_up_for_second_leg = self.db["outputs/frieght_calcs_to_actual_nominated_port/cbix_co_efficients_determination/final_costing_up_for_second_leg.csv"]
        
        qingdao_leg                  = self.db["outputs/freight_calcs_to_qingdao_first_leg/final_costing_up_for_leg.csv"]
        freights_qingdao_leg         = self.db["outputs/freight_calcs_to_qingdao_first_leg/freights/final_costing_up_for_leg.csv"]
        cbix_coe_qingdao_leg         = self.db["outputs/freight_calcs_to_qingdao_first_leg/cbix_co_efficients_determination/final_costing_up_for_leg.csv"]
        
        qingdao_second_leg           = self.db["outputs/freight_calcs_to_qingdao_first_leg/final_costing_up_for_second_leg.csv"]
        freights_qingdao_second_leg  = self.db["outputs/freight_calcs_to_qingdao_first_leg/freights/final_costing_up_for_second_leg.csv"]
        cbix_coe_qingdao_second_leg  = self.db["outputs/freight_calcs_to_qingdao_first_leg/cbix_co_efficients_determination/final_costing_up_for_second_leg.csv"]
        
        new_df      = pd.DataFrame(columns=["Qingdao Freight", "Actual Port Freight", "Freight Differential"])
        freights_df = pd.DataFrame(columns=["Qingdao Freight", "Actual Port Freight", "Freight Differential"])
        cbix_coe_df = pd.DataFrame(columns=["Qingdao Freight", "Actual Port Freight", "Freight Differential"])


        for i in range(len(self.trade_details)):
            new_df.at[i, "Qingdao Freight"] = sum([qingdao_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], qingdao_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            new_df.at[i, "Actual Port Freight"] = sum([actual_nominated_port_final_costing_up_for_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], actual_nominated_port_final_costing_up_for_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            new_df.at[i, "Freight Differential"] = new_df.loc[i, "Qingdao Freight"] - new_df.loc[i, "Actual Port Freight"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Qingdao Freight"] = sum([freights_qingdao_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], freights_qingdao_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            freights_df.at[i, "Actual Port Freight"] = sum([freights_actual_nominated_port_final_costing_up_for_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], freights_actual_nominated_port_final_costing_up_for_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            freights_df.at[i, "Freight Differential"] = freights_df.loc[i, "Qingdao Freight"] - freights_df.loc[i, "Actual Port Freight"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Qingdao Freight"] = sum([cbix_coe_qingdao_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], cbix_coe_qingdao_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            cbix_coe_df.at[i, "Actual Port Freight"] = sum([cbix_coe_actual_nominated_port_final_costing_up_for_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], cbix_coe_actual_nominated_port_final_costing_up_for_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            cbix_coe_df.at[i, "Freight Differential"] = cbix_coe_df.loc[i, "Qingdao Freight"] - cbix_coe_df.loc[i, "Actual Port Freight"]
        
        self.db["outputs/final_freight_values.csv"] = new_df
        self.db["outputs/freights/final_freight_values.csv"] = freights_df
        self.db["outputs/cbix_co_efficients_determination/final_freight_values.csv"] = cbix_coe_df

    def final_specifications_to_viu_adjustment_continued1(self):
        glb_factors                   = self.db["outputs/global_factors.csv"]
        freights_glb_factors          = self.db["outputs/freights/global_factors.csv"]
        cbix_coe_glb_factors          = self.db["outputs/cbix_co_efficients_determination/global_factors.csv"]

        nominal_mine                  = self.db["outputs/nominal_mine_div_index_specifications.csv"]
        freights_nominal_mine         = self.db["outputs/freights/nominal_mine_div_index_specifications.csv"]
        cbix_coe_nominal_mine         = self.db["outputs/cbix_co_efficients_determination/nominal_mine_div_index_specifications.csv"]

        final_freight_values          = self.db["outputs/final_freight_values.csv"]
        freights_final_freight_values = self.db["outputs/freights/final_freight_values.csv"]
        cbix_coe_final_freight_values = self.db["outputs/cbix_co_efficients_determination/final_freight_values.csv"]

        new_df               = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_df          = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_df          = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Freight (adjusted to Qingdao)"] = final_freight_values.loc[i, "Qingdao Freight"]
            new_df.at[i, "Price CIF (Port adjustyed to Qingdao)"] = (self.trade_details.loc[i, "Price"] * (1 if (self.trade_details.loc[i, "Price Type"] == "CIF") else (1+ glb_factors.loc[i, "Freight Insurance Rate"])) + final_freight_values.loc[i, "Freight Differential"]) if ((self.trade_details.loc[i, "Price Type"] < "FOB" or self.trade_details.loc[i, "Price Type"] > "FOB") and self.trade_details.loc[i, "Price Basis"]== "dmt") else self.trade_details.loc[i, "Price"] * (1 if (self.trade_details.loc[i, "Price Type"] == "CIF") else (1+ glb_factors.loc[i, "Freight Insurance Rate"])) / (1-new_df.loc[i, "Moisture"]) if ((self.trade_details.loc[i, "Price Type"] < "FOB" or self.trade_details.loc[i, "Price Type"] > "FOB") and self.trade_details.loc[i, "Price Basis"]== "wmt") else new_df.loc[i, "Price FOB"] + new_df.loc[i, "Freight (adjusted to Qingdao)"]
            new_df.at[i, "Price FOB"] = self.trade_details.loc[i, "Price"] if (self.trade_details.loc[i, "Price Type"]== "FOB" and self.trade_details.loc[i, "Price Basis"]== "dmt") else self.trade_details.loc[i, "Price"] / (1 - new_df.loc[i, "Moisture"]) if (self.trade_details.loc[i, "Price Type"]== "FOB" and self.trade_details.loc[i, "Price Basis"]== "wmt") else new_df.loc[i, "Price CIF (Port adjustyed to Qingdao)"] - new_df.loc[i, "Freight (adjusted to Qingdao)"]
        
        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Freight (adjusted to Qingdao)"] = freights_final_freight_values.loc[i, "Qingdao Freight"]
            freights_df.at[i, "Price CIF (Port adjustyed to Qingdao)"] = (self.freights_trade_details.loc[i, "Price"] * (1 if (self.freights_trade_details.loc[i, "Price Type"] == "CIF") else (1+ freights_glb_factors.loc[i, "Freight Insurance Rate"])) + freights_final_freight_values.loc[i, "Freight Differential"]) if ((self.freights_trade_details.loc[i, "Price Type"] < "FOB" or self.freights_trade_details.loc[i, "Price Type"] > "FOB") and self.freights_trade_details.loc[i, "Price Basis"]== "dmt") else self.freights_trade_details.loc[i, "Price"] * (1 if (self.freights_trade_details.loc[i, "Price Type"] == "CIF") else (1+ freights_glb_factors.loc[i, "Freight Insurance Rate"])) / (1-freights_df.loc[i, "Moisture"]) if ((self.freights_trade_details.loc[i, "Price Type"] < "FOB" or self.freights_trade_details.loc[i, "Price Type"] > "FOB") and self.freights_trade_details.loc[i, "Price Basis"]== "wmt") else freights_df.loc[i, "Price FOB"] + freights_df.loc[i, "Freight (adjusted to Qingdao)"]
            freights_df.at[i, "Price FOB"] = self.freights_trade_details.loc[i, "Price"] if (self.freights_trade_details.loc[i, "Price Type"]== "FOB" and self.freights_trade_details.loc[i, "Price Basis"]== "dmt") else self.freights_trade_details.loc[i, "Price"] / (1 - freights_df.loc[i, "Moisture"]) if (self.freights_trade_details.loc[i, "Price Type"]== "FOB" and self.freights_trade_details.loc[i, "Price Basis"]== "wmt") else freights_df.loc[i, "Price CIF (Port adjustyed to Qingdao)"] - freights_df.loc[i, "Freight (adjusted to Qingdao)"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Freight (adjusted to Qingdao)"] = cbix_coe_final_freight_values.loc[i, "Qingdao Freight"]
            cbix_coe_df.at[i, "Price CIF (Port adjustyed to Qingdao)"] = (self.cbix_cf_trade_details.loc[i, "Price"] * (1 if (self.cbix_cf_trade_details.loc[i, "Price Type"] == "CIF") else (1+ cbix_coe_glb_factors.loc[i, "Freight Insurance Rate"])) + cbix_coe_final_freight_values.loc[i, "Freight Differential"]) if ((self.cbix_cf_trade_details.loc[i, "Price Type"] < "FOB" or self.cbix_cf_trade_details.loc[i, "Price Type"] > "FOB") and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "dmt") else self.cbix_cf_trade_details.loc[i, "Price"] * (1 if (self.cbix_cf_trade_details.loc[i, "Price Type"] == "CIF") else (1+ cbix_coe_glb_factors.loc[i, "Freight Insurance Rate"])) / (1-cbix_coe_df.loc[i, "Moisture"]) if ((self.cbix_cf_trade_details.loc[i, "Price Type"] < "FOB" or self.cbix_cf_trade_details.loc[i, "Price Type"] > "FOB") and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "wmt") else cbix_coe_df.loc[i, "Price FOB"] + cbix_coe_df.loc[i, "Freight (adjusted to Qingdao)"]
            cbix_coe_df.at[i, "Price FOB"] = self.cbix_cf_trade_details.loc[i, "Price"] if (self.cbix_cf_trade_details.loc[i, "Price Type"]== "FOB" and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "dmt") else self.cbix_cf_trade_details.loc[i, "Price"] / (1 - cbix_coe_df.loc[i, "Moisture"]) if (self.cbix_cf_trade_details.loc[i, "Price Type"]== "FOB" and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "wmt") else cbix_coe_df.loc[i, "Price CIF (Port adjustyed to Qingdao)"] - cbix_coe_df.loc[i, "Freight (adjusted to Qingdao)"]

        self.db["outputs/final_specifications_to_viu_adjustment.csv"] = new_df
        self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"] = freights_df
        self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"] = cbix_coe_df


    def bauxite_details_input_func_continued1(self):
        final_specs          = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_final_specs = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_final_specs = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df           = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        freights_df      = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        cbix_coe_df      = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]


        for i in range(len(self.trade_details)):
            new_df.at[i, "Price"] = final_specs.loc[i, "Price CIF (Port adjustyed to Qingdao)"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Price"] = freights_final_specs.loc[i, "Price CIF (Port adjustyed to Qingdao)"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Price"] = cbix_coe_final_specs.loc[i, "Price CIF (Port adjustyed to Qingdao)"]

        
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = cbix_coe_df


    def alumina_production_cost_calcs_ece_continued1(self):
        bx_details          = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        freights_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        cbix_coe_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]

        china_prc           = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        freights_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        cbix_coe_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]

        china_inps          = self.db["outputs/common_data_inputs/common_data_inputs_china_input_prices.csv"]
        freights_china_inps = self.db["outputs/common_data_inputs/freights/common_data_inputs_china_input_prices.csv"]
        cbix_coe_china_inps = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_china_input_prices.csv"]

        new_df          = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]
        freights_df     = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]
        cbix_coe_df     = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (bx_details.loc[i, "Price"] + bx_details.loc[i, "Processing Penalties"]) * new_df.loc[i, "Tonnes per Tonne"]
            new_df.at[i, "Caustic cost"]          = new_df.loc[i, "Caustic Use t.NAOH / t.AA"] * china_inps.loc[i, "Caustic Price"]
            new_df.at[i, "Thermal Energy Cost"]   = china_prc.loc[i, "Lig Coal (GJ/t)"] * china_inps.loc[i, "Energy Price"]
            new_df.at[i, "Lime Cost"]             = china_prc.loc[i, "Lime rate (wt/wt_AA)"] * china_inps.loc[i, "Lime Price"]
            new_df.at[i, "Mud make"]              = (new_df.loc[i, "Tonnes per Tonne"] - 1 ) + new_df.loc[i, "Caustic Use t.NAOH / t.AA"] + china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            new_df.at[i, "Mud Disposal Cost"]     = new_df.loc[i, "Mud make"] * china_inps.loc[i, "Mud Disposal Cost"]
            new_df.at[i, "Total Cost"]            = new_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + new_df.loc[i, "Caustic cost"] + new_df.loc[i, "Thermal Energy Cost"] + new_df.loc[i, "Lime Cost"] + new_df.loc[i, "Mud Disposal Cost"]
        
        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (freights_bx_details.loc[i, "Price"] + freights_bx_details.loc[i, "Processing Penalties"]) * freights_df.loc[i, "Tonnes per Tonne"]
            freights_df.at[i, "Caustic cost"]          = freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] * freights_china_inps.loc[i, "Caustic Price"]
            freights_df.at[i, "Thermal Energy Cost"]   = freights_china_prc.loc[i, "Lig Coal (GJ/t)"] * freights_china_inps.loc[i, "Energy Price"]
            freights_df.at[i, "Lime Cost"]             = freights_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * freights_china_inps.loc[i, "Lime Price"]
            freights_df.at[i, "Mud make"]              = (freights_df.loc[i, "Tonnes per Tonne"] - 1 ) + freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] + freights_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            freights_df.at[i, "Mud Disposal Cost"]     = freights_df.loc[i, "Mud make"] * freights_china_inps.loc[i, "Mud Disposal Cost"]
            freights_df.at[i, "Total Cost"]            = freights_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + freights_df.loc[i, "Caustic cost"] + freights_df.loc[i, "Thermal Energy Cost"] + freights_df.loc[i, "Lime Cost"] + freights_df.loc[i, "Mud Disposal Cost"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_coe_df.loc[i, "Tonnes per Tonne"]
            cbix_coe_df.at[i, "Caustic cost"]          = cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_coe_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_coe_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_coe_df.at[i, "Mud make"]              = (cbix_coe_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_coe_df.at[i, "Mud Disposal Cost"]     = cbix_coe_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_coe_df.at[i, "Total Cost"]            = cbix_coe_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_coe_df.loc[i, "Caustic cost"] + cbix_coe_df.loc[i, "Thermal Energy Cost"] + cbix_coe_df.loc[i, "Lime Cost"] + cbix_coe_df.loc[i, "Mud Disposal Cost"]
        
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"] = cbix_coe_df
        

    #SPECIFIC INDEX

    def spec_index_bauxite_details_func(self):
        final_specs = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_final_specs = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_final_specs = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame()
        freights_df = pd.DataFrame()
        cbix_coe_df = pd.DataFrame()


        def lookup(search1, search2, target):
            ind = (self.indexes_mines.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.indexes_mines.loc[:, "Mine"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            new_df.at[i, "Total Alumina"]             = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Alumina"])
            new_df.at[i, "LT Avail. Alumina"]         = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            new_df.at[i, "Total Silica"]              = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Silica"])
            new_df.at[i, "LT R.Silica"]               = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            new_df.at[i, "Quartz / HT Silica"]        = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            new_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            new_df.at[i, "Moisture"]                  = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Moisture"])
            new_df.at[i, "Processing"]                = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing"])
            new_df.at[i, "Processing Penalties"]      = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            freights_df.at[i, "Total Alumina"]             = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Alumina"])
            freights_df.at[i, "LT Avail. Alumina"]         = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            freights_df.at[i, "Total Silica"]              = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Silica"])
            freights_df.at[i, "LT R.Silica"]               = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            freights_df.at[i, "Quartz / HT Silica"]        = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            freights_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            freights_df.at[i, "Moisture"]                  = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Moisture"])
            freights_df.at[i, "Processing"]                = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing"])
            freights_df.at[i, "Processing Penalties"]      = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            cbix_coe_df.at[i, "Total Alumina"]             = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Alumina"])
            cbix_coe_df.at[i, "LT Avail. Alumina"]         = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            cbix_coe_df.at[i, "Total Silica"]              = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Silica"])
            cbix_coe_df.at[i, "LT R.Silica"]               = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            cbix_coe_df.at[i, "Quartz / HT Silica"]        = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            cbix_coe_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            cbix_coe_df.at[i, "Moisture"]                  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Moisture"])
            cbix_coe_df.at[i, "Processing"]                = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing"])
            cbix_coe_df.at[i, "Processing Penalties"]      = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_index_bauxite_details.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_index_bauxite_details.csv"] = cbix_coe_df

    def spec_index_china_processing_factors_func(self):
        baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.csv"]
        freights_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_index_bauxite_details.csv"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        freights_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

        def lookup(search1, search2, target):
            ind = (self.processing_factors.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.processing_factors.loc[:, "Processing Regime"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_china_processing_factors.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_china_processing_factors.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_china_processing_factors.csv"] = cbix_coe_df

    def spec_index_alumina_production_cost_calcs_ece(self):
        bx_details          = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.csv"]
        freights_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_index_bauxite_details.csv"]
        
        china_prc           = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_china_processing_factors.csv"]
        freights_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_china_processing_factors.csv"]
        cbix_coe_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_china_processing_factors.csv"]
        
        china_inps          = self.db["outputs/common_data_inputs/common_data_inputs_china_input_prices.csv"]
        freights_china_inps = self.db["outputs/common_data_inputs/freights/common_data_inputs_china_input_prices.csv"]
        cbix_coe_china_inps = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_china_input_prices.csv"]
        
        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        freights_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Reactive Alumina"]     =  (bx_details.loc[i, "Total Alumina"] - bx_details.loc[i, "LT Avail. Alumina"] - bx_details.loc[i, "LT R.Silica"]) * china_prc.loc[i, "HT Alumina Dissolution"] + bx_details.loc[i, "LT Avail. Alumina"] + bx_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Reactive Silica"]      =  bx_details.loc[i, "LT R.Silica"] + china_prc.loc[i, "Quartz Attack"] * (bx_details.loc[i, "Total Silica"] - bx_details.loc[i, "LT R.Silica"])
            new_df.at[i, "Available Alumina"]    =  new_df.loc[i, "Reactive Alumina"] - china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * new_df.loc[i, "Reactive Silica"]
            new_df.at[i, "Tonnes per Tonne"]     =  1 / new_df.loc[i, "Available Alumina"] / china_prc.loc[i, "Extraction Efficiency %"]
            new_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (bx_details.loc[i, "Price"] + bx_details.loc[i, "Processing Penalties"]) * new_df.loc[i, "Tonnes per Tonne"]
            new_df.at[i, "Caustic Use t.NAOH / t.AA"] = new_df.loc[i, "Reactive Silica"] * china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * new_df.loc[i, "Tonnes per Tonne"] + china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            new_df.at[i, "Caustic cost"]          = new_df.loc[i, "Caustic Use t.NAOH / t.AA"] * china_inps.loc[i, "Caustic Price"]
            new_df.at[i, "Thermal Energy Cost"]   = china_prc.loc[i, "Lig Coal (GJ/t)"] * china_inps.loc[i, "Energy Price"]
            new_df.at[i, "Lime Cost"]             = china_prc.loc[i, "Lime rate (wt/wt_AA)"] * china_inps.loc[i, "Lime Price"]
            new_df.at[i, "Mud make"]              = (new_df.loc[i, "Tonnes per Tonne"] - 1 ) + new_df.loc[i, "Caustic Use t.NAOH / t.AA"] + china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            new_df.at[i, "Mud Disposal Cost"]     = new_df.loc[i, "Mud make"] * china_inps.loc[i, "Mud Disposal Cost"]
            new_df.at[i, "Total Cost"]            = new_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + new_df.loc[i, "Caustic cost"] + new_df.loc[i, "Thermal Energy Cost"] + new_df.loc[i, "Lime Cost"] + new_df.loc[i, "Mud Disposal Cost"]
        
        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Reactive Alumina"]     =  (freights_bx_details.loc[i, "Total Alumina"] - freights_bx_details.loc[i, "LT Avail. Alumina"] - freights_bx_details.loc[i, "LT R.Silica"]) * freights_china_prc.loc[i, "HT Alumina Dissolution"] + freights_bx_details.loc[i, "LT Avail. Alumina"] + freights_bx_details.loc[i, "LT R.Silica"]
            freights_df.at[i, "Reactive Silica"]      =  freights_bx_details.loc[i, "LT R.Silica"] + freights_china_prc.loc[i, "Quartz Attack"] * (freights_bx_details.loc[i, "Total Silica"] - freights_bx_details.loc[i, "LT R.Silica"])
            freights_df.at[i, "Available Alumina"]    =  freights_df.loc[i, "Reactive Alumina"] - freights_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * freights_df.loc[i, "Reactive Silica"]
            freights_df.at[i, "Tonnes per Tonne"]     =  1 / freights_df.loc[i, "Available Alumina"] / freights_china_prc.loc[i, "Extraction Efficiency %"]
            freights_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (freights_bx_details.loc[i, "Price"] + freights_bx_details.loc[i, "Processing Penalties"]) * freights_df.loc[i, "Tonnes per Tonne"]
            freights_df.at[i, "Caustic Use t.NAOH / t.AA"] = freights_df.loc[i, "Reactive Silica"] * freights_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * freights_df.loc[i, "Tonnes per Tonne"] + freights_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            freights_df.at[i, "Caustic cost"]          = freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] * freights_china_inps.loc[i, "Caustic Price"]
            freights_df.at[i, "Thermal Energy Cost"]   = freights_china_prc.loc[i, "Lig Coal (GJ/t)"] * freights_china_inps.loc[i, "Energy Price"]
            freights_df.at[i, "Lime Cost"]             = freights_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * freights_china_inps.loc[i, "Lime Price"]
            freights_df.at[i, "Mud make"]              = (freights_df.loc[i, "Tonnes per Tonne"] - 1 ) + freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] + freights_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            freights_df.at[i, "Mud Disposal Cost"]     = freights_df.loc[i, "Mud make"] * freights_china_inps.loc[i, "Mud Disposal Cost"]
            freights_df.at[i, "Total Cost"]            = freights_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + freights_df.loc[i, "Caustic cost"] + freights_df.loc[i, "Thermal Energy Cost"] + freights_df.loc[i, "Lime Cost"] + freights_df.loc[i, "Mud Disposal Cost"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_coe_df.at[i, "Available Alumina"]    =  cbix_coe_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Reactive Silica"]
            cbix_coe_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_coe_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_coe_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_coe_df.loc[i, "Tonnes per Tonne"]
            cbix_coe_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_coe_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_coe_df.at[i, "Caustic cost"]          = cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_coe_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_coe_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_coe_df.at[i, "Mud make"]              = (cbix_coe_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_coe_df.at[i, "Mud Disposal Cost"]     = cbix_coe_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_coe_df.at[i, "Total Cost"]            = cbix_coe_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_coe_df.loc[i, "Caustic cost"] + cbix_coe_df.loc[i, "Thermal Energy Cost"] + cbix_coe_df.loc[i, "Lime Cost"] + cbix_coe_df.loc[i, "Mud Disposal Cost"]
        

        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = cbix_coe_df


    #GENERAL INDEX

    def general_index_bauxite_details_func(self):
        final_specs = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_final_specs = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_final_specs = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame()
        freights_df = pd.DataFrame()
        cbix_coe_df = pd.DataFrame()

        def lookup(search1, search2, target):
            ind = (self.indexes_mines.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.indexes_mines.loc[:, "Mine"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            new_df.at[i, "Total Alumina"]             = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Alumina"])
            new_df.at[i, "LT Avail. Alumina"]         = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            new_df.at[i, "Total Silica"]              = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Silica"])
            new_df.at[i, "LT R.Silica"]               = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            new_df.at[i, "Quartz / HT Silica"]        = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            new_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            new_df.at[i, "Moisture"]                  = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Moisture"])
            new_df.at[i, "Processing"]                = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing"])
            new_df.at[i, "Processing Penalties"]      = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            freights_df.at[i, "Total Alumina"]             = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Alumina"])
            freights_df.at[i, "LT Avail. Alumina"]         = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            freights_df.at[i, "Total Silica"]              = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Silica"])
            freights_df.at[i, "LT R.Silica"]               = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            freights_df.at[i, "Quartz / HT Silica"]        = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            freights_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            freights_df.at[i, "Moisture"]                  = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Moisture"])
            freights_df.at[i, "Processing"]                = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing"])
            freights_df.at[i, "Processing Penalties"]      = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            cbix_coe_df.at[i, "Total Alumina"]             = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Alumina"])
            cbix_coe_df.at[i, "LT Avail. Alumina"]         = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            cbix_coe_df.at[i, "Total Silica"]              = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Silica"])
            cbix_coe_df.at[i, "LT R.Silica"]               = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            cbix_coe_df.at[i, "Quartz / HT Silica"]        = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            cbix_coe_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            cbix_coe_df.at[i, "Moisture"]                  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Moisture"])
            cbix_coe_df.at[i, "Processing"]                = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing"])
            cbix_coe_df.at[i, "Processing Penalties"]      = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_index_bauxite_details.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_index_bauxite_details.csv"] = cbix_coe_df

    def general_index_china_processing_factors_func(self):
        baux_dets_inputs          = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.csv"]
        freights_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_index_bauxite_details.csv"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        freights_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

        def lookup(search1, search2, target):
            ind = (self.processing_factors.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.processing_factors.loc[:, "Processing Regime"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_china_processing_factors.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_china_processing_factors.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_china_processing_factors.csv"] = cbix_coe_df

    def general_index_alumina_production_cost_calcs_ece(self):
        bx_details             = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.csv"]
        freights_bx_details    = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_bx_details    = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_index_bauxite_details.csv"]

        china_prc              = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_china_processing_factors.csv"]
        freights_china_prc     = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_china_processing_factors.csv"]
        cbix_coe_china_prc     = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_china_processing_factors.csv"]

        china_prc_tb           = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        freights_china_prc_tb  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        cbix_coe_china_prc_tb  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]

        china_inps             = self.db["outputs/common_data_inputs/common_data_inputs_china_input_prices.csv"]
        freights_china_inps    = self.db["outputs/common_data_inputs/freights/common_data_inputs_china_input_prices.csv"]
        cbix_coe_china_inps    = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_china_input_prices.csv"]

        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        freights_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Reactive Alumina"]     =  (bx_details.loc[i, "Total Alumina"] - bx_details.loc[i, "LT Avail. Alumina"] - bx_details.loc[i, "LT R.Silica"]) * china_prc.loc[i, "HT Alumina Dissolution"] + bx_details.loc[i, "LT Avail. Alumina"] + bx_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Reactive Silica"]      =  bx_details.loc[i, "LT R.Silica"] + china_prc.loc[i, "Quartz Attack"] * (bx_details.loc[i, "Total Silica"] - bx_details.loc[i, "LT R.Silica"])
            new_df.at[i, "Available Alumina"]    =  new_df.loc[i, "Reactive Alumina"] - china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * new_df.loc[i, "Reactive Silica"]
            new_df.at[i, "Tonnes per Tonne"]     =  1 / new_df.loc[i, "Available Alumina"] / china_prc.loc[i, "Extraction Efficiency %"]
            new_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (bx_details.loc[i, "Price"] + bx_details.loc[i, "Processing Penalties"]) * new_df.loc[i, "Tonnes per Tonne"]
            new_df.at[i, "Caustic Use t.NAOH / t.AA"] = new_df.loc[i, "Reactive Silica"] * china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * new_df.loc[i, "Tonnes per Tonne"] + china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            new_df.at[i, "Caustic cost"]          = new_df.loc[i, "Caustic Use t.NAOH / t.AA"] * china_inps.loc[i, "Caustic Price"]
            new_df.at[i, "Thermal Energy Cost"]   = china_prc.loc[i, "Lig Coal (GJ/t)"] * china_inps.loc[i, "Energy Price"]
            new_df.at[i, "Lime Cost"]             = china_prc.loc[i, "Lime rate (wt/wt_AA)"] * china_inps.loc[i, "Lime Price"]
            new_df.at[i, "Mud make"]              = (new_df.loc[i, "Tonnes per Tonne"] - 1 ) + new_df.loc[i, "Caustic Use t.NAOH / t.AA"] + china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            new_df.at[i, "Mud Disposal Cost"]     = new_df.loc[i, "Mud make"] * china_inps.loc[i, "Mud Disposal Cost"]
            new_df.at[i, "Total Cost"]            = new_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + new_df.loc[i, "Caustic cost"] + new_df.loc[i, "Thermal Energy Cost"] + new_df.loc[i, "Lime Cost"] + new_df.loc[i, "Mud Disposal Cost"]
        
        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Reactive Alumina"]     =  (freights_bx_details.loc[i, "Total Alumina"] - freights_bx_details.loc[i, "LT Avail. Alumina"] - freights_bx_details.loc[i, "LT R.Silica"]) * freights_china_prc_tb.loc[i, "HT Alumina Dissolution"] + freights_bx_details.loc[i, "LT Avail. Alumina"] + freights_bx_details.loc[i, "LT R.Silica"]
            freights_df.at[i, "Reactive Silica"]      =  freights_bx_details.loc[i, "LT R.Silica"] + freights_china_prc_tb.loc[i, "Quartz Attack"] * (freights_bx_details.loc[i, "Total Silica"] - freights_bx_details.loc[i, "LT R.Silica"])
            freights_df.at[i, "Available Alumina"]    =  freights_df.loc[i, "Reactive Alumina"] - freights_china_prc_tb.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * freights_df.loc[i, "Reactive Silica"]
            freights_df.at[i, "Tonnes per Tonne"]     =  1 / freights_df.loc[i, "Available Alumina"] / freights_china_prc_tb.loc[i, "Extraction Efficiency %"]
            freights_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (freights_bx_details.loc[i, "Price"] + freights_bx_details.loc[i, "Processing Penalties"]) * freights_df.loc[i, "Tonnes per Tonne"]
            freights_df.at[i, "Caustic Use t.NAOH / t.AA"] = freights_df.loc[i, "Reactive Silica"] * freights_china_prc_tb.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * freights_df.loc[i, "Tonnes per Tonne"] + freights_china_prc_tb.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            freights_df.at[i, "Caustic cost"]          = freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] * freights_china_inps.loc[i, "Caustic Price"]
            freights_df.at[i, "Thermal Energy Cost"]   = freights_china_prc_tb.loc[i, "Lig Coal (GJ/t)"] * freights_china_inps.loc[i, "Energy Price"]
            freights_df.at[i, "Lime Cost"]             = freights_china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"] * freights_china_inps.loc[i, "Lime Price"]
            freights_df.at[i, "Mud make"]              = (freights_df.loc[i, "Tonnes per Tonne"] - 1 ) + freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] + freights_china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            freights_df.at[i, "Mud Disposal Cost"]     = freights_df.loc[i, "Mud make"] * freights_china_inps.loc[i, "Mud Disposal Cost"]
            freights_df.at[i, "Total Cost"]            = freights_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + freights_df.loc[i, "Caustic cost"] + freights_df.loc[i, "Thermal Energy Cost"] + freights_df.loc[i, "Lime Cost"] + freights_df.loc[i, "Mud Disposal Cost"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_coe_df.at[i, "Available Alumina"]    =  cbix_coe_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Reactive Silica"]
            cbix_coe_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_coe_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_coe_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_coe_df.loc[i, "Tonnes per Tonne"]
            cbix_coe_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_coe_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_coe_df.at[i, "Caustic cost"]          = cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_coe_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_coe_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_coe_df.at[i, "Mud make"]              = (cbix_coe_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_coe_df.at[i, "Mud Disposal Cost"]     = cbix_coe_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_coe_df.at[i, "Total Cost"]            = cbix_coe_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_coe_df.loc[i, "Caustic cost"] + cbix_coe_df.loc[i, "Thermal Energy Cost"] + cbix_coe_df.loc[i, "Lime Cost"] + cbix_coe_df.loc[i, "Mud Disposal Cost"]

        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = cbix_coe_df


    # OLD CBIX
    def old_cbix_bauxite_details_input_func(self):
        final_spec          = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_final_spec = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_final_spec = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame(columns=self.bauxite_details_columns)
        freights_df = pd.DataFrame(columns=self.bauxite_details_columns)
        cbix_coe_df = pd.DataFrame(columns=self.bauxite_details_columns)


        for i in range(len(self.trade_details)):
            new_df.at[i, "Price"]                   = final_spec.loc[i, "Price CIF (Port adjustyed to Qingdao)"]
            new_df.at[i, "Total Alumina"]           = final_spec.loc[i, "Total Alumina"]
            new_df.at[i, "LT Avail. Alumina"]       = final_spec.loc[i, "LT Avail. Alumina"]
            new_df.at[i, "Total Silica"]            = final_spec.loc[i, "Total Silica"]
            new_df.at[i, "LT R.Silica"]             = final_spec.loc[i, "LT R.Silica"]
            new_df.at[i, "Quartz / HT Silica"]      = final_spec.loc[i, "Quartz / HT Silica"]
            new_df.at[i, "Mono-hydrate / HT Alumina"] = final_spec.loc[i, "Mono-hydrate / HT Extble Alumina"]
            new_df.at[i, "Moisture"]                = final_spec.loc[i, "Moisture"]
            new_df.at[i, "Processing"]              = final_spec.loc[i, "Processing"]
            new_df.at[i, "Processing Penalties"]    = 0

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Price"]                   = freights_final_spec.loc[i, "Price CIF (Port adjustyed to Qingdao)"]
            freights_df.at[i, "Total Alumina"]           = freights_final_spec.loc[i, "Total Alumina"]
            freights_df.at[i, "LT Avail. Alumina"]       = freights_final_spec.loc[i, "LT Avail. Alumina"]
            freights_df.at[i, "Total Silica"]            = freights_final_spec.loc[i, "Total Silica"]
            freights_df.at[i, "LT R.Silica"]             = freights_final_spec.loc[i, "LT R.Silica"]
            freights_df.at[i, "Quartz / HT Silica"]      = freights_final_spec.loc[i, "Quartz / HT Silica"]
            freights_df.at[i, "Mono-hydrate / HT Alumina"] = freights_final_spec.loc[i, "Mono-hydrate / HT Extble Alumina"]
            freights_df.at[i, "Moisture"]                = freights_final_spec.loc[i, "Moisture"]
            freights_df.at[i, "Processing"]              = freights_final_spec.loc[i, "Processing"]
            freights_df.at[i, "Processing Penalties"]    = 0

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Price"]                   = cbix_coe_final_spec.loc[i, "Price CIF (Port adjustyed to Qingdao)"]
            cbix_coe_df.at[i, "Total Alumina"]           = cbix_coe_final_spec.loc[i, "Total Alumina"]
            cbix_coe_df.at[i, "LT Avail. Alumina"]       = cbix_coe_final_spec.loc[i, "LT Avail. Alumina"]
            cbix_coe_df.at[i, "Total Silica"]            = cbix_coe_final_spec.loc[i, "Total Silica"]
            cbix_coe_df.at[i, "LT R.Silica"]             = cbix_coe_final_spec.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Quartz / HT Silica"]      = cbix_coe_final_spec.loc[i, "Quartz / HT Silica"]
            cbix_coe_df.at[i, "Mono-hydrate / HT Alumina"] = cbix_coe_final_spec.loc[i, "Mono-hydrate / HT Extble Alumina"]
            cbix_coe_df.at[i, "Moisture"]                = cbix_coe_final_spec.loc[i, "Moisture"]
            cbix_coe_df.at[i, "Processing"]              = cbix_coe_final_spec.loc[i, "Processing"]
            cbix_coe_df.at[i, "Processing Penalties"]    = 0

        
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"] = cbix_coe_df


    def old_cbix_china_processing_factors_func(self):
        baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        freights_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        cbix_coe_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        freights_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)


        def lookup(search1, search2, target):
            ind = (self.processing_factors.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.processing_factors.loc[:, "Processing Regime"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if col == "Extraction Efficiency %":
                    new_df.at[i, col] = 0.92
                else:
                    new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if col == "Extraction Efficiency %":
                    freights_df.at[i, col] = 0.92
                else:
                    freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if col == "Extraction Efficiency %":
                    cbix_coe_df.at[i, col] = 0.92
                else:
                    cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = cbix_coe_df

    def old_cbix_alumina_production_cost_calcs_ece(self):
        bx_details          = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        freights_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        cbix_coe_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]

        china_prc           = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        freights_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        cbix_coe_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]

        china_inps          = self.db["outputs/common_data_inputs/common_data_inputs_china_input_prices.csv"]
        freights_china_inps = self.db["outputs/common_data_inputs/freights/common_data_inputs_china_input_prices.csv"]
        cbix_coe_china_inps = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_china_input_prices.csv"]

        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        freights_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Reactive Alumina"]     =  (bx_details.loc[i, "Total Alumina"] - bx_details.loc[i, "LT Avail. Alumina"] - bx_details.loc[i, "LT R.Silica"]) * china_prc.loc[i, "HT Alumina Dissolution"] + bx_details.loc[i, "LT Avail. Alumina"] + bx_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Reactive Silica"]      =  bx_details.loc[i, "LT R.Silica"] + china_prc.loc[i, "Quartz Attack"] * (bx_details.loc[i, "Total Silica"] - bx_details.loc[i, "LT R.Silica"])
            new_df.at[i, "Available Alumina"]    =  new_df.loc[i, "Reactive Alumina"] - china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * new_df.loc[i, "Reactive Silica"]
            new_df.at[i, "Tonnes per Tonne"]     =  1 / new_df.loc[i, "Available Alumina"] / china_prc.loc[i, "Extraction Efficiency %"]
            new_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (bx_details.loc[i, "Price"] + bx_details.loc[i, "Processing Penalties"]) * new_df.loc[i, "Tonnes per Tonne"]
            new_df.at[i, "Caustic Use t.NAOH / t.AA"] = new_df.loc[i, "Reactive Silica"] * china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * new_df.loc[i, "Tonnes per Tonne"] + china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            new_df.at[i, "Caustic cost"]          = new_df.loc[i, "Caustic Use t.NAOH / t.AA"] * china_inps.loc[i, "Caustic Price"]
            new_df.at[i, "Thermal Energy Cost"]   = china_prc.loc[i, "Lig Coal (GJ/t)"] * china_inps.loc[i, "Energy Price"]
            new_df.at[i, "Lime Cost"]             = china_prc.loc[i, "Lime rate (wt/wt_AA)"] * china_inps.loc[i, "Lime Price"]
            new_df.at[i, "Mud make"]              = (new_df.loc[i, "Tonnes per Tonne"] - 1 ) + new_df.loc[i, "Caustic Use t.NAOH / t.AA"] + china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            new_df.at[i, "Mud Disposal Cost"]     = new_df.loc[i, "Mud make"] * china_inps.loc[i, "Mud Disposal Cost"]
            new_df.at[i, "Total Cost"]            = new_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + new_df.loc[i, "Caustic cost"] + new_df.loc[i, "Thermal Energy Cost"] + new_df.loc[i, "Lime Cost"] + new_df.loc[i, "Mud Disposal Cost"]
        
        
        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Reactive Alumina"]     =  (freights_bx_details.loc[i, "Total Alumina"] - freights_bx_details.loc[i, "LT Avail. Alumina"] - freights_bx_details.loc[i, "LT R.Silica"]) * freights_china_prc.loc[i, "HT Alumina Dissolution"] + freights_bx_details.loc[i, "LT Avail. Alumina"] + freights_bx_details.loc[i, "LT R.Silica"]
            freights_df.at[i, "Reactive Silica"]      =  freights_bx_details.loc[i, "LT R.Silica"] + freights_china_prc.loc[i, "Quartz Attack"] * (freights_bx_details.loc[i, "Total Silica"] - freights_bx_details.loc[i, "LT R.Silica"])
            freights_df.at[i, "Available Alumina"]    =  freights_df.loc[i, "Reactive Alumina"] - freights_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * freights_df.loc[i, "Reactive Silica"]
            freights_df.at[i, "Tonnes per Tonne"]     =  1 / freights_df.loc[i, "Available Alumina"] / freights_china_prc.loc[i, "Extraction Efficiency %"]
            freights_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (freights_bx_details.loc[i, "Price"] + freights_bx_details.loc[i, "Processing Penalties"]) * freights_df.loc[i, "Tonnes per Tonne"]
            freights_df.at[i, "Caustic Use t.NAOH / t.AA"] = freights_df.loc[i, "Reactive Silica"] * freights_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * freights_df.loc[i, "Tonnes per Tonne"] + freights_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            freights_df.at[i, "Caustic cost"]          = freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] * freights_china_inps.loc[i, "Caustic Price"]
            freights_df.at[i, "Thermal Energy Cost"]   = freights_china_prc.loc[i, "Lig Coal (GJ/t)"] * freights_china_inps.loc[i, "Energy Price"]
            freights_df.at[i, "Lime Cost"]             = freights_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * freights_china_inps.loc[i, "Lime Price"]
            freights_df.at[i, "Mud make"]              = (freights_df.loc[i, "Tonnes per Tonne"] - 1 ) + freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] + freights_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            freights_df.at[i, "Mud Disposal Cost"]     = freights_df.loc[i, "Mud make"] * freights_china_inps.loc[i, "Mud Disposal Cost"]
            freights_df.at[i, "Total Cost"]            = freights_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + freights_df.loc[i, "Caustic cost"] + freights_df.loc[i, "Thermal Energy Cost"] + freights_df.loc[i, "Lime Cost"] + freights_df.loc[i, "Mud Disposal Cost"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_coe_df.at[i, "Available Alumina"]    =  cbix_coe_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Reactive Silica"]
            cbix_coe_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_coe_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_coe_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_coe_df.loc[i, "Tonnes per Tonne"]
            cbix_coe_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_coe_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_coe_df.at[i, "Caustic cost"]          = cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_coe_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_coe_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_coe_df.at[i, "Mud make"]              = (cbix_coe_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_coe_df.at[i, "Mud Disposal Cost"]     = cbix_coe_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_coe_df.at[i, "Total Cost"]            = cbix_coe_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_coe_df.loc[i, "Caustic cost"] + cbix_coe_df.loc[i, "Thermal Energy Cost"] + cbix_coe_df.loc[i, "Lime Cost"] + cbix_coe_df.loc[i, "Mud Disposal Cost"]
        
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_alumina_production_cost_calcs_excluding_common_elements.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_alumina_production_cost_calcs_excluding_common_elements.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_alumina_production_cost_calcs_excluding_common_elements.csv"] = cbix_coe_df

    # OLD CBIX CALCULATIONS
    def old_cbix_calculation_bauxite_details_func(self):
        final_specs          = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_final_specs = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_final_specs = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]

        new_df      = pd.DataFrame()
        freights_df = pd.DataFrame()
        cbix_coe_df = pd.DataFrame()

        def lookup(search1, search2, target):
            ind = (self.indexes_mines.loc[:, "Date"].map(lambda x: x.date()) <= search1)
            idx = (self.indexes_mines.loc[:, "Mine"].map(lambda x: x == search2))
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            new_df.at[i, "Total Alumina"]             = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Alumina"])
            new_df.at[i, "LT Avail. Alumina"]         = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            new_df.at[i, "Total Silica"]              = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Silica"])
            new_df.at[i, "LT R.Silica"]               = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT R.Silica"])
            new_df.at[i, "Quartz / HT Silica"]        = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            new_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            new_df.at[i, "Moisture"]                  = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Moisture"])
            new_df.at[i, "Processing"]                = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing"])
            new_df.at[i, "Processing Penalties"]      = lookup(self.trade_details.loc[i, "Date"].date(), final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            freights_df.at[i, "Total Alumina"]             = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Alumina"])
            freights_df.at[i, "LT Avail. Alumina"]         = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            freights_df.at[i, "Total Silica"]              = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Silica"])
            freights_df.at[i, "LT R.Silica"]               = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT R.Silica"])
            freights_df.at[i, "Quartz / HT Silica"]        = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            freights_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            freights_df.at[i, "Moisture"]                  = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Moisture"])
            freights_df.at[i, "Processing"]                = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing"])
            freights_df.at[i, "Processing Penalties"]      = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            cbix_coe_df.at[i, "Total Alumina"]             = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Alumina"])
            cbix_coe_df.at[i, "LT Avail. Alumina"]         = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            cbix_coe_df.at[i, "Total Silica"]              = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Silica"])
            cbix_coe_df.at[i, "LT R.Silica"]               = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT R.Silica"])
            cbix_coe_df.at[i, "Quartz / HT Silica"]        = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            cbix_coe_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            cbix_coe_df.at[i, "Moisture"]                  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Moisture"])
            cbix_coe_df.at[i, "Processing"]                = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing"])
            cbix_coe_df.at[i, "Processing Penalties"]      = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])
            
            
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_index_bauxite_details.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_index_bauxite_details.csv"] = cbix_coe_df

    def old_cbix_calculation_china_processing_factors_func(self):
        baux_dets_inputs          = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.csv"]
        freights_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_index_bauxite_details.csv"]
        cbix_coe_baux_dets_inputs = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_index_bauxite_details.csv"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        freights_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

        def lookup(search1, search2, target):
            ind = (self.processing_factors.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.processing_factors.loc[:, "Processing Regime"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]
            try:
                return v.iloc[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                if col == "Extraction Efficiency %":
                    new_df.at[i, col] = 0.92
                else:
                    new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.freights_trade_details)):
            for col in freights_df.columns:
                if col == "Extraction Efficiency %":
                    freights_df.at[i, col] = 0.92
                else:
                    freights_df.at[i, col] = lookup(self.freights_trade_details.loc[i, "Date"].date(), freights_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_coe_df.columns:
                if col == "Extraction Efficiency %":
                    cbix_coe_df.at[i, col] = 0.92
                else:
                    cbix_coe_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"] = cbix_coe_df

    def old_cbix_calculation_alumina_production_cost_calcs_ece(self):
        bx_details          = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.csv"]
        freights_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_index_bauxite_details.csv"]
        cbix_coe_bx_details = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_index_bauxite_details.csv"]

        china_prc           = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        freights_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        cbix_coe_china_prc  = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]

        china_prc_tb           = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        freights_china_prc_tb  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]
        cbix_coe_china_prc_tb  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_china_processing_factors.csv"]

        china_inps          = self.db["outputs/common_data_inputs/common_data_inputs_china_input_prices.csv"]
        freights_china_inps = self.db["outputs/common_data_inputs/freights/common_data_inputs_china_input_prices.csv"]
        cbix_coe_china_inps = self.db["outputs/common_data_inputs/cbix_co_efficients_determination/common_data_inputs_china_input_prices.csv"]

        new_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        freights_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_coe_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Reactive Alumina"]     =  (bx_details.loc[i, "Total Alumina"] - bx_details.loc[i, "LT Avail. Alumina"] - bx_details.loc[i, "LT R.Silica"]) * china_prc.loc[i, "HT Alumina Dissolution"] + bx_details.loc[i, "LT Avail. Alumina"] + bx_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Reactive Silica"]      =  bx_details.loc[i, "LT R.Silica"] + china_prc.loc[i, "Quartz Attack"] * (bx_details.loc[i, "Total Silica"] - bx_details.loc[i, "LT R.Silica"])
            new_df.at[i, "Available Alumina"]    =  new_df.loc[i, "Reactive Alumina"] - china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * new_df.loc[i, "Reactive Silica"]
            new_df.at[i, "Tonnes per Tonne"]     =  1 / new_df.loc[i, "Available Alumina"] / china_prc.loc[i, "Extraction Efficiency %"]
            new_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (bx_details.loc[i, "Price"] + bx_details.loc[i, "Processing Penalties"]) * new_df.loc[i, "Tonnes per Tonne"]
            new_df.at[i, "Caustic Use t.NAOH / t.AA"] = new_df.loc[i, "Reactive Silica"] * china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * new_df.loc[i, "Tonnes per Tonne"] + china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            new_df.at[i, "Caustic cost"]          = new_df.loc[i, "Caustic Use t.NAOH / t.AA"] * china_inps.loc[i, "Caustic Price"]
            new_df.at[i, "Thermal Energy Cost"]   = china_prc.loc[i, "Lig Coal (GJ/t)"] * china_inps.loc[i, "Energy Price"]
            new_df.at[i, "Lime Cost"]             = china_prc.loc[i, "Lime rate (wt/wt_AA)"] * china_inps.loc[i, "Lime Price"]
            new_df.at[i, "Mud make"]              = (new_df.loc[i, "Tonnes per Tonne"] - 1 ) + new_df.loc[i, "Caustic Use t.NAOH / t.AA"] + china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            new_df.at[i, "Mud Disposal Cost"]     = new_df.loc[i, "Mud make"] * china_inps.loc[i, "Mud Disposal Cost"]
            new_df.at[i, "Total Cost"]            = new_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + new_df.loc[i, "Caustic cost"] + new_df.loc[i, "Thermal Energy Cost"] + new_df.loc[i, "Lime Cost"] + new_df.loc[i, "Mud Disposal Cost"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Reactive Alumina"]     =  (freights_bx_details.loc[i, "Total Alumina"] - freights_bx_details.loc[i, "LT Avail. Alumina"] - freights_bx_details.loc[i, "LT R.Silica"]) * freights_china_prc.loc[i, "HT Alumina Dissolution"] + freights_bx_details.loc[i, "LT Avail. Alumina"] + freights_bx_details.loc[i, "LT R.Silica"]
            freights_df.at[i, "Reactive Silica"]      =  freights_bx_details.loc[i, "LT R.Silica"] + freights_china_prc.loc[i, "Quartz Attack"] * (freights_bx_details.loc[i, "Total Silica"] - freights_bx_details.loc[i, "LT R.Silica"])
            freights_df.at[i, "Available Alumina"]    =  freights_df.loc[i, "Reactive Alumina"] - freights_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * freights_df.loc[i, "Reactive Silica"]
            freights_df.at[i, "Tonnes per Tonne"]     =  1 / freights_df.loc[i, "Available Alumina"] / freights_china_prc.loc[i, "Extraction Efficiency %"]
            freights_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (freights_bx_details.loc[i, "Price"] + freights_bx_details.loc[i, "Processing Penalties"]) * freights_df.loc[i, "Tonnes per Tonne"]
            freights_df.at[i, "Caustic Use t.NAOH / t.AA"] = freights_df.loc[i, "Reactive Silica"] * freights_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * freights_df.loc[i, "Tonnes per Tonne"] + freights_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            freights_df.at[i, "Caustic cost"]          = freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] * freights_china_inps.loc[i, "Caustic Price"]
            freights_df.at[i, "Thermal Energy Cost"]   = freights_china_prc.loc[i, "Lig Coal (GJ/t)"] * freights_china_inps.loc[i, "Energy Price"]
            freights_df.at[i, "Lime Cost"]             = freights_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * freights_china_inps.loc[i, "Lime Price"]
            freights_df.at[i, "Mud make"]              = (freights_df.loc[i, "Tonnes per Tonne"] - 1 ) + freights_df.loc[i, "Caustic Use t.NAOH / t.AA"] + freights_china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            freights_df.at[i, "Mud Disposal Cost"]     = freights_df.loc[i, "Mud make"] * freights_china_inps.loc[i, "Mud Disposal Cost"]
            freights_df.at[i, "Total Cost"]            = freights_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + freights_df.loc[i, "Caustic cost"] + freights_df.loc[i, "Thermal Energy Cost"] + freights_df.loc[i, "Lime Cost"] + freights_df.loc[i, "Mud Disposal Cost"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_coe_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_coe_df.at[i, "Available Alumina"]    =  cbix_coe_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Reactive Silica"]
            cbix_coe_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_coe_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_coe_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_coe_df.loc[i, "Tonnes per Tonne"]
            cbix_coe_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_coe_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_coe_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_coe_df.at[i, "Caustic cost"]          = cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_coe_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_coe_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_coe_df.at[i, "Mud make"]              = (cbix_coe_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_coe_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_coe_df.at[i, "Mud Disposal Cost"]     = cbix_coe_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_coe_df.at[i, "Total Cost"]            = cbix_coe_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_coe_df.loc[i, "Caustic cost"] + cbix_coe_df.loc[i, "Thermal Energy Cost"] + cbix_coe_df.loc[i, "Lime Cost"] + cbix_coe_df.loc[i, "Mud Disposal Cost"]
        
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"] = cbix_coe_df

    # Complete uncompleted tables
    def spec_index_bauxite_details_func_continued1(self):
        trade_al_prod          = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]
        freights_trade_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]
        cbix_coe_trade_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]

        spec_ind_al_prod          = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]
        freights_spec_ind_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]
        cbix_coe_spec_ind_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]

        new_df      = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.csv"]
        freights_df = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_df = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_index_bauxite_details.csv"]


        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = (trade_al_prod.loc[i, "Total Cost"] - spec_ind_al_prod.loc[i, "Total Cost"]) / spec_ind_al_prod.loc[i, "Tonnes per Tonne"] - new_df.loc[i, "Processing Penalties"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Calculated Index Price Equivalent"]   = (freights_trade_al_prod.loc[i, "Total Cost"] - spec_ind_al_prod.loc[i, "Total Cost"]) / freights_spec_ind_al_prod.loc[i, "Tonnes per Tonne"] - freights_df.loc[i, "Processing Penalties"]
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Calculated Index Price Equivalent"]   = (cbix_coe_trade_al_prod.loc[i, "Total Cost"] - spec_ind_al_prod.loc[i, "Total Cost"]) / cbix_coe_spec_ind_al_prod.loc[i, "Tonnes per Tonne"] - cbix_coe_df.loc[i, "Processing Penalties"]
        
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_index_bauxite_details.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_index_bauxite_details.csv"] = cbix_coe_df


    def general_index_bauxite_details_func_continued1(self):
        trade_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]
        freights_trade_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/freights/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]
        cbix_coe_trade_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]

        gnr_ind_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]
        freights_gnr_ind_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]
        cbix_coe_gnr_ind_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]

        new_df = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.csv"]
        freights_df = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_df = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_index_bauxite_details.csv"]


        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = (trade_al_prod.loc[i, "Total Cost"] - gnr_ind_al_prod.loc[i, "Total Cost"]) / gnr_ind_al_prod.loc[i, "Tonnes per Tonne"] - new_df.loc[i, "Processing Penalties"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Calculated Index Price Equivalent"]   = (freights_trade_al_prod.loc[i, "Total Cost"] - freights_gnr_ind_al_prod.loc[i, "Total Cost"]) / freights_gnr_ind_al_prod.loc[i, "Tonnes per Tonne"] - freights_df.loc[i, "Processing Penalties"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Calculated Index Price Equivalent"]   = (cbix_coe_trade_al_prod.loc[i, "Total Cost"] - cbix_coe_gnr_ind_al_prod.loc[i, "Total Cost"]) / cbix_coe_gnr_ind_al_prod.loc[i, "Tonnes per Tonne"] - cbix_coe_df.loc[i, "Processing Penalties"]

        
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_index_bauxite_details.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_index_bauxite_details.csv"] = cbix_coe_df


    def old_cbix_calculation_bauxite_details_func_continued1(self):
        old_cbix_al_prod          = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_alumina_production_cost_calcs_excluding_common_elements.csv"]
        freights_old_cbix_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_alumina_production_cost_calcs_excluding_common_elements.csv"]
        cbix_coe_old_cbix_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_alumina_production_cost_calcs_excluding_common_elements.csv"]

        old_cbix_clac_al_prod          = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]
        freights_old_cbix_clac_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]
        cbix_coe_old_cbix_clac_al_prod = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]

        new_df      = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.csv"]
        freights_df = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_index_bauxite_details.csv"]
        cbix_coe_df = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_index_bauxite_details.csv"]
        
        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = (old_cbix_al_prod.loc[i, "Total Cost"] - old_cbix_clac_al_prod.loc[i, "Total Cost"]) / old_cbix_clac_al_prod.loc[i, "Tonnes per Tonne"] - new_df.loc[i, "Processing Penalties"]

        for i in range(len(self.freights_trade_details)):
            freights_df.at[i, "Calculated Index Price Equivalent"]   = (freights_old_cbix_al_prod.loc[i, "Total Cost"] - freights_old_cbix_clac_al_prod.loc[i, "Total Cost"]) / freights_old_cbix_clac_al_prod.loc[i, "Tonnes per Tonne"] - freights_df.loc[i, "Processing Penalties"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_df.at[i, "Calculated Index Price Equivalent"]   = (cbix_coe_old_cbix_al_prod.loc[i, "Total Cost"] - cbix_coe_old_cbix_clac_al_prod.loc[i, "Total Cost"]) / cbix_coe_old_cbix_clac_al_prod.loc[i, "Tonnes per Tonne"] - cbix_coe_df.loc[i, "Processing Penalties"]

        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.csv"] = new_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_index_bauxite_details.csv"] = freights_df
        self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_index_bauxite_details.csv"] = cbix_coe_df
    
    def final_specifications_to_viu_adjustment_continued2(self):
        spec_ind_bx             = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.csv"]
        freights_spec_ind_bx    = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/freights/specific_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_spec_ind_bx    = self.db["outputs/workings_for_viu_adjustmnet_of_specific_index/cbix_co_efficients_determination/specific_index_ViU_calculation_index_bauxite_details.csv"]

        gnr_ind_bx              = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.csv"]
        freights_gnr_ind_bx     = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/freights/general_index_ViU_calculation_index_bauxite_details.csv"]
        cbix_coe_gnr_ind_bx     = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_index_bauxite_details.csv"]

        old_cbix_clacs          = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.csv"]
        freights_old_cbix_clacs = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/freights/old_cbix_calculation_index_bauxite_details.csv"]
        cbix_coe_old_cbix_clacs = self.db["outputs/workings_for_viu_adjustmnet_of_old_cbix_index/cbix_co_efficients_determination/old_cbix_calculation_index_bauxite_details.csv"]

        final_spec              = self.db["outputs/final_specifications_to_viu_adjustment.csv"]
        freights_final_spec     = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        cbix_coe_final_spec     = self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"]


        for i in range(len(self.trade_details)):
            final_spec.at[i, "INDEX VALUE  Guinea 45/3 LT"] = spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "Specific Index"] == "Guinea 45/3 LT" else 0
            final_spec.at[i, "INDEX VALUE  Indonesian LT"]  = spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "Specific Index"] == "Indonesian LT" else 0
            final_spec.at[i, "INDEX VALUE  Australia HT"]   = spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "Specific Index"] == "Australia HT" else 0
            final_spec.at[i, "CBIX LT"]  = gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "General Index"] == "CBIX LT" else 0
            final_spec.at[i, "CBIX HT"]  = gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "General Index"] == "CBIX HT" else 0
            final_spec.at[i, "Old CBIX"] = old_cbix_clacs.loc[i, "Calculated Index Price Equivalent"]

        for i in range(len(self.freights_trade_details)):
            freights_final_spec.at[i, "INDEX VALUE  Guinea 45/3 LT"] = freights_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if freights_final_spec.loc[i, "Specific Index"] == "Guinea 45/3 LT" else 0
            freights_final_spec.at[i, "INDEX VALUE  Indonesian LT"]  = freights_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if freights_final_spec.loc[i, "Specific Index"] == "Indonesian LT" else 0
            freights_final_spec.at[i, "INDEX VALUE  Australia HT"]   = freights_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if freights_final_spec.loc[i, "Specific Index"] == "Australia HT" else 0
            freights_final_spec.at[i, "CBIX LT"]  = freights_gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if freights_final_spec.loc[i, "General Index"] == "CBIX LT" else 0
            freights_final_spec.at[i, "CBIX HT"]  = freights_gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if freights_final_spec.loc[i, "General Index"] == "CBIX HT" else 0
            freights_final_spec.at[i, "Old CBIX"] = freights_old_cbix_clacs.loc[i, "Calculated Index Price Equivalent"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_final_spec.at[i, "INDEX VALUE  Guinea 45/3 LT"] = cbix_coe_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "Specific Index"] == "Guinea 45/3 LT" else 0
            cbix_coe_final_spec.at[i, "INDEX VALUE  Indonesian LT"]  = cbix_coe_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "Specific Index"] == "Indonesian LT" else 0
            cbix_coe_final_spec.at[i, "INDEX VALUE  Australia HT"]   = cbix_coe_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "Specific Index"] == "Australia HT" else 0
            cbix_coe_final_spec.at[i, "CBIX LT"]  = cbix_coe_gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "General Index"] == "CBIX LT" else 0
            cbix_coe_final_spec.at[i, "CBIX HT"]  = cbix_coe_gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "General Index"] == "CBIX HT" else 0
            cbix_coe_final_spec.at[i, "Old CBIX"] = cbix_coe_old_cbix_clacs.loc[i, "Calculated Index Price Equivalent"]
        
        self.db["outputs/final_specifications_to_viu_adjustment.csv"] = final_spec
        self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"] = freights_final_spec
        self.db["outputs/cbix_co_efficients_determination/final_specifications_to_viu_adjustment.csv"] = cbix_coe_final_spec


    def freight_US_per_wmt(self):
        final_spec = self.db["outputs/freights/final_specifications_to_viu_adjustment.csv"]
        for i in range(self.freights_trade_details.shape[0]):
            self.freights_trade_details.at[i, "Freights US$/wmt"] = final_spec.loc[i, "Freight (adjusted to Qingdao)"] * (1 -final_spec.loc[i, "Moisture"])
        
        self.db["outputs/Freights US$ per wmt Trade Details.csv"] = self.freights_trade_details

    def cbix_coefficients_determination(self):
        trade_dets   = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_bauxite_details-input.csv"]
        alumina_prd  = self.db["outputs/workings_for_viu_adjustmnet_of_trade_bauxite/cbix_co_efficients_determination/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.csv"]
        gnr_ind_alu  = self.db["outputs/workings_for_viu_adjustmnet_of_general_index/cbix_co_efficients_determination/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.csv"]

        for i in range(self.cbix_cf_trade_details.shape[0]):
            self.cbix_cf_trade_details.at[i, "CBIX Co-Efficients Determination"] = ((gnr_ind_alu.loc[i, "Total Cost"] + gnr_ind_alu.loc[i, "Tonnes per Tonne"] * self.target_cbix_price.loc[0, "Target CBIX Price (CBIX LT)"]) - (alumina_prd.loc[i, "Total Cost"]-alumina_prd.loc[i, "Bauxite Cost Incl. Processing Penalties"]+trade_dets.loc[i, "Processing Penalties"]*alumina_prd.loc[i, "Tonnes per Tonne"])) / alumina_prd.loc[i, "Tonnes per Tonne"]
        
        self.db["outputs/CBIX Co-Efficients Determination Trade Details.csv"] = self.cbix_cf_trade_details

        





    def calcall(self):
        self.nominal_mine_div_index_specifications()
        self.global_factors_func()
        self.final_specifications_to_viu_adjustment()
        self.bauxite_details_input_func()
        self.china_processing_factors_func()
        self.alumina_production_cost_calcs_ece()
        self.common_data_inputs_fx_rates()
        self.common_data_inputs_glb_facs()
        self.china_input_prices()

        # FREIGHT CALCULATIONS TO ACTUAL NOMINATED PORT
        self.frieght_calculations()
        self.exporting_port_details_lr()
        self.port_linkages_funct()
        self.fuel_prices_func()
        self.importing_port_details()
        self.canals_details()
        self.canals_costs_workings()
        self.final_costing_up_for_leg()
        self.freight_calcualtions_actual_port_second_leg()
        self.exporting_port_details_loading_rate_second_leg()
        self.port_linkages_funct_second_leg()
        self.fuel_prices_func_second_leg()
        self.importing_port_details_second_leg()
        self.canals_details_second_leg()
        self.canals_costs_workings_second_leg()
        self.final_costing_up_for_second_leg()

        # FREIGHT CALCULATIONS TO QINGDAO (THE REFERENCE PORT USED FOR ALL THE INDEX CALCULATIONS)
        self.frieght_calculations(folder="freight_calcs_to_qingdao_first_leg")
        self.exporting_port_details_lr(folder="freight_calcs_to_qingdao_first_leg")
        self.port_linkages_funct(folder="freight_calcs_to_qingdao_first_leg")
        self.fuel_prices_func(folder="freight_calcs_to_qingdao_first_leg")
        self.importing_port_details(folder="freight_calcs_to_qingdao_first_leg")
        self.canals_details(folder="freight_calcs_to_qingdao_first_leg")
        self.canals_costs_workings(folder="freight_calcs_to_qingdao_first_leg")
        self.final_costing_up_for_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.freight_calcualtions_actual_port_second_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.exporting_port_details_loading_rate_second_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.port_linkages_funct_second_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.fuel_prices_func_second_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.importing_port_details_second_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.canals_details_second_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.canals_costs_workings_second_leg(folder="freight_calcs_to_qingdao_first_leg")
        self.final_costing_up_for_second_leg(folder="freight_calcs_to_qingdao_first_leg")

        self.final_freight_values_func()
        self.final_specifications_to_viu_adjustment_continued1() # Last function

        self.bauxite_details_input_func_continued1()
        self.alumina_production_cost_calcs_ece_continued1()

        self.spec_index_bauxite_details_func()
        self.spec_index_china_processing_factors_func()
        self.spec_index_alumina_production_cost_calcs_ece()

        self.general_index_bauxite_details_func()
        self.general_index_china_processing_factors_func()
        self.general_index_alumina_production_cost_calcs_ece()

        self.old_cbix_bauxite_details_input_func()
        self.old_cbix_china_processing_factors_func()
        self.old_cbix_alumina_production_cost_calcs_ece()

        self.old_cbix_calculation_bauxite_details_func()
        self.old_cbix_calculation_china_processing_factors_func()
        self.old_cbix_calculation_alumina_production_cost_calcs_ece()

        # Complete all uncompleted tables
        self.spec_index_bauxite_details_func_continued1()
        self.general_index_bauxite_details_func_continued1()
        self.old_cbix_calculation_bauxite_details_func_continued1()
        self.final_specifications_to_viu_adjustment_continued2()

        # save input files containing calculations
        self.freight_US_per_wmt()
        self.cbix_coefficients_determination()

        #Save files
        for filepath, file in self.db.items():
            dirname = os.path.dirname(filepath)

            if os.path.exists(dirname):
                pass
            else:
                os.mkdir(dirname)

            filename = filepath.split('/')[-1].split('.')[0]
            if filepath.split('/')[-2] != "outputs":
                filename = f"{filepath.split('/')[-2]} {filename}"
            dblist.append(db_conv.single_year_mult_out(file, filename))
            file.to_csv(filepath, index=False)





start = time.process_time()

cbix_obj = CBIX()
cbix_obj.calcall()

end = time.process_time() - start

print(f"{round(end/60, 2)} minutes")



dbflat_time = time.perf_counter()

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)



print(time.perf_counter() - dbflat_time)
uploadtodb.upload(snapshot_output_data)
'''
a = connect.to_db()
conn = a.outputstart()
cursor = conn.cursor()
cursor.fast_executemany = True
output_db_upload_time = time.perf_counter()
for i in range(snapshot_output_data.shape[0]):
    x = snapshot_output_data.loc[i]
    cursor.execute("INSERT INTO dbo.snapshot_output_data([model_id],[output_set],[output_lebel],[outrow],[output_value],[override_value],[actual_value]) values (?,?,?,?,?,?,?)",str(x[1]),str(x[2]),str(x[5]),str(x[4]),str(x[6]),'' if pd.isna(x[7]) else str(x[7]),'' if pd.isna(x[8]) else str(x[8]))
print("Time taken to upload to output db: {0} ".format(time.perf_counter() - output_db_upload_time))

for i in range(snapshot_output_data.shape[0]):
    chunk = snapshot_output_data.iloc[i:i+1,:].values.tolist()
    t =  tuple(tuple(x) for x in chunk)
    cursor.executemany("INSERT INTO dbo.snapshot_output_data([model_id],[output_set],[output_lebel],[outrow],[output_value],[override_value],[actual_value]) values (?,?,?,?,?,?,?)",t)
    
    
cursor.close()
print("done")
'''
