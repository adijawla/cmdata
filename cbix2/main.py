import time, os
from pathlib import Path, PureWindowsPath
import numpy  as np
import pandas as pd
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb
import re
import cbix2_script as cs

cbix_input = cs.restruct()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_conv = Flatdbconverter("CBIX machine version used in price forecasts")
dblist = []

class CBIX2:
    def __init__(self):
        """
        self.master_date_cell                      = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Master date cell")        
        self.cbix_cf_trade_details                 = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Actual Price determination from CBIX price")
        self.target_cbix_price                     = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Target CBIX Price")
        self.cbix_cf_trade_details.loc[:, "Date"]  = self.master_date_cell.loc[0, "Date"]
        """
        self.shipping_distn_costs_specs            = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Shipping Distn n Costs n Specs")
        self.shipping_distn_costs_specs.loc[:, "Bauxite style"]  =  self.shipping_distn_costs_specs.loc[:, "Bauxite style"].astype(str)
        self.shipping_distn_costs_specs.loc[:, "Total Organic Carbon"]  =  self.shipping_distn_costs_specs.loc[:, "Total Organic Carbon"].astype(str)
        self.shipping_distn_costs_specs.loc[:, "Vessel Class Choosen"]  =  self.shipping_distn_costs_specs.loc[:, "Vessel Class Choosen"].astype(str)
        
        self.shipping_distn_costs_specs_inps       = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Shipping inps")
    

        self.freight_datatable_row                 = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Freight inputs for")
        self.freight_datatable                     = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Data Table for Freights")
        self.price_forecast_datatable              = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Data Table for Forecast prices")

        """
        self.indexes_mines_2                       = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Indices Mines Exporting Ports 2")
        self.indexes_mines                         = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Indices Mines Exporting Ports")
        self.spc_leg_shp_table                     = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Special 2 Leg Shipping Table")
        self.mj_max_cargo                          = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="MRN or Juruti max cargo")
        self.ship_time_cr                          = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Ship Time Charter Rates")
        self.china_imp_prts                        = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="China n Importing Ports")
        self.freight_table                         = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Freight table selector")

        """


        self.viu_cost_data_table                   = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="ViU cost data table")

        """
        
        self.china_ps                              = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="China Price Series")
        self.processing_factors                    = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="processing factors")
        self.mud_disposal_cost                     = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Mud disposal cost")
        self.ship_fuel_prices                      = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Ship Fuel Prices")
        self.lignitious_coal                       = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Lignitious Coal")
        self.vessel_class                          = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="vessel classes")
        self.global_factors                        = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Global factors")
        self.trade_details                         = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Trade Details")
        self.trade_details.loc[:, "Date"]          = self.master_date_cell.loc[0, "Date"]
        self.port_linkages                         = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Port Linkages")
        self.canals_class                          = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Canals_Class")
        self.caustic_soda                          = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Caustic Soda")
        self.ship_speeds                           = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Ship Speeds")
        self.fx_rates                              = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="FX Rates")
        self.canals                                = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Canals")
        self.lime                                  = pd.read_excel(os.path.join(BASE_DIR, "cbix2input.xlsx"), sheet_name="Lime")
    
        """

        self.master_date_cell                      = cbix_input["Master_date_cell"]
        self.cbix_cf_trade_details                 = cbix_input["Actual_Price_determination_from_CBIX_price"]
        self.target_cbix_price                     = cbix_input["Target_CBIX_Price"]
        self.cbix_cf_trade_details.loc[:, "Date"]  = self.master_date_cell.loc[0, "Date"]
        # self.shipping_distn_costs_specs            = cbix_input["cbix2_Shipping_Distn_n_Costs_n_Specs"]
        # self.shipping_distn_costs_specs.loc[:, "Bauxite style"]  =  self.shipping_distn_costs_specs.loc[:, "Bauxite style"].astype(str)
        # self.shipping_distn_costs_specs.loc[:, "Total Organic Carbon"]  =  self.shipping_distn_costs_specs.loc[:, "Total Organic Carbon"].astype(str)
        # self.shipping_distn_costs_specs.loc[:, "Vessel Class Choosen"]  =  self.shipping_distn_costs_specs.loc[:, "Vessel Class Choosen"].astype(str)
        """
        self.shipping_distn_costs_specs_inps       = cbix_input["cbix2_Shipping_Distn_n_Costs_n_Specs_Calc_inps"]
        new_cols = []
        for c in self.shipping_distn_costs_specs_inps.columns:
            try:
                c = int(float(c))
                new_cols.append(c)
                print(c)
            except:
                new_cols.append(c)
        self.shipping_distn_costs_specs_inps.columns = new_cols
        """
        # self.freight_datatable_row                 = cbix_input["cbix2_Freight_inputs_for_ROW_Mining_Model"]
        # self.freight_datatable                     = cbix_input["cbix2_Data_Table_for_Freights"]
        # self.price_forecast_datatable              = cbix_input["cbix2_Data_Table_for_Forecast_prices"]
        self.indexes_mines_2                       = cbix_input["Indices_Mines_Exporting_Port_2"]
        self.indexes_mines                         = cbix_input["Indices_Mines_Exporting_Port"]
        self.spc_leg_shp_table                     = cbix_input["special_leg_shipping"]
        self.mj_max_cargo                          = cbix_input["MRN_Juruti_max_cargo"]
        self.ship_time_cr                          = cbix_input["Ship_Time_Charter_Rates"]
        self.china_imp_prts                        = cbix_input["China_Importing_Ports"]
        self.freight_table                         = cbix_input["Freights_table_selector"]


        # self.viu_cost_data_table                   = cbix_input["cbix2_ViU_cost_data_table"]
        self.processing_factors                    = cbix_input["processing_factors"]
        self.ship_fuel_prices                      = cbix_input["Ship_Fuel_Prices"]

        self.china_ps                              = cbix_input["china_price_series"]
        self.china_ps["Lignitious Coal – Date"]    = self.china_ps["Lignitious Coal – Date"].astype("datetime64[ns]")
        self.china_ps["Caustic Soda – Date"]       = self.china_ps["Caustic Soda – Date"].astype("datetime64[ns]")
        self.china_ps["Lime – Date"]               = self.china_ps["Lime – Date"].astype("datetime64[ns]")
        self.china_ps["Mud disposal cost - Date"]  = self.china_ps["Mud disposal cost - Date"].astype("datetime64[ns]")


        # self.indexes_mines                         = self.indexes_mines[self.indexes_mines["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.spc_leg_shp_table                     = self.spc_leg_shp_table[self.spc_leg_shp_table["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.mj_max_cargo                          = self.mj_max_cargo[self.mj_max_cargo["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.ship_time_cr                          = self.ship_time_cr[self.ship_time_cr["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.china_imp_prts                        = self.china_imp_prts[self.china_imp_prts["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)        
        # self.processing_factors                     = self.processing_factors[self.processing_factors["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.ship_fuel_prices                       = self.ship_fuel_prices[self.ship_fuel_prices["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.china_ps                               = self.china_ps[self.china_ps["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)

        self.lignitious_coal             = self.china_ps.loc[:, ["Lignitious Coal – Date", 'Lignitious Coal – Price', 'Lignitious Coal – Energy value']]
        self.lignitious_coal.columns     = ["Date", "Lignitious Coal Price (RMB/t) (exc VAT)", "Energy value (kcal/kg)"]

        self.mud_disposal_cost           = self.china_ps.loc[:, ["Mud disposal cost - Date", "Mud disposal cost – Price"]]
        self.mud_disposal_cost.columns   = ["Date", "Mud disposal cost Price (RMB/t.mud dry)"]

        self.caustic_soda                = self.china_ps.loc[:, ["Caustic Soda – Date","Caustic Soda – Price", "Caustic Soda – Grade"]]
        self.caustic_soda.columns        = ["Date", "Caustic Soda Price (RMB/t) (exc VAT)", "Grade"]

        self.lime                        = self.china_ps.loc[:, ["Lime – Date", "Lime – Price"]]
        self.lime.columns                = ["Date", "Lime Price (RMB/t)"]

        self.vessel_class                          = cbix_input["vessel_class"]
        self.global_factors                        = cbix_input["global_factors"]
        self.trade_details                         = cbix_input["Trade_Details"]
        self.trade_details.loc[:, "Date"]          = self.master_date_cell.loc[0, "Date"]
        self.port_linkages                         = cbix_input["Port_Linkages"]
        self.canals_class                          = cbix_input["Canals_Class"]
        self.ship_speeds                           = cbix_input["ship_speed"]
        self.fx_rates                              = cbix_input["fxrates_withdates"]
        self.canals                                = cbix_input["Canal"]
        self.freight_table_value                   = "default"
        
        # self.canals_class                = self.canals_class[self.canals_class["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.global_factors              = self.global_factors[self.global_factors["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.trade_details               = self.trade_details[self.trade_details["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.port_linkages               = self.port_linkages[self.port_linkages["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.ship_speeds                 = self.ship_speeds[self.ship_speeds["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.fx_rates                    = self.fx_rates[self.fx_rates["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        # self.canals                      = self.canals[self.canals["model_id"] == "model_9"].reset_index().drop(['index'],axis=1)
        self.db                          = {} #All generated outputs are temporarily stored here
        print("start".upper())
        print("shipping_distn_costs_specs\n", self.shipping_distn_costs_specs)
        print("shipping_distn_costs_specs_inps\n", self.shipping_distn_costs_specs_inps)
        print("freight_datatable_row\n", self.freight_datatable_row)
        print("freight_datatable\n", self.freight_datatable)
        print("price_forecast_datatable\n", self.price_forecast_datatable)
        print("viu_cost_data_table\n", self.viu_cost_data_table)
        print("master_date_cell\n", self.master_date_cell)
        print("cbix_cf_trade_details\n", self.cbix_cf_trade_details)
        print("target_cbix_price\n", self.target_cbix_price)
        print("indexes_mines_2\n", self.indexes_mines_2)
        print("indexes_mines\n", self.indexes_mines)
        print("spc_leg_shp_table\n", self.spc_leg_shp_table)
        print("mj_max_cargo\n", self.mj_max_cargo)
        print("ship_time_cr\n", self.ship_time_cr)
        print("china_imp_prts\n", self.china_imp_prts)
        print("freight_table\n", self.freight_table)
        print("processing_factors\n", self.processing_factors)
        print("ship_fuel_prices\n", self.ship_fuel_prices)
        print("china_ps\n", self.china_ps)
        print("lignitious_coal\n", self.lignitious_coal)
        print("mud_disposal_cost\n", self.mud_disposal_cost)
        print("caustic_soda\n", self.caustic_soda)
        print("lime\n", self.lime)
        print("end".upper())

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
    
    def custom_init(self):
        choosen_year = self.master_date_cell.loc[0, "Date"].year
        print(f"Datatable Year {choosen_year}")
        '''Calc One'''
        self.shipping_distn_costs_specs_inps.columns = self.shipping_distn_costs_specs_inps.columns.map(lambda x: str(x))
        
        if int(choosen_year) == int(self.shipping_distn_costs_specs_inps.columns[-12]):
            temp_df = self.shipping_distn_costs_specs_inps.loc[:, str(choosen_year):]
        else:
            temp_df = self.shipping_distn_costs_specs_inps.loc[:, str(choosen_year):str(choosen_year+1)][:-1]

        temp_df.columns = temp_df.loc[0, :]
        temp_df.drop(0, axis=0, inplace=True)
        temp_df.index = range(temp_df.shape[0])
        
        def calc1_vlookup(search1, target):
            v = temp_df.loc[:, "Mine"] == search1
            try:
                return target[v].tolist()[0]
            except Exception as e:
                return 0

        def calc3_vlookup(search1, target):
            v = self.indexes_mines_2.loc[:, "Mine"] == search1
            try:
                return target[v].tolist()[0]
            except Exception:
                return np.nan

        for i in range(self.shipping_distn_costs_specs.shape[0]):
            self.shipping_distn_costs_specs.at[i, "Alumina (%)"]                           = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Alumina (%)"])   * 100.0
            self.shipping_distn_costs_specs.at[i, "Silica (%)"]                            = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Silica (%)"])    * 100.0
            self.shipping_distn_costs_specs.at[i, "Moisture (%)"]                          = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Moisture (%)"])  * 100.0
            self.shipping_distn_costs_specs.at[i, "Bauxite style"]                         = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Bauxite style"])
            self.shipping_distn_costs_specs.at[i, "BAR t/t (dry)"]                         = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "BAR t/t (dry)"])
            self.shipping_distn_costs_specs.at[i, "Full Cost (US$/dmt CFR)"]               = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Full Cost (US$/dmt CFR)"])
            self.shipping_distn_costs_specs.at[i, "Monohydrate (%)"]                       = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Monohydrate (%)"]) * 100.0
            self.shipping_distn_costs_specs.at[i, "Total Organic Carbon"]                  = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Total Organic Carbon"])
            self.shipping_distn_costs_specs.at[i, "Other processing Penalties (US$/t_bx)"] = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Other processing Penalties (US$/t_bx)"])
            self.shipping_distn_costs_specs.at[i, "Vessel Class Choosen"]                  = calc1_vlookup(self.shipping_distn_costs_specs.loc[i, "Mine"], temp_df.loc[:, "Vessel Class Choosen"])
            # print(i)
            self.shipping_distn_costs_specs.at[i, "Total Processing Penalties (US$/t_bx)"] = (1 if self.shipping_distn_costs_specs.loc[i, "Total Organic Carbon"] == "V_High" else (0.67 if self.shipping_distn_costs_specs.loc[i, "Total Organic Carbon"] == "High" else (0.33 if self.shipping_distn_costs_specs.loc[i, "Total Organic Carbon"] == "Moderate" else (0 if self.shipping_distn_costs_specs.loc[i, "Total Organic Carbon"] == "Low" else 0))) + float(self.shipping_distn_costs_specs.loc[i, "Other processing Penalties (US$/t_bx)"]) + (0.5 * (self.shipping_distn_costs_specs.loc[i, "Monohydrate (%)"] - 4) if (self.shipping_distn_costs_specs.loc[i, "Bauxite style"] == "LT" and self.shipping_distn_costs_specs.loc[i, "Monohydrate (%)"] > 4) else 0))
        
        '''Calc Two'''
       
        self.indexes_mines_2.loc[:, "Workings"] = self.indexes_mines_2.loc[:, "Workings"].astype(str)
        for a in range(self.indexes_mines_2.shape[0]):            
            if self.indexes_mines_2.loc[a, "Mine"] in ["Metro BH1", "Metro LT", "Metro Blend", "Metallica"]:
                self.indexes_mines_2.at[a, "special add ons"] = 1.56 if choosen_year < 2019 else 1.56/2 if choosen_year == 2019 else 0            
            else:
                self.indexes_mines_2.at[a, "special add ons"] = 0
            # print(self.indexes_mines_2.shape[0],)
            self.indexes_mines_2.at[a, "Workings"] = self.shipping_distn_costs_specs.loc[a, "Vessel Class Choosen"] if self.freight_table_value == "default" else self.freight_table_value
            self.indexes_mines_2.at[a, "Final Vessel Size"] = self.indexes_mines_2.loc[a, "P'max dwt"] if self.indexes_mines_2.loc[a, "Workings"] == "Panamax" else self.indexes_mines_2.loc[a, "Cape dwt"]
            self.indexes_mines_2.at[a, "Final Add-On"] = sum([self.indexes_mines_2.loc[a, "Extra transloading charge US$/wmt if to Capesize"], self.indexes_mines_2.loc[a, "special add ons"]]) if self.indexes_mines_2.loc[a, "Workings"] == "Capesize" else 0

        '''Calc Three'''
        for b in range(self.indexes_mines.shape[0]):
            self.indexes_mines.at[b, "DWT"] = calc3_vlookup(self.indexes_mines.loc[b, "Mine"], self.indexes_mines_2.loc[:, "Final Vessel Size"])            

    def nominal_mine_div_index_specifications(self):
        new_df = pd.DataFrame(columns=self.nominal_mine_col)
        cbix_ap_df = pd.DataFrame(columns=self.nominal_mine_col)

        new_df.at[:, "Mine"] = self.trade_details.loc[:, "Mine"]
        new_df.at[:, "Date"] = self.trade_details.loc[:, "Date"].map(lambda x: x.date())
        cbix_ap_df.at[:, "Mine"] = self.cbix_cf_trade_details.loc[:, "Mine"]
        cbix_ap_df.at[:, "Date"] = self.cbix_cf_trade_details.loc[:, "Date"].map(lambda x: x.date())

        def lookup(search1, search2, target):
            for ind in range(target.shape[0]):
                if self.indexes_mines.loc[ind, "Mine"] == search1 and self.indexes_mines.loc[ind, "Date"].date() <= search2:
                    return target.iloc[ind]
            return 0

        #trade details
        for col in self.nominal_mine_col[2:]:
            for i in range(self.trade_details.shape[0]):
                new_df.at[i, col] = lookup(new_df.loc[i, "Mine"], new_df.loc[i, "Date"], self.indexes_mines.loc[:, "DWT" if col == 'Tonnage typical vessel' else col])

        for col in self.nominal_mine_col[2:]:
            for i in range(self.cbix_cf_trade_details.shape[0]):
                cbix_ap_df.at[i, col] = lookup(cbix_ap_df.loc[i, "Mine"], cbix_ap_df.loc[i, "Date"], self.indexes_mines.loc[:, "DWT" if col == 'Tonnage typical vessel' else col])
        
        self.db[f"outputs/{self.freight_table_value}/nominal_mine_div_index_specifications.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/nominal_mine_div_index_specifications.xlsx"] = cbix_ap_df

    def global_factors_func(self):
        cols = self.global_factors.columns.tolist()
        cols.insert(23, "MDO/MGO burn vessel DWT ^2 coefficient")
        cols.insert(26, "Main engine burn vessel DWT base constant")
        cols.insert(27, "Main engine burn vessel DWT base slope")

        new_df       = pd.DataFrame(columns=cols)
        cbix_ap_df  = pd.DataFrame(columns=cols)

        for i in range(len(self.trade_details)):
            for j in range(len(new_df.columns)):
                if new_df.columns[j] in self.global_factors.columns:
                    new_df.at[i, new_df.columns[j]] = self.global_factors.loc[0, new_df.columns[j]]
                else:
                    new_df.at[i, new_df.columns[j]] = 0

        for i in range(len(self.cbix_cf_trade_details)):
            for j in range(len(cbix_ap_df.columns)):
                if cbix_ap_df.columns[j] in self.global_factors.columns:
                    cbix_ap_df.at[i, cbix_ap_df.columns[j]] = self.global_factors.loc[0, cbix_ap_df.columns[j]]
                else:
                    cbix_ap_df.at[i, cbix_ap_df.columns[j]] = 0

        new_df.drop("Date", axis=1, inplace=True)
        cbix_ap_df.drop("Date", axis=1, inplace=True)

        self.db[f"outputs/{self.freight_table_value}/global_factors.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/global_factors.xlsx"] = cbix_ap_df
    
    def final_specifications_to_viu_adjustment(self):
        glb_factors  = self.db[f"outputs/{self.freight_table_value}/global_factors.xlsx"]
        cbix_coe_glb_factors  = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/global_factors.xlsx"]
        
        nominal_mine = self.db[f"outputs/{self.freight_table_value}/nominal_mine_div_index_specifications.xlsx"]
        cbix_coe_nominal_mine = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/nominal_mine_div_index_specifications.xlsx"]        
        
        new_df       = pd.DataFrame(columns=self.final_specs_columns)
        cbix_ap_df  = pd.DataFrame(columns=self.final_specs_columns)

        def bool_lookup(search1, search2):
            for ind in range(self.spc_leg_shp_table.shape[0]):
                if self.spc_leg_shp_table.loc[ind, "Mine"] == search1 and self.spc_leg_shp_table.loc[ind, "Date"].date() <= search2:
                    return True
            return False

        def lookup(search1, search2, target):
            ind = self.spc_leg_shp_table.loc[:, "Mine"] == search1
            idx = self.spc_leg_shp_table.loc[:, "Date"].map(lambda x: x.date() <= search2)
            v = (ind & idx)
            try:
                return target[v].tolist()[-1]
            except Exception:
                return 0

        def ship_vlookup(search1, target):
            v = self.shipping_distn_costs_specs.loc[:, "Mine"] == search1
            try:
                return target[v].tolist()[0]
            except Exception:
                return 0


        for i in range(len(self.trade_details)):
            new_df.at[i, "Mine"]                             = self.trade_details.loc[i, "Mine"]
            new_df.at[i, "Total Alumina"]                    = nominal_mine.loc[i, "Total Alumina"] if pd.isna(self.trade_details.loc[i, "Total Alumina"]) else self.trade_details.loc[i, "Total Alumina"]
            new_df.at[i, "LT Avail. Alumina"]                = nominal_mine.loc[i, "LT Avail. Alumina"] if pd.isna(self.trade_details.loc[i, "LT Avail. Alumina"]) else self.trade_details.loc[i, "LT Avail. Alumina"]
            new_df.at[i, "Total Silica"]                     = nominal_mine.loc[i, "Total Silica"] if pd.isna(self.trade_details.loc[i, "Total Silica"]) else self.trade_details.loc[i, "Total Silica"]
            new_df.at[i, "LT R.Silica"]                      = nominal_mine.loc[i, "LT R.Silica"] if pd.isna(self.trade_details.loc[i, "LT R.Silica"]) else self.trade_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Quartz / HT Silica"]               = nominal_mine.loc[i, "Quartz / HT Silica"] if pd.isna(self.trade_details.loc[i, "Quartz / HT Silica"]) else self.trade_details.loc[i, "Quartz / HT Silica"]
            new_df.at[i, "Mono-hydrate / HT Extble Alumina"] = ship_vlookup(new_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Monohydrate (%)"]) / 100.0
            new_df.at[i, "Moisture"]                         = ship_vlookup(new_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Moisture (%)"]) / 100.0
            new_df.at[i, "Processing"]                       = ship_vlookup(new_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Bauxite style"])
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
            new_df.at[i, "Price CIF (Port adjustyed to Qingdao)"] = ship_vlookup(new_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Full Cost (US$/dmt CFR)"])
            new_df.at[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"] = ship_vlookup(new_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Total Processing Penalties (US$/t_bx)"])
            new_df.at[i, "EXPLANITORY NOTES RECORD (on changes made to final spec etc)"] = np.nan
            new_df.at[i, "Old CBIX type Calc"] = self.trade_details.loc[i, "Old CBIX type Calc"]
            

        #Actual Price determination from CBIX price
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Mine"]                             = self.cbix_cf_trade_details.loc[i, "Mine"]
            cbix_ap_df.at[i, "Total Alumina"]                    = cbix_coe_nominal_mine.loc[i, "Total Alumina"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Total Alumina"]) else self.cbix_cf_trade_details.loc[i, "Total Alumina"]
            cbix_ap_df.at[i, "LT Avail. Alumina"]                = cbix_coe_nominal_mine.loc[i, "LT Avail. Alumina"] if pd.isna(self.cbix_cf_trade_details.loc[i, "LT Avail. Alumina"]) else self.cbix_cf_trade_details.loc[i, "LT Avail. Alumina"]
            cbix_ap_df.at[i, "Total Silica"]                     = cbix_coe_nominal_mine.loc[i, "Total Silica"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Total Silica"]) else self.cbix_cf_trade_details.loc[i, "Total Silica"]
            cbix_ap_df.at[i, "LT R.Silica"]                      = cbix_coe_nominal_mine.loc[i, "LT R.Silica"] if pd.isna(self.cbix_cf_trade_details.loc[i, "LT R.Silica"]) else self.cbix_cf_trade_details.loc[i, "LT R.Silica"]
            cbix_ap_df.at[i, "Quartz / HT Silica"]               = cbix_coe_nominal_mine.loc[i, "Quartz / HT Silica"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Quartz / HT Silica"]) else self.cbix_cf_trade_details.loc[i, "Quartz / HT Silica"]
            cbix_ap_df.at[i, "Mono-hydrate / HT Extble Alumina"] = ship_vlookup(cbix_ap_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Monohydrate (%)"]) / 100.0
            cbix_ap_df.at[i, "Moisture"]                         = ship_vlookup(cbix_ap_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Moisture (%)"]) / 100.0
            cbix_ap_df.at[i, "Processing"]                       = ship_vlookup(cbix_ap_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Bauxite style"])
            cbix_ap_df.at[i, "Cargo Tonnage"]                    = float(cbix_coe_nominal_mine.loc[i, "Tonnage typical vessel"]) * float(cbix_coe_glb_factors.loc[i, "Max % of vessel deadweight allowed for loading"]) * (1 - float(cbix_ap_df.loc[i, "Moisture"])) if pd.isna(self.cbix_cf_trade_details.loc[i, "Tonnage"]) else float(self.cbix_cf_trade_details.loc[i, "Tonnage"]) * (1 if self.cbix_cf_trade_details.loc[i, "Price Basis"] == "dmt" else (1 - float(cbix_ap_df.loc[i, "Moisture"])))
            cbix_ap_df.at[i, "Double Leg Shipping? (South America special transload leg)"] = "Yes" if (cbix_ap_df.loc[i, "Cargo Tonnage"] > self.mj_max_cargo.loc[0, "MRN/Juruti max cargo single leg shipping"] and bool_lookup(self.cbix_cf_trade_details.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"])) else "No"
            cbix_ap_df.at[i, "Exporting Port"]                   = lookup(cbix_ap_df.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 1"]) if cbix_ap_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else (cbix_coe_nominal_mine.loc[i, "Exporting Port"] if pd.isna(self.cbix_cf_trade_details.loc[i, "Exporting Port"]) else self.cbix_cf_trade_details.loc[i, "Exporting Port"])
            cbix_ap_df.at[i, "South America special transloading Port"] = 0 if cbix_ap_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else lookup(cbix_ap_df.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "First Leg 2"])
            cbix_ap_df.at[i, "South america special transloading cargo tonnage"] = 0 if cbix_ap_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else float(cbix_ap_df.loc[i, "Cargo Tonnage"])/round(float(cbix_ap_df.loc[i, "Cargo Tonnage"])/float(lookup(cbix_ap_df.loc[i, "Mine"], self.cbix_cf_trade_details.loc[i, "Date"], self.spc_leg_shp_table.loc[:, "Typical River Capable Cargo (dmt)"])), 0)
            cbix_ap_df.at[i, "Importing Port"]                   = "Qingdao" if pd.isna(self.cbix_cf_trade_details.loc[i, "Importing Port"]) else self.cbix_cf_trade_details.loc[i, "Importing Port"]
            cbix_ap_df.at[i, "Specific Index"]                   = cbix_coe_nominal_mine.loc[i, "Specific Index"]
            cbix_ap_df.at[i, "General Index"]                    = cbix_coe_nominal_mine.loc[i, "General Index"]
            cbix_ap_df.at[i, "Price Type"]                       = self.cbix_cf_trade_details.loc[i, "Final Specs Price Type"]
            cbix_ap_df.at[i, "Price Basis"]                      = self.cbix_cf_trade_details.loc[i, "Final Specs Price Basis"]
            cbix_ap_df.at[i, "Price CIF (Port adjustyed to Qingdao)"] = ship_vlookup(cbix_ap_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Full Cost (US$/dmt CFR)"])
            cbix_ap_df.at[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"] = ship_vlookup(cbix_ap_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Total Processing Penalties (US$/t_bx)"])
            cbix_ap_df.at[i, "EXPLANITORY NOTES RECORD (on changes made to final spec etc)"] = np.nan
            cbix_ap_df.at[i, "Old CBIX type Calc"] = self.cbix_cf_trade_details.loc[i, "Old CBIX type Calc"]
            
        self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"] = cbix_ap_df


    def bauxite_details_input_func(self):
        fin_spec_df = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec_df = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.bauxite_details_columns)
        cbix_ap_df = pd.DataFrame(columns=self.bauxite_details_columns)

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
        
        for i in range(len(self.cbix_cf_trade_details)):
            # cbix_ap_df.at[i, "Price"]   = 
            cbix_ap_df.at[i, "Total Alumina"]           = cbix_coe_fin_spec_df.loc[i, "Total Alumina"]
            cbix_ap_df.at[i, "LT Avail. Alumina"]       = cbix_coe_fin_spec_df.loc[i, "LT Avail. Alumina"]
            cbix_ap_df.at[i, "Total Silica"]            = cbix_coe_fin_spec_df.loc[i, "Total Silica"]
            cbix_ap_df.at[i, "LT R.Silica"]             = cbix_coe_fin_spec_df.loc[i, "LT R.Silica"]
            cbix_ap_df.at[i, "Quartz / HT Silica"]      = cbix_coe_fin_spec_df.loc[i, "Quartz / HT Silica"]
            cbix_ap_df.at[i, "Mono-hydrate / HT Alumina"] = cbix_coe_fin_spec_df.loc[i, "Mono-hydrate / HT Extble Alumina"]
            cbix_ap_df.at[i, "Moisture"]                = cbix_coe_fin_spec_df.loc[i, "Moisture"]
            cbix_ap_df.at[i, "Processing"]              = cbix_coe_fin_spec_df.loc[i, "Processing"]
            cbix_ap_df.at[i, "Processing Penalties"]    = cbix_coe_fin_spec_df.loc[i, "Processing Penalties to be applied (for organics, goehtite, impurities, crushing etc)"]
        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"] = cbix_ap_df

    def china_processing_factors_func(self):
        baux_dets_inputs          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        cbix_coe_baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        
        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

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
        
       #Actual Price determination from CBIX price
        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"] = cbix_ap_df

    def alumina_production_cost_calcs_ece(self):
        fin_spec_df = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec_df = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]


        # bx_details = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]     # pd.read_excel("outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx")
        # cbix_coe_baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        
        china_prc  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]  # pd.read_excel("outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx")
        cbix_coe_china_prc  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]
        
        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        
        def ship_vlookup(search1, target):
            v = self.shipping_distn_costs_specs.loc[:, "Mine"] == search1
            try:
                return target[v].tolist()[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            new_df.at[i, "Reactive Alumina"]     =  ship_vlookup(fin_spec_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Alumina (%)"]) / 100.0
            new_df.at[i, "Reactive Silica"]      =  ship_vlookup(fin_spec_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Silica (%)"]) / 100.0
            new_df.at[i, "Available Alumina"]    =  new_df.loc[i, "Reactive Alumina"] - china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * new_df.loc[i, "Reactive Silica"]                        
            new_df.at[i, "Tonnes per Tonne"]     = 0 if (new_df.loc[i, "Available Alumina"] == 0 and china_prc.loc[i, "Extraction Efficiency %"] == 0) else 1 / new_df.loc[i, "Available Alumina"] / china_prc.loc[i, "Extraction Efficiency %"]
            new_df.at[i, "Caustic Use t.NAOH / t.AA"] = new_df.loc[i, "Reactive Silica"] * china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * new_df.loc[i, "Tonnes per Tonne"] + china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Reactive Alumina"]     =  ship_vlookup(cbix_coe_fin_spec_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Alumina (%)"]) / 100.0
            cbix_ap_df.at[i, "Reactive Silica"]      =  ship_vlookup(cbix_coe_fin_spec_df.loc[i, "Mine"], self.shipping_distn_costs_specs.loc[:, "Silica (%)"]) / 100.0
            cbix_ap_df.at[i, "Available Alumina"]    =  cbix_ap_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Reactive Silica"]
            cbix_ap_df.at[i, "Tonnes per Tonne"]     =  0 if (cbix_ap_df.loc[i, "Available Alumina"] == 0 and cbix_coe_china_prc.loc[i, "Extraction Efficiency %"] == 0) else 1 / cbix_ap_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_ap_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_ap_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]


        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = cbix_ap_df


    def common_data_inputs_glb_facs(self):
        new_df      = pd.DataFrame(columns=self.cdi_glb_factors_columns)
        cbix_ap_df = pd.DataFrame(columns=self.cdi_glb_factors_columns)

        def lookup(search, target):           
            ind = (self.global_factors.loc[:, "Date"].map(lambda x: x.date()) <= search).map(lambda i: 1 if i == True else 0)
            idx = (target.map(lambda x: pd.notna(x))).map(lambda i: 1 if i == True else 0)            
            v = 1 / (ind * idx) 

            try:
                return target.iloc[max(v.value_counts())-1]
            except Exception:
                return 0


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

      
        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                if col in hardcoded:
                    v = 0
                else:
                    v = lookup(self.cbix_cf_trade_details.loc[i, "Date"], self.global_factors.loc[:, col])

                cbix_ap_df.at[i, col] = v

        self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_global_factors.xlsx"] = cbix_ap_df


    def common_data_inputs_fx_rates(self):
        comm_cols = [
            "RMB per US$",
            "AU$ per US$",
            "US$ per US$",
            "IMF Special Drawing Rights to US Dollar",
        ]

        new_df      = pd.DataFrame(columns=comm_cols)
        cbix_ap_df = pd.DataFrame(columns=comm_cols)

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


        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "RMB per US$"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "RMB per US$"])
            cbix_ap_df.at[i, "AU$ per US$"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "AU$ per US$"])
            cbix_ap_df.at[i, "US$ per US$"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "US$ per US$"])
            cbix_ap_df.at[i, "IMF Special Drawing Rights to US Dollar"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.fx_rates.loc[:, "IMF Special Drawing Rights to US Dollar"])            
        
        self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_fx_rates.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_fx_rates.xlsx"] = cbix_ap_df

    def  china_input_prices(self):
        glb_facs = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]     # pd.read_excel("outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx")
        cbix_coe_glb_facs = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_global_factors.xlsx"]

        fx_rates = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_fx_rates.xlsx"]       # pd.read_excel("outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_fx_rates.xlsx")
        cbix_coe_fx_rates = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_fx_rates.xlsx"]

        cip_cols = [
            "Energy Price",
            "Caustic Price",
            "Lime Price",
            "Mud Disposal Cost",
        ]

        new_df      = pd.DataFrame(columns=cip_cols)
        cbix_ap_df = pd.DataFrame(columns=cip_cols)

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
        
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Energy Price"]  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"] / (lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lignitious Coal – Date"], self.china_ps.loc[:, "Lignitious Coal – Energy value"]) * cbix_coe_glb_facs.loc[i, "conversion kt to t"] / cbix_coe_glb_facs.loc[i, "kcal per GJ"])
            cbix_ap_df.at[i, "Caustic Price"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"] / (lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Caustic Soda – Date"], self.china_ps.loc[:, "Caustic Soda – Grade"]))
            cbix_ap_df.at[i, "Lime Price"]    = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Lime – Date"], self.china_ps.loc[:, "Lime – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"]
            cbix_ap_df.at[i, "Mud Disposal Cost"]    = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), self.china_ps.loc[:, "Mud disposal cost - Date"], self.china_ps.loc[:, "Mud disposal cost – Price"]) / cbix_coe_fx_rates.loc[i, "RMB per US$"]
        
        self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_china_input_prices.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_china_input_prices.xlsx"] = cbix_ap_df

    def frieght_calculations(self, folder=None):
        glb_facs          = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]
        cbix_coe_glb_facs = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_global_factors.xlsx"]

        final_spec           = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec_df = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.frieght_calcs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.frieght_calcs_columns)
        
        def lookup(search, target_check, target):
            filt = target[target_check.iloc[:] <= search]
            if filt is None:
                    filt = target[target_check.iloc[:] >= search]
                    return filt.iloc[0]
            else:
                if len(filt) == 0:
                    return np.nan
                return filt.iloc[-1]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Vessel dwt"] = ((final_spec.loc[i, "Cargo Tonnage"] if final_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else final_spec.loc[i, "South america special transloading cargo tonnage"]) / (1 - final_spec.loc[i, "Moisture"])) / glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            new_df.at[i, "Vessel Class for timecharter & fuel burn rates"] = lookup(new_df.loc[i, "Vessel dwt",], self.vessel_class.loc[:, "DWT< tonnes"], self.vessel_class.loc[:, "Class"])
            new_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(new_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            new_df.at[i, "Estimated LOA (length over all) (m)"]    = (new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "LOA estimate correlation multiplier"] + glb_facs.loc[i, "LOA estimate correlation constant"])
            new_df.at[i, "Estimated NRT (net register tons)"]  = (new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "NRT estimate correlation multiplier"] + glb_facs.loc[i, "NRT estimate correlation constant"])
            new_df.at[i, "Estimated GRT (gross register tons)"]    = (new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "GRT estimate correlation multiplier"] + glb_facs.loc[i, "GRT estimate correlation constant"])

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Vessel dwt"] = ((cbix_coe_fin_spec_df.loc[i, "Cargo Tonnage"] if cbix_coe_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fin_spec_df.loc[i, "South america special transloading cargo tonnage"]) / (1 - cbix_coe_fin_spec_df.loc[i, "Moisture"])) / cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_ap_df.at[i, "Vessel Class for timecharter & fuel burn rates"] = lookup(cbix_ap_df.loc[i, "Vessel dwt",], self.vessel_class.loc[:, "DWT< tonnes"], self.vessel_class.loc[:, "Class"])
            cbix_ap_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(cbix_ap_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            cbix_ap_df.at[i, "Estimated LOA (length over all) (m)"]    = (cbix_ap_df.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "LOA estimate correlation multiplier"] + cbix_coe_glb_facs.loc[i, "LOA estimate correlation constant"])
            cbix_ap_df.at[i, "Estimated NRT (net register tons)"]  = (cbix_ap_df.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "NRT estimate correlation multiplier"] + cbix_coe_glb_facs.loc[i, "NRT estimate correlation constant"])
            cbix_ap_df.at[i, "Estimated GRT (gross register tons)"]    = (cbix_ap_df.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "GRT estimate correlation multiplier"] + cbix_coe_glb_facs.loc[i, "GRT estimate correlation constant"])
        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/frieght_calculations-actual_port-first_leg.xlsx"] = cbix_ap_df

    def exporting_port_details_lr(self, folder=None):
        final_spec           = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]     # pd.read_excel("outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx")
        cbix_coe_fin_spec_df = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]


        new_df      = pd.DataFrame(columns=self.exporting_port_dts_cols)
        cbix_ap_df = pd.DataFrame(columns=self.exporting_port_dts_cols)

        def lookup(search1, search2, target):            
            ind = (self.indexes_mines.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)
            idx = (self.indexes_mines.loc[:, "Exporting Port"].map(lambda x: x == search2)) #.map(lambda i: 1 if i == True else 0)
            v = target[ind & idx]

            try:
                return v.tolist()[0]
            except Exception:
                return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(self.trade_details.loc[i, "Date"].date(), final_spec.loc[i, "Exporting Port"], self.indexes_mines.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_fin_spec_df.loc[i, "Exporting Port"], self.indexes_mines.loc[:, col])

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details-loading_rates.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/exporting_port_details-loading_rates.xlsx"] = cbix_ap_df



    def port_linkages_funct(self, folder=None):
        fn_spec = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]    # pd.read_excel("outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx")
        cbix_coe_fin_spec_df = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]
        
        new_df = pd.DataFrame(columns=self.port_linkages_cols)
        cbix_ap_df = pd.DataFrame(columns=self.port_linkages_cols)
        
        def lookup(search1, search2, search3, target):
            for j in range(target.shape[0]):
                if (self.port_linkages.loc[j, "Exporting Port"] == search1) and (self.port_linkages.loc[j, "Importing Port"] == search2) and (self.port_linkages.loc[j, "Date"].date() <= search3):
                    return target.iloc[j]
            return 0

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                new_df.at[i, col] = lookup(
                    fn_spec.loc[i, "Exporting Port"], 
                    (fn_spec.loc[i, "South America special transloading Port"] if fn_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else fn_spec.loc[i, "Importing Port"]), 
                    self.trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                cbix_ap_df.at[i, col] = lookup(
                    cbix_coe_fin_spec_df.loc[i, "Exporting Port"], 
                    (cbix_coe_fin_spec_df.loc[i, "South America special transloading Port"] if cbix_coe_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fin_spec_df.loc[i, "Importing Port"]), 
                    self.cbix_cf_trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])
        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages.xlsx"] = cbix_ap_df

    def fuel_prices_func(self, folder=None):
        port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.xlsx"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages.xlsx"]

        frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.xlsx"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/frieght_calculations-actual_port-first_leg.xlsx"]

        new_df      = pd.DataFrame(columns=self.fuel_prices_cols)
        cbix_ap_df = pd.DataFrame(columns=self.fuel_prices_cols)

        def lookup(search1, search2, target):
            for j in range(target.shape[0]):
                ind = (self.ship_fuel_prices.loc[:, "Fuel Region"] == search1) #.map(lambda i: 1 if i == True else 0)
                idx = (self.ship_fuel_prices.loc[:, "Date"].map(lambda x: x.date()) <= search2) #.map(lambda i: 1 if i == True else 0)            
                v = (ind & idx)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
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

                cbix_ap_df.at[i, col] = lookup(v, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/fuel_prices.xlsx"] = cbix_ap_df

    def importing_port_details(self, folder=None):
        final_spec           = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec_df = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.importing_port_det_cols)
        cbix_ap_df = pd.DataFrame(columns=self.importing_port_det_cols)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                if cbix_coe_fin_spec_df.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes":
                    val = cbix_coe_fin_spec_df.loc[i, "South America special transloading Port"]
                else:
                    val = cbix_coe_fin_spec_df.loc[i, "Importing Port"]

                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        
        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/importing_port_details.xlsx"] = cbix_ap_df


    def canals_details(self, folder=None):
        port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.xlsx"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages.xlsx"]

        frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.xlsx"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/frieght_calculations-actual_port-first_leg.xlsx"]

        new_df      = pd.DataFrame(columns=self.cost_details_cols)
        cbix_ap_df = pd.DataFrame(columns=self.cost_details_cols)

        def lookup(search1, search2, target):
            idx = (self.canals.loc[:, "Date"].map(lambda x: x.date() <= search1)) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.canals.loc[:, "Canal"] == search2)
            v = (ind & idx)
            #print(target[v])
            try:
                return target[ind].tolist()[-1]
            except Exception:
                return np.nan

        for i in range(len(self.trade_details)):
            for col in new_df.columns:
                #print(frieght_calcs.loc[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"])
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


        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
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
                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_details.xlsx"] = cbix_ap_df

    def canals_costs_workings(self, folder=None):
        canals_details             = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details.xlsx"]
        cbix_coe_df_canals_details = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_details.xlsx"]
        
        fc_actual_port             = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.xlsx"]
        cbix_coe_df_fc_actual_port = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/frieght_calculations-actual_port-first_leg.xlsx"]
        
        glb_facs                   = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]
        cbix_coe_df_glb_facs       = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_global_factors.xlsx"]

        new_df      = pd.DataFrame(columns=self.canal_cost_workings_cols)
        cbix_ap_df = pd.DataFrame(columns=self.canal_cost_workings_cols)

        for i in range(len(self.trade_details)):
            #print(f'{fc_actual_port.loc[i, "Vessel dwt"]} if {canals_details.loc[i, "Tonnage Reference"]} == "DWT" else {fc_actual_port.loc[i, "Estimated NRT (net register tons)"]} if {canals_details.loc[i, "Tonnage Reference"]} == "NRT" else {fc_actual_port.loc[i, "Estimated NRT (net register tons)"]} if {canals_details.loc[i, "Tonnage Reference"]} == "GRT" else {np.nan}')
            new_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = fc_actual_port.loc[i, "Vessel dwt"] if canals_details.loc[i, "Tonnage Reference"] == "DWT" else fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if canals_details.loc[i, "Tonnage Reference"] == "NRT" else fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if canals_details.loc[i, "Tonnage Reference"] == "GRT" else np.nan            
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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = cbix_coe_df_fc_actual_port.loc[i, "Vessel dwt"] if cbix_coe_df_canals_details.loc[i, "Tonnage Reference"] == "DWT" else cbix_coe_df_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_df_canals_details.loc[i, "Tonnage Reference"] == "NRT" else cbix_coe_df_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_df_canals_details.loc[i, "Tonnage Reference"] == "GRT" else np.nan
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = cbix_coe_df_fc_actual_port.loc[i, "Vessel dwt"] * cbix_coe_df_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_df_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_costs_workings.xlsx"] = cbix_ap_df

         
    def final_costing_up_for_leg(self, folder=None):
        fxrates                = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_fx_rates.xlsx"]
        cbix_coe_fxrates       = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_fx_rates.xlsx"]
        
        fuel_prices            = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices.xlsx"]
        cbix_coe_fuel_prices   = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/fuel_prices.xlsx"]
        
        port_lkgs              = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages.xlsx"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages.xlsx"]
        
        canals_detls           = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details.xlsx"]
        cbix_coe_canals_detls  = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_details.xlsx"]
        
        glb_facs               = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]
        cbix_coe_glb_facs      = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_global_factors.xlsx"]
        
        canals_costs           = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings.xlsx"]
        cbix_coe_canals_costs  = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_costs_workings.xlsx"]
        
        imp_port_dets          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details.xlsx"]
        cbix_coe_imp_port_dets = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/importing_port_details.xlsx"]
        
        loading_rates          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details-loading_rates.xlsx"]
        cbix_coe_loading_rates = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/exporting_port_details-loading_rates.xlsx"]
        
        frieght_calcs          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/frieght_calculations-actual_port-first_leg.xlsx"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/frieght_calculations-actual_port-first_leg.xlsx"]
        
        fnl_specs              = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fnl_specs     = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)
        cbix_ap_df = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)

        fuel_prices_lookup = ["HSFO", "VLSFO", "LNG", "MGO_Regular", "MGO_Low_Sulph"]
        fx_rates_lookup    = ["RMB", "AUD", "USD", "SDR"]

        def lookup(search1, search2, target):
            idx = (self.ship_speeds.loc[:, "Date"].map(lambda x: x.date() <= search1)) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.ship_speeds.loc[:, "Vesssel class"] == search2)
            v = (idx & ind)
            
            try:
                return target[ind].tolist()[-1]
            except Exception:
                return 0

        def lookup1(search1, search2, target):
            x = self.ship_time_cr.loc[:, "Applicable Time Charter Index"] == search1
            y = self.ship_time_cr.loc[:, "Date"].map(lambda x: x.date() <= search2) #.map(lambda i: 1 if i == True else 0)            
            z = self.ship_time_cr.loc[:, "Vessel Time Charter Rates"].map(lambda x: pd.notna(x))
            v =  (x &  z)

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
                rt_dist_main_fuel = np.nan
                rt_dist_aux_fuel = np.nan
                vl_loading = np.nan
                vl_unloading = np.nan
                main_engine_fuel = np.nan
                auxiliary_fuel = np.nan
                charter_ind = np.nan

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
            #print(vl_loading)
            new_df.at[i, "Vessel Un-Loading rate"]                 = vl_unloading
            #print(vl_unloading)
            new_df.at[i, "Vessel Speed"]                           = lookup(self.trade_details.loc[i, "Date"].date(), frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (new_df.loc[i, "Vessel Speed"] ** 2) + glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (new_df.loc[i, "Vessel Speed"] ** 3) + glb_facs.loc[i, "Main engine burn speed linear coefficient"] * new_df.loc[i, "Vessel Speed"])
            v2 = ((frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * glb_facs.loc[i, "Main engine burn overall correction factor"] + (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            new_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            new_df.at[i, "MDO / MGO burn rate"]                    = (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (frieght_calcs.loc[i, "Vessel dwt"] / glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            #print(f'{new_df.loc[i, "Round trip distance Main fuel"]} / {new_df.loc[i, "Vessel Speed"]} / {glb_facs.loc[i, "hours per day"]}')
            new_df.at[i, "Days at Avg Speed Main Fuel"]            = new_df.loc[i, "Round trip distance Main fuel"] / new_df.loc[i, "Vessel Speed"] / glb_facs.loc[i, "hours per day"]
            new_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = new_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  new_df.loc[i, "Vessel Speed"] / glb_facs.loc[i, "hours per day"]
            new_df.at[i, "Days Loading"]                           = (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - fnl_specs.loc[i, "Moisture"]) / new_df.loc[i, "Vessel Loading rate"]
            new_df.at[i, "Days unloading"]                         = (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - fnl_specs.loc[i, "Moisture"]) / new_df.loc[i, "Vessel Un-Loading rate"]
            new_df.at[i, "Extra Days due to Canal use"]            = canals_detls.loc[i, "Days delay"] * glb_facs.loc[i, "legs per round trip"]
            new_df.at[i, "Lay days allowance (50% each port)"]     = max([(new_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),glb_facs.loc[i, "Minimum Lay days allowed"] ])
            new_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = 0 if fuel_lookup_col == None else fuel_prices.iloc[i, fuel_lookup_col]
            new_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = 0 if aux_fuel_lookup == None else fuel_prices.iloc[i, aux_fuel_lookup]
            new_df.at[i, "Main Fuel Cost"]      = new_df.loc[i, "Days at Avg Speed Main Fuel"] * new_df.loc[i, "Main Engine Fuel burn rate"] * new_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            new_df.at[i, "Auxiliary Fuel Cost"] = new_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * new_df.loc[i, "MDO / MGO burn rate"] * new_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + new_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * new_df.loc[i, "Main Engine Fuel burn rate"] * new_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            new_df.at[i, "FX rate per USD"]     = 0 if fxrate_lookup == None else fxrates.iloc[i, fxrate_lookup]
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
            #print(f'{lookup1(charter_ind, self.trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])}')
            new_df.at[i, "Time Charter Rate"] = lookup1(charter_ind, self.trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            #print(f'sum([{new_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].tolist()}, {glb_facs.loc[i, "legs per round trip"]}*{glb_facs.loc[i, "Lay allowance Loading port"]}]) * {new_df.loc[i, "Time Charter Rate"]}')
            
            new_df.at[i, "Time Charter Cost"] = sum([new_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), glb_facs.loc[i, "legs per round trip"]*glb_facs.loc[i, "Lay allowance Loading port"]]) * new_df.loc[i, "Time Charter Rate"]
            #print(f'np.array({canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()}) * np.array({canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist()})')
            sum1 = np.array(canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            new_df.at[i, "Canal Costs"] = sum(sum1) + sum(sum2)
            #print(f'{new_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum()} + {new_df.loc[i, "Time Charter Cost"]} + {new_df.loc[i, "Canal Costs"]} + {new_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()}')
            new_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = new_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + new_df.loc[i, "Time Charter Cost"] + new_df.loc[i, "Canal Costs"] + new_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            #print(f'{new_df.loc[i, "Total Cost for Leg - before Insurance & Commission"]} * (1+{glb_facs.loc[i, "Freight Insurance Rate"]}) * (1+{glb_facs.loc[i, "Freight Commission"]})')
            new_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = new_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+glb_facs.loc[i, "Freight Insurance Rate"]) * (1+glb_facs.loc[i, "Freight Commission"])
            #print(f'{new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]} == 0 else {new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]}/({frieght_calcs.loc[i, "Vessel dwt"]} * (1 - {fnl_specs.loc[i, "Moisture"]}) * {glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]})\n\n')
            new_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else new_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(frieght_calcs.loc[i, "Vessel dwt"] * (1 - fnl_specs.loc[i, "Moisture"]) * glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        #Actual Price determination from CBIX price
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
                rt_dist_main_fuel = np.nan
                rt_dist_aux_fuel = np.nan
                vl_loading = np.nan
                vl_unloading = np.nan
                main_engine_fuel = np.nan
                auxiliary_fuel = np.nan
                charter_ind = np.nan

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

            cbix_ap_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_ap_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_ap_df.at[i, "Vessel Loading rate"]                    = np.nan if vl_loading == 0  else vl_loading
            cbix_ap_df.at[i, "Vessel Un-Loading rate"]                 = np.nan if vl_unloading == 0  else vl_unloading
            cbix_ap_df.at[i, "Vessel Speed"]                           = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (cbix_coe_glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (cbix_ap_df.loc[i, "Vessel Speed"] ** 2) + cbix_coe_glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (cbix_ap_df.loc[i, "Vessel Speed"] ** 3) + cbix_coe_glb_facs.loc[i, "Main engine burn speed linear coefficient"] * cbix_ap_df.loc[i, "Vessel Speed"])
            v2 = ((cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * cbix_coe_glb_facs.loc[i, "Main engine burn overall correction factor"] + (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            cbix_ap_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            cbix_ap_df.at[i, "MDO / MGO burn rate"]                    = (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            cbix_ap_df.at[i, "Days at Avg Speed Main Fuel"]            = cbix_ap_df.loc[i, "Round trip distance Main fuel"] / cbix_ap_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_ap_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = cbix_ap_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  cbix_ap_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_ap_df.at[i, "Days Loading"]                           = (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_ap_df.loc[i, "Vessel Loading rate"]
            cbix_ap_df.at[i, "Days unloading"]                         = (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_ap_df.loc[i, "Vessel Un-Loading rate"]
            cbix_ap_df.at[i, "Extra Days due to Canal use"]            = cbix_coe_canals_detls.loc[i, "Days delay"] * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_ap_df.at[i, "Lay days allowance (50% each port)"]     = max([(cbix_ap_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed"] ])
            cbix_ap_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = 0 if fuel_lookup_col == None else cbix_coe_fuel_prices.iloc[i, fuel_lookup_col]
            cbix_ap_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = 0 if aux_fuel_lookup == None else cbix_coe_fuel_prices.iloc[i, aux_fuel_lookup]
            cbix_ap_df.at[i, "Main Fuel Cost"]      = cbix_ap_df.loc[i, "Days at Avg Speed Main Fuel"] * cbix_ap_df.loc[i, "Main Engine Fuel burn rate"] * cbix_ap_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            cbix_ap_df.at[i, "Auxiliary Fuel Cost"] = cbix_ap_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * cbix_ap_df.loc[i, "MDO / MGO burn rate"] * cbix_ap_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + cbix_ap_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * cbix_ap_df.loc[i, "Main Engine Fuel burn rate"] * cbix_ap_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            cbix_ap_df.at[i, "FX rate per USD"]     = 0 if fxrate_lookup == None else  cbix_coe_fxrates.iloc[i, fxrate_lookup]
            cbix_ap_df.at[i, "Fixed fee"]        = cbix_coe_imp_port_dets.loc[i, "RMB Fixed fee"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Berthing Charge"]  = cbix_coe_imp_port_dets.loc[i, "RMB per day berthed"] * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Anchorage Charge"] = cbix_coe_imp_port_dets.loc[i, "RMB per day anchored"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Per T_Cargo (wet) Charge"]       = cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/NRT"] = cbix_coe_imp_port_dets.loc[i, "RMB/NRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/NRT/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/NRT/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/GRT"] = cbix_coe_imp_port_dets.loc[i, "RMB/GRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/GRT/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/GRT/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/LOA"] = cbix_coe_imp_port_dets.loc[i, "RMB/LOA"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/LOA/day berthed"]  = cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/LOA/day anchored"] = cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Time Charter Rate"] = lookup1(charter_ind, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            cbix_ap_df.at[i, "Time Charter Cost"] = sum([cbix_ap_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), cbix_coe_glb_facs.loc[i, "legs per round trip"]*cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"]]) * cbix_ap_df.loc[i, "Time Charter Rate"]
            
            sum1 = np.array(cbix_coe_canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(cbix_coe_canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            cbix_ap_df.at[i, "Canal Costs"] = sum(sum1) + sum(sum2)
            cbix_ap_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = cbix_ap_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + cbix_ap_df.loc[i, "Time Charter Cost"] + cbix_ap_df.loc[i, "Canal Costs"] + cbix_ap_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            cbix_ap_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = cbix_ap_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+cbix_coe_glb_facs.loc[i, "Freight Insurance Rate"])
            cbix_ap_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if cbix_ap_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else cbix_ap_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] * (1 - cbix_coe_fnl_specs.loc[i, "Moisture"]) * cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/final_costing_up_for_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/final_costing_up_for_leg.xlsx"] = cbix_ap_df
        

    def freight_calcualtions_actual_port_second_leg(self, folder=None):
        fnl_specs     = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        glb_facs      = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]
        cbix_coe_glb_factors  = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/global_factors.xlsx"]

        new_df      = pd.DataFrame(columns=self.frieght_calcs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.frieght_calcs_columns)

        def lookup(search, target_check, target):
            for ind in range(target_check.shape[0]):
                filt = target[target_check.iloc[:] <= search]
                if filt is None:
                     filt = target[target_check.iloc[:] >= search]
                     return filt.iloc[0]
                else:
                    if len(filt) == 0:
                        return 0
                    return filt.iloc[-1]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Vessel dwt"]  = (fnl_specs.loc[i, "Cargo Tonnage"] if fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1 - fnl_specs.loc[i,"Moisture"])/ glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            new_df.at[i, "Vessel Class for timecharter & fuel burn rates"]  = lookup(new_df.loc[i, "Vessel dwt"], self.vessel_class.loc[:, "DWT< tonnes"], self.vessel_class.loc[:, "Class"])
            new_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(new_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            new_df.at[i, "Estimated LOA (length over all) (m)"] = new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "LOA estimate correlation multiplier"] + glb_facs.loc[i, "LOA estimate correlation constant"]
            new_df.at[i, "Estimated NRT (net register tons)"]   = new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "NRT estimate correlation multiplier"] + glb_facs.loc[i, "NRT estimate correlation constant"]
            new_df.at[i, "Estimated GRT (gross register tons)"] = new_df.loc[i, "Vessel dwt"] * glb_facs.loc[i, "GRT estimate correlation multiplier"] + glb_facs.loc[i, "GRT estimate correlation constant"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Vessel dwt"]  = (cbix_coe_fin_spec.loc[i, "Cargo Tonnage"] if cbix_coe_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fin_spec.loc[i, "South america special transloading cargo tonnage"]) / (1 - cbix_coe_fin_spec.loc[i,"Moisture"])/ cbix_coe_glb_factors.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_ap_df.at[i, "Vessel Class for timecharter & fuel burn rates"]  = lookup(cbix_ap_df.loc[i, "Vessel dwt"], self.vessel_class.loc[:, "DWT< tonnes"], self.vessel_class.loc[:, "Class"])
            cbix_ap_df.at[i, "Vessel Class for distance, canals, loading and unloading rates, fuel prices"] = lookup(cbix_ap_df.loc[i, "Vessel dwt"], self.canals_class.loc[:, "DWT< tonnes"], self.canals_class.loc[:, "Class"])
            cbix_ap_df.at[i, "Estimated LOA (length over all) (m)"] = cbix_ap_df.loc[i, "Vessel dwt"] * cbix_coe_glb_factors.loc[i, "LOA estimate correlation multiplier"] + cbix_coe_glb_factors.loc[i, "LOA estimate correlation constant"]
            cbix_ap_df.at[i, "Estimated NRT (net register tons)"]   = cbix_ap_df.loc[i, "Vessel dwt"] * cbix_coe_glb_factors.loc[i, "NRT estimate correlation multiplier"] + cbix_coe_glb_factors.loc[i, "NRT estimate correlation constant"]
            cbix_ap_df.at[i, "Estimated GRT (gross register tons)"] = cbix_ap_df.loc[i, "Vessel dwt"] * cbix_coe_glb_factors.loc[i, "GRT estimate correlation multiplier"] + cbix_coe_glb_factors.loc[i, "GRT estimate correlation constant"]

        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/freight_calcualtions_actual_port_second_leg.xlsx"] = cbix_ap_df

    def exporting_port_details_loading_rate_second_leg(self, folder=None):
        final_spec        = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.exporting_port_dts_cols)
        cbix_ap_df = pd.DataFrame(columns=self.exporting_port_dts_cols)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_fin_spec.loc[i, "South America special transloading Port"], self.indexes_mines.loc[:, col])

        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details_loading_rate_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/exporting_port_details_loading_rate_second_leg.xlsx"] = cbix_ap_df

    def port_linkages_funct_second_leg(self, folder=None):
        fn_spec           = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]
        
        new_df      = pd.DataFrame(columns=self.port_linkages_cols)
        cbix_ap_df = pd.DataFrame(columns=self.port_linkages_cols)
        
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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                cbix_ap_df.at[i, col] = lookup(
                    cbix_coe_fin_spec.loc[i, "South America special transloading Port"], 
                    (cbix_coe_fin_spec.loc[i, "South America special transloading Port"] if cbix_coe_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No" else cbix_coe_fin_spec.loc[i, "Importing Port"]), 
                    self.cbix_cf_trade_details.loc[i, "Date"].date(), 
                    self.port_linkages.loc[:, col])

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages_second_leg.xlsx"] = cbix_ap_df
        
    def fuel_prices_func_second_leg(self, folder=None):
        port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.xlsx"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages_second_leg.xlsx"]

        frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.xlsx"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/freight_calcualtions_actual_port_second_leg.xlsx"]

        new_df      = pd.DataFrame(columns=self.fuel_prices_cols)
        cbix_ap_df = pd.DataFrame(columns=self.fuel_prices_cols)
        
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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
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

                cbix_ap_df.at[i, col] = lookup(v, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_fuel_prices.loc[:, col] )

        
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/fuel_prices_second_leg.xlsx"] = cbix_ap_df
        
    def importing_port_details_second_leg(self, folder=None):
        final_spec = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fin_spec = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.importing_port_det_cols)
        cbix_ap_df = pd.DataFrame(columns=self.importing_port_det_cols)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                if cbix_coe_fin_spec.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "No":
                    val = cbix_coe_fin_spec.loc[i, "South America special transloading Port"]
                else:
                    val = cbix_coe_fin_spec.loc[i, "Importing Port"]

                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), val, self.china_imp_prts.loc[:, col])

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/importing_port_details_second_leg.xlsx"] = cbix_ap_df
        
    def canals_details_second_leg(self, folder=None):
        port_lkgs              = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.xlsx"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages_second_leg.xlsx"]
        
        frieght_calcs          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.xlsx"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/freight_calcualtions_actual_port_second_leg.xlsx"]
        
        new_df      = pd.DataFrame(columns=self.cost_details_cols)
        cbix_ap_df = pd.DataFrame(columns=self.cost_details_cols)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
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
                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), v, self.canals.loc[:, col])

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_details_second_leg.xlsx"] = cbix_ap_df

    def canals_costs_workings_second_leg(self, folder=None):
        canals_details          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details_second_leg.xlsx"]
        cbix_coe_canals_details = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_details_second_leg.xlsx"]
        
        fc_actual_port          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.xlsx"]
        cbix_coe_fc_actual_port = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/freight_calcualtions_actual_port_second_leg.xlsx"]
        
        glb_facs                = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]
        cbix_coe_glb_facs       = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_global_factors.xlsx"]

        new_df = pd.DataFrame(columns=self.canal_cost_workings_cols)
        cbix_ap_df = pd.DataFrame(columns=self.canal_cost_workings_cols)

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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] = cbix_coe_fc_actual_port.loc[i, "Vessel dwt"] if cbix_coe_canals_details.loc[i, "Tonnage Reference"] == "DWT" else cbix_coe_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_canals_details.loc[i, "Tonnage Reference"] == "NRT" else cbix_coe_fc_actual_port.loc[i, "Estimated NRT (net register tons)"] if cbix_coe_canals_details.loc[i, "Tonnage Reference"] == "GRT" else 0
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 1st"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st"] else cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"]
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 2nd"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 2nd"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 2nd"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st"])
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 3rd"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 3rd"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 3rd"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 2nd"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 4th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 4th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 4th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 3rd"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 5th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 5th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 5th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 4th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 6th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 6th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 6th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 5th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 7th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 7th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 7th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 6th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 8th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 8th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 8th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 7th"].sum())
            cbix_ap_df.at[i, "Amount per Capacity Tarrif Graduations - 9th"] = cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 9th"]  if cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Capacity Tarrif Graduations - 1st":"Capacity Tarrif Graduations - 9th"].sum() else (cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 8th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] = cbix_coe_fc_actual_port.loc[i, "Vessel dwt"] * cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 1st"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st"] else cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"]
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 2nd"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 2nd"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 2nd"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st"])
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 3rd"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 3rd"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 3rd"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 2nd"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 4th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 4th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 4th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 3rd"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 5th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 5th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 5th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 4th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 6th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 6th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 6th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 5th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 7th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 7th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 7th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 6th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 8th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 8th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 8th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 7th"].sum())
            cbix_ap_df.at[i, "Amounts per Cargo Tarrif Graduations - 9th"] = cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 9th"]  if cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] > cbix_coe_canals_details.loc[i, "Cargo Tarrif Graduations - 1st":"Cargo Tarrif Graduations - 9th"].sum() else (cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - Tonnage Reference"] - cbix_ap_df.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 8th"].sum())

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_costs_workings_second_leg.xlsx"] = cbix_ap_df

    def final_costing_up_for_second_leg(self, folder=None):
        fxrates                = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_fx_rates.xlsx"]
        cbix_coe_fxrates       = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_fx_rates.xlsx"]
        
        glb_facs               = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]
        cbix_coe_glb_facs      = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_global_factors.xlsx"]
        
        fuel_prices            = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/fuel_prices_second_leg.xlsx"]
        cbix_coe_fuel_prices   = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/fuel_prices_second_leg.xlsx"]
        
        port_lkgs              = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/port_linkages_second_leg.xlsx"]
        cbix_coe_port_lkgs     = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/port_linkages_second_leg.xlsx"]
        
        canals_detls           = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_details_second_leg.xlsx"]
        cbix_coe_canals_detls  = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_details_second_leg.xlsx"]
        
        canals_costs           = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/canals_costs_workings_second_leg.xlsx"]
        cbix_coe_canals_costs  = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/canals_costs_workings_second_leg.xlsx"]
        
        imp_port_dets          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/importing_port_details_second_leg.xlsx"]
        cbix_coe_imp_port_dets = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/importing_port_details_second_leg.xlsx"]
        
        loading_rates          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/exporting_port_details_loading_rate_second_leg.xlsx"]
        cbix_coe_loading_rates = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/exporting_port_details_loading_rate_second_leg.xlsx"]
        
        frieght_calcs          = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/freight_calcualtions_actual_port_second_leg.xlsx"]
        cbix_coe_frieght_calcs = self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/freight_calcualtions_actual_port_second_leg.xlsx"]
        
        fnl_specs              = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_fnl_specs     = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)
        cbix_ap_df = pd.DataFrame(columns=self.final_costing_up_for_leg_cols)

        fuel_prices_lookup = ["HSFO", "VLSFO", "LNG", "MGO_Regular", "MGO_Low_Sulph"]
        fx_rates_lookup    = ["RMB", "AUD", "USD", "SDR"]

        def lookup(search1, search2, target):
            idx = (self.ship_speeds.loc[:, "Date"].map(lambda x: x.date()) <= search1) #.map(lambda i: 1 if i == True else 0)            
            ind = (self.ship_speeds.loc[:, "Vesssel class"] == search2)
            v = (ind & idx)
            try:
                return target[v].tolist()[-1]
            except Exception:
                return 0

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
                rt_dist_main_fuel = np.nan
                rt_dist_aux_fuel = np.nan
                vl_loading = np.nan
                vl_unloading = np.nan
                main_engine_fuel = np.nan
                auxiliary_fuel = np.nan
                charter_ind = np.nan
                
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
                rt_dist_main_fuel = np.nan
                rt_dist_aux_fuel = np.nan
                vl_loading = np.nan
                vl_unloading = np.nan
                main_engine_fuel = np.nan
                auxiliary_fuel = np.nan
                charter_ind = np.nan
                
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
                    
            cbix_ap_df.at[i, "Round trip distance Main fuel"] = rt_dist_main_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_ap_df.at[i, "Round Trip Distance on Auxiliary fuel"]  = rt_dist_aux_fuel * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_ap_df.at[i, "Vessel Loading rate"]                    = np.nan if vl_loading == 0 else vl_loading
            cbix_ap_df.at[i, "Vessel Un-Loading rate"]                 = np.nan if vl_unloading == 0 else vl_unloading
            cbix_ap_df.at[i, "Vessel Speed"]                           = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_frieght_calcs.loc[i, "Vessel Class for timecharter & fuel burn rates"],self.ship_speeds.loc[:, "Vessel Cruising speed"])
            v1 = (cbix_coe_glb_facs.loc[i, "Main engine burn speed ^2 coefficient"] * (cbix_ap_df.loc[i, "Vessel Speed"] ** 2) + cbix_coe_glb_facs.loc[i, "Main engine burn speed ^3 coefficient"] * (cbix_ap_df.loc[i, "Vessel Speed"] ** 3) + cbix_coe_glb_facs.loc[i, "Main engine burn speed linear coefficient"] * cbix_ap_df.loc[i, "Vessel Speed"])
            v2 = ((cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) ** cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT exponent"]) * cbix_coe_glb_facs.loc[i, "Main engine burn overall correction factor"] + (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base slope"] + cbix_coe_glb_facs.loc[i, "Main engine burn vessel DWT base constant"]
            cbix_ap_df.at[i, "Main Engine Fuel burn rate"]             = ( v1 * v2 )
            cbix_ap_df.at[i, "MDO / MGO burn rate"]                    = (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) * cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT slope"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT constant"] + cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT ^2 coefficient"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] / cbix_coe_glb_facs.loc[i, "MDO/MGO burn vessel DWT denominator"]) ** 2
            cbix_ap_df.at[i, "Days at Avg Speed Main Fuel"]            = 0 if pd.isna(cbix_ap_df.loc[i, "Round trip distance Main fuel"] / cbix_ap_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]) else cbix_ap_df.loc[i, "Round trip distance Main fuel"] / cbix_ap_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_ap_df.at[i, "Days at Avg Speed Auxiliary Fuel"]       = 0 if pd.isna(cbix_ap_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  cbix_ap_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]) else cbix_ap_df.loc[i, "Round Trip Distance on Auxiliary fuel"] /  cbix_ap_df.loc[i, "Vessel Speed"] / cbix_coe_glb_facs.loc[i, "hours per day"]
            cbix_ap_df.at[i, "Days Loading"]                           = 0 if cbix_ap_df.loc[i, "Vessel Loading rate"] == 0 else (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_ap_df.loc[i, "Vessel Loading rate"]
            cbix_ap_df.at[i, "Days unloading"]                         = 0 if cbix_ap_df.loc[i, "Vessel Un-Loading rate"] == 0 else (cbix_coe_fnl_specs.loc[i, "Cargo Tonnage"] if cbix_coe_fnl_specs.loc[i, "Double Leg Shipping? (South America special transload leg)"] == "Yes" else cbix_coe_fnl_specs.loc[i, "South america special transloading cargo tonnage"]) / (1.0 - cbix_coe_fnl_specs.loc[i, "Moisture"]) / cbix_ap_df.loc[i, "Vessel Un-Loading rate"]
            cbix_ap_df.at[i, "Extra Days due to Canal use"]            = cbix_coe_canals_detls.loc[i, "Days delay"] * cbix_coe_glb_facs.loc[i, "legs per round trip"]
            cbix_ap_df.at[i, "Lay days allowance (50% each port)"]     = 0 if cbix_ap_df.loc[i, "Vessel Un-Loading rate"] == 0 else max([(cbix_ap_df.loc[i, "Days at Avg Speed Main Fuel":"Extra Days due to Canal use"].sum() * cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed as % of sailing time"]),cbix_coe_glb_facs.loc[i, "Minimum Lay days allowed"] ])
            cbix_ap_df.at[i, "Main Fuel HFO / VLSFO Fuel Price"]       = 0 if fuel_lookup_col == None else cbix_coe_fuel_prices.iloc[i, fuel_lookup_col]
            cbix_ap_df.at[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] = 0 if aux_fuel_lookup == None else cbix_coe_fuel_prices.iloc[i, aux_fuel_lookup]
            cbix_ap_df.at[i, "Main Fuel Cost"]      = cbix_ap_df.loc[i, "Days at Avg Speed Main Fuel"] * cbix_ap_df.loc[i, "Main Engine Fuel burn rate"] * cbix_ap_df.loc[i, "Main Fuel HFO / VLSFO Fuel Price"]
            cbix_ap_df.at[i, "Auxiliary Fuel Cost"] = cbix_ap_df.loc[i, "Days Loading":"Lay days allowance (50% each port)"].sum() * cbix_ap_df.loc[i, "MDO / MGO burn rate"] * cbix_ap_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"] + cbix_ap_df.loc[i, "Days at Avg Speed Auxiliary Fuel"] * cbix_ap_df.loc[i, "Main Engine Fuel burn rate"] * cbix_ap_df.loc[i, "Auxiliary Fuel (MDO / MGO / LNG) Price"]
            cbix_ap_df.at[i, "FX rate per USD"]     = 0 if (cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 or fxrate_lookup == None) else cbix_coe_fxrates.iloc[i, fxrate_lookup]            
            cbix_ap_df.at[i, "Fixed fee"]        = 0 if (cbix_coe_imp_port_dets.loc[i, "RMB Fixed fee"] == 0 and cbix_ap_df.loc[i, "FX rate per USD"] == 0) else cbix_coe_imp_port_dets.loc[i, "RMB Fixed fee"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Berthing Charge"]  = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else  cbix_coe_imp_port_dets.loc[i, "RMB per day berthed"] * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Anchorage Charge"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB per day anchored"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Per T_Cargo (wet) Charge"]       = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/T_Cargo (wet)/day berthed"]  = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day berthed"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/T_Cargo (wet)/day anchored"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/T_Cargo (wet)/day anchored"] * (cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] *  cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"]) * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/NRT"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/NRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/NRT/day berthed"]  = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"]  * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/NRT/day anchored"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/NRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated NRT (net register tons)"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/GRT"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/GRT"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/GRT/day berthed"]  = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"]  * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/GRT/day anchored"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/GRT/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated GRT (gross register tons)"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/LOA"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/LOA"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/LOA/day berthed"]  = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day berthed"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"]  * sum([cbix_ap_df.loc[i, "Days unloading"], cbix_coe_glb_facs.loc[i, "Extra time tie up + untie at each port"]]) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "RMB/LOA/day anchored"] =0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_coe_imp_port_dets.loc[i, "RMB/LOA/day anchored"] * cbix_coe_frieght_calcs.loc[i, "Estimated LOA (length over all) (m)"] * (cbix_ap_df.loc[i, "Lay days allowance (50% each port)"] * (1 - cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"])) / cbix_ap_df.loc[i, "FX rate per USD"]
            cbix_ap_df.at[i, "Time Charter Rate"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else lookup1(charter_ind, self.cbix_cf_trade_details.loc[i, "Date"].date(), self.ship_time_cr.loc[:, "Vessel Time Charter Rates"])
            cbix_ap_df.at[i, "Time Charter Cost"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else sum([cbix_ap_df.loc[i, "Days at Avg Speed Main Fuel":"Lay days allowance (50% each port)"].sum(), cbix_coe_glb_facs.loc[i, "legs per round trip"]*cbix_coe_glb_facs.loc[i, "Lay allowance Loading port"]]) * cbix_ap_df.loc[i, "Time Charter Rate"]            
            sum1 = np.array(cbix_coe_canals_detls.loc[i, "Capacity Tarrifs - 1st":"Capacity Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amount per Capacity Tarrif Graduations - 1st":"Amount per Capacity Tarrif Graduations - 9th"].tolist())
            sum2 = np.array(cbix_coe_canals_detls.loc[i, "Cargo Tarrifs - 1st":"Cargo Tarrifs - 9th"].tolist()) * np.array(cbix_coe_canals_costs.loc[i, "Amounts per Cargo Tarrif Graduations - 1st":"Amounts per Cargo Tarrif Graduations - 9th"].tolist())
            cbix_ap_df.at[i, "Canal Costs"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else sum(sum1) + sum(sum2)
            cbix_ap_df.at[i, "Total Cost for Leg - before Insurance & Commission"] = 0 if cbix_ap_df.loc[i, "Round trip distance Main fuel"] == 0 else cbix_ap_df.loc[i, "Main Fuel Cost":"Auxiliary Fuel Cost"].sum() + cbix_ap_df.loc[i, "Time Charter Cost"] + cbix_ap_df.loc[i, "Canal Costs"] + cbix_ap_df.loc[i, "Fixed fee":"RMB/LOA/day anchored"].sum()
            cbix_ap_df.at[i, "Total Cost for Leg - with Insurance & Commission"]   = cbix_ap_df.loc[i, "Total Cost for Leg - before Insurance & Commission"] * (1+cbix_coe_glb_facs.loc[i, "Freight Insurance Rate"])
            cbix_ap_df.at[i, "Per tonne cargo incl. Insurance & Commission"]       = 0 if cbix_ap_df.loc[i, "Total Cost for Leg - with Insurance & Commission"] == 0 else cbix_ap_df.loc[i, "Total Cost for Leg - with Insurance & Commission"]/(cbix_coe_frieght_calcs.loc[i, "Vessel dwt"] * (1 - cbix_coe_fnl_specs.loc[i, "Moisture"]) * cbix_coe_glb_facs.loc[i, "Max % of vessel deadweight allowed for loading"])

        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/final_costing_up_for_second_leg.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/{'frieght_calcs_to_actual_nominated_port' if folder == None else folder}/actual_price_determination_from_CBIX_price/final_costing_up_for_second_leg.xlsx"] =cbix_ap_df

    def final_freight_values_func(self):
        actual_nominated_port_final_costing_up_for_leg                 = self.db[f"outputs/{self.freight_table_value}/frieght_calcs_to_actual_nominated_port/final_costing_up_for_leg.xlsx"]
        cbix_coe_actual_nominated_port_final_costing_up_for_leg        = self.db[f"outputs/{self.freight_table_value}/frieght_calcs_to_actual_nominated_port/actual_price_determination_from_CBIX_price/final_costing_up_for_leg.xlsx"]
        
        actual_nominated_port_final_costing_up_for_second_leg          = self.db[f"outputs/{self.freight_table_value}/frieght_calcs_to_actual_nominated_port/final_costing_up_for_second_leg.xlsx"]
        cbix_coe_actual_nominated_port_final_costing_up_for_second_leg = self.db[f"outputs/{self.freight_table_value}/frieght_calcs_to_actual_nominated_port/actual_price_determination_from_CBIX_price/final_costing_up_for_second_leg.xlsx"]
        
        qingdao_leg                  = self.db[f"outputs/{self.freight_table_value}/freight_calcs_to_qingdao_first_leg/final_costing_up_for_leg.xlsx"]
        cbix_coe_qingdao_leg         = self.db[f"outputs/{self.freight_table_value}/freight_calcs_to_qingdao_first_leg/actual_price_determination_from_CBIX_price/final_costing_up_for_leg.xlsx"]
        
        qingdao_second_leg           = self.db[f"outputs/{self.freight_table_value}/freight_calcs_to_qingdao_first_leg/final_costing_up_for_second_leg.xlsx"]
        cbix_coe_qingdao_second_leg  = self.db[f"outputs/{self.freight_table_value}/freight_calcs_to_qingdao_first_leg/actual_price_determination_from_CBIX_price/final_costing_up_for_second_leg.xlsx"]
        
        new_df      = pd.DataFrame(columns=["Qingdao Freight", "Actual Port Freight", "Freight Differential"])
        cbix_ap_df = pd.DataFrame(columns=["Qingdao Freight", "Actual Port Freight", "Freight Differential"])


        for i in range(len(self.trade_details)):
            #print(f'{qingdao_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]}, {qingdao_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]}')
            new_df.at[i, "Qingdao Freight"] = sum([qingdao_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], qingdao_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            new_df.at[i, "Actual Port Freight"] = sum([actual_nominated_port_final_costing_up_for_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], actual_nominated_port_final_costing_up_for_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            new_df.at[i, "Freight Differential"] = new_df.loc[i, "Qingdao Freight"] - new_df.loc[i, "Actual Port Freight"]
            
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Qingdao Freight"] = sum([cbix_coe_qingdao_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], cbix_coe_qingdao_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            cbix_ap_df.at[i, "Actual Port Freight"] = sum([cbix_coe_actual_nominated_port_final_costing_up_for_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"], cbix_coe_actual_nominated_port_final_costing_up_for_second_leg.loc[i, "Per tonne cargo incl. Insurance & Commission"]])
            cbix_ap_df.at[i, "Freight Differential"] = cbix_ap_df.loc[i, "Qingdao Freight"] - cbix_ap_df.loc[i, "Actual Port Freight"]
        
        self.db[f"outputs/{self.freight_table_value}/final_freight_values.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_freight_values.xlsx"] = cbix_ap_df

    def final_specifications_to_viu_adjustment_continued1(self):
        glb_factors                   = self.db[f"outputs/{self.freight_table_value}/global_factors.xlsx"]
        cbix_coe_glb_factors          = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/global_factors.xlsx"]

        nominal_mine                  = self.db[f"outputs/{self.freight_table_value}/nominal_mine_div_index_specifications.xlsx"]
        cbix_coe_nominal_mine         = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/nominal_mine_div_index_specifications.xlsx"]

        final_freight_values          = self.db[f"outputs/{self.freight_table_value}/final_freight_values.xlsx"]
        cbix_coe_final_freight_values = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_freight_values.xlsx"]

        new_df               = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_ap_df          = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Freight (adjusted to Qingdao)"] = final_freight_values.loc[i, "Qingdao Freight"]            
            #print(f"Freight (adjusted to Qingdao)1 at {i} = ", new_df.loc[i, "Freight (adjusted to Qingdao)"])
            new_df.at[i, "Price FOB"] = self.trade_details.loc[i, "Price"] if (self.trade_details.loc[i, "Price Type"]== "FOB" and self.trade_details.loc[i, "Price Basis"]== "dmt") else self.trade_details.loc[i, "Price"] / (1 - new_df.loc[i, "Moisture"]) if (self.trade_details.loc[i, "Price Type"]== "FOB" and self.trade_details.loc[i, "Price Basis"]== "wmt") else new_df.loc[i, "Price CIF (Port adjustyed to Qingdao)"] - new_df.loc[i, "Freight (adjusted to Qingdao)"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Freight (adjusted to Qingdao)"] = cbix_coe_final_freight_values.loc[i, "Qingdao Freight"]
            #cbix_ap_df.at[i, "Price CIF (Port adjustyed to Qingdao)"] = (self.cbix_cf_trade_details.loc[i, "Price"] * (1 if (self.cbix_cf_trade_details.loc[i, "Price Type"] == "CIF") else (1+ cbix_coe_glb_factors.loc[i, "Freight Insurance Rate"])) + cbix_coe_final_freight_values.loc[i, "Freight Differential"]) if ((self.cbix_cf_trade_details.loc[i, "Price Type"] < "FOB" or self.cbix_cf_trade_details.loc[i, "Price Type"] > "FOB") and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "dmt") else self.cbix_cf_trade_details.loc[i, "Price"] * (1 if (self.cbix_cf_trade_details.loc[i, "Price Type"] == "CIF") else (1+ cbix_coe_glb_factors.loc[i, "Freight Insurance Rate"])) / (1-cbix_ap_df.loc[i, "Moisture"]) if ((self.cbix_cf_trade_details.loc[i, "Price Type"] < "FOB" or self.cbix_cf_trade_details.loc[i, "Price Type"] > "FOB") and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "wmt") else cbix_ap_df.loc[i, "Price FOB"] + cbix_ap_df.loc[i, "Freight (adjusted to Qingdao)"]
            cbix_ap_df.at[i, "Price FOB"] = self.cbix_cf_trade_details.loc[i, "Price"] if (self.cbix_cf_trade_details.loc[i, "Price Type"]== "FOB" and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "dmt") else self.cbix_cf_trade_details.loc[i, "Price"] / (1 - cbix_ap_df.loc[i, "Moisture"]) if (self.cbix_cf_trade_details.loc[i, "Price Type"]== "FOB" and self.cbix_cf_trade_details.loc[i, "Price Basis"]== "wmt") else cbix_ap_df.loc[i, "Price CIF (Port adjustyed to Qingdao)"] - cbix_ap_df.loc[i, "Freight (adjusted to Qingdao)"]

        self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"] = cbix_ap_df


    def bauxite_details_input_func_continued1(self):
        final_specs          = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_final_specs = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df           = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        cbix_ap_df      = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]


        for i in range(len(self.trade_details)):
            new_df.at[i, "Price"] = final_specs.loc[i, "Price CIF (Port adjustyed to Qingdao)"]
            
        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Price"] = cbix_coe_final_specs.loc[i, "Price CIF (Port adjustyed to Qingdao)"]

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/cbauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"] = cbix_ap_df


    def alumina_production_cost_calcs_ece_continued1(self):
        bx_details          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        cbix_coe_bx_details = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]

        china_prc           = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]
        cbix_coe_china_prc  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]

        china_inps          = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_china_input_prices.xlsx"]
        cbix_coe_china_inps = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_china_input_prices.xlsx"]

        new_df          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        cbix_ap_df     = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        for i in range(len(self.trade_details)):
            new_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (bx_details.loc[i, "Price"] + bx_details.loc[i, "Processing Penalties"]) * new_df.loc[i, "Tonnes per Tonne"]
            new_df.at[i, "Caustic cost"]          = new_df.loc[i, "Caustic Use t.NAOH / t.AA"] * china_inps.loc[i, "Caustic Price"]
            new_df.at[i, "Thermal Energy Cost"]   = china_prc.loc[i, "Lig Coal (GJ/t)"] * china_inps.loc[i, "Energy Price"]
            new_df.at[i, "Lime Cost"]             = china_prc.loc[i, "Lime rate (wt/wt_AA)"] * china_inps.loc[i, "Lime Price"]
            new_df.at[i, "Mud make"]              = (new_df.loc[i, "Tonnes per Tonne"] - 1 ) + new_df.loc[i, "Caustic Use t.NAOH / t.AA"] + china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            new_df.at[i, "Mud Disposal Cost"]     = new_df.loc[i, "Mud make"] * china_inps.loc[i, "Mud Disposal Cost"]
            new_df.at[i, "Total Cost"]            = new_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + new_df.loc[i, "Caustic cost"] + new_df.loc[i, "Thermal Energy Cost"] + new_df.loc[i, "Lime Cost"] + new_df.loc[i, "Mud Disposal Cost"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_ap_df.loc[i, "Tonnes per Tonne"]
            cbix_ap_df.at[i, "Caustic cost"]          = cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_ap_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_ap_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_ap_df.at[i, "Mud make"]              = (cbix_ap_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_ap_df.at[i, "Mud Disposal Cost"]     = cbix_ap_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_ap_df.at[i, "Total Cost"]            = cbix_ap_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_ap_df.loc[i, "Caustic cost"] + cbix_ap_df.loc[i, "Thermal Energy Cost"] + cbix_ap_df.loc[i, "Lime Cost"] + cbix_ap_df.loc[i, "Mud Disposal Cost"]
        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = cbix_ap_df
        

    #SPECIFIC INDEX

    def spec_index_bauxite_details_func(self):
        final_specs = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_final_specs = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame()
        cbix_ap_df = pd.DataFrame()


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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            cbix_ap_df.at[i, "Total Alumina"]             = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Alumina"])
            cbix_ap_df.at[i, "LT Avail. Alumina"]         = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            cbix_ap_df.at[i, "Total Silica"]              = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Total Silica"])
            cbix_ap_df.at[i, "LT R.Silica"]               = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            cbix_ap_df.at[i, "Quartz / HT Silica"]        = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            cbix_ap_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            cbix_ap_df.at[i, "Moisture"]                  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Moisture"])
            cbix_ap_df.at[i, "Processing"]                = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing"])
            cbix_ap_df.at[i, "Processing Penalties"]      = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Specific Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_index_bauxite_details.xlsx"] = cbix_ap_df

    def spec_index_china_processing_factors_func(self):
        baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_coe_baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_index_bauxite_details.xlsx"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_china_processing_factors.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_china_processing_factors.xlsx"] = cbix_ap_df

    def spec_index_alumina_production_cost_calcs_ece(self):
        bx_details          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_coe_bx_details = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_index_bauxite_details.xlsx"]
        
        china_prc           = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_china_processing_factors.xlsx"]
        cbix_coe_china_prc  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_china_processing_factors.xlsx"]
        
        china_inps          = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_china_input_prices.xlsx"]
        cbix_coe_china_inps = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_china_input_prices.xlsx"]
        
        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_ap_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_ap_df.at[i, "Available Alumina"]    =  cbix_ap_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Reactive Silica"]
            cbix_ap_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_ap_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_ap_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_ap_df.loc[i, "Tonnes per Tonne"]
            cbix_ap_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_ap_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_ap_df.at[i, "Caustic cost"]          = cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_ap_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_ap_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]            
            cbix_ap_df.at[i, "Mud make"]              = (cbix_ap_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"]            
            cbix_ap_df.at[i, "Mud Disposal Cost"]     = cbix_ap_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]        
            cbix_ap_df.at[i, "Total Cost"]            = cbix_ap_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_ap_df.loc[i, "Caustic cost"] + cbix_ap_df.loc[i, "Thermal Energy Cost"] + cbix_ap_df.loc[i, "Lime Cost"] + cbix_ap_df.loc[i, "Mud Disposal Cost"]
        

        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = cbix_ap_df


    #GENERAL INDEX

    def general_index_bauxite_details_func(self):
        final_specs = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_final_specs = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame()
        cbix_ap_df = pd.DataFrame()

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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            cbix_ap_df.at[i, "Total Alumina"]             = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Alumina"])
            cbix_ap_df.at[i, "LT Avail. Alumina"]         = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            cbix_ap_df.at[i, "Total Silica"]              = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Total Silica"])
            cbix_ap_df.at[i, "LT R.Silica"]               = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "LT R.Silica"])
            cbix_ap_df.at[i, "Quartz / HT Silica"]        = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            cbix_ap_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            cbix_ap_df.at[i, "Moisture"]                  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Moisture"])
            cbix_ap_df.at[i, "Processing"]                = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing"])
            cbix_ap_df.at[i, "Processing Penalties"]      = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "General Index"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_index_bauxite_details.xlsx"] = cbix_ap_df

    def general_index_china_processing_factors_func(self):
        baux_dets_inputs          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_coe_baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_index_bauxite_details.xlsx"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_china_processing_factors.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_china_processing_factors.xlsx"] = cbix_ap_df

    def general_index_alumina_production_cost_calcs_ece(self):
        bx_details             = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_coe_bx_details    = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_index_bauxite_details.xlsx"]

        china_prc              = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_china_processing_factors.xlsx"]
        cbix_coe_china_prc     = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_china_processing_factors.xlsx"]

        china_prc_tb           = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]
        cbix_coe_china_prc_tb  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]

        china_inps             = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_china_input_prices.xlsx"]
        cbix_coe_china_inps    = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_china_input_prices.xlsx"]

        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_ap_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_ap_df.at[i, "Available Alumina"]    =  cbix_ap_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Reactive Silica"]
            cbix_ap_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_ap_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_ap_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_ap_df.loc[i, "Tonnes per Tonne"]
            cbix_ap_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_ap_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_ap_df.at[i, "Caustic cost"]          = cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_ap_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_ap_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]            
            cbix_ap_df.at[i, "Mud make"]              = (cbix_ap_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_ap_df.at[i, "Mud Disposal Cost"]     = cbix_ap_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]            
            cbix_ap_df.at[i, "Total Cost"]            = cbix_ap_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_ap_df.loc[i, "Caustic cost"] + cbix_ap_df.loc[i, "Thermal Energy Cost"] + cbix_ap_df.loc[i, "Lime Cost"] + cbix_ap_df.loc[i, "Mud Disposal Cost"]

        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = cbix_ap_df


    # OLD CBIX
    def old_cbix_bauxite_details_input_func(self):
        final_spec          = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_final_spec = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame(columns=self.bauxite_details_columns)
        cbix_ap_df = pd.DataFrame(columns=self.bauxite_details_columns)


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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Price"]                   = cbix_coe_final_spec.loc[i, "Price CIF (Port adjustyed to Qingdao)"]
            cbix_ap_df.at[i, "Total Alumina"]           = cbix_coe_final_spec.loc[i, "Total Alumina"]
            cbix_ap_df.at[i, "LT Avail. Alumina"]       = cbix_coe_final_spec.loc[i, "LT Avail. Alumina"]
            cbix_ap_df.at[i, "Total Silica"]            = cbix_coe_final_spec.loc[i, "Total Silica"]
            cbix_ap_df.at[i, "LT R.Silica"]             = cbix_coe_final_spec.loc[i, "LT R.Silica"]
            cbix_ap_df.at[i, "Quartz / HT Silica"]      = cbix_coe_final_spec.loc[i, "Quartz / HT Silica"]
            cbix_ap_df.at[i, "Mono-hydrate / HT Alumina"] = cbix_coe_final_spec.loc[i, "Mono-hydrate / HT Extble Alumina"]
            cbix_ap_df.at[i, "Moisture"]                = cbix_coe_final_spec.loc[i, "Moisture"]
            cbix_ap_df.at[i, "Processing"]              = cbix_coe_final_spec.loc[i, "Processing"]
            cbix_ap_df.at[i, "Processing Penalties"]    = 0

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"] = cbix_ap_df


    def old_cbix_china_processing_factors_func(self):
        baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        cbix_coe_baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)


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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                if col == "Extraction Efficiency %":
                    cbix_ap_df.at[i, col] = 0.92
                else:
                    cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"] = cbix_ap_df

    def old_cbix_alumina_production_cost_calcs_ece(self):
        bx_details          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        cbix_coe_bx_details = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]

        china_prc           = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]
        cbix_coe_china_prc  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]

        china_inps          = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_china_input_prices.xlsx"]
        cbix_coe_china_inps = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_china_input_prices.xlsx"]

        new_df      = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

        for i in range(len(self.trade_details)):
            new_df.at[i, "Reactive Alumina"]     =  (bx_details.loc[i, "Total Alumina"] - bx_details.loc[i, "LT Avail. Alumina"] - bx_details.loc[i, "LT R.Silica"]) * china_prc.loc[i, "HT Alumina Dissolution"] + bx_details.loc[i, "LT Avail. Alumina"] + bx_details.loc[i, "LT R.Silica"]
            new_df.at[i, "Reactive Silica"]      =  bx_details.loc[i, "LT R.Silica"] + china_prc.loc[i, "Quartz Attack"] * (bx_details.loc[i, "Total Silica"] - bx_details.loc[i, "LT R.Silica"])
            new_df.at[i, "Available Alumina"]    =  new_df.loc[i, "Reactive Alumina"] - china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * new_df.loc[i, "Reactive Silica"]
            new_df.at[i, "Tonnes per Tonne"]     =  1 / (np.nan if new_df.loc[i, "Available Alumina"] == 0 else new_df.loc[i, "Available Alumina"]) / (np.nan if china_prc.loc[i, "Extraction Efficiency %"] == 0 else china_prc.loc[i, "Extraction Efficiency %"])
            new_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (bx_details.loc[i, "Price"] + bx_details.loc[i, "Processing Penalties"]) * new_df.loc[i, "Tonnes per Tonne"]
            new_df.at[i, "Caustic Use t.NAOH / t.AA"] = new_df.loc[i, "Reactive Silica"] * china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * new_df.loc[i, "Tonnes per Tonne"] + china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            new_df.at[i, "Caustic cost"]          = new_df.loc[i, "Caustic Use t.NAOH / t.AA"] * china_inps.loc[i, "Caustic Price"]
            new_df.at[i, "Thermal Energy Cost"]   = china_prc.loc[i, "Lig Coal (GJ/t)"] * china_inps.loc[i, "Energy Price"]
            new_df.at[i, "Lime Cost"]             = china_prc.loc[i, "Lime rate (wt/wt_AA)"] * china_inps.loc[i, "Lime Price"]
            new_df.at[i, "Mud make"]              = (new_df.loc[i, "Tonnes per Tonne"] - 1 ) + new_df.loc[i, "Caustic Use t.NAOH / t.AA"] + china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            new_df.at[i, "Mud Disposal Cost"]     = new_df.loc[i, "Mud make"] * china_inps.loc[i, "Mud Disposal Cost"]
            new_df.at[i, "Total Cost"]            = new_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + new_df.loc[i, "Caustic cost"] + new_df.loc[i, "Thermal Energy Cost"] + new_df.loc[i, "Lime Cost"] + new_df.loc[i, "Mud Disposal Cost"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_ap_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_ap_df.at[i, "Available Alumina"]    =  cbix_ap_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Reactive Silica"]            
            cbix_ap_df.at[i, "Tonnes per Tonne"]     =  1 / (np.nan if cbix_ap_df.loc[i, "Available Alumina"] == 0 else cbix_ap_df.loc[i, "Available Alumina"]) / (np.nan if cbix_coe_china_prc.loc[i, "Extraction Efficiency %"] == 0 else cbix_coe_china_prc.loc[i, "Extraction Efficiency %"])
            cbix_ap_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_ap_df.loc[i, "Tonnes per Tonne"]
            cbix_ap_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_ap_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_ap_df.at[i, "Caustic cost"]          = cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_ap_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_ap_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_ap_df.at[i, "Mud make"]              = (cbix_ap_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_ap_df.at[i, "Mud Disposal Cost"]     = cbix_ap_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_ap_df.at[i, "Total Cost"]            = cbix_ap_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_ap_df.loc[i, "Caustic cost"] + cbix_ap_df.loc[i, "Thermal Energy Cost"] + cbix_ap_df.loc[i, "Lime Cost"] + cbix_ap_df.loc[i, "Mud Disposal Cost"]
        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = cbix_ap_df

    # OLD CBIX CALCULATIONS
    def old_cbix_calculation_bauxite_details_func(self):
        final_specs          = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_final_specs = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]

        new_df      = pd.DataFrame()
        cbix_ap_df = pd.DataFrame()

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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Calculated Index Price Equivalent"]   = np.nan #Calculate later
            cbix_ap_df.at[i, "Total Alumina"]             = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Alumina"])
            cbix_ap_df.at[i, "LT Avail. Alumina"]         = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT Avail. Alumina"])
            cbix_ap_df.at[i, "Total Silica"]              = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Total Silica"])
            cbix_ap_df.at[i, "LT R.Silica"]               = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "LT R.Silica"])
            cbix_ap_df.at[i, "Quartz / HT Silica"]        = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Quartz / HT Silica"])
            cbix_ap_df.at[i, "Mono-hydrate / HT Alumina"] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Mono-hydrate / HT Alumina"])
            cbix_ap_df.at[i, "Moisture"]                  = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Moisture"])
            cbix_ap_df.at[i, "Processing"]                = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing"])
            cbix_ap_df.at[i, "Processing Penalties"]      = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_final_specs.loc[i, "Old CBIX type Calc"], self.indexes_mines.loc[:, "Processing Penalties to be applied"])
            
            
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_index_bauxite_details.xlsx"] = cbix_ap_df

    def old_cbix_calculation_china_processing_factors_func(self):
        baux_dets_inputs          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.xlsx"]
        cbix_coe_baux_dets_inputs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_index_bauxite_details.xlsx"]

        new_df      = pd.DataFrame(columns=self.china_prcs_factrs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.china_prcs_factrs_columns)

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

        for i in range(len(self.cbix_cf_trade_details)):
            for col in cbix_ap_df.columns:
                if col == "Extraction Efficiency %":
                    cbix_ap_df.at[i, col] = 0.92
                else:
                    cbix_ap_df.at[i, col] = lookup(self.cbix_cf_trade_details.loc[i, "Date"].date(), cbix_coe_baux_dets_inputs.loc[i, "Processing"], self.processing_factors.loc[:, col])

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"] = cbix_ap_df

    def old_cbix_calculation_alumina_production_cost_calcs_ece(self):
        bx_details          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.xlsx"]
        cbix_coe_bx_details = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_index_bauxite_details.xlsx"]

        china_prc           = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]
        cbix_coe_china_prc  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]

        china_prc_tb           = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]
        cbix_coe_china_prc_tb  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_china_processing_factors.xlsx"]

        china_inps          = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/common_data_inputs_china_input_prices.xlsx"]
        cbix_coe_china_inps = self.db[f"outputs/{self.freight_table_value}/common_data_inputs/actual_price_determination_from_CBIX_price/common_data_inputs_china_input_prices.xlsx"]

        new_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)
        cbix_ap_df = pd.DataFrame(columns=self.alumina_prod_costs_columns)

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

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Reactive Alumina"]     =  (cbix_coe_bx_details.loc[i, "Total Alumina"] - cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] - cbix_coe_bx_details.loc[i, "LT R.Silica"]) * cbix_coe_china_prc.loc[i, "HT Alumina Dissolution"] + cbix_coe_bx_details.loc[i, "LT Avail. Alumina"] + cbix_coe_bx_details.loc[i, "LT R.Silica"]
            cbix_ap_df.at[i, "Reactive Silica"]      =  cbix_coe_bx_details.loc[i, "LT R.Silica"] + cbix_coe_china_prc.loc[i, "Quartz Attack"] * (cbix_coe_bx_details.loc[i, "Total Silica"] - cbix_coe_bx_details.loc[i, "LT R.Silica"])
            cbix_ap_df.at[i, "Available Alumina"]    =  cbix_ap_df.loc[i, "Reactive Alumina"] - cbix_coe_china_prc.loc[i, "DSP Al2O3:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Reactive Silica"]
            cbix_ap_df.at[i, "Tonnes per Tonne"]     =  1 / cbix_ap_df.loc[i, "Available Alumina"] / cbix_coe_china_prc.loc[i, "Extraction Efficiency %"]
            cbix_ap_df.at[i, "Bauxite Cost Incl. Processing Penalties"]  = 0 # (cbix_coe_bx_details.loc[i, "Price"] + cbix_coe_bx_details.loc[i, "Processing Penalties"]) * cbix_ap_df.loc[i, "Tonnes per Tonne"]
            cbix_ap_df.at[i, "Caustic Use t.NAOH / t.AA"] = cbix_ap_df.loc[i, "Reactive Silica"] * cbix_coe_china_prc.loc[i, "DSP NaOH:SiO2 (wt/wt)"] * cbix_ap_df.loc[i, "Tonnes per Tonne"] + cbix_coe_china_prc.loc[i, "Caustic wash loss (t.NaOH/t.aa)"]
            cbix_ap_df.at[i, "Caustic cost"]          = cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] * cbix_coe_china_inps.loc[i, "Caustic Price"]
            cbix_ap_df.at[i, "Thermal Energy Cost"]   = cbix_coe_china_prc.loc[i, "Lig Coal (GJ/t)"] * cbix_coe_china_inps.loc[i, "Energy Price"]
            cbix_ap_df.at[i, "Lime Cost"]             = cbix_coe_china_prc.loc[i, "Lime rate (wt/wt_AA)"] * cbix_coe_china_inps.loc[i, "Lime Price"]
            cbix_ap_df.at[i, "Mud make"]              = (cbix_ap_df.loc[i, "Tonnes per Tonne"] - 1 ) + cbix_ap_df.loc[i, "Caustic Use t.NAOH / t.AA"] + cbix_coe_china_prc_tb.loc[i, "Lime rate (wt/wt_AA)"]
            cbix_ap_df.at[i, "Mud Disposal Cost"]     = cbix_ap_df.loc[i, "Mud make"] * cbix_coe_china_inps.loc[i, "Mud Disposal Cost"]
            cbix_ap_df.at[i, "Total Cost"]            = cbix_ap_df.loc[i, "Bauxite Cost Incl. Processing Penalties"] + cbix_ap_df.loc[i, "Caustic cost"] + cbix_ap_df.loc[i, "Thermal Energy Cost"] + cbix_ap_df.loc[i, "Lime Cost"] + cbix_ap_df.loc[i, "Mud Disposal Cost"]
        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"] = cbix_ap_df

    # Complete uncompleted tables
    def spec_index_bauxite_details_func_continued1(self):
        trade_al_prod          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        cbix_coe_trade_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        spec_ind_al_prod          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        cbix_coe_spec_ind_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        new_df      = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_ap_df = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_index_bauxite_details.xlsx"]


        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = (trade_al_prod.loc[i, "Total Cost"] - spec_ind_al_prod.loc[i, "Total Cost"]) / spec_ind_al_prod.loc[i, "Tonnes per Tonne"] - new_df.loc[i, "Processing Penalties"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Calculated Index Price Equivalent"]   = (cbix_coe_trade_al_prod.loc[i, "Total Cost"] - spec_ind_al_prod.loc[i, "Total Cost"]) / cbix_coe_spec_ind_al_prod.loc[i, "Tonnes per Tonne"] - cbix_ap_df.loc[i, "Processing Penalties"]
        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_index_bauxite_details.xlsx"] = cbix_ap_df


    def general_index_bauxite_details_func_continued1(self):
        trade_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        cbix_coe_trade_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        gnr_ind_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        cbix_coe_gnr_ind_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        new_df = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_ap_df = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_index_bauxite_details.xlsx"]


        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = (trade_al_prod.loc[i, "Total Cost"] - gnr_ind_al_prod.loc[i, "Total Cost"]) / gnr_ind_al_prod.loc[i, "Tonnes per Tonne"] - new_df.loc[i, "Processing Penalties"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Calculated Index Price Equivalent"]   = (cbix_coe_trade_al_prod.loc[i, "Total Cost"] - cbix_coe_gnr_ind_al_prod.loc[i, "Total Cost"]) / cbix_coe_gnr_ind_al_prod.loc[i, "Tonnes per Tonne"] - cbix_ap_df.loc[i, "Processing Penalties"]

        
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_index_bauxite_details.xlsx"] = cbix_ap_df


    def old_cbix_calculation_bauxite_details_func_continued1(self):
        old_cbix_al_prod          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        cbix_coe_old_cbix_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        old_cbix_clac_al_prod          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        cbix_coe_old_cbix_clac_al_prod = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        new_df      = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.xlsx"]
        cbix_ap_df = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_index_bauxite_details.xlsx"]
        
        for i in range(len(self.trade_details)):
            new_df.at[i, "Calculated Index Price Equivalent"]   = (old_cbix_al_prod.loc[i, "Total Cost"] - old_cbix_clac_al_prod.loc[i, "Total Cost"]) / old_cbix_clac_al_prod.loc[i, "Tonnes per Tonne"] - new_df.loc[i, "Processing Penalties"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_ap_df.at[i, "Calculated Index Price Equivalent"]   = (cbix_coe_old_cbix_al_prod.loc[i, "Total Cost"] - cbix_coe_old_cbix_clac_al_prod.loc[i, "Total Cost"]) / cbix_coe_old_cbix_clac_al_prod.loc[i, "Tonnes per Tonne"] - cbix_ap_df.loc[i, "Processing Penalties"]

        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.xlsx"] = new_df
        self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_index_bauxite_details.xlsx"] = cbix_ap_df
    
    def final_specifications_to_viu_adjustment_continued2(self):
        spec_ind_bx             = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/specific_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_coe_spec_ind_bx    = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_specific_index/actual_price_determination_from_CBIX_price/specific_index_ViU_calculation_index_bauxite_details.xlsx"]

        gnr_ind_bx              = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/general_index_ViU_calculation_index_bauxite_details.xlsx"]
        cbix_coe_gnr_ind_bx     = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_index_bauxite_details.xlsx"]

        old_cbix_clacs          = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/old_cbix_calculation_index_bauxite_details.xlsx"]
        cbix_coe_old_cbix_clacs = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_old_cbix_index/actual_price_determination_from_CBIX_price/old_cbix_calculation_index_bauxite_details.xlsx"]

        final_spec              = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        cbix_coe_final_spec     = self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"]


        for i in range(len(self.trade_details)):
            final_spec.at[i, "INDEX VALUE  Guinea 45/3 LT"] = spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "Specific Index"] == "Guinea 45/3 LT" else 0
            final_spec.at[i, "INDEX VALUE  Indonesian LT"]  = spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "Specific Index"] == "Indonesian LT" else 0
            final_spec.at[i, "INDEX VALUE  Australia HT"]   = spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "Specific Index"] == "Australia HT" else 0
            final_spec.at[i, "CBIX LT"]  = gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "General Index"] == "CBIX LT" else 0
            final_spec.at[i, "CBIX HT"]  = gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if final_spec.loc[i, "General Index"] == "CBIX HT" else 0
            final_spec.at[i, "Old CBIX"] = old_cbix_clacs.loc[i, "Calculated Index Price Equivalent"]
            final_spec.at[i, "CBIX overall"] = final_spec.loc[i, "CBIX LT"] + final_spec.loc[i, "CBIX HT"]

        for i in range(len(self.cbix_cf_trade_details)):
            cbix_coe_final_spec.at[i, "INDEX VALUE  Guinea 45/3 LT"] = cbix_coe_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "Specific Index"] == "Guinea 45/3 LT" else 0
            cbix_coe_final_spec.at[i, "INDEX VALUE  Indonesian LT"]  = cbix_coe_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "Specific Index"] == "Indonesian LT" else 0
            cbix_coe_final_spec.at[i, "INDEX VALUE  Australia HT"]   = cbix_coe_spec_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "Specific Index"] == "Australia HT" else 0
            cbix_coe_final_spec.at[i, "CBIX LT"]  = cbix_coe_gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "General Index"] == "CBIX LT" else 0
            cbix_coe_final_spec.at[i, "CBIX HT"]  = cbix_coe_gnr_ind_bx.loc[i, "Calculated Index Price Equivalent"] if cbix_coe_final_spec.loc[i, "General Index"] == "CBIX HT" else 0
            cbix_coe_final_spec.at[i, "Old CBIX"] = cbix_coe_old_cbix_clacs.loc[i, "Calculated Index Price Equivalent"]
            cbix_coe_final_spec.at[i, "CBIX overall"] = cbix_coe_final_spec.loc[i, "CBIX LT"] + cbix_coe_final_spec.loc[i, "CBIX HT"]
        
        self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"] = final_spec
        self.db[f"outputs/{self.freight_table_value}/actual_price_determination_from_CBIX_price/final_specifications_to_viu_adjustment.xlsx"] = cbix_coe_final_spec



    def actual_price_determination_CBIX_price(self):
        trade_dets   = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_bauxite_details-input.xlsx"]
        alumina_prd  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_trade_bauxite/actual_price_determination_from_CBIX_price/bauxite_at_hand_cost_to_alumina_production_cost_calcs_excluding_common_elements.xlsx"]
        gnr_ind_alu  = self.db[f"outputs/{self.freight_table_value}/workings_for_viu_adjustmnet_of_general_index/actual_price_determination_from_CBIX_price/general_index_ViU_calculation_alumina_production_cost_calcs_excluding_common_elements.xlsx"]

        for i in range(self.cbix_cf_trade_details.shape[0]):
            self.cbix_cf_trade_details.at[i, "Actual Price determination from CBIX price"] = ((gnr_ind_alu.loc[i, "Total Cost"] + gnr_ind_alu.loc[i, "Tonnes per Tonne"] * self.target_cbix_price.loc[0, "Target CBIX Price (CBIX LT)"]) - (alumina_prd.loc[i, "Total Cost"]-alumina_prd.loc[i, "Bauxite Cost Incl. Processing Penalties"]+trade_dets.loc[i, "Processing Penalties"]*alumina_prd.loc[i, "Tonnes per Tonne"])) / alumina_prd.loc[i, "Tonnes per Tonne"]
        
        self.db[f"outputs/{self.freight_table_value}/Actual Price determination from CBIX price Trade Details.xlsx"] = self.cbix_cf_trade_details
    
    
    # Data Tables
    def viu_cost_data_table_func(self):
        final_spec = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]

        def vlookup(search1):
            v = final_spec.loc[:, "Mine"] == search1
            try:
                return final_spec.loc[:, "CBIX overall"][v].tolist()[0]
            except Exception as e:
                return 0

        for i in range(self.viu_cost_data_table.shape[0]):
            self.viu_cost_data_table.at[i, self.master_date_cell.loc[0, "Date"]] = vlookup(self.viu_cost_data_table.loc[i, "Mine"])

        self.db[f"outputs/{self.freight_table_value}/data_tables/viu_cost_data_table.xlsx"] = self.viu_cost_data_table
        #print(self.viu_cost_data_table)

    def freight_datatable_func(self):
        final_freight = self.db[f"outputs/{self.freight_table_value}/final_freight_values.xlsx"]
        final_spec    = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]

        for i in range(self.freight_datatable.shape[0]):
            self.freight_datatable.at[i, self.master_date_cell.loc[0, "Date"]] = final_freight.loc[i, "Qingdao Freight"] * (1 - final_spec.loc[i, "Moisture"])

        self.db[f"outputs/{self.freight_table_value}/data_tables/data_table_for_freights.xlsx"] = self.freight_datatable
        #print(self.freight_datatable)

    def forecast_prices_datatable(self):
        price_determination = self.db[f"outputs/{self.freight_table_value}/Actual Price determination from CBIX price Trade Details.xlsx"]

        #def gen_lookup(search):
            #v = self.cbix_cf_trade_details.loc[:, "Mine"] == search
            #target  = price_determination.loc[:, "Actual Price determination from CBIX price"]
            #try:
                #return target[v].tolist()[0]
            #except Exception:
                #return 0

        #for i in range(self.price_forecast_datatable.shape[0]):
        self.price_forecast_datatable.at[:, self.master_date_cell.loc[0, "Date"]] = price_determination.loc[:, "Actual Price determination from CBIX price"]
        
        self.db[f"outputs/{self.freight_table_value}/data_tables/price_forecast_datatable.xlsx"] = self.price_forecast_datatable

    def freight_inputs_for_row_datatable_func(self):
        final_spec = self.db[f"outputs/{self.freight_table_value}/final_specifications_to_viu_adjustment.xlsx"]
        def vlookup(search1):
            v = self.indexes_mines_2.loc[:, "Mine"] == search1
            try:
                return self.indexes_mines_2.loc[:, "Final Add-On"][v].tolist()[0]
            except Exception:
                return 0

        def gen_lookup(search, lookup, target):
            v = lookup == search
            #print(target[v])
            try:
                return target[v].tolist()[0]
            except Exception:
                return 0
        #print( final_spec.loc[:, "Freight (adjusted to Qingdao)"])
        for i in range(self.freight_datatable_row.shape[0]):
            for column in self.freight_datatable_row.columns[3:]:
                self.freight_datatable_row.at[i, "Add ons"] = vlookup(self.freight_datatable_row.loc[i, "Mine"])

            #print(f'{gen_lookup(self.freight_datatable_row.loc[i, "Mine"], final_spec.loc[:, "Mine"], final_spec.loc[:, "Freight (adjusted to Qingdao)"])} * (1 - {gen_lookup(self.freight_datatable_row.loc[i, "Mine"], final_spec.loc[:, "Mine"], final_spec.loc[:, "Moisture"])}) + {self.freight_datatable_row.loc[i, "Add ons"]}')
            self.freight_datatable_row.at[i, self.master_date_cell.loc[0, "Date"]] = gen_lookup(self.freight_datatable_row.loc[i, "Mine"], final_spec.loc[:, "Mine"], final_spec.loc[:, "Freight (adjusted to Qingdao)"]) * (1 - gen_lookup(self.freight_datatable_row.loc[i, "Mine"], final_spec.loc[:, "Mine"], final_spec.loc[:, "Moisture"])) + self.freight_datatable_row.loc[i, "Add ons"]
            
            # Add ons, Moisture, Freight (adjusted to Qingdao)

        #print(self.freight_datatable_row)

        self.db[f"outputs/{self.freight_table_value}/data_tables/freight_inputs_for_ROW_mining_model.xlsx"] = self.freight_datatable_row





    def calcall(self):
        self.custom_init()
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
        self.actual_price_determination_CBIX_price()

        #data tables
        self.viu_cost_data_table_func()
        self.freight_datatable_func()
        self.forecast_prices_datatable()
        self.freight_inputs_for_row_datatable_func()




    def data_table(self):
        #Run all functions
        for freight_value in self.freight_table.loc[:, "Freight table selector"]:
            self.freight_table_value = freight_value
            count = 1
            for master_date in self.freight_datatable_row.columns[3:]:
                self.master_date_cell.at[0, "Date"] = master_date
                self.cbix_cf_trade_details.loc[:, "Date"] = master_date
                self.trade_details.loc[:, "Date"] = master_date
                self.calcall()
                print(f"Runtime {count} for model successfully completed with date {master_date.date()}")
                count += 1
            #Save files
        for filepath, filee in self.db.items():
            dirname = PureWindowsPath(os.path.dirname(filepath))
            Path(dirname).mkdir(parents=True, exist_ok=True)

            if os.path.exists(dirname):
                dirname1 =  os.path.dirname(dirname)
                if not os.path.exists(dirname1):
                    os.mkdir(dirname1)
            else:
                os.mkdir(dirname)

            # store in flatdb
            print(filepath)
            filename = filepath.split('/')[-1].split('.')[0]
            filename = f"{' '.join(filepath.split('/')[1:-1])} {filename}"
            # if filepath.split('/')[-2] != "outputs":
            #     filename = f"{filepath.split('/')[-2]} {filename}"
            if filepath.split('/')[-2] != "data_tables":
                filee.to_clipboard()
                print("")
                print(filepath, filee)
                dblist.append(db_conv.single_year_mult_out(filee, filename))
            else:
                name = filepath.split('/')[-1].split('.')[0]
                if name == 'data_table_for_freights':
                    dblist.append(db_conv.mult_year_single_output(filee, filename, [[0,1]], [[1,]], label="Date"))
                elif name == 'freight_inputs_for_ROW_mining_model':
                    dblist.append(db_conv.mult_year_single_output(filee, filename, [[0,3]], [[3,]], label="Date"))
                elif name == 'price_forecast_datatable':
                    dblist.append(db_conv.mult_year_single_output(filee, filename, [[0,1]], [[1,]], label="Date"))
                elif name == 'viu_cost_data_table':
                    dblist.append(db_conv.mult_year_single_output(filee, filename, [[0,1]], [[1,]], label="Date"))
            print(PureWindowsPath(os.path.join(BASE_DIR, filepath)))
            try:
                filee.to_excel(PureWindowsPath(os.path.join(BASE_DIR, filepath)), index=False)
            except Exception as err:
                print(err)
            self.db = {}


# if __name__ == "__main__":
start = time.process_time()

cbix_obj = CBIX2()
cbix_obj.data_table()

end = time.process_time() - start
print(f"For CBIX2 Processing => {round(end/60, 2)} minutes")

print("\n\n\n\n")

print("Starting Flat DB conversion ---")

dbflat_time = time.perf_counter()

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[: , db_conv.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
uploadtodb.upload(snapshot_output_data)

print("Total time taken for FlatDB Conversion : " + str(round((time.perf_counter() - dbflat_time) / 60, 2)))
