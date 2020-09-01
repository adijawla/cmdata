import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine
import difflib

# basedata = pd.read_csv('basedata.csv')
# meidata = pd.read_csv('meiaadata.csv')

engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server")
def read_from_database(table):
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(sql=query, con=engine)
    # converts number strings to numeric
    # df1 = data.copy()
    # df1 = df1.loc[:].str.strip('"').strip('$')
    if "Date" in data.columns:
        data["Date"] = data["Date"].astype('datetime64[ns]')
    if "date" in data.columns:
        data["date"] = data["date"].astype('datetime64[ns]')
    return data.apply(lambda x: pd.to_numeric(x.astype(str).str.strip('"').str.strip('$').str.replace(',',''), errors='coerce')).fillna(data)


def read_tables():
    engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server")
    query = """
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES
            """
    data = pd.read_sql_query(sql=query, con=engine)
    return data

def get_table_name():
    db_table_names = list(read_tables().values.flatten())
    tables =  [
    "Master date cell",
    "Freights US$ per wmt Trade Details",
    "CBIX Co-Efficients Determination", 
    "Target CBIX Price",
    "Indices Mines Exporting Ports",
    "Special 2 Leg Shipping Table",
    "MRN or Juruti max cargo",
    "Ship Time Charter Rates",
    "China n Importing Ports",
    "China Price Series", # mud disposal needs to completed in china price series table
    "processing factors",
    "Mud disposal cost",
    "Ship Fuel Prices", # lNG column missing
    "Lignitious Coal",
    "Sheetname_Class",  
    "Global factors", # needs special treatment
    "Trade Details", 
    "Port Linkages",
    "Canals_Class",
    "Caustic Soda",
    "Ship Speeds", 
    "FX Rates", # "fUS$ per US$"  and  "IMF Special Drawing Rights to US Dollar" columns are missing
    "Canals",
    "Lime", # lime found in global factors 😅
    ]
    # print(db_table_names)


    tablenames = [difflib.get_close_matches(a, db_table_names, n=1, cutoff=0.6) for a in tables]
    st = {}
    for a in tables:
        h = difflib.get_close_matches(a, db_table_names, n=1, cutoff=0.6)
        if len(h) > 0:
            print(h)
            st[a] = read_from_database(h[0]).to_csv(f"inp/{a}")
    # print(st)
    print(tablenames)

# get_table_name()




canal_list = read_from_database('canals_list')
mine = read_from_database('mine')
port = read_from_database('port')
vessel = read_from_database('vessel')
currency = read_from_database('currency')

# to dict
canal_store = dict(canal_list.loc[:, ['canals_id', 'canals']].values)
mine_store = dict(mine.loc[:, ['mine_id', 'mine']].values)
port_store = dict(port.loc[:, ['port_id', 'port']].values)
vessel_store = dict(vessel.loc[:, ['vessel_id', 'vessel_class']].values)
currency_store = dict(currency.loc[:, ['currency_id', 'currency']].values)

store = {**canal_store, **mine_store, **port_store, **vessel_store, **currency_store}
# not found: lime

def normalize_col(df, ref, nan_col=[], percent_col=[]):
    df = df.rename(columns=ref)
    df = df.loc[:, ref.values()]
    df = df.reset_index(drop=True)
    if len(percent_col) > 0:
        df.loc[:, percent_col] = df.loc[:, percent_col].apply(lambda x: x.astype(str).str.strip('%').astype('float') / 100.0 )
    if len(nan_col) > 0:
        df.loc[:, nan_col] = df.loc[:,nan_col].replace(0, np.nan)
    return df


# restruct all
def restruct_all():
    # master date cell
    master_date_cell = read_from_database('Master_date_cell')
    master_date_cell = master_date_cell.loc[master_date_cell["model_id"] == 'model_8']

    # cbix_coefficients_determination
    cbix_coefficients_determination = read_from_database('cbix_coefficients_determination')
    cbix_coefficients_determination = cbix_coefficients_determination.replace(store)
    cbix_coefficients_determination = cbix_coefficients_determination.replace({0: np.nan, "0": np.nan})
    # cbix_coefficients_determination = make_nan(cbix_coefficients_determination, ["Total Alumina","LT Avail. Alumina","Total Silica","LT R.Silica","Quartz / HT Silica","Mono-hydrate / HT Extble Alumina","Moisture","Exporting_Port"])

    
    # freight
    freights_perwmt_tradedata = read_from_database('freights_perwmt_tradedata')
    # freights_perwmt_tradedata = make_nan(freights_perwmt_tradedata, ["Exporting_Port"])
    freights_perwmt_tradedata = freights_perwmt_tradedata.replace(store)
    freights_perwmt_tradedata = freights_perwmt_tradedata.replace({0: np.nan, "0": np.nan})

    # "Target CBIX Price"
    target_cbix = read_from_database('Target_CBIX_Price')
    target_cbix = target_cbix.loc[target_cbix["model_id"] == 'model_8']

    # indices mines exporting
    indices_Mines_Exporting_Ports = read_from_database('Indices_Mines_Exporting_Ports')
    indices_Mines_Exporting_Ports = indices_Mines_Exporting_Ports.replace(store)

    # special
    special_leg_shipping = read_from_database('special_leg_shipping')
    special_leg_shipping = special_leg_shipping.replace(store)

    # MRN or Juruti max cargo
    mrn_juruti = read_from_database('MRN_Juruti_max_cargo')
    # mrn_juruti["MRN_Juruti_max_cargo_single_leg_shipping"] = mrn_juruti["MRN_Juruti_max_cargo_single_leg_shipping"].str.strip('"').strip(',').as_float() 


    # ship time charter rates
    Ship_Time_Charter_Rates = read_from_database('Ship_Time_Charter_Rates')
    Ship_Time_Charter_Rates = Ship_Time_Charter_Rates.replace(store)

    # china importing 
    China_Importing_Ports = read_from_database('China_Importing_Ports')
    China_Importing_Ports = China_Importing_Ports.loc[China_Importing_Ports["model_id"] == 'model_8']
    China_Importing_Ports =  China_Importing_Ports.replace(store)
    China_Importing_Ports = China_Importing_Ports.replace({0: np.nan, "0": np.nan})
    
    # not found: "China Price Series"

    #not found: processing factor
    processing_factors = read_from_database('processing_factors')
    processing_factors["spare"] = np.nan

    # mud disposal
    mud_disposal = read_from_database("lime_mud_disposal")

    # ship fuel prices
    ship_fuel_prices = read_from_database("Ship_Fuel_Prices")
    ship_fuel_prices["LNG"] = np.nan
    ship_fuel_prices = ship_fuel_prices.replace({0: np.nan, "0": np.nan})

    Lignitious_Coal = read_from_database('Lignitious_Coal_caustic_soda')
    # caustic also in lignitious coal caustic soda

    # only store for cbix2 (using like that for now)
    Sheetname_Class = read_from_database('vessel_class')
    Sheetname_Class = Sheetname_Class.replace(store)

    
    Port_Linkages = read_from_database('Port_Linkages')
    Port_Linkages = Port_Linkages.loc[Port_Linkages["model_id"] == 'model_8']
    Port_Linkages = Port_Linkages.replace(store)

    temp_global_factors = read_from_database('cbix_global_factors') # incomplete
    print(temp_global_factors)
    global_factor = pd.DataFrame(columns=["Date", *temp_global_factors["global_factor"]])
    global_factor.loc[0] = [temp_global_factors["Date"][0], *temp_global_factors["value"]]
    print(global_factor)
    # not found: "Trade Details" 

    trade_details = read_from_database('Trade_Details')
    trade_details["exporting_port"] = trade_details["exporting_port"].replace(0, np.nan) 
    trade_details = trade_details.replace(store)
    trade_details = trade_details.replace({0: np.nan, "0": np.nan})

    # canal class
    Canals_Class = read_from_database('Canals_Class')
    Canals_Class = Canals_Class.loc[Canals_Class["model_id"] == 'model_8']
    Canals_Class = Canals_Class.replace(store)

    # not found: fx rates
    fx_rates = read_from_database("fxrates_withdates")
    fx_rates = fx_rates.loc[fx_rates["model_id"] == 'model_8']
    fx_rates = fx_rates.replace(store)
    fx_rates = fx_rates.replace({0: np.nan, "0": np.nan})

    # ship speed
    ship_speed = read_from_database('ship_speed')
    ship_speed = ship_speed.loc[ship_speed["model_id"] == 'model_8']
    ship_speed = ship_speed.replace(store)

    # canal
    Canal = read_from_database('Canal')
    Canal = Canal.loc[Canal["model_id"] == 'model_8']
    Canal = Canal.replace(store)

    lime = read_from_database('global_factors')


    master_date_cell_col = { 
        "date": "Date"
    }

    cbix_col = {
        "Date": "Date",
        "CM_Data_Source": "CM Data Source",
        "mine_id": "Mine",
        "Price": "Price",
        "Price_Type": "Price Type",
        "Price_Basis": "Price Basis",
        "Total_Alumina": "Total Alumina",
        "LT_Avail_Alumina": "LT Avail. Alumina",
        "Total_Silica": "Total Silica",
        "LT_R_Silica": "LT R.Silica",
        "Quartz_HT_Silica": "Quartz / HT Silica",
        "Mono_hydrate_HT_Extble_Alumina": "Mono-hydrate / HT Extble Alumina",
        "Moisture": "Moisture",
        "Tonnage": "Tonnage",
        "Exporting_Port": "Exporting Port",
        "port_id": "Importing Port",
        "Final_Specs_Price_Type": "Final Specs Price Type",
        "Final_Specs_Price_Basis": "Final Specs Price Basis",
        "Old_CBIX_type_Calc": "Old CBIX type Calc"
    }

    # [Quartz / HT Silica	Mono-hydrate / HT Extble Alumina	Moisture	Tonnage	Exporting Port	Importing Port	Final Specs Price Type	Final Specs Price Basis	Old CBIX type Calc]
    # freights_perwmt_tradedata_id,Date,CM_Data_Source,mine_id,Price,Price_Type,Price_Basis,Total_Alumina,LT_Avail_Alumina,Total_Silica,LT_R_Silica,Quartz_HT_Silica,Mono_hydrate_HT_Extble_Alumina,Moisture,Tonnage,Exporting_Port,Importing_Port,Final_Specs_Price_Type,Final_Specs_Price_Basis,Old_CBIX_type_Calc,creation_date,updation_date

    # Date,CM_Data_Source,mine_id,Price,Price_Type,Price_Basis,Total_Alumina,LT_Avail_Alumina,Total_Silica,LT_R_Silica
    # Date	CM Data Source	Mine	Price	Price Type	Price Basis	Total Alumina	LT Avail. Alumina	Total Silica	LT R.Silica


    freight_col = {
        "Date": "Date",
        "CM_Data_Source": "CM Data Source",
        "mine_id": "Mine",
        "Price": "Price",
        "Price_Type": "Price Type",
        "Price_Basis": "Price Basis",
        "Total_Alumina": "Total Alumina",
        "LT_Avail_Alumina": "LT Avail. Alumina",
        "Total_Silica": "Total Silica",
        "LT_R_Silica": "LT R.Silica",
        "Quartz_HT_Silica": "Quartz / HT Silica",
        "Mono_hydrate_HT_Extble_Alumina": "Mono-hydrate / HT Extble Alumina",
        "Moisture": "Moisture",
        "Tonnage": "Tonnage",
        "Exporting_Port": "Exporting Port",
        "Importing_Port": "Importing Port",
        "Final_Specs_Price_Type": "Final Specs Price Type",
        "Final_Specs_Price_Basis": "Final Specs Price Basis",
        "Old_CBIX_type_Calc": "Old CBIX type Calc"
    }

    target_cbix_col = {
        "target_cbix_price_CBIX_LT": "Target CBIX Price (CBIX LT)"
    }


    indexes_col = {
        "mine_id": "Mine",
        "Date": "Date",
        "Total_Alumina": "Total Alumina",
        "LT_Avail_Alumina": "LT Avail. Alumina",
        "Total_Silica": "Total Silica",
        "LT_R_Silica": "LT R.Silica",
        "Quartz_HT_Silica": "Quartz / HT Silica",
        "Mono_hydrate_HT_Alumina": "Mono-hydrate / HT Alumina",
        "Moisture": "Moisture",
        "Processing": "Processing",
        "Processing_Penalties_to_be_applied" : "Processing Penalties to be applied",
        "DWT" :"DWT", 
        "Specific_Index" : "Specific Index",
        "General_Index" : "General Index", 
        "Exporting_Port" : "Exporting Port", 
        "Handysize" : "Handysize",
        "Supramax_loading_rates": "Supramax loading rates", 
        "Panamax_loading_rates" : "Panamax loading rates",
        "NeoPanamax_loading_rate" : "NeoPanamax loading rate", 
        "Suezmax_loading_rate" : "Suezmax loading rate",
        "Capesize_loading_rates" : "Capesize loading rates",
        "VLOC_loading_rates": "VLOC loading rates",
        "Spare": "Spare"
    }

    # Date	Mine	First Leg 1	First Leg 2	Typical River Capable Cargo (dmt)
    # special_leg_shipping_id,date,mine_id,First_Leg_1,First_Leg_2,Typical_River_Capable_Cargo,creation_date,updation_date

    special_leg_shipping_col = {
        "date": "Date",
        "mine_id": "Mine", 
        "First_Leg_1": "First Leg 1",
        "First_Leg_2": "First Leg 2",
        "Typical_River_Capable_Cargo": "Typical River Capable Cargo (dmt)"
    }

    mrn_juruti_col = {
        "MRN_Juruti_max_cargo_single_leg_shipping": "MRN/Juruti max cargo single leg shipping"
    }


    Ship_Time_Charter_Rates_col = {
        "Date": "Date",
        "vessel_id": "Vesssel class",
        "Applicable_Time_Charter_Index": "Applicable Time Charter Index",
        "Vessel_Time_Charter_Rates": "Vessel Time Charter Rates"
    }

    

    China_Importing_Ports_col = {
        "port_id": "Port",
        "Date": "Date",
        "Unloading_rate_Handysize": "Unloading rate Handysize",
        "Unloading_rate_Supramax": "Unloading rate Supramax",
        "Unloading_rate_Panamax": "Unloading rate Panamax",
        "Unloading_rate_NeoPanamax": "Unloading rate NeoPanamax",
        "Unloading_rate_Suezmax": "Unloading rate Suezmax",
        "Unloading_rate_Capesize": "Unloading rate Capesize",
        "Unloading_rate_VLOC": "Unloading rate VLOC",
        "Currency": "Currency",
        "RMB_Fixed_fee": "RMB Fixed fee",
        "RMB_per_day_berthed": "RMB per day berthed",
        "RMB_per_day_anchored": "RMB per day anchored",
        "RMB_T_Cargo_wet": "RMB/T_Cargo (wet)",
        "RMB_T_Cargo_wet_day_berthed": "RMB/T_Cargo (wet)/day berthed",
        "RMB_T_Cargo_wet_day_anchored": "RMB/T_Cargo (wet)/day anchored",
        "RMB_NRT": "RMB/NRT",
        "RMB_NRT_day_berthed": "RMB/NRT/day berthed",
        "RMB_NRT_day_anchored": "RMB/NRT/day anchored",
        "RMB_GRT": "RMB/GRT",
        "RMB_GRT_day_berthed": "RMB/GRT/day berthed",
        "RMB_GRT_day_anchored": "RMB/GRT/day anchored",
        "RMB_LOA": "RMB/LOA",
        "RMB_LOA_day_berthed": "RMB/LOA/day berthed",
        "RMB_LOA_day_anchored": "RMB/LOA/day anchored",
    }

    processing_factors_col = {
        "date": "Date",
        "processing_regime": "Processing Regime",
        "ht_alumina_dissolution": "HT Alumina Dissolution",
        "dsp_al2o3_sio2_wt_wt": "DSP Al2O3:SiO2 (wt/wt)",
        "dsp_naoh_sio2_wt_wt": "DSP NaOH:SiO2 (wt/wt)",
        "lime_rate_wt_wt_aa": "Lime rate (wt/wt_AA)",
        "lig_coal_gj_t": "Lig Coal (GJ/t)",
        "quartz_attack": "Quartz Attack",
        "extraction_efficiency": "Extraction Efficiency %",
        "caustic_wash_loss_t_naoh_t_aa": "Caustic wash loss (t.NaOH/t.aa)",
        "spare": "spare"
    }


    mud_disposal_col = {
        # "lime_price_rmb_t": "",
        "date": "Date",
        "mud_disposal_cost_price_rmb_t_mud_dry": "Mud disposal cost Price (RMB/t.mud dry)"
    }

    ship_fuel_prices_col = {
        "date": "Date",
        "fuel_region": "Fuel Region",
        "hsfo_180_380": "HSFO(180 / 380)",
        "vlsfo": "VLSFO",
        "LNG": "LNG",
        "mdo_mgo_regular": "MDO/MGO Regular",
        "mdo_mgo_low_sulphur": "MDO/MGO Low Sulphur"
    }

    Lignitious_Coal_col = {
        "Date": "Date",
        "Lignitious_Coal_Price_RMB_t_exc_VAT": "Lignitious Coal Price(RMB/t)(exc VAT)",
        "Energy_value_kcal_kg": "Energy value (kcal/kg)"
    }

    Sheetname_Class_col = {
        "DWT_tonnes": "DWT< tonnes",
        "vessel_id": "Class"
    }

    caustic_soda_col = {
        "Date": "Date",
        "Caustic_Soda_Price_RMB_t_exc_VAT" : "Caustic Soda Price (RMB/t) (exc VAT)",
        "Grade": "Grade"
    }					

    Port_Linkages_col  = {
        "Date": "Date",
        "mine_id": "Typical Mine",
        "port_id": "Exporting Port",
        "port_id2": "Importing Port",
        "VLOC_Distance_using_Main_Engine_Fuel": "VLOC – Distance using Main Engine Fuel",
        "VLOC_Distance_using_Auxiliary_Fuel": "VLOC – Distance using Auxiliary Fuel",
        "VLOC_Applicable_Time_Charter_Index": "VLOC – Applicable Time Charter Index",
        "VLOC_Applicable_Fuel_Region": "VLOC – Applicable Fuel Region",
        "VLOC_Main_Engine_Fuel": "VLOC – Main Engine Fuel",
        "VLOC_Auxiliary_Fuel": "VLOC – Auxiliary Fuel",
        "VLOC_Canals_used": "VLOC – Canals used",
        "Capesize_Distance_using_Main_Engine_Fuel": "Capesize – Distance using Main Engine Fuel",
        "Capesize_Distance_using_Auxiliary_Fuel": "Capesize – Distance using Auxiliary Fuel",
        "Capesize_Applicable_Time_Charter_Index": "Capesize – Applicable Time Charter Index",
        "Capesize_Applicable_Fuel_Region": "Capesize – Applicable Fuel Region",
        "Capesize_Main_Engine_Fuel": "Capesize – Main Engine Fuel",
        "Capesize_Auxiliary_Fuel": "Capesize – Auxiliary Fuel",
        "Capesize_Canals_used": "Capesize – Canals used",
        "Suezmax_Distance_using_Main_Engine_Fuel": "Suezmax – Distance using Main Engine Fuel",
        "Suezmax_Distance_using_Auxiliary_Fuel": "Suezmax – Distance using Auxiliary Fuel",
        "Suezmax_Applicable_Time_Charter_Index": "Suezmax – Applicable Time Charter Index",
        "Suezmax_Applicable_Fuel_Region": "Suezmax – Applicable Fuel Region",
        "Suezmax_Main_Engine_Fuel": "Suezmax – Main Engine Fuel",
        "Suezmax_Auxiliary_Fuel": "Suezmax – Auxiliary Fuel",
        "Suezmax_Canals_used": "Suezmax – Canals used",
        "NeoPanamax_Distance_using_Main_Engine_Fuel": "NeoPanamax – Distance using Main Engine Fuel",
        "NeoPanamax_Distance_using_Auxiliary_Fuel": "NeoPanamax – Distance using Auxiliary Fuel",
        "NeoPanamax_Applicable_Time_Charter_Index": "NeoPanamax – Applicable Time Charter Index",
        "NeoPanamax_Applicable_Fuel_Region": "NeoPanamax – Applicable Fuel Region",
        "NeoPanamax_Main_Engine_Fuel": "NeoPanamax – Main Engine Fuel",
        "NeoPanamax_Auxiliary_Fuel": "NeoPanamax – Auxiliary Fuel",
        "NeoPanamax_Canals_used": "NeoPanamax – Canals used",
        "Panamax_Distance_using_Main_Engine_Fuel": "Panamax – Distance using Main Engine Fuel",
        "Panamax_Distance_using_Auxiliary_Fuel": "Panamax – Distance using Auxiliary Fuel",
        "Panamax_Applicable_Time_Charter_Index": "Panamax – Applicable Time Charter Index",
        "Panamax_Applicable_Fuel_Region": "Panamax – Applicable Fuel Region",
        "Panamax_Main_Engine_Fuel": "Panamax – Main Engine Fuel",
        "Panamax_Auxiliary_Fuel": "Panamax – Auxiliary Fuel",
        "Panamax_Canals_used":  "Panamax – Canals used",
        "Supramax_Distance_using_Main_Engine_Fuel": "Supramax – Distance using Main Engine Fuel",
        "Supramax_Distance_using_Auxiliary_Fuel": "Supramax – Distance using Auxiliary Fuel",
        "Supramax_Applicable_Time_Charter_Index": "Supramax – Applicable Time Charter Index",
        "Supramax_Applicable_Fuel_Region": "Supramax – Applicable Fuel Region",
        "Supramax_Main_Engine_Fuel": "Supramax – Main Engine Fuel",
        "Supramax_Auxiliary_Fuel": "Supramax – Auxiliary Fuel",
        "Supramax_Canals_used": "Supramax – Canals used",
        "Handysize_Distance_using_Main_Engine_Fuel": "Handysize – Distance using Main Engine Fuel",
        "Handysize_Distance_using_Auxiliary_Fuel": "Handysize – Distance using Auxiliary Fuel",
        "Handysize_Applicable_Time_Charter_Index": "Handysize – Applicable Time Charter Index",
        "Handysize_Applicable_Fuel_Region": "Handysize – Applicable Fuel Region",
        "Handysize_Main_Engine_Fuel": "Handysize – Main Engine Fuel",
        "Handysize_Auxiliary_Fuel": "Handysize – Auxiliary Fuel",
        "Handysize_Canals_used": "Handysize – Canals used"
    }  
    
     

    global_factor_col = {
        "Date": "Date",
        "kcal_per_GJ": "kcal per GJ",
        "conversion_kt_to_t": "conversion kt to t",
        "hours_per_day": "hours per day",
        "legs_per_round_trip": "legs per round trip",
        "shipping_profit_rate": "shipping profit rate",
        "Alumina_mol_weight": "Alumina mol weight",
        "Silica_Mol_Wt": "Silica Mol Wt",
        "Kaolinite_(Al2O3_2SiO2_2H20)_-_Al2O3:SiO2_ratio": "Kaolinite (Al2O3.2SiO2.2H20) - Al2O3:SiO2 ratio",
        "Max_%_of_vessel_deadweight_allowed_for_loading": "Max % of vessel deadweight allowed for loading",
        "LOA_estimate_correlation_multiplier": "LOA estimate correlation multiplier",
        "LOA_estimate_correlation_constant": "LOA estimate correlation constant",
        "NRT_estimate_correlation_multiplier": "NRT estimate correlation multiplier",
        "NRT_estimate_correlation_constant": "NRT estimate correlation constant",
        "GRT_estimate_correlation_multiplier": "GRT estimate correlation multiplier",
        "GRT_estimate_correlation_constant": "GRT estimate correlation constant",
        "Minimum_Lay_days_allowed": "Minimum Lay days allowed",
        "Minimum_Lay_days_allowed_as_%_of_sailing_time": "Minimum Lay days allowed as % of sailing time",
        "Lay_allowance_Loading_port": "Lay allowance Loading port",
        "Extra_time_tie_up_+_untie_at_each_port": "Extra time tie up + untie at each port",
        "MDO_MGO_burn_vessel_DWT_denominator": "MDO/MGO burn vessel DWT denominator",
        "MDO_MGO_burn_vessel_DWT_slope": "MDO/MGO burn vessel DWT slope",
        "MDO_MGO_burn_vessel_DWT_constant": "MDO/MGO burn vessel DWT constant",
        "Main_engine_burn_vessel_DWT_denominator": "Main engine burn vessel DWT denominator",
        "Main_engine_burn_vessel_DWT_exponent": "Main engine burn vessel DWT exponent",
        "Main_engine_burn_speed_linear_coefficient": "Main engine burn speed linear coefficient",
        "Main_engine_burn_speed_^2_coefficient": "Main engine burn speed ^2 coefficient",
        "Main_engine_burn_speed_^3_coefficient": "Main engine burn speed ^3 coefficient",
        "Main_engine_burn_overall_correction_factor": "Main engine burn overall correction factor",
        "Reference_Port": "Reference Port",
        "Freight_Insurance_Rate": "Freight Insurance Rate",
        "Freight_Commission": "Freight Commission"
    }

    
    trade_details_col = {
        "date": "Date",
        "cm_data_source": "CM Data Source",
        "mine_id": "Mine",
        "price": "Price",
        "price_type": "Price Type",
        "price_basis": "Price Basis",
        "total_alumina": "Total Alumina",
        "lt_avail_alumina": "LT Avail. Alumina",
        "total_silica": "Total Silica",
        "lt_r_silica": "LT R.Silica",
        "quartz_ht_silica": "Quartz / HT Silica",
        "mono_hydrate_ht_extble_alumina": "Mono-hydrate / HT Extble Alumina",
        "moisture": "Moisture",
        "tonnage": "Tonnage",
        "exporting_port": "Exporting Port",
        "port_id": "Importing Port",
        "final_specs_price_type": "Final Specs Price Type",
        "final_specs_price_basis": "Final Specs Price Basis",
        "old_cbix_type_calc": "Old CBIX type Calc",
    }


    Canals_Class_col = {
        "dwt_tonnes": "DWT< tonnes",
        "canals_id": "Class"
    }


    ship_speed_col = {
        "date": "Date",
        "vessel_id": "Vesssel class",
        "vessel_cruising_speed": "Vessel Cruising speed"
    }


    fx_rates_col = {
        "date": "Date",
        "rmb_per_us": "RMB per US$",
        "au_per_us": "AU$ per US$",
        "fus_per_us": "US$ per US$",
        "imf_special_drawing_rights_to_us_dollar": "IMF Special Drawing Rights to US Dollar"
    }


    canals_col = {
        "Date": "Date",			
        "canals_id": "Canal",
        "currency_id": "Currency",
        "Tonnage_Reference": "Tonnage Reference",
        "Capacity_Tarrif_Graduations_1st": "Capacity Tarrif Graduations - 1st",
        "Capacity_Tarrif_Graduations_2nd": "Capacity Tarrif Graduations - 2nd",
        "Capacity_Tarrif_Graduations_3rd": "Capacity Tarrif Graduations - 3rd",
        "Capacity_Tarrif_Graduations_4th": "Capacity Tarrif Graduations - 4th",
        "Capacity_Tarrif_Graduations_5th": "Capacity Tarrif Graduations - 5th",
        "Capacity_Tarrif_Graduations_6th": "Capacity Tarrif Graduations - 6th",
        "Capacity_Tarrif_Graduations_7th": "Capacity Tarrif Graduations - 7th",
        "Capacity_Tarrif_Graduations_8th": "Capacity Tarrif Graduations - 8th",
        "Capacity_Tarrif_Graduations_9th": "Capacity Tarrif Graduations - 9th",
        "Capacity_Tarrifs_1st": "Capacity Tarrifs - 1st",
        "Capacity_Tarrifs_2nd": "Capacity Tarrifs - 2nd",
        "Capacity_Tarrifs_3rd": "Capacity Tarrifs - 3rd",
        "Capacity_Tarrifs_4th": "Capacity Tarrifs - 4th",
        "Capacity_Tarrifs_5th": "Capacity Tarrifs - 5th",
        "Capacity_Tarrifs_6th": "Capacity Tarrifs - 6th",
        "Capacity_Tarrifs_7th": "Capacity Tarrifs - 7th",
        "Capacity_Tarrifs_8th": "Capacity Tarrifs - 8th",
        "Capacity_Tarrifs_9th": "Capacity Tarrifs - 9th",
        "Capacity_Tarrifs_?rem": "Capacity Tarrifs – rem",
        "Cargo_Tarrif_Graduations_1st": "Cargo Tarrif Graduations - 1st",
        "Cargo_Tarrif_Graduations_2nd": "Cargo Tarrif Graduations - 2nd",
        "Cargo_Tarrif_Graduations_3rd": "Cargo Tarrif Graduations - 3rd",
        "Cargo_Tarrif_Graduations_4th": "Cargo Tarrif Graduations - 4th",
        "Cargo_Tarrif_Graduations_5th": "Cargo Tarrif Graduations - 5th",
        "Cargo_Tarrif_Graduations_6th": "Cargo Tarrif Graduations - 6th",
        "Cargo_Tarrif_Graduations_7th": "Cargo Tarrif Graduations - 7th",
        "Cargo_Tarrif_Graduations_8th": "Cargo Tarrif Graduations - 8th",
        "Cargo_Tarrif_Graduations_9th": "Cargo Tarrif Graduations - 9th",
        "Cargo_Tarrifs_1st": "Cargo Tarrifs - 1st",
        "Cargo_Tarrifs_2nd": "Cargo Tarrifs - 2nd",
        "Cargo_Tarrifs_3rd": "Cargo Tarrifs - 3rd",
        "Cargo_Tarrifs_4th": "Cargo Tarrifs - 4th",
        "Cargo_Tarrifs_5th": "Cargo Tarrifs - 5th",
        "Cargo_Tarrifs_6th": "Cargo Tarrifs - 6th",
        "Cargo_Tarrifs_7th": "Cargo Tarrifs - 7th",
        "Cargo_Tarrifs_8th": "Cargo Tarrifs - 8th",
        "Cargo_Tarrifs_9th": "Cargo Tarrifs - 9th",
        "Cargo_Tarrifs_rem": "Cargo Tarrifs – rem",
        "Days_delay": "Days delay"
    }

    lime_col = {
        "date": "Date",
        "lime": "Lime Price (RMB/t)"
    }

    # assemble chine price series
    a_mud_disposal = mud_disposal.rename(columns={'date': "Mud_Date"})
    a_mud_disposal.drop(['creation_date', 'updation_date'], inplace=True, axis=1)
    lime.drop(['creation_date', 'updation_date'], inplace=True, axis=1)
    Lignitious_Coal.drop(['creation_date', 'updation_date'], inplace=True, axis=1)
    Lignitious_Coal["Date1"] = Lignitious_Coal["Date"]
    a = Lignitious_Coal.join(lime)
    china_price_series = a.join(a_mud_disposal)
    # china_price_series = pd.concat([Lignitious_Coal, lime, a_mud_disposal], axis='col' )

    china_price_series_col = {
        "Date": "Lignitious Coal – Date",
        "Lignitious_Coal_Price_RMB_t_exc_VAT": "Lignitious Coal – Price",
        "Energy_value_kcal_kg": "Lignitious Coal – Energy value",
        "Date1": "Caustic Soda – Date",
        "Caustic_Soda_Price_RMB_t_exc_VAT": "Caustic Soda – Price",
        "Grade": "Caustic Soda – Grade",
        "date": "Lime – Date",
        "lime": "Lime – Price",
        "Mud_Date": "Mud disposal cost - Date",
        "mud_disposal_cost_price_rmb_t_mud_dry": "Mud disposal cost – Price"
    }

    china_price_series.loc[:, ["Date", "Date1", "date", "Mud_Date"]] = china_price_series.loc[:, ["Date", "Date1", "date", "Mud_Date"]].astype("datetime64[ns]")
    
    result = {
        "master_date_cell": normalize_col(master_date_cell, master_date_cell_col),
        "cbix_coefficients_determination": normalize_col(cbix_coefficients_determination, cbix_col, ["Total Alumina","LT Avail. Alumina","Total Silica","LT R.Silica","Quartz / HT Silica","Mono-hydrate / HT Extble Alumina","Moisture", "Exporting Port"], ["Total Alumina","LT Avail. Alumina","Total Silica","LT R.Silica","Quartz / HT Silica","Mono-hydrate / HT Extble Alumina","Moisture", "Exporting Port"]),
        "freights_perwmt_tradedata": normalize_col(freights_perwmt_tradedata, freight_col, ["Total Alumina","LT Avail. Alumina","Total Silica","LT R.Silica","Quartz / HT Silica","Mono-hydrate / HT Extble Alumina","Moisture", "Exporting Port"]),
        "target_cbix": normalize_col(target_cbix, target_cbix_col),
        "indices_Mines_Exporting_Ports": normalize_col(indices_Mines_Exporting_Ports, indexes_col),
        "special_leg_shipping": normalize_col(special_leg_shipping, special_leg_shipping_col),
        "mrn_juruti": normalize_col(mrn_juruti, mrn_juruti_col),
        "Ship_Time_Charter_Rates": normalize_col(Ship_Time_Charter_Rates, Ship_Time_Charter_Rates_col),
        "China_Importing_Ports": normalize_col(China_Importing_Ports, China_Importing_Ports_col, ['RMB per day berthed', 'RMB per day anchored', 'RMB/T_Cargo (wet)/day berthed', 'RMB/T_Cargo (wet)/day anchored', 'RMB/GRT', 'RMB/GRT/day berthed', 'RMB/GRT/day anchored', 'RMB/LOA/day berthed', 'RMB/LOA/day anchored']),
        "processing_factors": normalize_col(processing_factors, processing_factors_col),
        "mud_disposal": normalize_col(mud_disposal, mud_disposal_col),
        "ship_fuel_prices": normalize_col(ship_fuel_prices, ship_fuel_prices_col,  ["VLSFO", "LNG", "MDO/MGO Low Sulphur"]),
        "Lignitious_Coal": normalize_col(Lignitious_Coal, Lignitious_Coal_col),
        "sheetname_class": normalize_col(Sheetname_Class, Sheetname_Class_col),
        "caustic_soda": normalize_col(Lignitious_Coal, caustic_soda_col),
        "Port_Linkages": normalize_col(Port_Linkages, Port_Linkages_col),
        "trade_details": normalize_col(trade_details, trade_details_col, ["Total Alumina","LT Avail. Alumina","Total Silica","LT R.Silica","Quartz / HT Silica","Mono-hydrate / HT Extble Alumina","Moisture", "Exporting Port"]),
        "global_factors": normalize_col(global_factor, global_factor_col),
        "Canals_Class": normalize_col(Canals_Class, Canals_Class_col),
        "ship_speed": normalize_col(ship_speed, ship_speed_col),
        "canal": normalize_col(Canal, canals_col),
        "fx_rates": normalize_col(fx_rates, fx_rates_col, ["IMF Special Drawing Rights to US Dollar"]),
        "lime": normalize_col(lime, lime_col),
        "china_price_series": normalize_col(china_price_series, china_price_series_col)
    }

    for a in result.keys():
        result[a].to_csv(f"outs/{a}.csv")
    # print(result)
    return result

# def match_col_name(df, db_df):

# restruct_all()



def restruct(data, ind, col, val):
    cols = data[col].unique()
    result = pd.DataFrame(columns=[ind, *cols])
    indexes =  data.loc[0::len(cols), ind].values
    result[ind] = indexes 
    values = data[val].values.reshape(-1, len(cols))
    result.loc[:, cols] = list(values)
    result = result.rename(columns=yg)
    result = result.replace(meidata_store)
    return result.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce')).fillna(result)

    

# basedata_r = restruct(basedata, 'ind', 'year_id', 'value')
# print(basedata_r)
# meidata_r = restruct(meidata, 'province_id', 'year_id', 'Value').replace(meidata_store)
# meidata_r = meidata_r.rename(columns={'province_id': 'province'})
# print(meidata_r)

# SELECT TABLE_NAME 
# FROM INFORMATION_SCHEMA.TABLES
# WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='dbName' 