import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine
import difflib

# basedata = pd.read_csv('basedata.csv')
# meidata = pd.read_csv('meiaadata.csv')

def read_from_database(table):
    engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server")
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(sql=query, con=engine)
    # converts number strings to numeric
    return data.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce')).fillna(data)


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
    "China Price Series", # not found
    "processing factors", # not found 
    "Mud disposal cost", # not found 
    "Ship Fuel Prices", # not found
    "Lignitious Coal",
    "Sheetname_Class",  # only store for cbix2 (using like that for now)
    "Global factors", # found lime data here
    "Trade Details", # not found
    "Port Linkages",
    "Canals_Class",
    "Caustic Soda",
    "Ship Speeds", 
    "FX Rates", # found incorrect dataframe
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

get_table_name()














# caustic = read_from_database('caustic')







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

def normalize_col(df, ref):
    df = df.rename(columns=ref)
    df = df.loc[:, ref.values()]
    return df

# restruct all
def restruct_all():
    # master date cell
    master_date_cell = read_from_database('Master_date_cell')
    master_date_cell = master_date_cell.loc[master_date_cell["model_id"] == 'model_8']

    # cbix_coefficients_determination
    cbix_coefficients_determination = read_from_database('cbix_coefficients_determination')
    cbix_coefficients_determination = cbix_coefficients_determination.replace(store)

    
    # freight
    freights_perwmt_tradedata = read_from_database('freights_perwmt_tradedata')
    freights_perwmt_tradedata = freights_perwmt_tradedata.replace(store)

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


    # ship time charter rates
    Ship_Time_Charter_Rates = read_from_database('Ship_Time_Charter_Rates')
    Ship_Time_Charter_Rates = Ship_Time_Charter_Rates.replace(store)

    # china importing 
    China_Importing_Ports = read_from_database('China_Importing_Ports')
    China_Importing_Ports = China_Importing_Ports.loc[China_Importing_Ports["model_id"] == 'model_8']
    China_Importing_Ports =  China_Importing_Ports.replace(store)
    
    # not found: "China Price Series"

    #not found: processing factor

    Lignitious_Coal = read_from_database('Lignitious_Coal_caustic_soda')
    # caustic also in lignitious coal caustic soda

    # only store for cbix2 (using like that for now)
    Sheetname_Class = read_from_database('vessel_class')
    Sheetname_Class = Sheetname_Class.loc[Sheetname_Class["model_id"] == 'model_8']
    Sheetname_Class = Sheetname_Class.replace(store)

    
    Port_Linkages = read_from_database('Port_Linkages')
    Port_Linkages = Port_Linkages.loc[Port_Linkages["model_id"] == 'model_8']
    Port_Linkages = Port_Linkages.replace(store)

    global_factors = read_from_database('global_factors') # incomplete
    # not found: "Trade Details" 

    # canal class
    Canals_Class = read_from_database('Canals_Class')
    Canals_Class = Canals_Class.loc[Canals_Class["model_id"] == 'model_8']
    Canals_Class = Canals_Class.replace(store)

    # not found: fx rates

    # ship speed
    ship_speed = read_from_database('ship_speed')
    ship_speed = ship_speed.loc[ship_speed["model_id"] == 'model_8']

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
        "Old_CBIX_type_Calc": "Old CBIX type Cal"
    }

    # [Quartz / HT Silica	Mono-hydrate / HT Extble Alumina	Moisture	Tonnage	Exporting Port	Importing Port	Final Specs Price Type	Final Specs Price Basis	Old CBIX type Calc]
    # freights_perwmt_tradedata_id,Date,CM_Data_Source,mine_id,Price,Price_Type,Price_Basis,Total_Alumina,LT_Avail_Alumina,Total_Silica,LT_R_Silica,Quartz_HT_Silica,Mono_hydrate_HT_Extble_Alumina,Moisture,Tonnage,Exporting_Port,Importing_Port,Final_Specs_Price_Type,Final_Specs_Price_Basis,Old_CBIX_type_Calc,creation_date,updation_date

    freight_col = {
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

    # China_Importing_Ports_id,model_id,port_id,Date,Unloading_rate_Handysize,Unloading_rate_Supramax,Unloading_rate_Panamax,Unloading_rate_NeoPanamax,Unloading_rate_Suezmax,Unloading_rate_Capesize,Unloading_rate_VLOC,Currency,RMB_Fixed_fee,RMB_per_day_berthed,RMB_per_day_anchored,RMB_T_Cargo_wet,RMB_T_Cargo_wet_day_berthed,RMB_T_Cargo_wet_day_anchored,RMB_NRT,RMB_NRT_day_berthed,RMB_NRT_day_anchored,RMB_GRT,RMB_GRT_day_berthed,RMB_GRT_day_anchored,RMB_LOA,RMB_LOA_day_berthed,RMB_LOA_day_anchored,creation_date,updation_date

    # Port	Date	Unloading rate Handysize	Unloading rate Supramax	Unloading rate Panamax	Unloading rate NeoPanamax	Unloading rate Suezmax	Unloading rate Capesize	Unloading rate VLOC	Currency	RMB Fixed fee	RMB per day berthed	RMB per day anchored	RMB/T_Cargo (wet)	RMB/T_Cargo (wet)/day berthed	RMB/T_Cargo (wet)/day anchored	RMB/NRT	RMB/NRT/day berthed	RMB/NRT/day anchored	RMB/GRT	RMB/GRT/day berthed	RMB/GRT/day anchored	RMB/LOA	RMB/LOA/day berthed	RMB/LOA/day anchored	spare	spare	spare	spare	Comment


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
        "RMB_T_Cargo_wet_day_anchored": "RMB/GRT/day anchored",
        "RMB_NRT": "RMB/LOA",
     	"RMB_NRT_day_berthed": "RMB/NRT/day berthed",
        "RMB_NRT_day_anchored": "RMB/NRT/day anchored",
        "RMB_LOA": "RMB/LOA",
        "RMB_LOA_day_berthed": "RMB/LOA/day berthed",
        "RMB_LOA_day_anchored": "RMB/LOA/day anchored",

    }

    Lignitious_Coal_col = {
        "Date": "Date",
        "Lignitious_Coal_Price_RMB_t_exc_VAT": "Lignitious Coal Price(RMB/t)(exc VAT)",
        "Energy_value_kcal_kg": "Energy value (kcal/kg)"
    }

    Sheetname_Class_col = {
        "DWT_tonnes": "DWT< tonnes",
        "vessel_id": "class"
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


    Canals_Class_col = {
        "dwt_tonnes": "DWT< tonnes",
        "canals_id": "Class"
    }


    ship_speed_col = {
        "date": "Date",
        "vessel_id": "Vesssel class",
        "vessel_cruising_speed": "Vessel Cruising speed"
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
    
    result = {
        "master_date_cell": normalize_col(master_date_cell, master_date_cell_col),
        "cbix_coefficients_determination": normalize_col(cbix_coefficients_determination, cbix_col),
        "freights_perwmt_tradedata": normalize_col(freights_perwmt_tradedata, freight_col),
        "target_cbix": normalize_col(target_cbix, target_cbix_col),
        "indices_Mines_Exporting_Ports": normalize_col(indices_Mines_Exporting_Ports, indexes_col),
        "special_leg_shipping": normalize_col(special_leg_shipping, special_leg_shipping_col),
        "mrn_juruti": normalize_col(mrn_juruti, mrn_juruti_col),
        "Ship_Time_Charter_Rates": normalize_col(Ship_Time_Charter_Rates, Ship_Time_Charter_Rates_col),
        "China_Importing_Ports": normalize_col(China_Importing_Ports, China_Importing_Ports_col),
        "Lignitious_Coal": normalize_col(Lignitious_Coal, Lignitious_Coal_col),
        "sheetname_class": normalize_col(Sheetname_Class, Sheetname_Class_col),
        "caustic_soda": normalize_col(Lignitious_Coal, caustic_soda_col),
        "Port_Linkages": normalize_col(Port_Linkages, Port_Linkages_col),
        "Canals_Class": normalize_col(Canals_Class, Canals_Class_col),
        "ship_speed": normalize_col(ship_speed, ship_speed_col),
        "canal": normalize_col(Canal, canals_col),
        "lime": normalize_col(lime, lime_col)
    }

    for a in result.keys():
        result[a].to_csv(f"outs/{a}.csv")
    # print(result)
    return result

# def match_col_name(df, db_df):

restruct_all()



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