from flatdb.flatdbconverter import read_output_database, reverse
import pandas as pd
import numpy as np


# pr = pd.read_csv("price_forecast_snapshot.csv")
# row_o_set = ['Collector Full Cost Cfr (Us$-Dmt)', 'Collector Freight (Us$-Dmt)', 'Collector Organics', 'Collector Moisture', 'Collector Bauxite Style', 'Collector Total Alumina', 'Collector Lt Avail Alumina', 'Collector Vessels', 'Collector Monohydrate', 'Collector Total Silica', 'Collector Lt R. Silica',"Freight (US$/dmt)"]
# pf_o_set =  ["Lbt 451", "1688" , "1689", "1690", "1818"]
# row_o_set = [' '.join(a.split('_')).title() for a in row_o_set]


cbix_outs_col_names = [ "Total Alumina", "LT Avail Alumina", "Monohydrate", "Total Silica", "LT R. Silica", "Moisture","Organics", "Bauxite Style",  "Vessels", "Freight (US$/dmt)"]



def insert_row(row_number, df, row_value): 
    df1 = df[0:row_number] 
    df2 = df[row_number:] 
    df1.loc[row_number]=row_value 
    df_result = pd.concat([df1, df2]) 
    df_result.index = [*range(df_result.shape[0])] 
    return df_result


def interrelationship_rest():
    row_o_set = ['Collector Full Cost Cfr (Us$-Dmt)', 'Collector Total Alumina', 'Collector Lt Avail Alumina','Collector Monohydrate', 'Collector Total Silica', 'Collector Lt R. Silica', 'Collector Moisture', 'Collector Organics', 'Collector Bauxite Style', 'Collector Vessels', 'Collector Freight (Us$-Dmt)']
    row_o_set = [' '.join(a.split('_')).title() for a in row_o_set]
    pf_o_set =  ["Lbt 451", "1688" , "1689", "1690", "1818"]
    row = read_output_database(262, row_o_set)
    pf = read_output_database(302, pf_o_set)

    row_rev = reverse(row, "Row mining model", row_o_set)
    pr_rev = reverse(pf, "Price forecast model", pf_o_set)

    row_rev = row_rev["Row mining model"]

    # convert row mining inputs to the rigth format

    full_c_cfr_shadong_dataframe = row_rev["Collector Full Cost Cfr (Us$-Dmt)"] 
    full_c_cfr_shadong_dataframe.rename(columns={"mine":"Project", "country": "Country"}, inplace=True)
    full_c_cfr_shadong_dataframe.drop(list(map(str, range(2016, 2019))), inplace=True, axis=1)
    full_c_cfr_shadong_dataframe_df = full_c_cfr_shadong_dataframe.copy()
    full_c_cfr_shadong_dataframe_df.loc[:, "2020":full_c_cfr_shadong_dataframe_df.columns[-1]] = np.nan
    full_c_cfr_shadong_dataframe.drop(["2019"], inplace=True, axis=1)
    samp = pd.to_numeric(full_c_cfr_shadong_dataframe.columns, errors="coerce")
    full_c_cfr_shadong_dataframe.columns = [full_c_cfr_shadong_dataframe.columns[i] if np.isnan(samp[i]) else int(samp[i]) for i in np.arange(len(samp))]
    
    samp = pd.to_numeric(full_c_cfr_shadong_dataframe_df.columns, errors="coerce")
    full_c_cfr_shadong_dataframe_df.columns = [full_c_cfr_shadong_dataframe_df.columns[i] if np.isnan(samp[i]) else int(samp[i]) for i in np.arange(len(samp))]
    

    cbix_model_years = list(map(str, range(2016, 2031)))
    cbix_model_outputs = pd.DataFrame(columns=['Unnamed: 0'])
    cbix_model_outputs['Unnamed: 0'] = [np.nan, *row_rev["Collector Freight (Us$-Dmt)"].mine.values]
    count = 1
    for i, a in enumerate(row_o_set[1:]):
        count += 1
        new_df_unnamed = [f"Unnamed: {_}" for _ in range(count, count+14)]
        count += 14
        row_rev[a].drop(columns=["country", "mine"], inplace=True)
        cbix_out_unit = insert_row(0, row_rev[a], row_rev[a].columns)
        cbix_out_unit.columns = [cbix_outs_col_names[i], *new_df_unnamed]
        cbix_model_outputs = cbix_model_outputs.join(cbix_out_unit)
    cbix_model_outputs = cbix_model_outputs.replace({np.nan: 0, 'nan': 0})
    
    # for price forecast
    pr_rev = pr_rev["Price forecast model"]
    # convert all price forecast inputs to the rigt format
    demand_inps = pd.concat([pr_rev["1688"], pr_rev["1689"], pr_rev["1690"]], ignore_index=True)
    demand_inps.drop(list(map(str, [*range(2014, 2019), *range(2032, 2041)])), inplace=True, axis=1)
    demand_inps =  demand_inps.rename(columns={"2031":"Total"})
    samp1 = pd.to_numeric(demand_inps.columns, errors="coerce")
    demand_inps.columns = [demand_inps.columns[i] if np.isnan(samp1[i]) else int(samp1[i]) for i in np.arange(len(samp1))]

    cbix_price_forecast = pr_rev["1818"]
    cbix_price_forecast.drop(list(map(str, [*range(2014, 2019), *range(2031, 2041)])), inplace=True, axis=1)
    samp2 = pd.to_numeric(cbix_price_forecast.columns, errors="coerce")
    cbix_price_forecast.columns = [cbix_price_forecast.columns[i] if np.isnan(samp2[i]) else int(samp2[i]) for i in np.arange(len(samp2))]

    f_supply_mt = pr_rev["Lbt 451"]
    f_supply_mt.drop(list(map(str, [*range(2014, 2019), *range(2032, 2041)])), inplace=True, axis=1)
    f_supply_mt = f_supply_mt.rename(columns={"Name":"Project"})
    

    from_price_forecast_max = f_supply_mt.loc[(f_supply_mt['Project'] == 'Amrun') | (f_supply_mt['Project'] == "AMRUN HT")].reset_index(drop=True)
    f_supply_mt.drop(['2019', '2031'], inplace=True, axis=1)
    samp3 = pd.to_numeric(f_supply_mt.columns, errors="coerce")
    f_supply_mt.columns = [f_supply_mt.columns[i] if np.isnan(samp3[i]) else int(samp3[i]) for i in np.arange(len(samp3))]
    samp4 = pd.to_numeric(from_price_forecast_max.columns, errors="coerce")
    from_price_forecast_max.columns = [from_price_forecast_max.columns[i] if np.isnan(samp4[i]) else int(samp4[i]) for i in np.arange(len(samp4))]
    from_price_forecast_max = from_price_forecast_max.loc[: ,["Project", *range(2019, 2032)]]

    # print(from_price_forecast_max)
    return {
        "full_c_cfr_shadong_dataframe": full_c_cfr_shadong_dataframe.replace({'nan': 0, np.nan: 0}),
        "full_c_cfr_shadong_dataframe_df": full_c_cfr_shadong_dataframe_df.replace({'nan': 0, np.nan: 0}),
        "cbix_model_output": cbix_model_outputs.replace({'nan': 0, np.nan: 0}),
        "demand_inps": demand_inps.replace({'nan': 0, np.nan: 0}),
        "cbix_price_forecast": cbix_price_forecast.replace({'nan': 0, np.nan: 0}),
        "f_supply_mt": f_supply_mt.replace({'nan': 0, np.nan: 0}),
        "from_price_forecast_max": from_price_forecast_max.replace({'nan': 0, np.nan: 0}),
    }


# print(interrelationship_rest())