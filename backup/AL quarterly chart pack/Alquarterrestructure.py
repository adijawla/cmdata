import os
import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine


list_tb = ['aa_prodn_from_import_bx_mthly','alumina_monthly_supply','cm_global_bx_production_inputs','company_supply','data_charts_inputs','Data_Monthly_Inputs','Global_Bx_Quarterly_inputs','Refineries_Aa_Production','Rio_Quarterly_Reports','table_2','table_4_table_7','Rusal_Quarterly_Report','other_inputs_bxpro', 'cbixdata', 'changjiang_lme_daily_inputs', 'platts_vs_cmaax_inputs', 'raw_data', 't_per_t_for_different_bauxite']
list_tb_1 = ["cbix_data", "changjiang_lme_daily_inputs", "mines_bx_production", "platts_vs_cmaax_inputs"]
print(len(list_tb))



"""
"Data input annual", sheet_name='Sheet1'
bx_imports_wbk, sheet_name='Raw trade data'
bx_imports_wbk, sheet_name='Table 1'
bx_imports_wbk, sheet_name='Table 2'
bx_imports_wbk, sheet_name='Table 3'
bx_imports_wbk, sheet_name='Table 4'
bx_imports_wbk, sheet_name='Table 5'
bx_imports_wbk, sheet_name='Table 6'
bx_imports_wbk, sheet_name='Table 7'
'./Inputs/Changjiang & LME Daily inputs.xlsx', sheet_name='Table 1'
'./Inputs/Changjiang & LME Daily inputs.xlsx', sheet_name='Table 2'
'./Inputs/Platts Vs CMAAX inputs.xlsx',sheet_name='Sheet1'
other_inputs_bx_wbk, sheet_name='Table 1'
other_inputs_bx_wbk, sheet_name='Table 2'
ref_aa_prod_wbk, sheet_name='Sheet1'
data_input_monthly_wbk, sheet_name='Alumina Monthly Supply'
data_input_monthly_wbk, sheet_name='Aluminuim Monthly Supply'
data_input_monthly_wbk, sheet_name='Alumina Price Trend'
data_input_monthly_wbk, sheet_name='Bauxite Imports'
data_input_monthly_wbk, sheet_name='Bauxite Imports 2'
data_input_monthly_wbk, sheet_name='Bauxite Imports 3'
data_input_monthly_wbk, sheet_name='Bauxite Imports 4'
data_input_monthly_wbk, sheet_name='Bauxite Imports 5'
data_input_monthly_wbk, sheet_name='Bauxite Style'
data_input_monthly_wbk, sheet_name='Exchange Rate(R;U'
data_input_monthly_wbk, sheet_name='Input BAR'
data_input_monthly_wbk, sheet_name='BAR'
data_input_monthly_wbk, sheet_name='Bauxite Usage'
data_input_monthly_wbk, sheet_name='CM Port Bx Inventry'
data_input_monthly_wbk, sheet_name='Global Alumina Prd'
aa_production_bx_wbk, sheet_name='Sheet1'
mines_bx_prd_wbk, sheet_name='Sheet1'
mines_bx_prd_wbk, sheet_name='Sheet2'
mines_bx_prd_wbk, sheet_name='Sheet3'
bauxite_sources_wbk, sheet_name='Sheet1'
bauxite_sources_wbk, sheet_name='Summary'
data_monthly_wbk, sheet_name='Sheet1'
global_bx_wbk, sheet_name='Sheet1'
cm_global_wbk, sheet_name='Sheet1'
data_chartss_wbk, sheet_name='Chart11'
data_chartss_wbk, sheet_name='Chart12'

"""

def rev(s):
    return s[::-1]

list_dfs = []

engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server")
def read_from_database(table):
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(sql=query, con=engine)
    if "Date" in data.columns:
        data["Date"] = data["Date"].astype('datetime64[ns]')
    if "date" in data.columns:
        pass
        # data["date"] = data["date"].astype('datetime64[ns]')
    return data.apply(lambda x: pd.to_numeric(x.astype(str).str.strip('"').str.strip('$').str.replace(',',''), errors='coerce')).fillna(data)

print(read_from_database('month_id'))

all_foreignkeys = ['technology', 'refinery', 'province', 'year', 'company', 'ownership', 'groups', 'country', 'mine', 'regency']
store = {}
for a in all_foreignkeys:
    d = read_from_database(a)
    c = [f for f in d.columns if f[-3:] == '_id'][0]
    if c != 'group_id':
        s = dict(d.loc[:, [c, c[:-3]]].values)
    else:
        s = dict(d.loc[:, [c, 'groups']].values)
    store = {**store, **s}

month = dict(read_from_database('month_id').loc[:, ["month_id", 'month']].values)
store = {**store, **month}

for a in list_tb:
    data = read_from_database(a)
    data = data.apply(lambda d: d.map(lambda x: store.get(x,x)))
    data.to_csv(f'db_inputs/{a}.csv')

# raise Exception("stop here ")


for a in list_tb:
    data = pd.read_csv(f"db_inputs/{a}.csv")
    # data = data.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce')).fillna(data)
    list_dfs.append(data)

print(list_dfs[9])

# Function to insert row in the dataframe
def Insert_row(row_numbers, df, row_value=None,):
    if not isinstance(row_numbers, (list, tuple, set)):
        row_numbers = [row_numbers]
    if row_value is None:
        row_value = np.full(df.shape[1], np.nan)
    df_result = df.copy()
    for row_number in row_numbers:
        if row_number > df_result.index.max()+1:
            print(row_number, df_result.index.max()+1)
            df_result.to_csv('test.csv')
            raise Exception("Invalid row number")
        df1 = df_result[0:row_number]
        df2 = df_result[row_number:]
        df1.loc[row_number]=row_value
        df_result = pd.concat([df1, df2])
        df_result.index = [*range(df_result.shape[0])]
    return df_result

def make_col_num(df):
        new_col = list(range(1,len(df.columns)+1))
        df = Insert_row(0, df, df.columns)
        df.columns = new_col
        return df

def normalize_col(df, ref, nan_col=[], percent_col=[], num_col=False):
    df = df.rename(columns=ref)
    df = df.loc[:, ref.values()]
    df = df.reset_index(drop=True)
    if len(percent_col) > 0:
        df.loc[:, percent_col] = df.loc[:, percent_col].apply(lambda x: x.astype(str).str.strip('%').astype('float') / 100.0 )
    if len(nan_col) > 0:
        df.loc[:, nan_col] = df.loc[:,nan_col].replace(0, np.nan)
    if num_col:
        df = make_col_num(df)
    # df.drop(columns=["creation_date","updation_date"], inplace=True)
    return df


def pivot_helper(idx, val, df, col, multi_col=False, rename=None):
    if rename != None:
        idx = list(rename.values())
        df = df.rename(columns=rename)
    df = pd.pivot_table(df, index=idx,values=val,columns=col, aggfunc='sum').reindex(df.loc[:, idx])
    df = df.reset_index()
    if not multi_col:
        df = df.drop_duplicates(subset=idx).reset_index(drop=True)
    df.columns = [*idx, *[*df.columns][len(idx):]]

    if len(idx) == 1:
        df.loc[:, idx] = df.loc[:, idx].apply(lambda x: x.map(lambda x: x[0]))
    return df


def restruct():

    aa_prodn_from_import_bx_mthly_cols = {
        "refinery_no": "Refinery No.",
        "refinery_id": "Refinery",
        "province_id": "Province",
        "location": "Location",
        "input": "Input",
        "line": "Line",
        "technology_id": "Technology"
    }

    rio_quarterly_cols = {
        "mine_id": 'country'
    }

    refineries_aa_prod_cols = {
        "keyy": "Key",
        "refinery_id": "Refinery Name",
        "country_id": "Country"
    }

    rio_quarterly_reports_cols = {
        "mine_id": "Mine"
    }

    table_2_cols = {
        "mine_id": "Country"
    }

    table_4_cols = {
        "country_id": "country"
    }

    rusal_quarterly_cols = {
        "country_id": "country"
    }


    t_per_t_for_different_bauxite_cols = {
        "country_id": "Country",
        "regency_id": "Regency",
        "digestion_technology": "Technology",
    }

    def merge_col(dff, n, c, s_rev=False):
        cols = list(dff.columns[n:] )
        fun = lambda s: f"{s[0]}{c}{s[1]}"
        if s_rev:
            cols = list(map(fun, map(rev, cols)))
        else:
            cols = list(map(fun, cols))
        dff.columns = [*dff.columns[:n], *cols]
        return dff


    print("yeah")
    list_dfs[0] = pivot_helper(["refinery_no","refinery_id","province_id","location","input","line","technology_id"], "Value",list_dfs[0] , "Date", rename=aa_prodn_from_import_bx_mthly_cols)
    list_dfs[6] = pivot_helper("mine_id", 'Rio_Quarterly_Reports', list_dfs[6], ['year_id', "quarter"], multi_col = True, rename=rio_quarterly_cols)
    list_dfs[7] = pivot_helper(["keyy","refinery_id","country_id"], "aa_production", list_dfs[7], ['year_id',"quarter"], multi_col=True, rename=refineries_aa_prod_cols)
    list_dfs[7] = merge_col(list_dfs[7], 3, ' ', s_rev=True)
    print(list_dfs[7].columns)
    list_dfs[8] = pivot_helper("mine_id", 'Rio_Quarterly_Reports', list_dfs[8], ['year_id', "quarter"], True, rename=rio_quarterly_reports_cols)

    list_dfs[9] = pivot_helper('mine_id', 'table_2', list_dfs[9], 'year_id', rename=table_2_cols)
    list_dfs[10] = pivot_helper('country_id', 'Value', list_dfs[10], 'year_id', rename=table_4_cols)
    list_dfs[11] = pivot_helper("country_id", 'Rusal_Quarterly_Report', list_dfs[11], ['year_id', "quarter"], True, rename=rusal_quarterly_cols)
    list_dfs[17] = pivot_helper(["country_id", "regency_id", "digestion_technology"], "Value", list_dfs[17], ["month_id", "year_id"], multi_col=True, rename=t_per_t_for_different_bauxite_cols)
    list_dfs[17] = merge_col(list_dfs[17], 3, ' - ')
    print(list_dfs[17].columns)

    # list_dfs[6] = pd.pivot_table(list_dfs[6],index=["mine"],values='Rio_Quarterly_Reports',columns=['year',"quarter"])#,"quarter"]

    # # df1,df2,df3,df4,df5,df6,df7,df8,df9,df10 = list_dfs
    # print(list_dfs)
    # print(all_rest)

    alumina_monthly_supply_cols = {
        "Date": "Date",
        "year_id": "Year",
        "Quarter": "Quarter",
        "Imports_by_Source_kt_Aa": "Imports by Source, kt Aa",
        "Australia": "Australia",
        "India": "India",
        "Vietnam": "Vietnam",
        "Other": "Other",
        "Total_Import": "Total Import",
        "Export_kt_Aa_Total": "Export, kt Aa - Total",
        "Production_Mt_Aa": "Production, Mt Aa",
        "Production_Shandong": "Production - Shandong",
        "Production_Henan": "Production - Henan",
        "Production_Shanxi": "Production - Shanxi",
        "Production_Guizhou": "Production - Guizhou",
        "Production_Guangxi": "Production - Guangxi",
        "Production_Chongqing": "Production - Chongqing",
        "Production_Other": "Production - Other",
    }



    cm_global_bx_production_inputs_cols = {
        "name": "Name",
        "company_id": "Company",
        "group_id": "Group",
        "ownership_id": "Ownership",
        "ownership": "Ownership(%)",
        "offtake": "Offtake",
        "offtake1": "Offtake( %)",
        "country_id": "Country",
        "region": "Region",
    }


    data_charts_inputs_cols = {
        "province_id": "Country",
        "Q4_capacity": "Q4 Capacity",
        "Q4_operational": "Q4 Operational",
        "Q4_long_term_idled": "Q4 Long Term Idled",
        "Q3_capacity": "Q3 Capacity",
        "Q3_operational": "Q3 Operational"
    }


    Data_Monthly_Inputs_cols  = {
        "Date": "Date",
        "year_id": "Year",
        "Quarter": "Quarter",
        "Consumption_of_Bauxite_Mt": "Consumption of Bauxite, Mt",
        "Imported_Bauxite_revised": "Imported Bauxite - revised",
        "Domestic_Bauxite_revised": "Domestic Bauxite - revised",
        "Bauxite_Requirement_revised": "Bauxite Requirement - revised",
        "Bauxite_Consumption_Domestic_revised": "% Bauxite Consumption Domestic - revised",
    }

    other_inputs_bx_pro_cols = {
        "No": "No",
        "country_id": "Country",
        "IAI_Wiorld_Al_Region": "IAI Wiorld Al Region",
    }


    cbix_data_cols = {
        "date": "Date",
        "value": "Value"
    }

    changjiang_lme_daily_inputs_cols = {
        "Date_1": "Date 1",
        "Low": "Low",
        "High": "High",
        "Average": "Average",
        "Date_2": "Date 2",
        "Cash_Buyer": "Cash Buyer",
        "Cash_Seller_Settlement": "Cash Seller & Settlement",
        "Date_3": "Date 3",
        "USD_CNY": "USD:CNY",
    }


    platts_vs_cmaax_inputs_cols = {
        "date": "Date",
        "australia_fob_wa": "Australia (FOB WA)",
        "cmaax": "CMAAX",
        "frieght": "Frieght",
        "usd_rmb": "USD:RMB",
        "import_inducement_value": "Import Inducement Value",
        "vat": "VAT",
        "port_handling_rmb_t": "Port Handling (RMB/t)",
    }



    result = {
        'aa_prodn_from_import_bx_mthly': make_col_num(list_dfs[0]),
        "alumina_monthly_supply": normalize_col(list_dfs[1], alumina_monthly_supply_cols),
        "cm_global_bx_production_inputs": normalize_col(list_dfs[2], cm_global_bx_production_inputs_cols),
        "company_supply": list_dfs[3],
        "data_charts_inputs": normalize_col(list_dfs[4], data_charts_inputs_cols),
        "Data_Monthly_Inputs": normalize_col(list_dfs[5], Data_Monthly_Inputs_cols, num_col=True),
        'Global_Bx_Quarterly_inputs': make_col_num(list_dfs[6]),
        'Refineries_Aa_Production': list_dfs[7],
        'Rio_Quarterly_Reports': list_dfs[8],
        'table_2': list_dfs[9],
        'table_4_table_7': list_dfs[10],
        'Rusal_Quarterly_Report': list_dfs[11],
        'other_inputs_bxpro': normalize_col(list_dfs[12], other_inputs_bx_pro_cols),
        "cbix_data": normalize_col(list_dfs[13], cbix_data_cols),
        "changjiang_lme_daily_inputs": normalize_col(list_dfs[14], changjiang_lme_daily_inputs_cols),
        "platts_vs_cmaax_inputs": normalize_col(list_dfs[15], platts_vs_cmaax_inputs_cols),
        "table_6": list_dfs[17]
    }

    one = [*np.full(4, np.nan), "Henan", *np.full(101, np.nan)]
    two = [*np.full(4, np.nan), "Shanxi", *np.full(101, np.nan)]
    three = [*np.full(4, np.nan), "Guizhou", *np.full(101, np.nan)]
    four = [*np.full(4, np.nan), "Guangxi", *np.full(101, np.nan)]
    five = [*np.full(4, np.nan), "Chongqing", *np.full(101, np.nan)]
    six = [*np.full(4, np.nan), "Other", *np.full(101, np.nan)]

    days_of_month = [np.nan, np.nan, np.nan, np.nan, np.nan, "Days in month", np.nan, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 31, 29, 31]
    result["aa_prodn_from_import_bx_mthly"] = Insert_row(0, result["aa_prodn_from_import_bx_mthly"], days_of_month)
    result["aa_prodn_from_import_bx_mthly"] = Insert_row([2, 8, 14, 19, 24, 28, 32, 33, 39, 45, 50, 55, 59, 63, 64, 70, 76, 81, 86, 90, 94, 95, 101, 107, 112, 117, 121, 125, 126, 132, 138, 143, 148, 152, 156, 157, 163, 169, 174, 179, 183, 187, 188, 194, 200, 205, 210, 214, 218, 219, 225, 231, 236, 241, 245, 249, 250, 256, 262, 267, 272, 276, 280, 281, 287, 293, 298, 303, 307, 311, 312, 318, 324, 329, 334, 338, 342, 343, 349, 354, 355, 360, 365, 369, 373, 374, 380, 386, 391, 396, 400, 404, 405, 411, 416, 417, 422, 427, 431, 435, 436, 442, 447, 448, 453, 458, 462, 466, 467, 473, 479, 484, 489, 493, 497, 498, 504, 510, 515, 520, 524, 528, 529, 535, 541, 546, 551, 555, 559, 560, 566, 572, 577, 582, 586, 590, 591, 597, 603, 608, 613, 617, 621, 622, 628, 634, 639, 644, 648, 652, 653, 659, 665, 670, 675, 679, 683, 684, 690, 696, 701, 706, 710, 714, 715, 721, 727, 732, 737, 741, 745, 747, 756] , result["aa_prodn_from_import_bx_mthly"])
    result["aa_prodn_from_import_bx_mthly"] = Insert_row(758, result["aa_prodn_from_import_bx_mthly"], one)
    result["aa_prodn_from_import_bx_mthly"] = Insert_row(759, result["aa_prodn_from_import_bx_mthly"], two)
    result["aa_prodn_from_import_bx_mthly"] = Insert_row(760, result["aa_prodn_from_import_bx_mthly"], three)
    result["aa_prodn_from_import_bx_mthly"] = Insert_row(761, result["aa_prodn_from_import_bx_mthly"], four)
    result["aa_prodn_from_import_bx_mthly"] = Insert_row(762, result["aa_prodn_from_import_bx_mthly"], five)
    result["aa_prodn_from_import_bx_mthly"] = Insert_row(763, result["aa_prodn_from_import_bx_mthly"], six)
    result["aa_prodn_from_import_bx_mthly"] = Insert_row([765, 766, 767, 768, 769, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 780, 781, 782, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 793, 794, 795, 796, 797, 798, 799, 800, 801, 802, 803], result["aa_prodn_from_import_bx_mthly"])


    for a in result.keys():
        print(a)
        result[a] = result[a].apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce')).fillna(result[a])
        result[a].to_csv(f"outs/{a}.csv")
    return result

# restruct()
