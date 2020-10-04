import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine
import difflib
import re

engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

def read_from_database(table):
    query = f'SELECT * FROM {table}'
    data = pd.read_sql(sql=query, con=engine)
    cols = data.columns
    # if "Date" in cols:
    #     data["Date"] = data["Date"].astype('datetime64[ns]')
    # if "date" in cols:
    #     data["date"] = data["date"].astype('datetime64[ns]')
    return data.apply(lambda x: pd.to_numeric(x.astype(str).str.strip('"').str.strip('$').str.replace(',',''), errors='coerce')).fillna(data)




all_foreignkeys = ['technology', 'refinery', 'province', 'year', 'company', 'ownership', 'groups', 'country', 'mine', 'regency', 'port', 'region']
store = {}
for a in all_foreignkeys:
    d = read_from_database(a)
    c = [f for f in d.columns if f[-3:] == '_id'][0]
    if c != 'group_id':
        s = dict(d.loc[:, [c, c[:-3]]].values)
    else:
        s = dict(d.loc[:, [c, 'groups']].values)
    store[a] = s
    # store = {**store, **s}

month = dict(read_from_database('month_id').loc[:, ['month', "month_id"]].values)
store["month"] = month


all_fk_maps = {
    "aa_prodn_from_import_bx_mthly": {'Refinery': store['refinery'], 'Province': store['province'], 'Technology': store['technology']},
    "aa_prodn_from_import_bx_mthly_import_bx_consumed":{"Country": store['country']} ,
    "al_quart_chart_tt_for_different_bauxite": {'Country': store['country'], 'Regency': store['regency'], 'Technology Code': store['technology']},
    "al_quart_chart_smb_split_tonnes": {"Country": store['country']},
    "Refineries_Aa_Production_1": {"Refinery Name": store['refinery'], "Country": store['country']},
    "mines_bx_production_2": {"Name": store['mine'], 'Country': store['country']},
    "mines_bx_production_3": {"Name": store['mine'], 'Country': store['country']},
    "al_quart_chart_raw_trade_data": {'Country of Origin': store["country"], 'Regency': store['regency'], 'Month': store['month'], "Year": store['year'] },
    "al_quart_chart_tt_per_country": {'Country': store['country']},
    "al_quart_chart_table_1": {'Country': store['country'], 'Regency': store['regency']},
    "al_quart_chart_proportions": {'Country': store['country'], 'Regency': store['regency']},
    "cm_global_bx_production_inputs": {"Company": store["company"], "Group": store['groups'], "Ownership": store['ownership'], "Country": store['country'], "Region": store['region']},
    "al_quart_chart_chart9": {'Year': store['year']},
    "al_quart_chart_chart11": {"Country": store['country']},
    "al_quart_chart_chart12": {"Country": store['country']},
    "al_quart_chart_chartt_11": {"Country": store['country']},
    "al_quart_chart_chartt_12": {"Country": store['country']},
    "data_input_annually": {'Year': store['year']},
    "alumina_monthly_supply": {'Year': store['year']},
    "aluminium_monthly_supply": {'Year': store['year']},
    "alumina_price_trend": {'Year': store['year']},
    "Global_Bx_Quarterly_inputs": {'Year': store['year']},
    "data_input_monthly_bauxite_imports": {'Year': store['year']},
    "data_input_monthly_bauxite_imports_2": {'Year': store['year']},
    "data_input_monthly_bauxite_imports_3": {'Year': store['year']},
    "data_input_monthly_bauxite_imports_4": {'Year': store['year']},
    "data_input_monthly_bauxite_imports_5": {'Year': store['year']},
    "data_input_monthly_bauxite_style": {'Year': store['year']},
    "data_input_monthly_input_bar": {'Year': store['year']},
    "data_input_monthly_bar": {'Year': store['year']},
    "data_input_monthly_exchange_rate": {'Year': store['year']},
    "data_input_monthly_bauxite_usage": {'Year': store['year']},
    "data_input_monthly_cm_port_bx_inventry": {'Year': store['year']},
    "data_input_monthly_global_alumina_prd": {'Year': store['year']},
    "data_input_monthly_aa_imported_bauxite": {'Year': store['year']},
    "Data_Monthly_Inputs": {'Year': store['year']},
    "al_quart_chart_aa_from_imported_baux": {'Year': store['year']},
    "al_quart_chart_CGA": {'Year': store['year']},
    "al_quart_chart_CM_Imports_Bx-error Adjustment": {'Year': store['year']},
    "al_quart_chart_imported_bauxite_stocks_major": {'Year': store['year']},
    "Rio_Quarterly_Report": {'Year': store['year']},
    "Rusal_Quarterly_Report": {'Year': store['year']},
    "Hydro_Quarterly_Bx_": {'Year': store['year']},
    "South_32_Alumina_Production": {'Year': store['year']},
    "Hindalco": {'Year': store['year']},
    "Vedanta": {'Year': store['year']},
    "NALCO": {'Year': store['year']},
    "Production_for_Main_Companies": {'Year': store['year']},
    "mines_bx_production_1": {"Name": store['mine'], 'Country': store['country'], 'Region': store['region']},
    "other_inputs_bxpro": {"Country": store['country'], 'IAI Wiorld Al Region': store['region']},
    "summary_chinese_province": {"Province": store["province"]},
    "china_provincial_imp": {"Province": store["province"]}
    
}   



pivoted_tables = [
    "aa_prodn_from_import_bx_mthly",
    "aa_prodn_from_import_bx_mthly_import_bx_consumed",
    "aa_prodn_from_import_bx_mthly_1",
    "al_quart_chart_bauxite_sources",
    "al_quart_chart_tt_for_different_bauxite",
    "al_quart_chart_smb_split_tonnes",
    "Refineries_Aa_Production_1",
    "mines_bx_production_2",
    "mines_bx_production_3",
    "summary_chinese_province",
    "china_provincial_imp"
]


all_names = [
    "al_quart_chart_raw_trade_data",
    "al_quart_chart_tt_per_country",
    "al_quart_chart_table_1",
    "al_quart_chart_proportions",
    "cbix_data",
    "changjiang_lme_daily_inputs",
    "changjiang_lme_daily_return_to_contents",
    "cm_global_bx_production_inputs",
    "al_quart_chart_chart9",
    "al_quart_chart_chart11",
    "al_quart_chart_chart12",
    "al_quart_chart_chartt_11",
    "al_quart_chart_chartt_12",
    "data_input_annually",
    "alumina_monthly_supply",
    "aluminium_monthly_supply",
    "alumina_price_trend",
    "Global_Bx_Quarterly_inputs",
    "data_input_monthly_bauxite_imports",
    "data_input_monthly_bauxite_imports_2",
    "data_input_monthly_bauxite_imports_3",
    "data_input_monthly_bauxite_imports_4",
    "data_input_monthly_bauxite_imports_5",
    "data_input_monthly_bauxite_style",
    "data_input_monthly_input_bar",
    "data_input_monthly_bar",
    "data_input_monthly_exchange_rate",
    "data_input_monthly_bauxite_usage",
    "data_input_monthly_cm_port_bx_inventry",
    "data_input_monthly_global_alumina_prd",
    "data_input_monthly_aa_imported_bauxite",
    "Data_Monthly_Inputs",
    "al_quart_chart_aa_from_imported_baux",
    "al_quart_chart_CGA",
    "al_quart_chart_CM_Imports_Bx_error_Adjustment",
    "al_quart_chart_imported_bauxite_stocks_major",
    "Rio_Quarterly_Report",
    "Rusal_Quarterly_Report",
    "Hydro_Quarterly_Bx_",
    "South_32_Alumina_Production",
    "Hindalco",
    "Vedanta",
    "NALCO",
    "Production_for_Main_Companies",
    "mines_bx_production_1",
    "other_inputs_bxpro",
    "other_inputs_bxpro_1",
    "platts_vs_cmaax_inputs",
]   


def dropIfExists(df, cols):
    for c in cols:
        if c in df.columns:
            df.drop(columns=[c], inplace=True)
    return df

def rev_meltdown(df, idx=None, val='value', column='variable'):
    if idx is None:
        df = dropIfExists(df, ['creation_date', 'updation_date'])
        idx = list(df.columns)
        idx.remove(val)
    # return df.pivot(index= (idx)[val].aggregate('mean').unstack().reset_index()
    result = df.pivot(values=val, index=idx[:-1], columns=column).reset_index()
    return result


def final_touch(df, name):
    data = df.copy()
    data = data.replace({None: np.nan, "Not available": np.nan})
    cols = data.columns
    lo = ["al_quart_chart_chart9"]
    try:
        if "Date" in cols and not name in lo :
            data["Date"] = data["Date"].astype('datetime64[ns]')
        if "date" in cols and not name in lo:
            data["date"] = data["date"].astype('datetime64[ns]')
    except:
        print("A date exception here")
    data.reset_index(drop=True, inplace=True)
    # data.columns = pd.Series(data.columns).dt.normalize()
    return data

def convert_date_columns(data):
    temp_g = pd.DataFrame(list(data.columns), columns=['T'])
    temp_g['T'] = pd.to_datetime(temp_g['T'], errors='coerce').dt.normalize().fillna(temp_g['T'])
    data.columns = temp_g['T'].values
    print(data.columns)
    return data

def restruct():
    dfs = {}
    tnc = ["aa_prodn_from_import_bx_mthly", "aa_prodn_from_import_bx_mthly_1", "aa_prodn_from_import_bx_mthly_import_bx_consumed"]
    for sh in all_names:
        name = sh
        print(name)
        data = read_from_database(name)
        # data = data.replace(store)
        if name in all_fk_maps:
            print(name)
            for f in all_fk_maps[name]:
                data[f] = data[f].apply(lambda x: all_fk_maps[name][f].get(x,x))
        data.drop(columns=["creation_date", "updation_date", f"{name}_id"], inplace=True)
        data = convert_date_columns(data) if name in tnc else data
        dfs[name] = final_touch(data, name)
        data.to_csv(f"outs/{name}.csv",index=False )
    for c in pivoted_tables:
        name = c
        print(name)
        data = read_from_database(name)
        # data = data.replace(store)
        if name in all_fk_maps:
            print(name)
            for f in all_fk_maps[name]:
                data[f] = data[f].apply(lambda x: all_fk_maps[name][f].get(x,x))
        data = rev_meltdown(data)
        data.drop(columns=[f"{name}_id"], inplace=True)
        # print(data.columns)
        data = convert_date_columns(data) if name in tnc else data
        dfs[name] = final_touch(data, name)
        data.to_csv(f"outs/{name}.csv",index=False )
    return dfs


# rest = restruct()


"""
        self.dataInputAnnual= rest["data_input_annually"]
        self.bxImports_rawData = rest["al_quart_chart_raw_trade_data"]
        self.bxImports_tab1 = rest["al_quart_chart_table_1"]
        self.bxImports_tab2 = rest["al_quart_chart_smb_split_tonnes"]
        self.bxImports_tab3 = pd.read_excel(bx_imports_wbk, sheet_name='Re-done tonnes')#table3
        self.bxImports_tab4 = pd.read_excel(bx_imports_wbk, sheet_name='Re-done proportions within Ctry')#table4
        self.bxImports_tab5 = rest["al_quart_chart_proportions"]
        self.bxImports_tab6 = rest["al_quart_chart_tt_for_different_bauxite"]
        self.bxImports_tab7 = rest["al_quart_chart_tt_per_country"]
        self.bxImports_tab8 = pd.read_excel(bx_imports_wbk, sheet_name='Tt per Country no gaps')#tab8
        self.Changjiang_LME_tab1 = rest["changjiang_lme_daily_return_to_contents"]
        self.Changjiang_LME_tab2 = rest["changjiang_lme_daily_inputs"]
        self.platts_vs_cmaax = rest["platts_vs_cmaax_inputs"]
        self.otherInput_bx_tab1 = rest["other_inputs_bxpro"]
        self.otherInput_bx_tab2 = rest["other_inputs_bxpro_1"]
        self.ref_AA_productions = rest["Refineries_Aa_Production_1"]
        self.data_inp_montly_tab1 = rest["alumina_monthly_supply"]
        self.data_inp_montly_tab2 = rest["aluminium_monthly_supply"]
        self.data_inp_montly_tab3 = rest["alumina_price_trend"]
        self.data_inp_montly_bax1 = rest["data_input_monthly_bauxite_imports"]
        self.data_inp_montly_bax2 = rest["data_input_monthly_bauxite_imports_2"]
        self.data_inp_montly_bax3 = rest["data_input_monthly_bauxite_imports_3"]
        self.data_inp_montly_bax4 = rest["data_input_monthly_bauxite_imports_4"]
        self.data_inp_montly_bax5 = rest["data_input_monthly_bauxite_imports_5"]
        self.data_inp_montly_baxStyle = rest["data_input_monthly_bauxite_style"]
        self.data_inp_montly_exchange_rate = rest["data_input_monthly_exchange_rate"]
        self.data_inp_montly_inpBAR = rest["data_input_monthly_input_bar"]
        self.dataInputMontly_BAR = rest["data_input_monthly_bar"]
        self.data_inp_montly_bauxite_usage = rest["data_input_monthly_bauxite_usage"]
        self.data_imported_bauxite = rest["data_input_monthly_aa_imported_bauxite"]
        self.dataInputMontly_CmPortBx = rest["data_input_monthly_cm_port_bx_inventry"]
        self.dataInputMontly_global_alumina = rest["data_input_monthly_global_alumina_prd"]
        self.AAproductions_main         = rest["aa_prodn_from_import_bx_mthly"]
        self.AAproductions_bx_consumed  = rest["aa_prodn_from_import_bx_mthly_import_bx_consumed"]
        self.AAproductions_sheet3       = rest["aa_prodn_from_import_bx_mthly_1"]
        self.minesBxPrdocution1 = rest["mines_bx_production_1"]
        self.minesBxPrdocution2 = rest["mines_bx_production_2"]
        self.minesBxPrdocution3 = rest["mines_bx_production_3"]
        self.BauxiteSources_inputs = rest["al_quart_chart_bauxite_sources"]
        self.data_monthly_inp_comspt = rest["Data_Monthly_Inputs"]
        self.data_monthly_imp_CGA = rest["al_quart_chart_CGA"]
        self.data_monthly_imp_aaImportedBauxite = rest["al_quart_chart_aa_from_imported_baux"]
        self.data_monthly_imp_CMerrorAdjustment = rest["al_quart_chart_CM_Imports_Bx_error_Adjustment"]
        self.data_monthly_imp_ImportedBauxiteStocks = rest["al_quart_chart_imported_bauxite_stocks_major"]
        self.GlobalBx_cmglobal      = rest["Global_Bx_Quarterly_inputs"]
        self.GlobalBx_rioReport     = rest["Rio_Quarterly_Report"]
        self.GlobalBx_russelReport  = rest["Rusal_Quarterly_Report"]
        self.GlobalBx_hydro         = rest["Hydro_Quarterly_Bx_"]
        self.GlobalBx_south32       = rest["South_32_Alumina_Production"]
        self.GlobalBx_hindalco      = rest["Hindalco"]
        self.GlobalBx_vedanta       = rest["Vedanta"]
        self.GlobalBx_nalco         = rest["NALCO"]
        self.GlobalBx_productionMainCompanies = rest["Production_for_Main_Companies"]
        self.data_charts_11 = rest["al_quart_chart_chart11"]
        self.data_charts_12 = rest["al_quart_chart_chart12"]
        self.data_charts_9 = rest["al_quart_chart_chart9"]
        self.data_chartt_11 = rest["al_quart_chart_chartt_11"]
        self.data_chartt_12 = rest["al_quart_chart_chartt_12"]

"""

# you have to upload aa_prodn_from_import_bx_mthly_1