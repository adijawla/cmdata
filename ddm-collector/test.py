from flatdb.flatdbconverter import read_output_database, read_from_database, reverse
import extension as et
import pandas as pd
import numpy as np

# AA Prodction From Domestic Bx capprod 1589 
# AA Production from Import Bx capprod 1713

ids = ['refinery','province','category','technology','ownership','owner','bauxite','year','county']
idnames = ['refinery_id','province_id','category_id','technology_id','ownership_id','owner_id','bauxite_id','year_id','county_id']
x = et.inputprocess(ids,['proddatah'],idnames)
x.mergeall()
x.pivot('proddatah','year','production',['bauxite','owner','ownership','technology','category','province','refinery'])
col = ["refinery","province","category","technology","ownership","owner","bauxite","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019","2020","2021","2022","2023","2024","2025","2026","2027","2028","2029","2030","2031"]


def insert_row(row_number, df, row_value): 
    df1 = df[0:row_number] 
    df2 = df[row_number:]
    df1.loc[row_number]=row_value 
    df_result = pd.concat([df1, df2]) 
    df_result.index = [*range(df_result.shape[0])] 
    return df_result

ren =  {
    "refinery": "Refinery",
    "province": "Province",
    "category": "Province category",
    "technology": "Technology",
    "ownership": "Ownership",
    "owner": "Owner",
    "bauxite": "Bauxite"
}


def handle_prod(df):
    df = df.loc[:, col]
    df.rename(columns=ren, inplace=True)
    refno = ["1.1","1.2","2","3","4","5","6.1","6.2","7","8","9","10","11","12","13","14","15","16","17","18","19.1","19.2","20","21","22","23.1","23.2","24.1","24.2","24.3","25.1","25.2","26.1","26.2","27","28","29","30.1","30.2","31","32","33","34","35","36","37","38","39","40.1","40.2","41","42.1","42.2","43.1","43.2","44","45","46.1","46.2","46.3","47.1","47.2","48","49.1","49.2","50","51","52.1","52.2","53.1","53.2","54","55","56.1","56.2","57.1","57.2","58.1","58.2","59.1","59.2","60","61","62","63","64","65","66","67.1","67.2","68","69","70.1","70.2","71","72","73","74","75","76","77"]
    df_1 =  pd.DataFrame(np.array([np.full(len(refno), np.nan), refno]).transpose(), columns=["tonnage", "Ref. No."])
    df = df_1.join(df)
    df.insert(9, "ref cell", np.nan)
    df.insert(10, "ref cell row", np.nan)
    col_to_txt = pd.read_csv("DDM special links getter Inputs/Columns to text.csv")
    year_col_to_txt = col_to_txt.loc[4:len(col)-4,"Letter"].values
    year_col_to_txt = [*np.full(df.shape[1] - len(year_col_to_txt) - 1, np.nan), "ref cell column =>", *year_col_to_txt]
    df = insert_row(0, df, df.columns)
    df.columns = [i for i in range(1, len(df.columns)+1)]
    df = insert_row(0, df, year_col_to_txt)
    df = insert_row(0, df, np.full(df.shape[1], np.nan))
    return df 


def interrelationship_rest():
    proddatah = x.data('proddatah')
    proddatah = handle_prod(proddatah)
    print(proddatah)
    ddm_o_set = ["Aa Prodction From Domestic Bx", "Aa Production From Import Bx", "Bauxite Usage", "Closing Stock Output", "Bar By Refinery Alcoa Work "]
    # ddm_o_set = [' '.join(a.split('_')).title() for a in ddm_o_set]
    ddm_outs = read_output_database(280,  ddm_o_set)
    print(ddm_outs)
    ddm_rev = reverse(ddm_outs, "Draw Down Model CBIX 55", ddm_o_set)["Draw Down Model CBIX 55"]
    ddm_rev["Aa Prodction From Domestic Bx"] = handle_prod(ddm_rev["Aa Prodction From Domestic Bx"])
    ddm_rev["Aa Production From Import Bx"]  = handle_prod(ddm_rev["Aa Production From Import Bx"])
    # pf_o_set = ["J384"]
    # pf_sets = read_output_database(228, pf_o_set)
    # pf_rev = reverse(pf_sets, "Price forecast model", pf_o_set)
    proddatah.to_csv("proddatah.csv", index=False)
    for a in ddm_rev:
        ddm_rev[a].to_csv(f"inter/{a}.csv", index=False)
    # return  {

    # }

    print(ddm_outs)
    # print(pf_sets)


interrelationship_rest()