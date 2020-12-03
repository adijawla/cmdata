import pyodbc
import pandas as pd
import numpy as np
import csv
import os
pd.set_option('display.max_columns', None)


#Database connection
server = 'magdb.database.windows.net'
database = 'input_db'
username = 'letmetry'
password = 'T@lst0y50'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()



# Change column names
def chcol(df,sname):
    colname=pd.read_excel('Inputs/column_name.xlsx',sheet_name=sname)
    colname=list(colname.columns)
    print(colname)
    print(df.columns)
    df.columns=colname
    return df




def restruct():

    # Refineries PDT inputs 1
    refpdtip1 = pd.read_sql_query('SELECT r.refinery,o.ownership,* FROM input_db.dbo.refineries_PDT_inputs_1 p JOIN input_db.dbo.refinery r ON p.refinery_id=r.refinery_id JOIN input_db.dbo.ownership o ON p.ownership_id=o.ownership_id',conn)
    refpdtip1.drop(['creation_date','updation_date','refinery_id','ownership_id','Transport_distance', 'refineries_pdt_inputs_1_id'], axis=1, inplace=True)
    refpdtip1=refpdtip1.iloc[:, :-1]
    refpdtip1=refpdtip1.iloc[:, :-1]
    col = refpdtip1.pop("ref_no")
    refpdtip1.insert(0, col.name, col)

    refpdtip1=chcol(refpdtip1,'refpdtip1')
    # refpdtip1.to_csv('Inputs/outputcsv/refineries_pdt_inputs_1.csv',index=False)


    # Refineries PDT inputs 2
    refpdtip2 = pd.read_sql_query('SELECT *,c.port as Option_1_Port,d.port as Option_2_Port FROM input_db.dbo.transport_fee_of_import_bauxite p JOIN input_db.dbo.port c ON p.port_id=c.port_id JOIN input_db.dbo.port d ON p.port_id2=d.port_id',conn)
    refpdtip2.drop(['creation_date','updation_date','port_id','port_id2','transport_fee_of_import_bauxite_id'], axis=1, inplace=True)
    refpdtip2.insert(0,'Option 1 Railway(km)','')
    refpdtip2.insert(0,'Factor','')
    refpdtip2.insert(1,'Value','')
    col = refpdtip2.pop("Option_1_Port")
    refpdtip2.insert(2, col.name, col)
    col = refpdtip2.pop("Option_2_Port")
    refpdtip2.insert(7, col.name, col)
    refpdtip2=refpdtip2.iloc[:, :-1]
    refpdtip2=refpdtip2.iloc[:, :-1]

    refpdtip2.loc[0,'Factor']="Rail Loading"
    refpdtip2.loc[1,'Factor']="Rail Rate"
    refpdtip2.loc[2,'Factor']="Road Rate"
    refpdtip2.loc[3,'Factor']="Waterway Rate"
    refpdtip2.loc[4,'Factor']="Port Handling"
    refpdtip2.loc[0,'Value']="15.4"
    refpdtip2.loc[1,'Value']="0.14"
    refpdtip2.loc[2,'Value']="0.35"
    refpdtip2.loc[3,'Value']="0.018"
    refpdtip2.loc[4,'Value']="20"

    refpdtip2=chcol(refpdtip2,'refpdtip2')
    # refpdtip2.to_csv('Inputs/outputcsv/refineries_pdt_inputs_2.csv',index=False)




    # AA PRODUCTION DOM BAUXITE
    aaprod = pd.read_sql_query('SELECT p.ref_no as [Ref. No.],c.refinery as Refinery,y.year,value FROM input_db.dbo.aa_production_dom_bauxite p JOIN input_db.dbo.refinery c ON p.refinery_id=c.refinery_id JOIN input_db.dbo.year y ON p.year_id=y.year_id',conn)
    aaprod['value']=aaprod['value'].astype(str).astype(float)
    aaprod['Ref. No.']=aaprod['Ref. No.'].astype(str).astype(float)
    aaprod.insert(2,'Province','')
    aaprod.insert(3,'Prov - Category','')
    aaprod.insert(4,'Technology','')
    aaprod.insert(5,'Ownership','')
    aaprod.insert(6,'Owner','')
    aaprod.insert(7,'Bauxite Previous','')
    aaprod.insert(8,'Bauxite Now','')
    pivot_aaprod = aaprod.pivot_table(index=['Ref. No.','Refinery','Province','Prov - Category','Technology','Ownership','Owner','Bauxite Previous','Bauxite Now'], values='value',columns='year',aggfunc=sum)
    pivot_aaprod.reset_index(inplace=True)
    l=[]
    k=[]
    for i in range(0,len(pivot_aaprod.columns)):
        if i<9:
            k.append(pivot_aaprod.columns[i])
        else:
            l.append(pivot_aaprod.columns[i])
    l = list(map(int,l))
    l = k+l
    pivot_aaprod.columns=l


    # pivot_aaprod.to_csv('Inputs/outputcsv/aaprod_dom_bauxite.csv')




    # Demand
    pivot_demand=pd.DataFrame(columns=['Demand',2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030,2031])
    pivot_demand.loc[0]=['Total Alumina Apparent Demand (Mt)',73.9,77,79.8,81.3,82.3,82.5,82.9,83.3,83.7,83.9,83.9,83.9]
    pivot_demand.loc[1]=['Alumina Net Imports (Mt)',3,3,3,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5]
    pivot_demand.loc[2]=['Total AA to be produced in China',0,0,0,0,0,0,0,0,0,0,0,0]
    pivot_demand.loc[3]=['AA production from domestic bauxite',0,0,0,0,0,0,0,0,0,0,0,0]
    pivot_demand.loc[4]=['Demand of alumina produced by imported bauxite',0,0,0,0,0,0,0,0,0,0,0,0]

    # demand = pd.read_sql_query('SELECT demand as Demand,y.year,value FROM input_db.dbo.demand p JOIN input_db.dbo.year y ON p.year_id=y.year_id',conn)
    # demand['value']=demand['value'].astype(str).astype(float)
    # pivot_demand = demand.pivot_table(index='Demand', values='value',columns='year')
    # pivot_demand.reset_index(inplace=True)
    # l=[]
    # for i in range(1,len(pivot_demand.columns)):
    #    l.append(pivot_demand.columns[i])
    # l = list(map(int,l))
    # l = ['Demand']+l
    # pivot_demand.columns=l
    # pivot_demand=pivot_demand.loc[[4,1,3,0,2], :]
    # pivot_demand.to_csv("Inputs/outputcsv/demand.csv",index=False)





    # KeyInputs
    key=pd.DataFrame({'Factor':['First year of forecast','In forecast: Minimum refinery utilisation for viability','In forecast: Minimum refinery capacity for viability (MTPY)','In forecast: Domestic capacity overall utilisation on switch to imports'],'Value':[2020,0.2,0.1,0.8]})
    # key.to_csv('Inputs/outputcsv/keyinputs.csv',index=False)





    # Base Capacity
    basecap = pd.read_sql_query('SELECT r.ref_no as [Ref. No.],a.refinery as Refinery,b.province as Province,c.category as [Prov - Category],d.technology as Technology,e.ownership as Ownership,f.owner as Owner,g.bauxite as [Bauxite Previous],h.year,capacity FROM input_db.dbo.capbase p JOIN input_db.dbo.refineries_PDT_inputs_1 r ON p.refinery_id=r.refinery_id JOIN input_db.dbo.refinery a ON p.refinery_id=a.refinery_id JOIN input_db.dbo.province b ON p.province_id=b.province_id JOIN input_db.dbo.category c ON p.category_id=c.category_id JOIN input_db.dbo.technology d ON p.technology_id=d.technology_id JOIN input_db.dbo.ownership e ON p.ownership_id=e.ownership_id JOIN input_db.dbo.owner f ON p.owner_id=f.owner_id JOIN input_db.dbo.bauxite g ON p.bauxite_id=g.bauxite_id JOIN input_db.dbo.year h ON p.year_id=h.year_id',conn)
    basecap.insert(0,'Bauxite Now','')
    basecap['Bauxite Now']=basecap['Bauxite Previous'].values
    basecap['capacity']=basecap['capacity'].astype(str).astype(float)
    basecap['Ref. No.']=basecap['Ref. No.'].astype(str).astype(float)
    pivot_basecap = basecap.pivot_table(index=['Ref. No.','Refinery','Province','Prov - Category','Technology','Ownership','Owner','Bauxite Previous','Bauxite Now'], values='capacity',columns='year',aggfunc=sum)
    pivot_basecap.reset_index(inplace=True)
    l=[]
    k=[]
    for i in range(0,len(pivot_basecap.columns)):
        if i<9:
            k.append(pivot_basecap.columns[i])
        else:
            l.append(pivot_basecap.columns[i])
    l = list(map(int,l))
    l = k+l
    pivot_basecap.columns=l

    spare = [72,"spare", "spare", 'Other', "spare", "spare",np.nan,"spare", "spare", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    new_df = pd.DataFrame(columns=l)
    spare1= [72,"spare", "spare", 'Other', "spare", "spare","spare","spare", "spare", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    new_df.loc[0] = spare1


    for i in range(1,7):
        spare[0] += 1
        new_df.loc[i] = spare

    pivot_basecap = pd.concat([pivot_basecap, new_df], ignore_index=True)

    # pivot_basecap.to_csv('Inputs/outputcsv/basecapacity.csv',index=False)




    # Basecap by regions
    basecapbyreg = pd.DataFrame(columns=pivot_basecap.columns)
    rows=pd.DataFrame({'Prov - Category':['Guangxi','Guizhou','Henan','Shanxi','Other','','',''],'Technology':['','','','','','H-B','L-B','fly ash'],'Bauxite Now':['Domestic','Domestic','Domestic','Domestic','Domestic','Imported','Imported','Fly ash']})
    ll=[2005,2006]
    for i in range(2007,2032):
        ll.append(i)
    basecapbyreg=basecapbyreg.append(rows,ignore_index=True)
    for col in ll:
        basecapbyreg[col] = 0

    # basecapbyreg.to_csv('Inputs/outputcsv/basecapacitybyregions.csv',index=False)






    # Base Production

    baseprod = pd.read_sql_query('SELECT r.ref_no as [Ref. No.],a.refinery as Refinery,b.province as Province,c.category as [Prov - Category],d.technology as Technology,e.ownership as Ownership,f.owner as Owner,g.bauxite as [Bauxite Previous],h.year,production FROM input_db.dbo.proddatah p JOIN input_db.dbo.refineries_PDT_inputs_1 r ON p.refinery_id=r.refinery_id JOIN input_db.dbo.refinery a ON p.refinery_id=a.refinery_id JOIN input_db.dbo.province b ON p.province_id=b.province_id JOIN input_db.dbo.category c ON p.category_id=c.category_id JOIN input_db.dbo.technology d ON p.technology_id=d.technology_id JOIN input_db.dbo.ownership e ON p.ownership_id=e.ownership_id JOIN input_db.dbo.owner f ON p.owner_id=f.owner_id JOIN input_db.dbo.bauxite g ON p.bauxite_id=g.bauxite_id JOIN input_db.dbo.year h ON p.year_id=h.year_id',conn)
    baseprod.insert(0,'Bauxite Now','')
    baseprod['Bauxite Now']=baseprod['Bauxite Previous'].values
    baseprod['production']=baseprod['production'].astype(str).astype(float)
    baseprod['Ref. No.']=baseprod['Ref. No.'].astype(str).astype(float)
    pivot_baseprod = baseprod.pivot_table(index=['Ref. No.','Refinery','Province','Prov - Category','Technology','Ownership','Owner','Bauxite Previous','Bauxite Now'], values='production',columns='year',aggfunc=sum)
    pivot_baseprod.reset_index(inplace=True)
    l=[]
    k=[]
    for i in range(0,len(pivot_baseprod.columns)):
        if i<9:
            k.append(pivot_baseprod.columns[i])
        else:
            l.append(pivot_baseprod.columns[i])
    l = list(map(int,l))
    l = k+l
    pivot_baseprod.columns=l

    spare = [72,"spare", "spare", 'Other', "spare", "spare",np.nan,"spare", "spare", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    new_df = pd.DataFrame(columns=l)
    spare1= [72,"spare", "spare", 'Other', "spare", "spare","spare","spare", "spare", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    new_df.loc[0] = spare1


    for i in range(1,7):
        spare[0] += 1
        new_df.loc[i] = spare

    pivot_baseprod = pd.concat([pivot_baseprod, new_df], ignore_index=True)
    # pivot_baseprod.to_csv('Inputs/outputcsv/baseproduction.csv',index=False)




    # Baseprod by regions
    baseprodbyreg = pd.DataFrame(columns=pivot_baseprod.columns)
    rows=pd.DataFrame({'Prov - Category':['Guangxi','Guizhou','Henan','Shanxi','Other','','',''],'Technology':['','','','','','H-B','L-B','fly ash'],'Bauxite Now':['Domestic','Domestic','Domestic','Domestic','Domestic','Imported','Imported','Fly ash']})
    ll=[2005,2006]
    for i in range(2007,2032):
        ll.append(i)
    baseprodbyreg=baseprodbyreg.append(rows,ignore_index=True)
    for col in ll:
        baseprodbyreg[col] = 0

    # baseprodbyreg.to_csv('Inputs/outputcsv/baseproductionbyregons.csv',index=False)






    # RefPDT

    refpdt = pd.read_sql_query('SELECT r.refinery,o.ownership,* FROM input_db.dbo.refineries_PDT_inputs_1 p JOIN input_db.dbo.refinery r ON p.refinery_id=r.refinery_id JOIN input_db.dbo.ownership o ON p.ownership_id=o.ownership_id',conn)
    refpdt.drop(['creation_date','updation_date','refinery_id','ownership_id','Transport_distance','for_check_bar','for_check_caustic', 'refineries_pdt_inputs_1_id'], axis=1, inplace=True)
    refpdt=refpdt.iloc[:, :-1]
    refpdt=refpdt.iloc[:, :-1]
    col = refpdt.pop("ref_no")
    refpdt.insert(0, col.name, col)

    refpdt=chcol(refpdt,'RefPDT')
    # refpdt.to_csv('Inputs/outputcsv/RefPDT.csv',index=False)





    # lookup

    lookup = pd.read_sql_query('SELECT c.province,* FROM input_db.dbo.aacost_freight_prices p JOIN input_db.dbo.province c ON p.province_id=c.province_id',conn)
    lookup.drop(['province_id','creation_date','updation_date'], axis=1, inplace=True)
    lookup=lookup.iloc[:, :-1]
    df1=pd.DataFrame([[np.nan] * len(lookup.columns)], columns=lookup.columns)
    lookup=df1.append(lookup, ignore_index=True)

    colname=pd.read_excel('column_name.xlsx',sheet_name='lookup')
    rows=colname.loc[0]
    rows=list(rows)
    lookup.loc[0]=rows
    lookup=chcol(lookup,'lookup')
    lookup = lookup.replace({"Inner Mongolia": "IM"})
    # lookup.to_csv('Inputs/outputcsv/lookup.csv',index=False)





    # lookup1

    lookup1 = pd.read_sql_query('SELECT * FROM input_db.dbo.standard_digestion_factors',conn)
    lookup1.drop(['creation_date','updation_date','key'], axis=1, inplace=True)
    lookup1=chcol(lookup1,'lookup1')
    # lookup1.to_csv('Inputs/outputcsv/lookup1.csv',index=False)





    # Cbix Prices

    cbix = pd.read_sql_query("SELECT y.year,cbix_prices as [CBIX Prices],exchange_rate as [Exchange Rate] FROM input_db.dbo.cbix_prices p JOIN input_db.dbo.year y ON p.year_id=y.year_id",conn)
    pivot_cbix = cbix.pivot_table(columns='year',aggfunc=sum)
    pivot_cbix.reset_index(inplace=True)
    l=[]
    for i in range(1,len(pivot_cbix.columns)):
       l.append(pivot_cbix.columns[i])
    l = list(map(int,l))
    l = ['']+l
    pivot_cbix.columns=l
    row0=pd.DataFrame({"":['','Caustic Prices'], 2019:['','']})
    pivot_cbix=pivot_cbix.append(row0, ignore_index=True)

    price1 = pd.read_sql_query("SELECT c.province,y.year,caustic_prices FROM input_db.dbo.lightious_caustic_prices p JOIN input_db.dbo.province c ON p.province_id=c.province_id JOIN input_db.dbo.year y ON p.year_id=y.year_id",conn)
    pivot_price1 = price1.pivot_table(index='province',values='caustic_prices',columns='year',aggfunc=sum)
    pivot_price1.reset_index(inplace=True)
    l=[]
    for i in range(1,len(pivot_price1.columns)):
       l.append(pivot_price1.columns[i])
    l = list(map(int,l))
    l = ['']+l
    pivot_price1.columns=l
    row0=pd.DataFrame({"":['','','Lignitous Coal List Price'], 2019:['','','']})
    pivot_price1=pivot_price1.append(row0, ignore_index=True)

    price2 = pd.read_sql_query("SELECT c.province,y.year,lignitous_coal_list_price FROM input_db.dbo.lightious_caustic_prices p JOIN input_db.dbo.province c ON p.province_id=c.province_id JOIN input_db.dbo.year y ON p.year_id=y.year_id",conn)
    pivot_price2 = price2.pivot_table(index='province',values='lignitous_coal_list_price',columns='year',aggfunc=sum)
    pivot_price2.reset_index(inplace=True)
    l=[]
    for i in range(1,len(pivot_price2.columns)):
       l.append(pivot_price2.columns[i])
    l = list(map(int,l))
    l = ['']+l
    pivot_price2.columns=l

    pivot_cbixprices=pd.concat([pivot_cbix,pivot_price1,pivot_price2], axis=0, ignore_index=True)
    pivot_cbixprices=pivot_cbixprices.loc[[0,1,2,3,10,7,11,5,6,8,12,4,9,13,14,15,22,19,23,17,18,20,24,16,21], :]
    pl = list(pivot_cbixprices.columns)
    pl[0] = "Unnamed: 0"
    pivot_cbixprices.columns = pl
    # pivot_cbixprices.to_csv('Inputs/outputcsv/cbixprices.csv',index=True)






    # League master

    lm = pd.read_sql_query("SELECT refinery_referance_no as Ref,[current] as [Current],s_5,a.province as Provincial,regional as Regional,year as Year FROM input_db.dbo.league_master p JOIN input_db.dbo.province a ON p.province_id=a.province_id",conn)
    # pivot_basecap
    lm_df = pd.DataFrame(columns=['Ref'])
    lm_df["Ref"] = pivot_basecap["Ref. No."][:-1]
    lm.drop(columns=["Ref"], inplace=True)
    lm = lm_df.join(lm)
    # lm.to_csv('Inputs/outputcsv/leaguemaster.csv',index=False)



    #PDT

    # pdt = pd.read_sql_query('SELECT a.refinery,x.chinese_name,y.refinery as Subname,z.sub_chinese_name,b.province,region as Region,f.owner,* FROM input_db.dbo.aapf_pdt p JOIN input_db.dbo.refinery a ON p.summary_name=a.refinery_id JOIN input_db.dbo.chinese_name x ON p.chinese_name_id=x.chinese_name_id JOIN input_db.dbo.refinery y ON p.sub_name_technology_variant=y.refinery_id JOIN input_db.dbo.sub_chinese_name z ON p.sub_chinese_name_id=z.sub_chinese_name_id JOIN input_db.dbo.province b ON p.province_id=b.province_id JOIN input_db.dbo.owner f ON p.owner_id=f.owner_id',conn)
    pdt = pd.read_sql_query('SELECT * FROM input_db.dbo.aapf_pdt',conn)
    refinery =  dict(pd.read_sql_query("SELECT * FROM refinery",conn).loc[: , ['refinery_id', 'refinery']].values)
    chinese_name =  dict(pd.read_sql_query("SELECT * FROM chinese_name",conn).loc[:, ['chinese_name_id', 'chinese_name']].values)
    sub_chinese_name =  dict(pd.read_sql_query("SELECT * FROM sub_chinese_name",conn).loc[:, ['sub_chinese_name_id', 'sub_chinese_name']].values)
    province = dict(pd.read_sql_query("SELECT * FROM province", conn).loc[:, ["province_id", "province"]].values)
    lookup_store = {**refinery, **sub_chinese_name, **chinese_name, **province}
    # pdt.drop(['summary_name','chinese_name_id','sub_name_technology_variant','sub_chinese_name_id','province_id','region','owner_id'], axis=1, inplace=True)
    # for i in range(1,21):
    #     pdt=pdt.iloc[:, :-1]
    pdt.drop(['updation_date' , 'creation_date'], axis=1, inplace=True)
    pdt=chcol(pdt,'PDT')
    pdt = pdt.replace(lookup_store)


    # pdt.to_csv('Inputs/outputcsv/PDT.csv',index=False, encoding="utf-8")





    def to_num(df):
        df = df.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce')).fillna(df)
        return df.replace({None:np.nan})





    return {
        "refineries_pdt_inputs_1": to_num(refpdtip1),
        "refineries_pdt_inputs_2": to_num(refpdtip2) ,
        "aaprod_dom_bauxite": to_num(pivot_aaprod) ,
        "demand": pivot_demand,
        "keyinputs": key ,
        "basecapacity": to_num(pivot_basecap) ,
        "baseproduction": to_num(pivot_baseprod) ,
        "basecapacitybyregions": basecapbyreg ,
        "baseproductionbyregions": baseprodbyreg ,
        "PDT": to_num(pdt),
        "RefPDT": to_num(refpdt) ,
        "lookup": to_num(lookup) ,
        "lookup1": to_num(lookup1) ,
        "cbixprices": to_num(pivot_cbixprices) ,
        "leaguemaster": to_num(lm) ,
    }
