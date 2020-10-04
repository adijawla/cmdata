import extension1 as et
import numpy as np
import pandas as pd
inputs = [
'caustic',
'domesticbarscumulative',
'reserve',
'drawdowntechfactorinputs1',
'drawdowntechfactorinputs2',
'bauxite_allocations',
'country_reserve'
]
#inputs = ['capbase']
ids = ['refinery','province','category','technology','ownership','owner','bauxite','year','county']
idnames = ['refinery_id','province_id','category_id','technology_id','ownership_id','owner_id','bauxite_id','year_id','county_id']
x = et.inputprocess(ids,inputs,idnames)
x.mergeall()
'''
x.pivot('capbase','year','capacity',['bauxite','owner','ownership','technology','category','province','refinery'])
x.pivot('cockpit3rdpartyswitching','year','value',['category'])
x.pivot('cockpitallow3rdpartytrade','year','cockpitallow3rdpartytrade',['category'])
x.pivot('import','year','importe',['technology'])
x.pivot('proddatah','year','production',['bauxite','owner','ownership','technology','category','province','refinery'])
x.pivot('quicksummaryswitches','year','Value',['bauxite','owner','ownership','technology','category'])'''
x.pivot('domesticbarscumulative','year','domestic_bar_cumulative',['province'])

caustic_re = {'bayer_mud_sinter':'bayer mud sinter','sourcing_factor':'sourcing factor'}
ddin1_re = {'bauxite_consumption':'Bauxite Consumption','category':'province'}
ddin2_re = {'factor_inputs':'name'}
prov_re = {'province':'Province','power':'Power','labormining':'LaborMining',
           'laborug':'Laborug','laborgeneral':'LaborGeneral','mineautomation':'MineAutomation',
           'mdopenpit':'MDOpenPit','mdunderground':'MDUnderground',
           'dressingautomation':'DressingAutomation',
           'productionscale':'ProductionScale',
           'portionopenpit':'PortionOpenpit',
           'swropenpit':'SWROpenPit',
           'swrunderground':'SWRUnderground',
           'dieselprice':'DieselPrice',
           'mtcopenpit':'MTCOpenpit',
           'mtcunderground':'MTCUnderground',
           'omcopenpit':'OMCOpenpit',
           'omcunderground':'OMCUnderground',
           'dmcopenpit':'DMCOpenpit',
           'dmcunderground':'DMCUnderground',
           'gcprovincestate':'GCProvinceState',
           'gccountylocal':'GCCountyLocal',
           'frroad':'FRRoad',
           'frrail':'FRRail',
           'pricedomesticbxfaw':'PriceDomesticBxFAW',
           'causticsoda':'CausticSoda',
           'sodiumcarbonate':'SodiumCarbonate',
           'lime':'Lime',
           'flocculant':'Flocculant',
           'lcbasepricefaw':'LCBasepricefaw',
           'lcdistancetorefinery':'LCdistancetorefinery',
           'acbasepricefaw':'ACBasepricefaw',
           'acdistancetorefinery':'ACdistancetorefinery'
           }
reserve_re = {
    'county':'County',
    'province':'Province',
    'inventory':'Inventory',
    'geologicalendowment':'GeologicalEndowment',
    'ai203':'AI203',
    'as':'AS',
    'include_inventory_in_allocations_further_processed':'Include Inventory in Allocations Further Processed?',
    'include_in_endowment_in_further_processing_provinces_only':'Include in Endowment in Further Processing? (Provinces only)',
    'comment_by_row':'Comment by row',
    'deep_bauxite':'deep bauxite',
    'de_rating_factor_al2o3':'de-rating factor Al2O3',
    'de_rating_factor_as':'de-rating factor AS'
    

}
alloc_re = {
    'allocation':'Allocation',
    'avg_a_s':'Avg A/S',
    'avg_al2o3':'Avg %Al2O3',
    'avg_distance':'Avg Distance',
    'avg_sio2':'Avg %SiO2',
    'closed':'Closed',
    'purchased':'Purchased',
    'security':'Security'
    

}
def rev(s):
    return s[::-1]
x.rename('bauxite_allocations',alloc_re)
x.rename('caustic',caustic_re)
#x.rename('drawdowntechfactorinputs1',ddin1_re)
#x.rename('drawdowntechfactorinputs2',ddin2_re)
#x.rename('provincialdb',prov_re)
x.rename('reserve',reserve_re)
x.pivot('bauxite_allocations','refinery',['Allocation','Closed','Purchased','Avg Distance','Avg %Al2O3','Avg %SiO2','Avg A/S','Security'
],['county','province'])
m = x.data('bauxite_allocations')
n = x.data('reserve')
m.columns = m.columns.map(rev).map(' '.join).str.strip(' ')
#m = m.drop(m.loc[m.province=="China Total"].index)
#x.input_db[5] = m.reset_index()
n = n.drop(n.loc[n.Province=="N/A"].index)
x.input_db[2] = n.reset_index()
m = x.data('bauxite_allocations')
n = x.data('reserve')
o = x.data('country_reserve')
n['working_stock_prior_to_aa_and_as_de_rating000t'] = np.nan
n['de_rated_sio2'] = np.nan
n['de_rated_al2o3'] = np.nan
cc = n.shape[0]
for i in range(o.shape[0]):
    n.at[cc+i,"County"] = o.at[i,"county"]
    n.at[cc+i,"Province"] = o.at[i,"province"]
    n.at[cc+i,"working_stock_prior_to_aa_and_as_de_rating000t"] = o.at[i,"working_stock_prior_to_aa_and_as_de_rating000t"]
    n.at[cc+i,"de_rated_sio2"] = o.at[i,"de_rated_sio2"]
    n.at[cc+i,"de_rated_al2o3"] = o.at[i,"de_rated_al2o3"]
'''
for i in range(n.shape[0]):
    if n.at[i,"County"] == "Not Applicable":
        n.at[i,"County"] = n.at[i,"Province"] +n.at[i,"County"]
    if m.at[i,"county"] == "Not Applicable":
        m.at[i,"county"] = m.at[i,"province"] +m.at[i,"county"]'''


n.sort_values(by=['County','Province'],ascending=[True, True],inplace=True)
m.sort_values(by=['county','province'],ascending=[True, True],inplace=True)
x.input_db[5] = m.reset_index()
x.input_db[2] = n.reset_index()

#x.input_db.append(pd.read_sql_query("select top(1) * from snapshot_table where model_id = 3 order by shapshot_id desc",x.conn2))
x.export()

'''
x.pivot('cockpit3rdpartyswitching','year','value')
x.pivot('cockpitallow3rdpartytrade','year','cockpitallow3rdpartytrade')
x.pivot('capbase','year','capacity')
x.pivot('domesticbarscumulative','year','domestic_bar_cumulative')
x.pivot('proddatah','year','production')
x.pivot('import','year','importe')
x.pivot('quicksummaryswitches','year','value')
#x.pivot('')
'''
