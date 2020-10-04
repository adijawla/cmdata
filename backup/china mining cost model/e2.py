import pandas as pd

def asd():
    df = pd.read_csv('inputs/lookup.csv')
    id1 = pd.read_csv('expence.csv')
    id2 = pd.read_csv('province.csv')
    id3 = pd.read_csv('Year.csv')
    join4 = df.merge(id3, left_on='year_id',right_on='year_id',how='left')
    join5 = join4.merge(id2,on='prov_id',how='left')
    join5=join5.merge(id1,on='expence_id')
    join5 = join5.reset_index()
    join6 = join5.copy()
    '''
    join6['mine']=join6['mine'].astype(str)
    join6['Country']=join6['Country'].astype(str)
    join6['cost_spec']=join6['cost_spec'].astype(str)'''
    
    join6=join6.drop(['year_id','expence_id','prov_id'], axis=1)
    join6.to_csv('test.csv')
    join6=join6.pivot_table(index=['province','expence'], columns='Year', values='lookup',aggfunc='sum')
    
    #join6.to_csv('test.csv')
    print(join6.head())
