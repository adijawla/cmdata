import os
import pandas as pd
import numpy as np
import pyodbc

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print(BASE_DIR)

class DB_TO_FILE:
    def __init__(self):
        server   = 'magdb.database.windows.net'
        database = 'input_db'
        username = 'letmetry'
        password = 'T@lst0y50'
        driver   = '{ODBC Driver 17 for SQL Server}'

        self.conn = pyodbc.connect(f'DRIVER={driver};PORT=1433;SERVER={server};PORT=1443;DATABASE={database};UID={username};PWD={password}')
        
        self.df5 = pd.read_sql_query(f"SELECT * FROM [dbo].[year]", self.conn)
        self.df5.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

        self.df1 = pd.read_sql_query(f"SELECT * FROM [dbo].[capefreight]", self.conn)
        
        self.df2 = pd.read_sql_query(f"SELECT * FROM [dbo].[county]", self.conn)
        self.df2.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df3 = pd.read_sql_query(f"SELECT * FROM [dbo].[mine]", self.conn)
        self.df3 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df4 = pd.read_sql_query(f"SELECT * FROM [dbo].[capefreight]", self.conn)
        
        self.df6 = pd.read_sql_query(f"SELECT * FROM [dbo].[cost_n_spec_input]", self.conn)
        
        self.df7 = pd.read_sql_query(f"SELECT * FROM [dbo].[diesel_index]", self.conn)
        
        self.df8 = pd.read_sql_query(f"SELECT * FROM [dbo].[fxrates]", self.conn)
        self.df8.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df9 = pd.read_sql_query(f"SELECT * FROM [dbo].[mineinfo]", self.conn)
        self.df9.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df10 = pd.read_sql_query(f"SELECT * FROM [dbo].[panamax]", self.conn)
        self.df10.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df11 = pd.read_sql_query(f"SELECT * FROM [dbo].[fxratestomines]", self.conn)
        self.df11.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df12 = pd.read_sql_query(f"SELECT * FROM [dbo].[country]", self.conn)
        self.df12.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df14 = pd.read_sql_query(f"SELECT * FROM [dbo].[splittingfactor]", self.conn)
        self.df14.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df15 = pd.read_sql_query(f"SELECT * FROM [dbo].[costspecinput2]", self.conn)
        self.df15.drop(['creation_date', 'updation_date'], axis=1,inplace=True)

        self.collector_mines = pd.read_sql_query(f"SELECT m.mine FROM dbo.collector_mines c JOIN dbo.mine m ON(c.mine_id=m.mine_id)", self.conn)

    def capefreight(self):
        # capefrieght
        join1 = self.df1.merge(self.df3, left_on='mine_id', right_on='mine_id', how='left') # self.df1.set_index('mine_id').join(self.df3.set_index('mine_id'), how='left')
        join2 = join1.merge(self.df12, left_on='country_id', right_on='country_id', how='left') # join1.set_index('country_id').join(self.df12.set_index('country_id'), how='left')
        join3 = join2.merge(self.df5, left_on='year_id', right_on='year_id', how='left') # join2.set_index('year_id').join(self.df5.set_index('year_id'), how='left')
        join3['capefreight'] = join3['capefreight'].apply(pd.to_numeric)
        join3['year'] = join3['year'].apply(pd.to_numeric)
        join3 = join3.pivot_table(index=['country', 'mine'],  columns='year', values='capefreight').reset_index() 
        return join3

    def cost_spec_input1(self):
        # cost_spec_input
        join4 = self.df6.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join5 = join4.merge(self.df3,on='mine_id',how='left')
        join5=join5.merge(self.df12,on='country_id',how='left')
        join5 = join5.reset_index()
        join6 = join5.copy()
        join6['mine']=join6['mine'].astype(str)
        join6['country']=join6['country'].astype(str)
        join6['cost_spec']=join6['cost_spec'].astype(str)        
        join6=join6.drop(['year_id','country_id','country_id', 'index'], axis=1)
        join6=join6.pivot_table(index=['mine_id','mine','country','field'], columns='year', values='cost_spec',aggfunc='sum') 
        join6 = join6.reset_index().drop(['mine_id'], axis=1)
        return join6

    def cost_spec_input2(self):
        # cost_spec_input_sheet2
        join17 = self.df15.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join18 = join17.merge(self.df3,on='mine_id',how='left')
        join18=join18.drop(['year_id','mine_id'], axis=1)
        join18['mine']=join18['mine'].astype(str)
        join18['Field']=join18['Field'].astype(str)
        join18=join18.pivot_table(index=['mine','Field'], columns='year', values='value',aggfunc='sum')
        join18 = join18.reset_index()
        # print(join18)
        return join18

    def diesel_index(self):  
        # diesel_index
        join7 = self.df7.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join7=join7.drop(['year_id'], axis=1)
        join7=join7[['year','dieselindex']]
        # print(join7)
        return join7

    def fxrates(self):
        # fxrates
        join8 = self.df8.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join8=join8.merge(self.df12,on='country_id')
        join8=join8.reset_index()
        join8['year']=join8['year'].astype(str)
        join8['country_id']=join8['country_id'].astype(str)
        join8['index']=join8['index'].astype(str)
        join8['fxrates']=join8['value'].astype(float)
        join8['lookup']= join8['index']+''+join8['year']
        join8=join8.pivot_table(index='country', columns='year', values='fxrates',aggfunc='max')
        join8.reset_index(inplace=True)
        # print(join8)
        return join8

    def mineinfo(self):
        #MineInfo
        join9 = self.df9.merge(self.df3, left_on='mine_id',right_on='mine_id',how='left')
        join9 = join9.merge(self.df12,on='country_id')
        join9['Mine_Name'] = join9['mine']
        join9=join9.drop(['country_id','mine_id','mineinfo_id'], axis=1)
        join9=join9.set_index('mine').transpose()
        cols = list(join9.columns)
        cols = [cols[-2]] + cols[:-2]
        join9= join9[cols]
        join9 = join9.rename(index={'country': 'Location_Country'})
        join9 = join9.reset_index()
        join9 = join9.rename(columns={'index': 'Field'})
        # print(join9)
        return join9
        
    def panamax(self):
        #Paramax        
        join11=self.df10.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join12=join11.merge(self.df3,on='mine_id',how='left')
        join13=join12.merge(self.df12,on='country_id')
        join13['panamax']=join13['panamax'].astype(float)
        join13=join13.pivot_table(index=["country","mine"], values='panamax',columns='year') # ,aggfunc='sum')
        join13 = join13.reset_index()
        # print(join13)
        return join13

    def fxratestomines(self):
        #fxratestomines
        join15 = self.df11.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join15=join15.merge(self.df12,on='country_id')        
        join15=join15.reset_index()
        join15['year']=join15['year'].astype(str)
        join15['fxratestomines']=join15['fxratestomines'].astype(float)
        join15['country_id']=join15['country_id'].astype(str)
        join15=join15.pivot_table(index='country', columns='year', values='fxratestomines',aggfunc='max')
        join15.reset_index(inplace=True)
        return join15

    def splittingfactor(self):
        #Splitting Factor
        join16 = self.df14.merge(self.df12, on='country_id',how='left')
        cols = list(join16.columns)
        cols = [cols[-1]] + cols[:-1]
        join16= join16[cols]
        join16=join16.drop(['country_id','splittingfactor_id'], axis=1)
        join16.set_index('country', inplace=True)
        join16.reset_index(inplace=True)
        return join16
        
if __name__ == "__main__":
    ext = DB_TO_FILE()
    ext.capefreight()
    ext.cost_spec_input1()
    ext.cost_spec_input2()
    ext.diesel_index()
    ext.fxrates()
    ext.mineinfo()
    ext.panamax()
    ext.fxratestomines()
    ext.splittingfactor()

   
