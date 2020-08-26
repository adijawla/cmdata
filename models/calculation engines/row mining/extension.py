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

        self.df1 = pd.read_sql_query(f"SELECT * FROM [dbo].[capefright]", self.conn)
        self.df1 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df2 = pd.read_sql_query(f"SELECT * FROM [dbo].[county]", self.conn)
        self.df2 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)        
        
        self.df3 = pd.read_sql_query(f"SELECT * FROM [dbo].[mine]", self.conn)
        self.df3 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df4 = pd.read_sql_query(f"SELECT * FROM [dbo].[capefright]", self.conn)
        self.df4 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df6 = pd.read_sql_query(f"SELECT * FROM [dbo].[cost_spec_input]", self.conn)
        self.df6 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df7 = pd.read_sql_query(f"SELECT * FROM [dbo].[dieselindex]", self.conn)
        self.df7 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df8 = pd.read_sql_query(f"SELECT * FROM [dbo].[fxrates]", self.conn)
        self.df8 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df9 = pd.read_sql_query(f"SELECT * FROM [dbo].[mineinfo]", self.conn)
        self.df9 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df10 = pd.read_sql_query(f"SELECT * FROM [dbo].[panamax]", self.conn)
        self.df10.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df11 = pd.read_sql_query(f"SELECT * FROM [dbo].[fxratestomines]", self.conn)
        self.df11.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df12 = pd.read_sql_query(f"SELECT * FROM [dbo].[country]", self.conn)
        self.df12.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
                
        # self.df13 = pd.read_sql_query(f"SELECT * FROM [dbo].[country]", self.conn)
        # self.df13 .drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df14 = pd.read_sql_query(f"SELECT * FROM [dbo].[splittingfactor]", self.conn)
        self.df14.drop(['creation_date', 'updation_date'], axis=1,inplace=True)
        
        self.df15 = pd.read_sql_query(f"SELECT * FROM [dbo].[costspecinput2]", self.conn)
        self.df15.drop(['creation_date', 'updation_date'], axis=1,inplace=True)        

        #print(self.df1,self.df2,self.df3,self.df4,self.df5,self.df6,self.df7,self.df8,self.df9,self.df10,self.df11,self.df14,self.df15)
        
    def bd_to_excel(self):
        # capefrieght
        join1 = self.df1.set_index('mine_id').join(self.df3.set_index('mine_id'), how='left')
        join2 = join1.set_index('country_id').join(self.df12.set_index('country_id'), how='left')
        join3 = join2.set_index('year_id').join(self.df5.set_index('year_id'), how='left')        
        join3 = join3.pivot_table(index=['country', 'mine'],  columns='year', values='capfright').reset_index()
        print(join3)

        # cost_spec_input
        join4 = self.df6.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join5 = join4.merge(self.df3,on='mine_id',how='left')
        join5=join5.merge(self.df12,on='country_id')
        join5 = join5.reset_index()
        join6 = join5.copy()
        join6['mine']=join6['mine'].astype(str)
        join6['country']=join6['country'].astype(str)
        join6['cost_spec']=join6['cost_spec'].astype(str)        
        join6=join6.drop(['year_id','country_id','mine_id','country_id', 'index'], axis=1)
        join6=join6.pivot_table(index=['mine','country','Field'], columns='year', values='cost_spec',aggfunc='sum')        
        print(join6)            

        # cost_spec_input_sheet2
        join17 = self.df15.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join18 = join17.merge(self.df3,on='mine_id',how='left')
        join18=join18.drop(['year_id','mine_id'], axis=1)
        join18['mine']=join18['mine'].astype(str)
        join18['Field']=join18['Field'].astype(str)
        join18=join18.pivot_table(index=['mine','Field'], columns='year', values='value',aggfunc='sum')
        print(join18)
                
        # diesel_index
        join7 = self.df7.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join7=join7.drop(['year_id'], axis=1)
        join7=join7[['year','dieselindex']]
        print(join7)

        # fxrates
        join8 = self.df8.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join8=join8.merge(self.df12,on='country_id')
        join8=join8.reset_index()
        join8['year']=join8['year'].astype(str)
        join8['country_id']=join8['country_id'].astype(str)
        join8['index']=join8['index'].astype(str)
        join8['fxrates']=join8['value'].astype(str)
        join8['lookup']= join8['index']+''+join8['year']
        join8=join8.pivot_table(index='country', columns='year', values='fxrates',aggfunc='max')        
        print(join8)

        #MineInfo
        join9 = self.df9.merge(self.df3, left_on='mine_id',right_on='mine_id',how='left')
        join9 = join9.merge(self.df12,on='country_id')
        join9=join9.drop(['country_id','country_id'], axis=1)
        join9=join9.set_index('mine_id').transpose()
        cols = list(join9.columns)
        cols = [cols[-2]] + cols[:-2]
        join9= join9[cols]
        join9 = join9.rename(index={'mine': 'Field'})        
        print(join9)
        
        #Paramax        
        join11=self.df10.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join12=join11.merge(self.df3,on='mine_id',how='left')
        join13=join12.merge(self.df12,on='country_id')
        #join13=pd.crosstab(index=join13["mine"], values=join13['panamax'],columns=join13['year'],aggfunc='sum')
        join13=join13.pivot_table(index=["country","mine"], values='panamax',columns='year') # ,aggfunc='sum')
        join13.to_excel("db_inputs/Paramax2.xlsx",sheet_name='Sheet1',encoding='utf-8')
        print(join13)
        join13=join13.merge(join12,on='mine',how='left')
        join113=join13.merge(self.df12,on='country_id')
        join113=join113.drop(['year_id','country_id','mine_id','panamax','year','country_id'], axis=1)
        cols = list(join113.columns)
        cols = [cols[-1]] + cols[:-1]
        join113= join113[cols]
        join113.set_index('country', inplace=True)
        print(join113)

        #fxratestomines
        join15 = self.df11.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join15=join15.merge(self.df12,on='country_id')        
        join15=join15.reset_index()
        join15['year']=join15['year'].astype(str)
        join15['country_id']=join15['country_id'].astype(str)
        join15=join15.pivot_table(index='country', columns='year', values='fxratestomines',aggfunc='max')
        print(join15)

        #Splitting Factor
        join16 = self.df14.merge(self.df12, on='country_id',how='left')
        cols = list(join16.columns)
        cols = [cols[-1]] + cols[:-1]
        join16= join16[cols]
        join16=join16.drop(['country_id'], axis=1)
        join16.set_index('country', inplace=True)

        #Save all files
        join113.to_excel("db_inputs/Paramax1.xlsx",sheet_name='Sheet1',encoding='utf-8')
        join3.to_excel("db_inputs/capefright1.xlsx",sheet_name='Sheet1',encoding='utf-8',index=False)
        join9.to_excel("db_inputs/MineInfo1.xlsx",sheet_name='Sheet1')        
        join6.to_excel("db_inputs/cost_spec_input1.xlsx",sheet_name='Sheet1',encoding='utf-8')        
        join7.to_excel("db_inputs/diesel_index1.xlsx",sheet_name='Sheet1',encoding='utf-8',index=False)        
        join8.to_excel("db_inputs/fxrates1.xlsx",sheet_name='Sheet1',encoding='utf-8')
        join15.to_excel("db_inputs/fxratestomines1.xlsx",sheet_name='Sheet1',encoding='utf-8')
        join16.to_excel("db_inputs/splittingfactor1.xlsx",sheet_name='Sheet1',encoding='utf-8')        
        join18.to_excel("db_inputs/costspec2.xlsx",sheet_name='Sheet1',encoding='utf-8')

if __name__ == "__main__":
    ext = DB_TO_FILE()
    ext.bd_to_excel()

   
