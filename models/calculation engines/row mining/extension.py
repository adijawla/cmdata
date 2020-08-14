import os
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print(BASE_DIR)

class DB_TO_FILE:
    def __init__(self):
        self.df5 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/Year.csv"))
        self.df1 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/capefright.csv"))
        self.df2 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/county_id.csv"))
        self.df3 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/mine.csv"))
        self.df4 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/capefright.csv"))
        self.df6 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/cost_spec_input.csv"))
        self.df7 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/dieselindex.csv"))
        self.df8 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/fxrates.csv"))
        self.df9 = pd.read_csv(os.path.join(BASE_DIR,  "db_inputs/mineinfo.csv"),encoding='latin-1')
        self.df10 = pd.read_csv(os.path.join(BASE_DIR, "db_inputs/panamax.csv"),encoding='latin-1')
        self.df11 = pd.read_csv(os.path.join(BASE_DIR, "db_inputs/fxratestomines.csv"),encoding='latin-1')
        self.df12 = pd.read_csv(os.path.join(BASE_DIR, "db_inputs/Country.csv"),encoding='latin-1')
        # self.df13 = pd.read_excel(os.path.join(BASE_DIR, "db_inputs/cost_n_spec_input.xlsx"))
        self.df14 = pd.read_csv(os.path.join(BASE_DIR, "db_inputs/splittingfactor.csv"),encoding='latin-1')

        # Use below script if you want to read from db 
        # directly but there is one more table called 
        # cost_n_spec_input yet to be coded 
        # so use the offline version above

        # self.df5 = pd.read_sql_query(f"SELECT * FROM [dbo].[year]", self.conn)
        # self.df1 = pd.read_sql_query(f"SELECT * FROM [dbo].[capefright]", self.conn)
        # self.df2 = pd.read_sql_query(f"SELECT * FROM [dbo].[county]", self.conn)
        # self.df3 = pd.read_sql_query(f"SELECT * FROM [dbo].[mine]", self.conn)
        # self.df4 = pd.read_sql_query(f"SELECT * FROM [dbo].[capefright]", self.conn)
        # self.df6 = pd.read_sql_query(f"SELECT * FROM [dbo].[cost_spec_input]", self.conn)
        # self.df7 = pd.read_sql_query(f"SELECT * FROM [dbo].[dieselindex]", self.conn)
        # self.df8 = pd.read_sql_query(f"SELECT * FROM [dbo].[fxrates]", self.conn)
        # self.df9 = pd.read_sql_query(f"SELECT * FROM [dbo].[mineinfo]", self.conn)
        # self.df10 = pd.read_sql_query(f"SELECT * FROM [dbo].[panamax]", self.conn)
        # self.df11 = pd.read_sql_query(f"SELECT * FROM [dbo].[fxratestomines]", self.conn)
        # self.df12 = pd.read_sql_query(f"SELECT * FROM [dbo].[country]", self.conn)        
        # self.df14 = pd.read_sql_query(f"SELECT * FROM [dbo].[splittingfactor]", self.conn)
    
    def bd_to_excel(self):
        # capefright
        join1=self.df4.merge(self.df5, left_on='y_id',right_on='year_id',how='left')
        join2=join1.merge(self.df3,on='mine_id',how='left')
        join2=join2.merge(self.df12,on='country_id')
        join2=join2.reset_index()
        join3=join2.copy()
        print(join3.head())
        join3['country_id']=join3['country_id'].astype(str)
        join3['mine_id']=join3['mine_id'].astype(str)
        join3['index']=join3['index'].astype(str)
        print(join3['mine_id'])
        join3=pd.crosstab(index=join3['mine'], values=join3['capfright'],columns=join3['Year'],aggfunc='sum')
        join3=join3.merge(join2,on='mine',how='left')
        join3=join3.drop(['index','capfright','y_id','Country_id','year_id','Year','mine_id', 'country_id'], axis=1)
        cols = list(join3.columns)
        cols = [cols[-1]] + cols[:-1]
        join3= join3[cols]
        print(join3.head())
        




        # cost_spec_input
        join4 = self.df6.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join5 = join4.merge(self.df3,on='mine_id',how='left')
        join5=join5.merge(self.df12,on='country_id')
        join5 = join5.reset_index()
        join6 = join5.copy()
        join6['mine']=join6['mine'].astype(str)
        join6['Country']=join6['Country'].astype(str)
        join6['cost_spec']=join6['cost_spec'].astype(str)
        
        join6=join6.drop(['year_id','country_id','mine_id','Country_id', 'index'], axis=1)
        join6=join6.pivot_table(index=['mine','Country','Field'], columns='Year', values='cost_spec',aggfunc='sum')
        print(join6.head())
        # join6['Group'] = join6['mine'].str.extract('(\w+)').ffill()
        
        
        

        # cost_spec_input

        '''join6 = self.df13'''

        # diesel_index
        join7 = self.df7.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join7=join7.drop(['year_id'], axis=1)
        join7=join7[['Year','dieselindex']]

        # fxrates
        join8 = self.df8.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join8=join8.merge(self.df12,on='country_id')
        print(join8)
        join8=join8.reset_index()
        join8['Year']=join8['Year'].astype(str)
        join8['country_id']=join8['country_id'].astype(str)
        join8['index']=join8['index'].astype(str)
        join8['fxrates']=join8['fxrates'].astype(str)
        join8['lookup']= join8['index']+''+join8['Year']
        print(join8)
        
    

        join8=join8.pivot_table(index='Country', columns='Year', values='fxrates',aggfunc='max')
        print(join8.head())

        #MineInfo
        join9 = self.df9.merge(self.df3, left_on='mine_id',right_on='mine_id',how='left')
        join9=join9.drop(['mine_id','Country_id'], axis=1)
        cols = list(join9.columns)
        cols = [cols[-1]] + cols[:-1]
        join9= join9[cols]
        join9=join9.set_index('mine').transpose()
        

        #Paramax
        
        join11=self.df10.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join12=join11.merge(self.df3,on='mine_id',how='left')
        join13=join12.merge(self.df12,on='country_id')
        join13=pd.crosstab(index=join13["mine"], values=join13['panamax'],columns=join13['Year'],aggfunc='sum')
        print('Seetha')
        print(join13)
        join13=join13.merge(join12,on='mine',how='left')
        join113=join13.merge(self.df12,on='country_id')
        join113=join113.drop(['year_id','country_id','mine_id','panamax','Year','Country_id'], axis=1)
        cols = list(join113.columns)
        cols = [cols[-1]] + cols[:-1]
        join113= join113[cols]
        join113.set_index('Country', inplace=True)
        print('Seetha')
        print(join113.head())

        #fxratestomines

        join15 = self.df11.merge(self.df5, left_on='year_id',right_on='year_id',how='left')
        join15=join15.merge(self.df12,on='country_id')
        
        print(join15)
        join15=join15.reset_index()
        join15['Year']=join15['Year'].astype(str)
        join15['country_id']=join15['country_id'].astype(str)
        print(join15)
        
    

        join15=join15.pivot_table(index='Country', columns='Year', values='fxratestomines',aggfunc='max')
        print(join15.head())

        
        print(join15)

        #Splitting Factor

        join16 = self.df14.merge(self.df12, on='country_id',how='left')
        print('Splitting factor')
        cols = list(join16.columns)
        cols = [cols[-1]] + cols[:-1]
        join16= join16[cols]
        print(join16)
        join16=join16.drop(['country_id'], axis=1)
        join16.set_index('Country', inplace=True)
        
        #join113.to_excel(os.path.join(BASE_DIR, "panamax.xlsx"),sheet_name='Sheet1',encoding='utf-8')
        #join3.to_excel(os.path.join(BASE_DIR,   "capefreight.xlsx"),sheet_name='Sheet1',encoding='utf-8',index=False)
        # join9.to_excel(os.path.join(BASE_DIR,   "mineinfo.xlsx"),sheet_name='Sheet1',encoding='utf-8')    
        # join6.to_excel(os.path.join(BASE_DIR, "cost_n_spec_input.xlsx"),sheet_name='Sheet1',encoding='utf-8')    
        #join7.to_excel(os.path.join(BASE_DIR,   "diesel_index.xlsx"),sheet_name='Sheet1',encoding='utf-8',index=False)    
        #join8.to_excel(os.path.join(BASE_DIR,   "fxrates.xlsx"),sheet_name='Sheet1',encoding='utf-8')
        #join15.to_excel(os.path.join(BASE_DIR,  "fxratestominesasindex.xlsx"),sheet_name='Sheet1',encoding='utf-8')
        #join16.to_excel(os.path.join(BASE_DIR,  "splittingfactor.xlsx"),sheet_name='Sheet1',encoding='utf-8')

if __name__ == "__main__":
    ext = DB_TO_FILE()
    ext.bd_to_excel()

   
