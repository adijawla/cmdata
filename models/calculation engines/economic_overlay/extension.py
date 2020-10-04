import os
import pandas as pd
import numpy as np
import pyodbc



BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class DB_TO_FILE:
    def __init__(self):
        server = 'magdb.database.windows.net'
        database = 'input_db'
        username = 'letmetry'
        password = 'T@lst0y50'
        driver= '{ODBC Driver 17 for SQL Server}'
            
        conn = pyodbc.connect(f'DRIVER={driver};PORT=1433;SERVER={server};PORT=1443;DATABASE={database};UID={username};PWD={password}')
        
        self.df1=pd.read_sql_query("SELECT * FROM [dbo].[inputs]", conn)        
        # self.df1.drop(columns =["creation_date_y","updation_date_y"], inplace = True)

        self.df2=pd.read_sql_query("SELECT * FROM [dbo].[lookuptabels1]", conn)
        # self.df2.drop(columns =["creation_date_y","updation_date_y"], inplace = True)
        
        self.df3=pd.read_sql_query("SELECT * FROM [dbo].[province]", conn)
        # self.df3.drop(columns =["creation_date_y","updation_date_y"], inplace = True)

        self.df4=pd.read_sql_query("SELECT * FROM [dbo].[county]", conn)
        # self.df4.drop(columns =["creation_date_y","updation_date_y"], inplace = True)

        self.df5=pd.read_sql_query("SELECT * FROM [dbo].[depthbuckets_percentoftonnage]", conn)
        self.df5.drop(columns =["creation_date","updation_date"], inplace = True)  

        self.df6=pd.read_sql_query("SELECT * FROM [dbo].[other_controls]", conn)
        self.df6.drop(columns =["creation_date","updation_date"], inplace = True)

    # Input
    def inputs(self):
        join1=self.df1.merge(self.df3,on='province_id',how='left')
        join2=join1.merge(self.df4,on='county_id',how='left')
        
        print(join2.head())
        join2=join2.drop(['province_id','county_id',"creation_date_x","updation_date_x","creation_date_y","updation_date_y"], axis=1)
        cols = list(join2.columns)
        cols = [cols[-1]] + cols[:-1]
        join2= join2[cols]
        cols = list(join2.columns)
        cols = [cols[-1]] + cols[:-1]
        join2= join2[cols]
        # join2.to_excel("db_inputs/inputs.xlsx")
        return join2

    # lookuptabels
    def lookuptable(self):
        join3=self.df2.merge(self.df3,on='province_id',how='left')
        for i in join3.columns:
            try:
                join3[i] = join3[i].astype(float)
            except:
                pass        
        #join4=join3.copy()    
        join3=join3.pivot_table(index=['province'], columns=['cost_type'], values='cost') #,aggfunc='sum')
        join3.reset_index(inplace=True)
        # join3.to_excel("db_inputs/lookuptables.xlsx")
        return join3

    # depth_buckets
    def depth_buckets(self):
        # self.df5.to_excel("db_inputs/depth_buckets.xlsx")
        return self.df5

    #other controls and Province switch
    def other_controls(self):
        # self.df6.to_excel("db_inputs/other_controls_and_Province_switch.xlsx")
        return self.df6


if __name__ == '__main__':
    ext = DB_TO_FILE()
    ext.inputs()
    ext.lookuptable()
    ext.depth_buckets()
    ext.other_controls()