import pandas as pd
import os
import pyodbc
class inputprocess():
    def __init__(self,ids,inputs,id_name):
        self.ids = ids
        self.inputs = inputs
        self.id_db = []
        self.id_name = id_name
        self.input_db = []
        server = 'magdb.database.windows.net'
        database = 'input_db'
        username = 'letmetry'
        password = 'T@lst0y50'
        driver= '{ODBC Driver 17 for SQL Server}'
        conn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
        for i in ids:
            self.id_db.append(pd.read_sql_query("SELECT * FROM [dbo].["+str(i)+"]", conn))
            self.id_db[-1].name = i
        for i in inputs:
            self.input_db.append(pd.read_sql_query("SELECT * FROM [dbo].["+str(i)+"]", conn))
            self.input_db[-1].name = i
            
            try:
                self.input_db[-1].drop(columns =["creation_date","updation_date"], inplace = True)
            except:
                pass
            try:
                self.input_db[-1].drop(columns =["creation_date_x","updation_date_x"], inplace = True)
            except:
                pass
            try:
                self.input_db[-1].drop(columns =["creation_date_y","updation_date_y"], inplace = True)
            except:
                pass

            
    def mergeall(self):
        for i in range(len(self.input_db)):
            for j in self.input_db[i].columns:
                if j in self.id_name:
                    self.input_db[i] = pd.merge(self.id_db[self.id_name.index(j)],self.input_db[i],on=j)
                    self.input_db[i] = self.input_db[i].drop([j], axis=1)
                try:
                    self.input_db[j] = self.input_db[j].astype(float)
                except:
                    pass
    def export(self):
        if not os.path.isdir('exports'):
            os.makedirs('exports')
        for i in range(len(self.inputs)):
            self.input_db[i].to_csv('exports/'+self.inputs[i]+'.csv')
    def data(self,inputname):
        i = self.inputs.index(inputname)
        return self.input_db[i]
    def pivot(self,inputname,col,val,indx=[]):
        i = self.inputs.index(inputname)
        if indx == []:
            tc = list(self.input_db[i].columns)
            tc.remove(val)
            tc.remove(col)
            indx = tc.copy()

        self.input_db[i] = pd.pivot_table(self.input_db[i],index=indx, values=val,columns=col,aggfunc='sum')
        self.input_db[i].reset_index(inplace=True)
        for j in self.input_db[i].columns:
            self.input_db[i] = self.input_db[i].rename(columns={j:str(j)})
    def rename(self,inputname,from_to):
        i = self.inputs.index(inputname)
        self.input_db[i] = self.input_db[i].rename(columns=from_to)
    
            
        
        
