import pandas as pd
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
            #self.id_db.append(pd.read_csv(i+'.csv',encoding='latin-1'))
            #self.id_name.append(list(self.id_db[i].columns)[1])
        for i in inputs:
            #self.input_db.append(pd.read_csv('inputs/'+i+'.csv',encoding='latin-1'))
            self.input_db.append(pd.read_sql_query("SELECT * FROM [dbo].["+str(i)+"]", conn))
            #self.input_db.drop(columns =["creation_date","updation_date"], inplace = True)
            
            
            
    def mergeall(self):
        for i in range(len(self.input_db)):
            for j in self.input_db[i].columns:
                if j in self.id_name:
                    self.input_db[i] = pd.merge(self.id_db[self.id_name.index(j)],self.input_db[i],on=j)
                    self.input_db[i] = self.input_db[i].drop([j], axis=1)
    def export(self):
        for i in range(len(self.inputs)):
            self.input_db[i].to_csv('final/'+self.inputs[i]+'final.csv')
    def pivot(self,inputname,indx,col,val):
        i = self.inputs.index(inputname)
        #self.input_db[i] = self.input_db[i].reset_index()
        #self.input_db[i] = self.input_db[i].pivot(columns =col, values = val)
        self.input_db[i] = pd.pivot_table(self.input_db[i],index=indx, values=val,columns=col,aggfunc='sum')
        self.input_db[i].reset_index(inplace=True)
        for j in self.input_db[i].columns:
            self.input_db[i] = self.input_db[i].rename(columns={j:str(j)})
            try:
                self.input_db[i][j] = self.input_db[i][j].astype(float)
            except:
                pass
    def rename(self,inputname,from_to):
        i = self.inputs.index(inputname)
        self.input_db[i] = self.input_db[i].rename(columns=from_to)
'''
ids = ['county_id','expence','mine_status','province','rsdatabase_mine','Year']
inputs = ['costinputs','lookup','rsdatabase','taxtrance']
idname = ['county_id','expence_id','Status_id','province_id','Mine_id','year_id']
x = inputprocess(ids,inputs,idname)
x.mergeall()
x.pivot('lookup',['province','expence'],'Year','lookup')
x.export()
'''    
            
        
        
