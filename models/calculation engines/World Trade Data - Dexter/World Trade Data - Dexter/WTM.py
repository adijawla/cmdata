
import pandas as pd
import numpy as np
from statistics import mean 
import math
from flatdbconverter import Flatdbconverter
from outputdb import uploadtodb


#input db integration
from world_trade_data_db import world_trade_db

wtd_flat = Flatdbconverter('Trade Data Term Sheet')
snapshot_output_data = pd.DataFrame(columns=wtd_flat.out_col)
db_list = [snapshot_output_data]

override_store = {}
try:
    snaps = pd.read_csv('snapshot_output_data.csv')
    override_rows = snaps.loc[snaps['override_value'] == 1]
    print(override_rows.values)
    for v in override_rows.values:
        override_store[v[4]+ '_' + str(v[5])] = v[6]
    print(override_store)
except:
    pass


class WorldTradeModel():
    
    global Importer,Q,Y,projects1,Projects1,Dc,DeclaredExporter,Total1,Row1,input2,Row,input1,FinalTable,LastQuarter,LastYear,DM
    Q=92
    Y=365
    filename = 'World Trade Model.xlsx'
    projects1=pd.read_excel (filename, sheet_name='Importer.ExporterCombination')
    Importer=pd.read_excel (filename, sheet_name='DeclaredImporters')
    Projects1=pd.read_excel (filename, sheet_name='Importer.ExporterCombination')
    Dc=pd.read_excel (filename, sheet_name='CM China Import Data')
    DeclaredExporter=pd.read_excel (filename, sheet_name='DeclaredExporters')
    Total1=pd.read_excel (filename, sheet_name='Direct Input D26   H424')
    Row1=pd.read_excel (filename, sheet_name='Direct Input D26   H424')
    input2=pd.read_excel (filename, sheet_name='RowImporter')
    Row=pd.read_excel (filename, sheet_name='Direct Input D26   H424')
    input1=pd.read_excel (filename, sheet_name='RowExporter')
    FinalTable=pd.read_excel (filename, sheet_name='Final Table Final Processing')
    LastQuarter=pd.read_excel (filename, sheet_name='Final Processing Last Quarter')
    LastYear=pd.read_excel (filename, sheet_name='Final Processing Last Year')
    DM=pd.read_excel (filename, sheet_name='DM')
    
        
        
    def calc(self):
        WorldTradeModel.RT(self)
        WorldTradeModel.WtoAK(self)
        WorldTradeModel.Quarter1(self)
        WorldTradeModel.ARtoAT(self)
        WorldTradeModel.AVtoAX(self)
        WorldTradeModel.BBtoBP(self)
        WorldTradeModel.BStoCH(self)
        WorldTradeModel.Quarter2(self)
        WorldTradeModel.COtoCQ(self)
        WorldTradeModel.CStoCU(self)
        WorldTradeModel.CXtoDA(self)
        WorldTradeModel.DC(self)
        WorldTradeModel.DKtoDS(self)
        WorldTradeModel.DWtoEB(self)
        WorldTradeModel.FinalProcessing(self)
        WorldTradeModel.RowExportersModelRANK(self)
        WorldTradeModel.RowExportersModelRANK2(self)
        WorldTradeModel.RowExportersModelTotal(self)
        WorldTradeModel.RowExporterOutput2(self)
        WorldTradeModel.RowExporterOutpu1(self)
        
        
    def RT(self):
    
    
        projects2=Importer.copy()
        projects4=projects1.copy()
        print(projects2)
        
    
        projects2['YEAR'] = projects2['YEAR'].astype(str)
        projects2['MONTH'] = projects2['MONTH'].astype(str)
        projects2['UNIT'] = projects2['UNIT'].astype(str)
        projects3=projects2.copy()
        for j in range(projects2.shape[0]):
    
            projects2.iloc[j,0]=projects2.iloc[j,0]+'_'+projects2.iloc[j,1]
            j=j+1
        for j in range(projects2.shape[0]):
    
            projects3.iloc[j,0]=projects2.iloc[j,2]+'_'+projects2.iloc[j,4]
            projects3.iloc[j,1]=projects2.iloc[j,4]+'_'+projects2.iloc[j,2]
            j=j+1
        projects3=projects3.rename(columns={"YEAR": "Importer.Exporter Combination", "MONTH": "Exporter.Importer Combination"})
        projects3=projects3.iloc[:,0:2] 
    
        for j in range(projects2.shape[0]):
            projects2.iloc[j,5]=projects2.iloc[j,5]/1000000  
            if projects2.iloc[j,3]=='KG':
                projects2.iloc[j,5]=projects2.iloc[j,5]/1000
            else:
                projects2.iloc[j,5]=projects2.iloc[j,5]
        
            j=j+1
    
        projects2=pd.concat([projects2, projects3],axis=1)
        projects2 = projects2[['QUANTITY','REPORTING Importer COUNTRY','PARTNER exporter COUNTRY','YEAR',
                           'Importer.Exporter Combination','Exporter.Importer Combination'] ]  
        projects2=projects2.iloc[:,0:6]
        print(projects2)
        Projects2=projects2.copy()
    
        join1=pd.crosstab(index=projects2['Importer.Exporter Combination'],columns=projects2['YEAR'],
                  values=projects2['QUANTITY'], 
                   aggfunc='sum',dropna=False)
    
        join1 = join1.reindex(axis="columns")

        join5=join1.merge(projects4, on='Importer.Exporter Combination', how='right')

        join5=join5.replace(np.NaN,0)
        join5 = join5.reindex(axis="columns")
        print('join5')
        print(join5)
        db_list.append(wtd_flat.mult_year_single_output(join5, "BigMatrix RST"))
        import os
        print(os.getcwd())
        join5.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix RST.csv", index=False)
        return join5,Projects2 

    def WtoAK(self):
        DF1,X=self.RT()
        
        new = DF1["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
  
    
        DF1["Importer"]= new[0]
    
    
    
        DF1["Exporter"]= new[1]
    
    
    
        DF1 = DF1.groupby("Importer").sum()
   
    
    
        DF3=DF1.transpose()
        DF3=DF3.reset_index()
        new1 = DF3["index"].str.split("_", n = 1, expand = True)
        DF3["MONTH"]=new1[1]
    
        cols1 = list(DF3.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        DF3 = DF3[cols1]
        DF3["YEAR"]=new1[0]
        cols1 = list(DF3.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        DF3 = DF3[cols1]
        
        
        df4=DF3.copy()

        for i in range(3,DF3.shape[1]):
            for j in range(DF3.shape[0]):
                Sum=0
                for k in range(DF3.shape[0]):
                
                    if DF3.iloc[j,0]>DF3.iloc[k,0]:
                    
                        Sum=Sum+DF3.iloc[k,i]
                    
                    elif DF3.iloc[j,0]==DF3.iloc[k,0]:
                        if DF3.iloc[j,1]>DF3.iloc[k,1]:
                            Sum=Sum+DF3.iloc[k,i]
                    else:
                        Sum=0
                    
                        
                    k=k+1
                    df4.iloc[j,i]=Sum
                j=j+1
            i=i+1
   
        df4=df4.iloc[:,2:]
        df4=df4.transpose()
        new_header = df4.iloc[0] 
        
        df4 = df4[1:] 
        
        df4.columns = new_header
        
        df4.rename(columns={'index':'Importer'},inplace=True)

    
        DF1=DF1.sort_values(by=['Importer'])
        df4=df4.sort_values(by=['Importer'])
        db_list.append(wtd_flat.mult_year_single_output(DF1, "Big Matrix WtoAK"))
        DF1.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix WtoAK.csv", index=False)
        return DF1,df4

    def Quarter1(self):
        Quarter,X=self.RT()
        Quarter=Quarter.transpose()
        
        
        Quarter=Quarter.reset_index()
        
        new2 = Quarter["index"].str.split("_", n = 1, expand = True) 
        
        Quarter["YEAR"]= new2[0]
        
        cols1 = list(Quarter.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Quarter = Quarter[cols1]
        
    
    
        Quarter["MONTH"]= new2[1]
        cols1 = list(Quarter.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Quarter = Quarter[cols1]
        Quarter.iloc[:,1]=Quarter.iloc[:,1]
    
      
    
        Average1=Quarter.copy()
    
        for i in range(3,Quarter.shape[1]):
            for j in range(1,Quarter.shape[0]):
                Count=0 
                Sum=0
            
                if Quarter.iloc[j,0]=='10' or Quarter.iloc[j,0]=='11' or Quarter.iloc[j,0]=='12':
                    A=int(Quarter.iloc[j,1])
                    B=A
                    A=A-1
                    A=str(A)
                    A1=str(A)+'_'+'10'
                    A2=str(A)+'_'+'11'
                    A3=str(A)+'_'+'12'
                    A4=str(B)+'_'+'1'
                    A5=str(B)+'_'+'2'
                    A6=str(B)+'_'+'3'
                    A7=str(B)+'_'+'4'
                    A8=str(B)+'_'+'5'
                    A9=str(B)+'_'+'6'
                    A10=str(B)+'_'+'7'
                    A11=str(B)+'_'+'8'
                    A12=str(B)+'_'+'9' 
            

                elif Quarter.iloc[j,0]=='4' or Quarter.iloc[j,0]=='5' or Quarter.iloc[j,0]=='6':
                    A=int(Quarter.iloc[j,1])
                    B=A
                    A=A-1
                    A=str(A)
                    A1=str(B)+'_'+'1'
                    A2=str(B)+'_'+'2'
                    A3=str(B)+'_'+'3'
                    A4=str(A)+'_'+'4'
                    A5=str(A)+'_'+'5'
                    A6=str(A)+'_'+'6'
                    A7=str(A)+'_'+'7'
                    A8=str(A)+'_'+'8'
                    A9=str(A)+'_'+'9'
                    A10=str(A)+'_'+'10'
                    A11=str(A)+'_'+'11'
                    A12=str(A)+'_'+'12'
                    
                elif Quarter.iloc[j,0]=='1' or Quarter.iloc[j,0]=='2' or Quarter.iloc[j,0]=='3':
                        A=int(Quarter.iloc[j,1])
                        B=A
                        A=A-1
                        A=str(A)
                        A1=str(B)+'_'+'10'
                        A2=str(B)+'_'+'11'
                        A3=str(B)+'_'+'12'
                        A4=str(A)+'_'+'1'
                        A5=str(A)+'_'+'2'
                        A6=str(A)+'_'+'3'
                        A7=str(A)+'_'+'4'
                        A8=str(A)+'_'+'5'
                        A9=str(A)+'_'+'6'
                        A10=str(B)+'_'+'7'
                        A11=str(B)+'_'+'8'
                        A12=str(B)+'_'+'9'
                elif Quarter.iloc[j,0]=='7' or Quarter.iloc[j,0]=='8' or Quarter.iloc[j,0]=='9':
                        A=int(Quarter.iloc[j,1])
                        B=A
                        A=A-1
                        A=str(A)
                        A1=str(B)+'_'+'1'
                        A2=str(B)+'_'+'2'
                        A3=str(B)+'_'+'3'
                        A4=str(B)+'_'+'4'
                        A5=str(B)+'_'+'5'
                        A6=str(B)+'_'+'6'
                        A7=str(A)+'_'+'7'
                        A8=str(A)+'_'+'8'
                        A9=str(A)+'_'+'9'
                        A10=str(A)+'_'+'10'
                        A11=str(A)+'_'+'11'
                        A12=str(A)+'_'+'12'
                else:
                        A1=0
                        A2=0
                        A3=0
                        A4=0
                        A5=0
                        A6=0
                        A7=0
                        A8=0
                        A9=0
                        A10=0
                        A11=0
                        A12=0
            
                
             
               
                for k in range(1,Quarter.shape[0]):
                
                    if Quarter.iloc[k,2]==A1 or Quarter.iloc[k,2]==A2 or Quarter.iloc[k,2]==A3 or Quarter.iloc[k,2]==A4 or Quarter.iloc[k,2]==A5 or Quarter.iloc[k,2]==A6 or Quarter.iloc[k,2]==A7 or Quarter.iloc[k,2]==A8 or Quarter.iloc[k,2]==A9 or Quarter.iloc[k,2]==A10 or Quarter.iloc[k,2]==A11 or Quarter.iloc[k,2]==A12:
                    
                        Count=Count+1
                    
                        Sum=Sum+Quarter.iloc[k,i]
                    
                        if Count==12:
                        
                            Average1.iloc[j,i]=Sum/12
                    else:
                        Average1.iloc[j,i]=0
                    
                    k=k+1
            

                    
                j=j+1
            
        
            i=i+1
    
    
        Average1=Average1.iloc[:,2:]
        Average5=Average1.transpose()
        
        return Average5
    
    def ARtoAT(self):
        Value1,X=self.RT()
        Value,add=self.WtoAK()
        Average=self.Quarter1()
        
        
        
        
        Value1=Value1.reset_index()
        new2 = Value1["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        Value1["Importer"]= new2[0]
        
        cols1 = list(Value1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Value1 = Value1[cols1]
        
        
        
        Value1["Exporter"]= new2[1]
        cols1 = list(Value1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Value1 = Value1[cols1]
        Average=Average.reset_index()
        new_header = Average.iloc[0] 
        
        Average = Average[1:] 
        
        Average.columns = new_header
        new2 = Average["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        Average["Importer"]= new2[0]
        
        cols1 = list(Average.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Average = Average[cols1]
        
        
        
        Average["Exporter"]= new2[1]
        cols1 = list(Average.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Average = Average[cols1]
        Value=Value.sort_values(by=['Importer'])
        
        add=add.sort_values(by=['Importer'])
        for i in range(Value.shape[1]):
            for j in range(Value.shape[0]):
                if Value.iloc[j,i]==0 and add.iloc[j,i]!=0:
                    Value.iloc[j,i]='possible'
                j=j+1
            i=i+1
    
        Value2=Value1.copy()
        Value2=Value2.iloc[:,0:2]
        Value=Value2.merge(Value, on='Importer', how='right')
        
        Average.drop(columns =["index"], inplace = True)
        Value['Importer.Exporter Combination']=Value['Importer']+'_'+Value['Exporter']
        
        
        cols1 = list(Value.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Value = Value[cols1]
    
        Value=Value.sort_values(by=['Importer.Exporter Combination'])
        Average=Average.sort_values(by=['Importer.Exporter Combination'])
        
        for i in range(3,Average.shape[1]):
            for j in range(Average.shape[0]):
                if Value.iloc[j,i]=='possible':
                    Value.iloc[j,i]=Average.iloc[j,i]
                else:
                    Value.iloc[j,i]=0
                j=j+1
            i=i+1
        
        db_list.append(wtd_flat.mult_year_single_output(Value, "Big Matrix ARtoAT"))
        Value.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix ARtoAT.csv", index=False)

        return Value,Average
    def AVtoAX(self):

        table2,value2=self.ARtoAT()
        table1,value3=self.RT()
        
        AVToAX=table2.copy()
        
        new2 = table1["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        table1["Importer"]= new2[0]
        
        cols1 = list(table1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        table1 = table1[cols1]
        
        
        
        table1["Exporter"]= new2[1]
        cols1 = list(table1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        table1 = table1[cols1]
        table1=table1.sort_values(by=['Importer.Exporter Combination'])
        table2=table2.sort_values(by=['Importer.Exporter Combination'])
    

        for i in range(3,table2.shape[1]):
            for j in range(table2.shape[0]):
                if table1.iloc[j,i]>0:
                    AVToAX.iloc[j,i]=table1.iloc[j,i]
                else:
                    AVToAX.iloc[j,i]=table2.iloc[j,i]
                j=j+1
            i=i+1
    
        db_list.append(wtd_flat.mult_year_single_output(AVToAX, "BigMatrix AVtoAX"))
        AVToAX.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix AVtoAX.csv", index=False)

        return AVToAX
    def BBtoBP(self):
    
        projects12=DeclaredExporter.copy()
        projects14=projects1.copy()
        projects12['YEAR'] = projects12['YEAR'].astype(str)
        projects12['MONTH'] = projects12['MONTH'].astype(str)
        projects12['UNIT'] = projects12['UNIT'].astype(str)
        
        projects13=projects12.copy()
        for j in range(projects12.shape[0]):
            projects12.iloc[j,0]=projects12.iloc[j,0]+'_'+projects12.iloc[j,1]
            j=j+1
        for j in range(projects12.shape[0]):
        
            projects13.iloc[j,0]=str(projects12.iloc[j,4])+'_'+str(projects12.iloc[j,2])
            projects13.iloc[j,1]=str(projects12.iloc[j,2])+'_'+str(projects12.iloc[j,4])
            j=j+1
        projects13=projects13.rename(columns={"YEAR": "Importer.Exporter Combination", "MONTH": "Exporter.Importer Combination"})
        projects13=projects13.iloc[:,0:2] 
    
        for j in range(projects12.shape[0]):
            projects12.iloc[j,5]=float(projects12.iloc[j,5])/1000000  
            if projects12.iloc[j,3]=='KG':
                projects12.iloc[j,5]=float(projects12.iloc[j,5])/1000
            else:
                projects12.iloc[j,5]=projects12.iloc[j,5]
        
            j=j+1

        projects12=pd.concat([projects12, projects13],axis=1)
        projects12 = projects12[['QUANTITY','REPORTING exporter COUNTRY','PARTNER importer COUNTRY','YEAR',
                           'Importer.Exporter Combination','Exporter.Importer Combination'] ]  
        projects12=projects12.iloc[:,0:6]
    
        joinExporters=pd.crosstab(index=projects12['Importer.Exporter Combination'],columns=projects12['YEAR'],
                  values=projects12['QUANTITY'], 
                   aggfunc='sum',dropna=False)

    
        joinExporter=joinExporters.merge(projects14, on='Importer.Exporter Combination', how='right')

        joinExporter=joinExporter.replace(np.NaN,0)
    
        
    
        DF3=joinExporter.transpose()
        DF3=DF3.reset_index()
        new1 = DF3["index"].str.split("_", n = 1, expand = True)
        DF3["MONTH"]=new1[1]
    
        cols1 = list(DF3.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        DF3 = DF3[cols1]
        DF3["YEAR"]=new1[0]
        cols1 = list(DF3.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        DF3 = DF3[cols1]
        
        
        df4=DF3.copy()
        

        for i in range(3,DF3.shape[1]):
            for j in range(1,DF3.shape[0]):
                Sum=0
                for k in range(1,DF3.shape[0]):
                
                    if DF3.iloc[j,0]>DF3.iloc[k,0]:
                    
                        Sum=Sum+DF3.iloc[k,i]
                    
                    elif DF3.iloc[j,0]==DF3.iloc[k,0]:
                        if DF3.iloc[j,1]>DF3.iloc[k,1]:
                            Sum=Sum+DF3.iloc[k,i]
                    else:
                        Sum=0
                    
                        
                    k=k+1
                df4.iloc[j,i]=Sum
                j=j+1
            i=i+1
    
        df4=df4.transpose()
        new_header = df4.iloc[0] 

    

        df4.columns = new_header
        db_list.append(wtd_flat.mult_year_single_output(joinExporter, "BigMatrix BBtoBP"))
        joinExporter.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix BBtoBP.csv", index=False)
        return joinExporter,df4
   
    def BStoCH(self):
        DF2,X=self.BBtoBP()
        new1 = DF2["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
  
    # making separate first name column from new data frame 
        DF2["Exporter"]= new1[1]
    # Dropping old Name columns 
        DF2.drop(columns =["Importer.Exporter Combination"], inplace = True)
        cols1 = list(DF2.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        DF2 = DF2[cols1]
        DF2 = DF2.groupby("Exporter").sum()
        
        db_list.append(wtd_flat.mult_year_single_output(DF2, "BigMatrix BStoCH"))
        DF2.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix BStoCH.csv", index=False)
        DF3=DF2.transpose()
        DF3=DF3.reset_index()
        new1 = DF3["index"].str.split("_", n = 1, expand = True)
        DF3["MONTH"]=new1[1]
    
        cols1 = list(DF3.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        DF3 = DF3[cols1]
        DF3["YEAR"]=new1[0]
        cols1 = list(DF3.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        DF3 = DF3[cols1]
        
        DF3.iloc[:,0:2]=DF3.iloc[:,0:2].astype(int)
        df4=DF3.copy()

        for i in range(3,DF3.shape[1]):
            for j in range(DF3.shape[0]):
                Sum=0
                for k in range(DF3.shape[0]):
                
                    if DF3.iloc[j,0]>DF3.iloc[k,0]:
                    
                        Sum=Sum+DF3.iloc[k,i]
                    
                    elif DF3.iloc[j,0]==DF3.iloc[k,0]:
                        if DF3.iloc[j,1]>DF3.iloc[k,1]:
                            Sum=Sum+DF3.iloc[k,i]
                    
                        
                    k=k+1
                df4.iloc[j,i]=Sum
                j=j+1
            i=i+1
        df4=df4.iloc[:,2:]
        df4=df4.transpose()

   
        df4.rename(columns={'index':'Exporter'},inplace=True)
        
    

   
            
        return DF2,df4
        
    def Quarter2(self):
        Quarter,X=self.BBtoBP()
        
        Quarter=Quarter.transpose()
        
        
        Quarter=Quarter.reset_index()
        
        new2 = Quarter["index"].str.split("_", n = 1, expand = True) 
        
        Quarter["YEAR"]= new2[0]
                
        cols1 = list(Quarter.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Quarter = Quarter[cols1]
        
            
    
        Quarter["MONTH"]= new2[1]
        cols1 = list(Quarter.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Quarter = Quarter[cols1]
        Quarter.iloc[:,1]=Quarter.iloc[:,1]
        
        
    
        Average1=Quarter.copy()
    
        for i in range(3,Quarter.shape[1]):
            for j in range(1,Quarter.shape[0]):
                Count=0 
                Sum=0
            
                if Quarter.iloc[j,0]=='10' or Quarter.iloc[j,0]=='11' or Quarter.iloc[j,0]=='12':
                    A=int(Quarter.iloc[j,1])
                    B=A
                    A=A-1
                    A=str(A)
                    A1=str(A)+'_'+'10'
                    A2=str(A)+'_'+'11'
                    A3=str(A)+'_'+'12'
                    A4=str(B)+'_'+'1'
                    A5=str(B)+'_'+'2'
                    A6=str(B)+'_'+'3'
                    A7=str(B)+'_'+'4'
                    A8=str(B)+'_'+'5'
                    A9=str(B)+'_'+'6'
                    A10=str(B)+'_'+'7'
                    A11=str(B)+'_'+'8'
                    A12=str(B)+'_'+'9' 
                    
                    
                elif Quarter.iloc[j,0]=='4' or Quarter.iloc[j,0]=='5' or Quarter.iloc[j,0]=='6':
                    A=int(Quarter.iloc[j,1])
                    B=A
                    A=A-1
                    A=str(A)
                    A1=str(B)+'_'+'1'
                    A2=str(B)+'_'+'2'
                    A3=str(B)+'_'+'3'
                    A4=str(A)+'_'+'4'
                    A5=str(A)+'_'+'5'
                    A6=str(A)+'_'+'6'
                    A7=str(A)+'_'+'7'
                    A8=str(A)+'_'+'8'
                    A9=str(A)+'_'+'9'
                    A10=str(A)+'_'+'10'
                    A11=str(A)+'_'+'11'
                    A12=str(A)+'_'+'12'
                
                elif Quarter.iloc[j,0]=='1' or Quarter.iloc[j,0]=='2' or Quarter.iloc[j,0]=='3':
                    A=int(Quarter.iloc[j,1])
                    B=A
                    A=A-1
                    A=str(A)
                    A1=str(B)+'_'+'10'
                    A2=str(B)+'_'+'11'
                    A3=str(B)+'_'+'12'
                    A4=str(A)+'_'+'1'
                    A5=str(A)+'_'+'2'
                    A6=str(A)+'_'+'3'
                    A7=str(A)+'_'+'4'
                    A8=str(A)+'_'+'5'
                    A9=str(A)+'_'+'6'
                    A10=str(B)+'_'+'7'
                    A11=str(B)+'_'+'8'
                    A12=str(B)+'_'+'9'
                elif Quarter.iloc[j,0]=='7' or Quarter.iloc[j,0]=='8' or Quarter.iloc[j,0]=='9':
                    A=int(Quarter.iloc[j,1])
                    B=A
                    A=A-1
                    A=str(A)
                    A1=str(B)+'_'+'1'
                    A2=str(B)+'_'+'2'
                    A3=str(B)+'_'+'3'
                    A4=str(B)+'_'+'4'
                    A5=str(B)+'_'+'5'
                    A6=str(B)+'_'+'6'
                    A7=str(A)+'_'+'7'
                    A8=str(A)+'_'+'8'
                    A9=str(A)+'_'+'9'
                    A10=str(A)+'_'+'10'
                    A11=str(A)+'_'+'11'
                    A12=str(A)+'_'+'12'
                else:
                    A1=0
                    A2=0
                    A3=0
                    A4=0
                    A5=0
                    A6=0
                    A7=0
                    A8=0
                    A9=0
                    A10=0
                    A11=0
                    A12=0
                        
                
             
               
                for k in range(1,Quarter.shape[0]):
                
                    if Quarter.iloc[k,2]==A1 or Quarter.iloc[k,2]==A2 or Quarter.iloc[k,2]==A3 or Quarter.iloc[k,2]==A4 or Quarter.iloc[k,2]==A5 or Quarter.iloc[k,2]==A6 or Quarter.iloc[k,2]==A7 or Quarter.iloc[k,2]==A8 or Quarter.iloc[k,2]==A9 or Quarter.iloc[k,2]==A10 or Quarter.iloc[k,2]==A11 or Quarter.iloc[k,2]==A12:
                    
                        Count=Count+1
                    
                        Sum=Sum+Quarter.iloc[k,i]
                    
                        if Count==12:
                        
                            Average1.iloc[j,i]=Sum/12
                        else:
                            Average1.iloc[j,i]=0
                    
                    k=k+1
            

                    
                j=j+1
            
        
            i=i+1
        Average1=Average1.iloc[:,2:]
        Average1=Average1.transpose()
        return Average1
    
    

    def COtoCQ(self):
        value1,B=self.BBtoBP()
        value,add=self.BStoCH()
        Average=self.Quarter2()
        
        
        
        
        value1=value1.reset_index()
        new2 = value1["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        value1["Importer"]= new2[0]
        
        cols1 = list(value1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        value1 = value1[cols1]
        
        
        
        value1["Exporter"]= new2[1]
        cols1 = list(value1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        value1 = value1[cols1]
        Average=Average.reset_index()
        new_header = Average.iloc[0] 
        
        Average = Average[1:] 
        
        Average.columns = new_header
        new2 = Average["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        Average["Importer"]= new2[0]
        
        cols1 = list(Average.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Average = Average[cols1]
        
        
        
        Average["Exporter"]= new2[1]
        cols1 = list(Average.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Average = Average[cols1]
        value=value.sort_values(by=['Exporter'])
        print(add)
        add=add.sort_values(by=['Exporter'])
        for i in range(value.shape[1]):
            for j in range(value.shape[0]):
                if value.iloc[j,i]==0 and add.iloc[j,i]!=0:
                    value.iloc[j,i]='possible'
                j=j+1
            i=i+1
    
        value2=value1.copy()
        value2=value2.iloc[:,0:2]
        value=value2.merge(value, on='Exporter', how='right')
        
        Average.drop(columns =["index","Importer.Exporter Combination"], inplace = True)
        
        value=value.sort_values(by=['Importer','Exporter'])
        Average=Average.sort_values(by=['Importer','Exporter'])
        
        
        for i in range(2,Average.shape[1]):
            for j in range(Average.shape[0]):
                if value.iloc[j,i]=='possible':
                    value.iloc[j,i]=Average.iloc[j,i]
                else:
                    value.iloc[j,i]=0
                j=j+1
            i=i+1
    
    
        Average2=Average.copy()
        db_list.append(wtd_flat.mult_year_single_output(value, "BigMatrix COtoCQ" ))
        value.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix COtoCQ.csv", index=False)
        return value,Average2
    
    

    def CStoCU(self):
        Table2,value2=self.COtoCQ()
        Table1,value3=self.BBtoBP()
        
        
    
        new2 = Table1["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        Table1["Importer"]= new2[0]
   
        cols1 = list(Table1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Table1 = Table1[cols1]
    
    
    
        Table1["Exporter"]= new2[1]
        cols1 = list(Table1.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Table1 = Table1[cols1]
        Table2['Importer.Exporter Combination']=Table2['Importer']+'_'+Table2['Exporter']
    
   
        cols1 = list(Table2.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        Table2 = Table2[cols1]
        Table1=Table1.sort_values(by=['Importer.Exporter Combination'])
        Table2=Table2.sort_values(by=['Importer.Exporter Combination'])
        
        AVtoAX=Table2.copy()

        for i in range(3,Table2.shape[1]):
            for j in range(Table2.shape[0]):
                if Table1.iloc[j,i]>0:
                    AVtoAX.iloc[j,i]=Table1.iloc[j,i]
                else:
                    AVtoAX.iloc[j,i]=Table2.iloc[j,i]
                j=j+1
            i=i+1
        db_list.append(wtd_flat.mult_year_single_output(AVtoAX, "BigMatrix CStoCU"))
        AVtoAX.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix CStoCU.csv", index=False)
        return AVtoAX
    def CXtoDA(self):

        CS=self.CStoCU()
        AV=self.AVtoAX()
    
        CX=CS.copy()
    
        for i in range(3,CS.shape[1]):
            for j in range(CS.shape[0]):
                if CS.iloc[j,i]>0 and AV.iloc[j,i]>0:
                    A=CS.iloc[j,i]
                    B=AV.iloc[j,i]
                    CX.iloc[j,i]=(A+B)/2    
                else:
                    if CS.iloc[j,i]>AV.iloc[j,i]:
                        CX.iloc[j,i]=CS.iloc[j,i]
                    else:
                        CX.iloc[j,i]=AV.iloc[j,i]
                j=j+1
            i=i+1
    
        CX.drop(columns =["Importer","Exporter"], inplace = True)
    
        CX=CX.transpose()
        CX.reset_index( inplace=True)
    
        new_header = CX.iloc[0] 
        
        CX= CX[1:] 

        CX.columns = new_header
    
   
        new2 = CX["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        CX["YEAR"]= new2[0]
        
        cols1 = list(CX.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        CX = CX[cols1]
        
        
        
        CX["MONTH"]= new2[1]
        cols1 = list(CX.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        CX = CX[cols1]
        CX=CX.sort_values(by=['YEAR','MONTH'])
        
        CX.iloc[:,0:2]=CX.iloc[:,0:2].astype(int)
        CX1=CX.iloc[:,0:1]

        for j in range(CX.shape[0]):
        
            if CX.iloc[j,0]==1 or CX.iloc[j,0]==2 or CX.iloc[j,0]==3:
                CX.iloc[j,0]=1
            
        
            if CX.iloc[j,0]==4 or CX.iloc[j,0]==5 or CX.iloc[j,0]==6:
                CX.iloc[j,0]=2
        
            if CX.iloc[j,0]==7 or CX.iloc[j,0]==8 or CX.iloc[j,0]==9:
                CX.iloc[j,0]=3
        
            if CX.iloc[j,0]==10 or CX.iloc[j,0]==11 or CX.iloc[j,0]==12:
                CX.iloc[j,0]=4
            j=j+1
        CX.iloc[:,0:2]=CX.iloc[:,0:2].astype(str)
        CX['Quarter']=CX['YEAR']+''+CX['MONTH']

        CX=CX.sort_values(by=['Quarter'],ascending=False )
        cols1 = list(CX.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        CX = CX[cols1]
        for i in range(CX.shape[0]):
            Count=0
            for j in range(CX.shape[0]):
                if CX.iloc[i,0]==CX.iloc[j,0]:
                    Count=Count+1
                j=j+1
            CX1.iloc[i,0]=Count
        
            i=i+1
    
        CX1.rename(columns={'MONTH':'Value1'},inplace=True)
    
        CX=pd.concat([CX1, CX], axis=1)
        CX2=CX1.copy()
    
        CX = CX[CX['Value1']==3]
        CX=CX.sort_values(by=['Quarter'],ascending=False)
    
        CX=CX.iloc[0:3,4:]
        CX=CX.transpose().reset_index().rename(columns={'index':'Variable'})
        new_header = CX.iloc[0] 

        CX = CX[1:] 

        CX.columns = new_header
        CX2=CX.iloc[:,1:]
        CX['Total']= CX2.sum(axis=1)
        CX=CX.reset_index()
        
        CX=CX.iloc[:,1:]
        db_list.append(wtd_flat.mult_year_single_output(CX, "BigMatrix CXtoDA"))
        CX.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix CXtoDA.csv", index=False)

        return CX

    def DC(self):
        DC=Dc.copy()
        CX=self.CXtoDA()
        
        DC['Total']= (DC.sum(axis=1))/1000000
        
        new2 = CX["Importer.Exporter Combination"].str.split("_", n = 1, expand = True) 
        
        CX["Importer"]= new2[0]
        
        cols1 = list(CX.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        CX = CX[cols1]
        
        CX["Exporter"]= new2[1]
        cols1 = list(CX.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        CX = CX[cols1]
        
        
        
        DC1=CX.merge(DC, on='Exporter', how='left',suffixes=('', '_y') )
        
        DC2=CX.merge(DC, on='Exporter', how='outer',suffixes=('', '_1') )
        DC2=DC2[DC2.Total.isnull()]
        DC2=DC2[DC2.Total_1.notnull()]
        DC2=DC2['Total_1'].sum()
   

        for i in range(DC1.shape[1]):
            if DC1.iloc[i,1]!='China':
                DC1.iloc[i,10]=0
            i=i+1
    
        for i in range(DC1.shape[0]):
            if DC1.iloc[i,1]=='China':
                DC1.iloc[i,6]=DC1.iloc[i,10]
            i=i+1
        DC1=DC1.iloc[:,0:7]
        db_list.append(wtd_flat.mult_year_single_output(DC1, "BigMatrix DCtoDI"))
        DC1.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix DCtoDI.csv", index=False)

        return DC1,DC2                   

    def DKtoDS(self):
        DCV,China=self.DC()
   
        DM=pd.read_excel ('World Trade Model.xlsx',sheet_name='DM')

        DM=DM.iloc[:,0:2]
        
        DCV['Exporter.Importer Combination']=DCV['Exporter']+'_'+DCV['Importer']
        
        DCV=DM.merge(DCV, on='Exporter.Importer Combination', how='left')
        for i in range(DCV.shape[1]):
            if DCV.iloc[i,0]=='Other_China':
                DCV.iloc[i,7]=China
                DCV.iloc[i,1]='Other'
                
                DCV.iloc[i,2]='China'

            
            i=i+1    
        DCV2=DCV.copy()
        
        DCV2['Importer']=DCV['Importer'].astype(str).str[-1]
        DCV2['Exporter']=DCV['Exporter'].astype(str).str[0]
        
            
        for i in range(DCV.shape[0]):
            A=DCV2.iloc[i,1]
            B=DCV2.iloc[i,2]
            
            DCV2.iloc[i,0]=ord(A)
            DCV2.iloc[i,1]=ord(B)
            i=i+1
        DCV2=DCV2.iloc[:,0:2]

        DCV2.rename(columns={'Exporter.Importer Combination':'last Char() as code'},inplace=True)
        DCV2.rename(columns={'Exporter':'First Char() as code'},inplace=True)
        DCV=pd.concat([DCV, DCV2], axis=1)
    
        for i in range(DCV.shape[0]):
        
            j=5
            LastFirst=(DCV.iloc[i,9]/DCV.iloc[i,8])
            LastFirst=LastFirst+math.pi
            Row=(LastFirst/j)/1000000000
            DCV.iloc[i,3]=Row+DCV.iloc[i,7]
            j=j+1
            i=i+1
        
        DCV=DCV.replace(np.NaN,0)
        cols = [4,5,6]
        DCV.drop(DCV.columns[cols],axis=1,inplace=True)
        
        DCV.rename(columns={'Importer.Exporter Combination':'Ranking total'},inplace=True)
        DCV['Rank'] = DCV["Ranking total"].rank(ascending=0,method='max')
        db_list.append(wtd_flat.single_year_mult_out(DCV, "BigMatrix DKtoDS"))
        DCV.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix DKtoDS.csv", index=False)

        return DCV      
    
    
    def DWtoEB(self):
        DK=self.DKtoDS()
        Sum=0
        DK=DK.sort_values(by=['Rank'])
        cols = [3,5,6]
        DK.drop(DK.columns[cols],axis=1,inplace=True)
        Total12 = DK['Total'].sum()
        print(Total12)
        DK=DK.reset_index()
        for i in range(DK.shape[0]):
            Sum=Sum+DK.loc[i,'Total']
            DK.loc[i,'Sum']=Sum
            DK.loc[i,'% of total']=(Sum/Total12)*100
            
            
        
            DK.loc[i,'Annualised']=((DK.loc[i,'Total']/Q)*Y)
            i=i+1
        db_list.append(wtd_flat.single_year_mult_out(DK, "BigMatrix DWtoEB"))
        DK.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\BigMatrix DWtoEB.csv", index=False)
            
        return DK
  
    def FinalProcessing(self):
        
        LastQuarter2=LastQuarter.copy()
        
        LastYear2=LastYear.copy()

        # write FT2 here 
    
        FT2=FinalTable.copy()
        FT3=FinalTable.copy()
        
        DK=self.DWtoEB()
    
        T8=DK['Annualised'].sum()
        DK5=DK.copy()
        
        DK1=DK.copy()
        DK3=DK.copy()
        '''DtoI'''
        DK=DK[DK['Importer']=='China']
        DK2=DK.copy()
        Total1=DK2['Annualised'].sum()
        db_list.append(wtd_flat.single_year_mult_out(DK, "Final processing DtoI"))
        DK.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\Final processing DtoI.csv", index=False)

        '''MtoR'''
        
        DK1=DK1[DK1['Exporter']=='Guinea'] 
        DK1=DK1[DK1['Importer']!='China']
        Total2=DK1['Annualised'].sum()
        db_list.append(wtd_flat.single_year_mult_out(DK1, "Final processing MtoR"))
        DK1.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\Final processing MtoR.csv", index=False)

        '''VtoAA'''
        DK3=DK3[DK3['Exporter']!='Guinea'] 
        DK3=DK3[DK3['Importer']!='China']
        db_list.append(wtd_flat.single_year_mult_out(DK3, "Final processing VtoAA"))
        DK3.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\Final processing VtoAA.csv", index=False)

        Total3=DK3['Annualised'].sum()
        '''Final Table'''
        FT2['Exporter.Importer Combination'] = FT2['Exporter']+'_'+FT2['Importer']
        

        FT2=FT2.merge(DK5, on='Exporter.Importer Combination', how='left',suffixes=('', '_y') )
        
        FT2['Mln t']=FT2['Annualised']
        FT2=FT2.iloc[:,0:3]
        cols = list(FT2.columns)
        print(cols)
        # come back
        FT5=FT2.copy()
        FT6=FT2.copy()
        
        
        
        FT4=FT2.copy()
        
        FT4=FT4[FT4['Importer']=='China']
        T1=FT4['Mln t'].sum()
        R1=Total1-T1

        for i in range(FT2.shape[0]):
            if FT2.iloc[i,0]=='Other':
                if FT2.iloc[i,1]=='China':
                    if f'{cols[2]}_{i}' in override_store:
                        FT2.iloc[i,2] = override_store[f'{cols[2]}_{i}']
                    else:  
                        FT2.iloc[i,2]=R1
            i=i+1
            FT5=FT5[FT5['Exporter']=='Guinea']
            FT5=FT5[FT5['Importer']!='China']
            
            
            T2=FT5['Mln t'].sum()
            
            R2=Total2-T2
            
            for i in range(FT2.shape[0]):
                if FT2.iloc[i,0]=='Guinea':
                    if FT2.iloc[i,1]=='Other':
                        if f'{cols[2]}_{i}' in override_store:
                            FT2.iloc[i,2] = override_store[f'{cols[2]}_{i}']
                        else:  
                            FT2.iloc[i,2]=R2
                i=i+1
    
        FT6=FT6[FT6['Exporter']!='Guinea']
        FT6=FT6[FT6['Importer']!='China']
        
        
        T3=FT6['Mln t'].sum()
        
        R3=Total3-T3
        
        FT2=FT2.replace(np.NaN,0)

        for i in range(FT2.shape[0]):
            if FT2.iloc[i,0]=='Other':
                if FT2.iloc[i,1]==0:
                    if f'{cols[2]}_{i}' in override_store:
                        FT2.iloc[i,2] = override_store[f'{cols[2]}_{i}']
                    else:
                        FT2.iloc[i,2]=R3
            i=i+1
            FT7=FT2.copy()
            FT7=FT7[FT7['Importer']=='China']
    
            T4=FT7['Mln t'].sum()
        for i in range(FT2.shape[0]):
            if FT2.iloc[i,0]=='China Portion':
                    if f'{cols[2]}_{i}' in override_store:
                        FT2.iloc[i,2] = override_store[f'{cols[2]}_{i}']
                    else:  
                        FT2.iloc[i,2]=Total1
            i=i+1
        for i in range(FT2.shape[0]):
            if FT2.iloc[i,0]=='Seaborne':
                if f'{cols[2]}_{i}' in override_store:
                    FT2.iloc[i,2] = override_store[f'{cols[2]}_{i}']
                else:  
                    FT2.iloc[i,2]=T8
            i=i+1


    
        FT3=FT2.round(1)
        
        
        
        
        for i in range(LastQuarter.shape[0]):
            LastQuarter2.iloc[i,2]=(LastQuarter.iloc[i,2]/365)*92
            i=i+1
        LastQuarter2.rename(columns={'Mln t':'Last Quarter'},inplace=True)
        LastQuarter2=LastQuarter2.iloc[:,2]
            
            
        LastQuarter2=pd.concat([LastQuarter, LastQuarter2],axis=1)
            

        for i in range(LastYear.shape[0]):
            LastYear2.iloc[i,2]=(LastYear.iloc[i,2]/365)*92
            i=i+1
    
        LastYear2.rename(columns={'Mln t':'Last Year'},inplace=True)
        LastYear2=LastYear2.iloc[:,2]

        for h in override_store.keys():
            print(h)
            sp = h.split('_')
            override_store[h]
            print(sp)
            i, c = sp[-1], ''.join(sp[:-1])
            FT2.loc[int(i), c] = override_store[h]
            
        LastYear2=pd.concat([LastYear, LastYear2],axis=1)
        FT2.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\Final Processing AEtoAG.csv", index=False)
        FT3.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\Final Processing AItoAK.csv", index=False)
        LastQuarter2.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\Final Processing Last Quarter.csv", index=False)
        LastYear2.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\Final Processing Last Year.csv", index=False)
        db_list.append(wtd_flat.single_year_mult_out(FT2, "Final Processing AEtoAG"))
        db_list.append(wtd_flat.single_year_mult_out(FT3, "Final Processing AItoAK.csv"))
        db_list.append(wtd_flat.single_year_mult_out(LastQuarter2, "Final Processing Last Quarter"))
        db_list.append(wtd_flat.single_year_mult_out(LastYear2, "Final Processing Last Year"))



    def RowExportersModelRANK(self): 
    
     
        
        
        
        Row1=Row.iloc[:,0:3]
        
        Row1 = Row1.groupby("Exporter").sum()
        Rank=Row1.merge(input1, on='Exporter', how='right')
        
        Rank=Rank.replace(np.NaN,0)
        
        
        Rank['Rank'] = Rank["Total for Quarter"].rank(ascending=0,method='max')
        db_list.append(wtd_flat.single_year_mult_out(Rank, "ROWEXPORTER Rank1")) 
        Rank.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\ROWEXPORTER Rank1.csv", index=False)

        return Rank
    
    def RowExportersModelRANK2(self): 
        
        
       
        
        Row2=Row1.iloc[:,0:3]
            
        Row2 = Row2.groupby("Importer").sum()
        
        Rank1=Row2.merge(input2, on='Importer', how='left')
        
        Rank1=Rank1.replace(np.NaN,0)
        
        
        Rank1['Rank'] = Rank1["Total for Quarter"].rank(ascending=0,method='max') 
        db_list.append(wtd_flat.single_year_mult_out(Rank1, "ROWEXPORTER Rank2"))
        Rank1.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\ROWEXPORTER Rank2.csv", index=False)

        
        return Rank1

    def RowExportersModelTotal(self): 

    
        
        Value3=Total1['Total for Quarter'].sum()     
        
        print(Value3)
        return Value3
    
    
    
    

    def RowExporterOutput2(self)  :
        
        Total=self.RowExportersModelTotal()
        
        
        RankValues=self.RowExportersModelRANK2()
        RankValues=RankValues.sort_values(by=['Rank'])
        RankValues=RankValues.iloc[0:15,:]
        
        Rankvalues=RankValues.iloc[0:15,0]
        Rankvalues=pd.DataFrame(Rankvalues)
        RankValues.rename(columns={'Importer':'Value'},inplace=True)
    
        for i in range(RankValues.shape[0]):
            Rankvalues.iloc[i,0]=(RankValues.iloc[i,1])/(Total)
            i=i+1
    
        RankValues=pd.concat([RankValues,Rankvalues],axis=1)
    

        RankValues.rename(columns={'Value':'ROW Importer',
                          'Total for Quarter':'ROW Tonnage',
                          'Rank':'Rank','Importer':'ROW % World Imports'},inplace=True)
        RankValues = RankValues[['Rank','ROW Importer','ROW Tonnage','ROW % World Imports'] ] 
        db_list.append(wtd_flat.single_year_mult_out(RankValues, "ROW EXPORTER TABLE2")) 
        RankValues.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\ROWEXPORTERTABLE2.csv", index=False)

        return RankValues
    def RowExporterOutpu1(self)  :
        
        Total=self.RowExportersModelTotal()
        
        
        RankValues1=self.RowExportersModelRANK()
        RankValues1=RankValues1.sort_values(by=['Rank'])
        RankValues1=RankValues1.iloc[0:15,:]
        
        Rankvalues1=RankValues1.iloc[0:15,0]
        Rankvalues1=pd.DataFrame(Rankvalues1)
        RankValues1.rename(columns={'Exporter':'Value'},inplace=True)
    
        for i in range(RankValues1.shape[0]):
            Rankvalues1.iloc[i,0]=(RankValues1.iloc[i,1])/(Total)
            i=i+1
    
        RankValues1=pd.concat([RankValues1,Rankvalues1],axis=1)
        
        
        RankValues1.rename(columns={'Value':'ROW Importer',
                          'Total for Quarter':'ROW Tonnage',
                          'Rank':'Rank','Exporter':'ROW % World Exports'},inplace=True)
        RankValues1 = RankValues1[['Rank','ROW Importer','ROW Tonnage','ROW % World Exports'] ]  
        db_list.append(wtd_flat.single_year_mult_out(RankValues1, "ROW EXPORTER TABLE 1"))
        RankValues1.to_csv(r"C:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\World Trade Data - Dexter\World Trade Data - Dexter\Outputs\ROWEXPORTERTABLE1.csv", index=False)

        return RankValues1
        

    
    
        
self1 = WorldTradeModel()
self1.calc()

snapshot_output_data = pd.concat(db_list, ignore_index=True)
override_res = override_rows.values
for i, v in enumerate(override_rows.index):
    print(snapshot_output_data.loc[v], override_res[i])
    snapshot_output_data.loc[v] = override_res[i]
snapshot_output_data = snapshot_output_data.loc[:, wtd_flat.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)

snapshot_output_data = snapshot_output_data

uploadtodb.upload(snapshot_output_data)
