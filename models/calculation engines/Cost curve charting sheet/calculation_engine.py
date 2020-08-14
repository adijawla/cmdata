import pandas as pd
import math
import warnings
warnings.filterwarnings("ignore")
import costcurvesdb
from flatdbconverter import Flatdbconverter
from outputdb import uploadtodb
a,b = costcurvesdb.func()
db_conv = Flatdbconverter("Cost curve charting sheet")
class costcurve():
    
    def __init__(self,a):
        self.inputdata = a
        col1 = ["Rank","Name","Prodn / Capacity","Cumulative Prodn / Capacity","Series 1","Series 2","Series 3","Series 4","Series 5","Series 6","Series 7","Series 8","Series 9","Series 10","Total Cost","Comment"]
        col2 = ["Bar","Fill x","Fill under Series 1","Fill under Series 2","Fill under Series 3","Fill under Series 4","Fill under Series 5","Fill under Series 6","Fill under Series 7","Fill under Series 8","Fill under Series 9","Fill under Series 10"]
        col3 = ['initialisation','bar','x','line 1','line 2','line 3','line 4','line 5','line 6','line 7','line 8','line 9','line 10','labels for cc']
        col4 = ['Q1 x','Q1','Q2 x','Q2','Q3 x','Q3']
        col5 = ['initialisation','bar','x','area 1','area 2','area 3','area 4','area 5','area 6','area 7','area 8','area 9','area 10']
        self.table1data = pd.DataFrame(columns=col1)
        self.table2data = pd.DataFrame(columns=col2)
        self.table3data = pd.DataFrame(columns=col3)
        self.table4data = pd.DataFrame(columns=col4)
        self.table5data = pd.DataFrame(columns=col5,index=list(range(self.inputdata.shape[0]*5-4)))
        self.table6data = pd.DataFrame(columns=['Rank','Final Label'])
        self.minvalueshow = 3
        self.fullscalefactor = 1000
        self.minxvalue = 0
        self.yscalemax = 100
    def inputdata_totalcost(self):
        self.inputdata['Total Cost'] = self.inputdata['Series 1']+self.inputdata['Series 2']+self.inputdata['Series 3']+self.inputdata['Series 4']+self.inputdata['Series 5']+self.inputdata['Series 6']+self.inputdata['Series 7']+self.inputdata['Series 8']+self.inputdata['Series 9']+self.inputdata['Series 10']
    
    def inputdata_rankcost(self,indx):
        d = [17]
        value = max(self.inputdata['Total Cost'])+d[0]+indx if self.inputdata['Prodn / Capacity'][indx] <= 0 else self.inputdata['Total Cost'][indx]+(d[0]+indx)/1000000000
        self.inputdata.at[indx,"Rank Cost Proxy"] = value
        
    def inputdata_text(self,indx):
        d = []
        value = self.inputdata['Name'][indx]+" "+str(round(self.inputdata['Prodn / Capacity'][indx],1))+"mln t"+" $"+str(round(self.inputdata['Total Cost'][indx],1)) if self.inputdata['Label regardless'][indx] == 1 or self.inputdata['Prodn / Capacity'][indx] >= self.minvalueshow else 0
        self.inputdata.loc[indx,"Associated Text"] = value
        
    def inputdata_sort(self):
        s = self.inputdata['Rank Cost Proxy']
        p = self.inputdata['Rank Cost Proxy'].tolist()
        s = s.sort_values()
        s = s.tolist()
        print(p)
        self.sortedrank = p
        for i in range(len(p)):
            self.sortedrank[i] = s.index(p[i])+1

    def input_rank(self):
        self.inputdata['Rank'] = self.sortedrank
    
    def table1datacalc(self):
        self.inputdata.to_csv('outputdata/checkdata.csv')
        self.inputdata.sort_values(by=['Rank'],inplace=True)
        self.table1data = self.inputdata.reset_index()
        self.table6data['Final Label'] = self.table1data['Associated Text']
    def table1datacalc2(self,indx):
        value = self.table1data['Cumulative Prodn / Capacity'][indx-1]+ self.table1data['Prodn / Capacity'][indx] if indx > 0 else self.table1data['Prodn / Capacity'][indx] 
        self.table1data.at[indx,'Cumulative Prodn / Capacity'] = value
    def table2datamaxx(self):
        self.maxxvalue = max(self.table1data['Cumulative Prodn / Capacity'])
    def table2datacalc(self,indx):
        self.table2data["Fill under Series 1"] = self.table1data["Series 1"]
        self.table2data["Fill under Series 2"] = self.table1data["Series 2"]
        self.table2data["Fill under Series 3"] = self.table1data["Series 3"]
        self.table2data["Fill under Series 4"] = self.table1data["Series 4"]
        self.table2data["Fill under Series 5"] = self.table1data["Series 5"]
        self.table2data["Fill under Series 6"] = self.table1data["Series 6"]
        self.table2data["Fill under Series 7"] = self.table1data["Series 7"]
        self.table2data["Fill under Series 8"] = self.table1data["Series 8"]
        self.table2data["Fill under Series 9"] = self.table1data["Series 9"]
        self.table2data["Fill under Series 10"] = self.table1data["Series 10"]
        self.table2data["Fill x"]  =  (self.table1data['Cumulative Prodn / Capacity'] - self.minxvalue)/(self.maxxvalue - self.minxvalue)*self.fullscalefactor
    
    def table3start(self):
        value1 = 0
        value2 = 0
        value3 = self.table3data['line 2'][1]
        value4 = self.table3data['line 3'][1]
        value5 = self.table3data['line 4'][1]
        value6 = self.table3data['line 5'][1]
        value7 = self.table3data['line 6'][1]
        value8 = self.table3data['line 7'][1]
        value9 = self.table3data['line 8'][1]
        value10 = self.table3data['line 9'][1]
        value11 = self.table3data['line 10'][1]
        self.table3data.at[0,'x'] = value1
        self.table3data.at[0,'line 1'] = value2
        self.table3data.at[0,'line 2'] = value3
        self.table3data.at[0,'line 3'] = value4
        self.table3data.at[0,'line 4'] = value5
        self.table3data.at[0,'line 5'] = value6
        self.table3data.at[0,'line 6'] = value7
        self.table3data.at[0,'line 7'] = value8
        self.table3data.at[0,'line 8'] = value9
        self.table3data.at[0,'line 9'] = value10
        self.table3data.at[0,'line 10'] = value11

    def table3intialisation(self,indx):
        value1 = "bar "+str(((indx+4)//5))+" start"
        value2 = "bar "+str(((indx+4)//5))+"bn"
        value3 = "bar "+str(((indx+4)//5))+" naming"
        value4 = "bar "+str(((indx+4)//5))+"an"
        value5 = "bar "+str(((indx+4)//5))+" end"
        self.table3data.at[indx,'initialisation'] = value1
        self.table3data.at[indx+1,'initialisation'] = value2
        self.table3data.at[indx+2,'initialisation'] = value3
        self.table3data.at[indx+3,'initialisation'] = value4
        self.table3data.at[indx+4,'initialisation'] = value5

    def table3bar(self,indx):
        value = str(((indx+4)//5))
        self.table3data.at[indx,'bar'] = value
        self.table3data.at[indx+1,'bar'] = value
        self.table3data.at[indx+2,'bar'] = value
        self.table3data.at[indx+3,'bar'] = value
        self.table3data.at[indx+4,'bar'] = value


# formulas from here


    def table3capprod(self,indx):
        value1 = self.table3data['x'][indx-1] if self.table1data['Prodn / Capacity'][((indx+4)//5)-1] == 0 else self.table3data['x'][indx-1]
        value2 = value1  if self.table1data['Prodn / Capacity'][((indx+4)//5)-1] == 0 else value1
        value3 = value2 if self.table1data['Prodn / Capacity'][((indx+4)//5)-1] == 0 else (self.table1data['Cumulative Prodn / Capacity'][((indx+4)//5)-1]+value2)/2
        value4 = value3 if self.table1data['Prodn / Capacity'][((indx+4)//5)-1] == 0 else self.table1data['Cumulative Prodn / Capacity'][((indx+4)//5)-1]
        value5 = value4 if self.table1data['Prodn / Capacity'][((indx+4)//5)-1] == 0 else self.table1data['Cumulative Prodn / Capacity'][((indx+4)//5)-1]
        self.table3data.at[indx,'x'] = value1
        self.table3data.at[indx+1,'x'] = value2 + 0.00001
        self.table3data.at[indx+2,'x'] = value3
        self.table3data.at[indx+3,'x'] = value4 - 0.00002
        self.table3data.at[indx+4,'x'] = value5 - 0.00001

    def table3line(self,indx,i):
        value1 = self.table3data['line '+str(i)][indx-1] if self.table1data['Prodn / Capacity'][((indx+4)//5)-1] == 0 or i == 1 else self.table3data['line '+str(i-1)][indx+1]
        value2 = value1 if self.table1data['Prodn / Capacity'][((indx+4)//5)-1] == 0 else self.table1data['Series '+str(i)][((indx+4)//5)-1]+value1
        value3 = value2
        value4 = value3
        value5 = value1
        self.table3data.at[indx,'line '+str(i)] = value1
        self.table3data.at[indx+1,'line '+str(i)] = value2
        self.table3data.at[indx+2,'line '+str(i)] = value3
        self.table3data.at[indx+3,'line '+str(i)] = value4
        self.table3data.at[indx+4,'line '+str(i)] = value5
        

    def table3comment(self,indx):
        value = self.table6data['Final Label'][(indx+4)//5 ]
        self.table3data.loc[indx+2,'labels for cc'] = value


    def table4calc(self):
        self.vtotal = self.table1data['Prodn / Capacity'].sum()
        value1=self.vtotal/4
        value2=0
        value3=self.vtotal/2
        value4=0
        value5=self.vtotal*3/4
        value6=0
        value7=value1
        value8=self.yscalemax
        value9=value3
        value10=self.yscalemax
        value11=value5
        value12=self.yscalemax
        self.table4data.at[0,'Q1 x'] = value1
        self.table4data.at[0,'Q1'] = value2
        self.table4data.at[0,'Q2 x'] = value3
        self.table4data.at[0,'Q2'] = value4
        self.table4data.at[0,'Q3 x'] = value5
        self.table4data.at[0,'Q3'] = value6
        self.table4data.at[1,'Q1 x'] = value7
        self.table4data.at[1,'Q1'] = value8
        self.table4data.at[1,'Q2 x'] = value9
        self.table4data.at[1,'Q2'] = value10
        self.table4data.at[1,'Q3 x'] = value11
        self.table4data.at[1,'Q3'] = value12

    def table5start(self):
        value1 = 0
        value2 = 0
        value3 = 0
        value4 = 0
        value5 = 0
        value6 = 0
        value7 = 0
        value8 = 0
        value9 = 0
        value10 = 0
        value11 = 0
        self.table5data.at[0,'x'] = value1
        self.table5data.at[0,'area 1'] = value1
        self.table5data.at[0,'area 2'] = value1
        self.table5data.at[0,'area 3'] = value1
        self.table5data.at[0,'area 4'] = value1
        self.table5data.at[0,'area 5'] = value1
        self.table5data.at[0,'area 6'] = value1
        self.table5data.at[0,'area 7'] = value1
        self.table5data.at[0,'area 8'] = value1
        self.table5data.at[0,'area 9'] = value1
        self.table5data.at[0,'area 10'] = value1

    def table5calc1(self):
        self.table5data['initialisation'] = self.table3data['initialisation']
        self.table5data['bar'] = self.table3data['bar']
        print(self.table5data)

    def table5capprod(self,indx):
        value1 = self.table5data['x'][indx-1]
        value2 = value1
        value3 = (self.table2data['Fill x'][((indx+4)//5)-1]+value2)/2
        value4 = self.table2data['Fill x'][((indx+4)//5)-1]
        value5 = value4
        self.table5data.at[indx,'x'] = value1
        self.table5data.at[indx+1,'x'] = value2
        self.table5data.at[indx+2,'x'] = value3
        self.table5data.at[indx+3,'x'] = value4
        self.table5data.at[indx+4,'x'] = value5

    def table5area(self,indx):
        for i in range(1,11):
            value1 =  self.table5data['area '+str(i)][indx-1]
            value2 = self.table1data['Series '+str(i)][((indx+4)//5)-1]
            value3 = value2
            value4 = value3
            value5 = value1
            self.table5data.at[indx,'area '+str(i)] = value1
            self.table5data.at[indx+1,'area '+str(i)] = value2
            self.table5data.at[indx+2,'area '+str(i)] = value3
            self.table5data.at[indx+3,'area '+str(i)] = value4
            self.table5data.at[indx+4,'area '+str(i)] = value5

    def calcall(self):
        costcurve.inputdata_totalcost(self)
        for i in range(self.inputdata.shape[0]):
            costcurve.inputdata_rankcost(self,i)
            costcurve.inputdata_text(self,i)
        costcurve.inputdata_sort(self)
        costcurve.input_rank(self)
        costcurve.table1datacalc(self)
        for i in range(self.inputdata.shape[0]):
            costcurve.table1datacalc2(self,i)
        costcurve.table2datamaxx(self)
        self.table3data.loc[0,'x'] = 0
        self.table3data = self.table3data.fillna(0)
        #self.table3data[] = self.table3data[].astype(str)
        for i in range(self.inputdata.shape[0]):
            costcurve.table2datacalc(self,i)
        
        for i in range(1,self.inputdata.shape[0]*5-4,5):
            costcurve.table3intialisation(self,i)
            costcurve.table3bar(self,i)
            costcurve.table3capprod(self,i)
            
            costcurve.table3comment(self,i)

        for j in range(1,11):
            for i in range(1,self.inputdata.shape[0]*5-4,5):
                costcurve.table3line(self,i,j)
        costcurve.table3start(self)
        costcurve.table4calc(self)
        costcurve.table5start(self)
        costcurve.table5calc1(self)
        
        for i in range(1,self.inputdata.shape[0]*5-4,5):
            costcurve.table5capprod(self,i)
            costcurve.table5area(self,i)
            


r = costcurve(a)
r.calcall()
dblist = [
    db_conv.single_year_mult_out(r.table1data,"sorted input_data"),
    db_conv.single_year_mult_out(r.table2data,"Fill_data"),
    db_conv.single_year_mult_out(r.table3data,"Cost_Curve_Lines"),
    db_conv.single_year_mult_out(r.table4data,"Quartile_Lines"),
    db_conv.single_year_mult_out(r.table5data,"Cost_Curve_Fill"),
    db_conv.single_year_mult_out(r.table6data,"Final_Label_Rank"),
    

]
snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data.to_csv('output.csv')
uploadtodb.upload(snapshot_output_data)
'''
r.table1data.to_csv('outputdata/table1data.csv')
r.table2data.to_csv('outputdata/table2data.csv')
r.table3data.to_csv('outputdata/table3data.csv')
r.table4data.to_csv('outputdata/table4data.csv')
r.table5data.to_csv('outputdata/table5data.csv')
r.table6data.to_csv('outputdata/table6data.csv')'''

