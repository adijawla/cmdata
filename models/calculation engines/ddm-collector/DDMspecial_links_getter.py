import pandas as pd

class DDMspecialLinks():
    def __init__(self):
        self.DataPreserved = pd.read_csv('DDM special links getter Inputs\\Data Preserved.csv')
        self.ColumnsToText = pd.read_csv('DDM special links getter Inputs\\Columns to text.csv')
        self.allocationInputs = pd.read_csv('DDM special links getter Inputs\\inputs for Allocaion tab.csv')
        
        self.DDMengine = pd.read_csv('DDM special links getter Inputs\\newallocdb.csv')
        self.DDMrefinieries = pd.read_csv('DDM special links getter Inputs\\DDM refineries inputs.csv')

        self.allocationByCountry = pd.DataFrame(columns = list(map(str,range(1,30))), index= list(range(16745)))
        self.columns = ['Bx origin Prov', 'Bx Origin Cty', 'Refiney No.', 'Refiney Name', 'Refiney Prov', 'Allocation', 'Self Supply', 'Factored up allocation']
        self.count = 1
        
        self.newallocation = pd.DataFrame(columns=self.ColumnsToText['COLUMN NO.'])
        
    def newallocations(self):
        ind = 0
        for cols in list(range(896, len(self.ColumnsToText), 8)):
            self.newallocation[cols] = self.DDMengine['Province']
            self.newallocation[cols+1] = self.DDMengine['County']
            try:
                self.newallocation[cols+2] = self.DDMrefinieries.loc[ind,'Ref No.']
                self.newallocation[cols+3] = self.DDMrefinieries.loc[ind,'Refinery']
                self.newallocation[cols+4] = self.DDMrefinieries.loc[ind,'Province']
                self.newallocation[cols+5] = self.DDMengine[self.DDMrefinieries.loc[ind,'Refinery'] + ' NewCal']
                self.newallocation[cols+6] = self.DDMengine[self.DDMrefinieries.loc[ind,'Refinery'] + ' Closed']
            except:
                pass
            ind = ind + 1
        
        
    
    
    def main(self):
        self.d = {self.ColumnsToText['Letter'][x]:self.ColumnsToText['COLUMN NO.'][x] for x in range(len(self.ColumnsToText))}
        d2 = {self.ColumnsToText['COLUMN NO.'][x]:self.ColumnsToText['Letter'][x] for x in range(len(self.ColumnsToText))}
        self.allocationByCountry.loc[0, '11'] = 'SpreadSheet'
        self.allocationByCountry.loc[0, '12'] = self.allocationInputs.loc[0,'2']
        for col in list(map(str,range(11,19))):
            self.allocationByCountry.loc[2, col] = self.columns[int(col)-11]
            self.allocationByCountry.loc[3, col] = self.d[self.allocationInputs.loc[2, str(int(col) -10)]]
            self.allocationByCountry.loc[4, col] =self.allocationInputs.loc[2, str(int(col) -10)]
        
        self.allocationByCountry.loc[5, '1'] = self.count
        self.allocationByCountry.loc[5, '2'] = 26
        k = 1
        ind= 5
        for i in range(100):
            for b in range(26,187):
                self.allocationByCountry.loc[ind, '2'] = b
                for col in list(map(str,range(11,19))):
                    self.allocationByCountry.loc[ind,col] = self.allocationByCountry.loc[ind-1,col]
                ind = ind + 1
            k = k+1
            self.allocationByCountry.loc[ind, '1'] = k
            self.allocationByCountry.loc[ind-1, '1'] = k-1
            self.allocationByCountry.loc[ind-1, ['3','4','5','6','7','8','9','10']] = [0,	0,	0,	0,	0,	'-',  0, '-']
            for col in list(map(str,range(11,19))):
                self.allocationByCountry.loc[ind-1, col] = d2[self.allocationByCountry.loc[3, col]+8*self.allocationByCountry.loc[ind-1,'1']]
              
        for col in list(map(str,range(21,29))):
            self.allocationByCountry.loc[2, col] = self.columns[int(col)-21]
        for col in list(map(str,range(3,11))):
            self.allocationByCountry.loc[4, col] = self.columns[int(col)-3]         
            
    def allocations(self, row):
        for col in list(map(str,range(21,29))):
            try:
                Allo_column = int(self.d[self.allocationByCountry.loc[row,(str(int(col)-10))]])
                Allo_row = self.allocationByCountry.loc[row,'2'] -26
                self.allocationByCountry.loc[row, col] = self.newallocation.loc[Allo_row, Allo_column]
                self.allocationByCountry.loc[row, str(int(col)-18)] = self.allocationByCountry.loc[row, col]
            except:
                pass
       
        
    def cal(self):
        for row in range(5, len(self.allocationByCountry)):
            DDMspecialLinks.allocations(self, row)
        
        
    
    
t = DDMspecialLinks()
t.newallocations()
t.main()
t.cal()

#t.newallocation.to_excel('DDM special links getter\\Allocation TABLE.xlsx', index=False)
t.allocationByCountry.to_excel('DDM special links getter Outputs\\OUTPUTS TABLE.xlsx', index=False, header=False)