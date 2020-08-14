

import pandas as pd
from flatdbconverter import Flatdbconverter, upload
import time

ma_conv = Flatdbconverter("Margin Analysis preparation sheets")

class marginAnalysis:
    def __init__(self):
        self.cols1 = ['Name for Lookup', 'Name in mining model'] + [str(_) for _ in (map(float,range(2016,2031)))]
        self.cols2 = ['Cash FOB','Sustaining Capital','Capital','Full FOB', 'Freight','Moisture','Cash Price', 'Full Price',
            'Cash Cost CFR','Cash + Sustaining Capital CFR', 'Full Cost CFR','ViU Cash Cost CFR','ViU Cash Cost + Sustaining Capital CFR', 'ViU Full Cost CFR']
        self.cols3 = ['Name', '3rd Party Market','Integrated','Forward sold or HT 3rd Party Market','Error Check' ]
        self.cols4 = ['Name','tonnes','3rd Party Market', 'Integrated', 'Forward sold or HT 3rd Party Market'  ]
        self.cols5 = ['Cash FOB','Sustaining Capital','Capital','Cash + Sustain Capital FOB','Full FOB', 'Freight',
            'Cash Cost CFR','Cash + Sustaining Capital CFR', 'Full Cost CFR','ViU Cash Cost CFR', 'ViU Cash Cost + Sustaining Capital CFR',
            'ViU Full Cost CFR','Cash Price', 'Full Price','Cash-Cash Margin','Full-Full Margin','Moisture','Full Supply Tonnes Mt']
        
        
        

        # row mining inputs
        row = pd.read_csv('row_mining_snapshot.csv')
        all_rev = ma_conv.reverse(row, "ROW minind model", ["Collector Cash_Fob_(Us$-Dmt)", "Collector Full_Fob_(Us$-Dmt)", "Collector Cash_Cost_Cfr_(Us$-Dmt)", "Collector Full_Cost_Cfr_(Us$-Dmt)", "Collector Sustaining_Capital_(Us$-Dmt)", "Collector Capital_(Us$-Dmt)", "Collector Freight_(Us$-Dmt)", "Collector Moisture"])
        all_rev = all_rev["ROW minind model"]

        # replaced here

        # self.cashCost           = pd.read_csv('inputs\\Cash Cost CFR table.csv',  encoding='ISO-8859-1')
        self.cashCost           = all_rev["Collector Cash_Cost_Cfr_(Us$-Dmt)"]
        self.cashCost.columns   = self.cols1

        # self.sustCapital        = pd.read_csv('inputs\\Sustaining Capital table.csv',  encoding='ISO-8859-1')
        self.sustCapital         = all_rev["Collector Sustaining_Capital_(Us$-Dmt)"]
        self.sustCapital.columns = self.cols1

        # self.cashFOB            = pd.read_csv('inputs\\Cash FOB table.csv', encoding='ISO-8859-1')
        self.cashFOB            = all_rev["Collector Cash_Fob_(Us$-Dmt)"]
        self.cashFOB.columns    = self.cols1

        # self.capital            = pd.read_csv('inputs\\Capital table.csv', encoding='ISO-8859-1')
        self.capital            = all_rev["Collector Capital_(Us$-Dmt)"]
        self.capital.columns    = self.cols1

        # self.fullFOB            = pd.read_csv('inputs\\Full FOB table.csv', encoding='ISO-8859-1')
        self.fullFOB            = all_rev["Collector Full_Fob_(Us$-Dmt)"]
        self.fullFOB.columns    = self.cols1

        # self.freight            = pd.read_csv('inputs\\Freight table.csv', encoding='ISO-8859-1')
        self.freight            = all_rev["Collector Freight_(Us$-Dmt)"]
        self.freight.columns   = self.cols1

        # self.Moisture           = pd.read_csv('inputs\\Moisture table.csv', encoding='ISO-8859-1')
        self.Moisture           = all_rev["Collector Moisture"]
        self.Moisture.columns   = self.cols1

        self.CashPrice          = pd.read_csv('inputs\\Cash Price table.csv', encoding='ISO-8859-1')
        self.fullPrice          = pd.read_csv('inputs\\Full Price table.csv', encoding='ISO-8859-1')

        # self.fullCostCFR        = pd.read_csv('inputs\\Full Cost CFR table.csv', encoding='ISO-8859-1')
        self.fullCostCFR        = all_rev["Collector Full_Cost_Cfr_(Us$-Dmt)"]
        self.fullCostCFR.columns = self.cols1

        self.viuCashCost        = pd.read_csv('inputs\\ViU Cash Cost CFR table.csv', encoding='ISO-8859-1')
        self.viuCashSustCapital = pd.read_csv('inputs\\ViU Cash Cost + Sustaining Capital CFR table.csv', encoding='ISO-8859-1')
        self.viufullCost        = pd.read_csv('inputs\\ViU Full Cost CFR table.csv', encoding='ISO-8859-1')
        
        
        
        self.leaguetable = pd.read_csv(r'inputs\\league table bldr.csv',  encoding='ISO-8859-1')
        self.leaguetable_wSupply = pd.read_csv(r'inputs\\Weiqiao Self Supply.csv',  encoding='ISO-8859-1')
        #self.fullSupply  = pd.read_csv('inputs\\Full supply template.csv',  encoding='ISO-8859-1')
        self.conditionTable = pd.read_csv(r'inputs\\league bldr condtion table.csv',  encoding='ISO-8859-1')
        self.priceForcastInputs = pd.read_csv(r'inputs\\full supply inputs from Price Forecast Model.csv',  encoding='ISO-8859-1')
        self.thirdPartyInput = pd.read_csv('inputs\\Third party selections etc.csv',  encoding='ISO-8859-1')
        self.Minelisting = pd.read_csv('inputs\\One listing per Mine Only.csv',  encoding='ISO-8859-1')
        self.dataReAssembled = pd.read_csv('inputs\\Reassembled Tonnages of actual and forecast actual supply.csv',encoding='ISO-8859-1')
        
        self.projects = ['AGB2A',
                    'Alufer (Bel Air)',
                    'AMC',
                    'AMR',
                    'Amrun',
                    'Ashapura Guinea',
                    'Aurukun',
                    'Aurum Xinfa-Fiji',
                    'Awaso',
                    'CBG',
                    'Chalco Boffa',
                    'Dabiss N',
                    'Dian Dian',
                    'Discovery Bay',
                    'Dom. Rep.',
                    'Dynamic',
                    'Mokanji',
                    'GAC',
                    'GDM',
                    'Gove',
                    'Gujarat & Maharashtra',
                    'Henan-Chine',
                    'Huntly',
                    'Juruti',
                    'Kuantan & Johor',
                    'Metallica',
                    'Metro BH1',
                    'Metro Blend',
                    'Metro LT',
                    'Mirai',
                    'MRN',
                    'Other',
                    'Rondon do Para',
                    'Rundi',
                    'SMB Dabiss S',
                    'SMB Kaboye',
                    'SMB Malapouya',
                    'SMB Santou-Houda',
                    'Solomon Islands',
                    'SPIC',
                    'Vietnam Others',
                    'Weipa',
                    'Weiqiao Self Supply',
                    'West Kalimantan']
        
        self.country = ['Guinea',
                         'Guinea',
                         'Guinea',
                         'Guinea',
                         'Australia',
                         'Guinea',
                         'Australia',
                         'Fiji',
                         'Ghana',
                         'Guinea',
                         'Guinea',
                         'Guinea',
                         'Guinea',
                         'Jamaica',
                         'Dominican Republic',
                         'Guinea',
                         'Sierra Leone',
                         'Guinea',
                         'Guinea',
                         'Australia',
                         'India',
                         'Guinea',
                         'Australia',
                         'Brazil',
                         'Malaysia',
                         'Australia',
                         'Australia',
                         'Australia',
                         'Australia',
                         'Brazil',
                         'Brazil',
                         'Other',
                         'Brazil',
                         'Guinea',
                         'Guinea',
                         'Guinea',
                         'Guinea',
                         'Guinea',
                         'Solomon Islands',
                         'Guinea',
                         'Vietnam',
                         'Australia',
                         'Guinea',
                         'Indonesia']
        
        self.fullSupply = pd.DataFrame({'Project':self.projects, 'Country':self.country})
        
        self.cashSustCapital = pd.DataFrame(columns=self.cols1)
        self.fullSupplyTonnes = pd.DataFrame(columns=self.leaguetable.columns[:-1])
        self.thirdParty = pd.DataFrame(columns=self.cols3)
        self.thirdPartTables = pd.DataFrame(columns=self.cols4)
        
    def calc_1(self, col):
        self.cashSustCapital[col] = self.cashCost[col]
        if (col != 'Name for Lookup' and col != 'Name in mining model' ):
            self.cashSustCapital[col] = self.cashCost[col] + self.sustCapital[col]
   
    def calc_2(self):   #full supply
        table= pd.concat([self.priceForcastInputs , self.leaguetable_wSupply], ignore_index=True)
        table.iloc[32,0] = 'Amrun'
        table.iloc[33,0] = 'Metro BH1'
        for year in self.cols1[5:]: 
            try:
                self.fullSupply[(year)] = self.func_1(year, table, self.fullSupply['Project'])
            except:
                year = '2019'
                self.fullSupply[(year)] = self.func_1(year, table, self.fullSupply['Project'])
        self.fullSupply.iloc[42,2:] = 0
        #lol we are all in this together (^-^)
        
         
        self.leaguetable.loc[32, 'Project'] ='Amrun'
        self.leaguetable_wSupply.iloc[6,4] = self.leaguetable_wSupply.iloc[6,4] + self.leaguetable.iloc[40, 4]
        self.leaguetable_wSupply.iloc[6,5] = self.leaguetable_wSupply.iloc[6,5] + self.leaguetable.iloc[40, 5]
        self.leaguetable.iloc[41,4:] =0 
        self.leaguetable.iloc[40,4:6]=0
        self.leaguetable.iloc[38,4]=0
        self.leaguetable = pd.concat([self.leaguetable, self.leaguetable_wSupply], ignore_index=True, sort=False)
        condition = self.conditionTable.iloc[0,2]
        if condition == 'Yes':
            self.leaguetable.iloc[32,4:]=self.leaguetable.iloc[32,4:] + self.leaguetable.iloc[24,4:]
            self.leaguetable.iloc[24,4:]=0
            
        elif condition == 'NO':
            pass
        self.leaguetable.iloc[41,4:] = 0
        self.leaguetable.iloc[50, 4] = self.priceForcastInputs.iloc[38,6]
       
        
        fullSuppInp = self.priceForcastInputs.iloc[:,2:-3]
        self.fullSupplyTonnes['Project'] =self.leaguetable['Project']
        self.fullSupplyTonnes['Country'] = self.leaguetable['Country']
        self.fullSupplyTonnes = self.fullSupplyTonnes.iloc[:-8,:]
        for k in range(5, len(self.fullSupplyTonnes.columns)):
            self.fullSupplyTonnes.iloc[:,k] = fullSuppInp.iloc[:,k]
        self.fullSupplyTonnes = pd.concat([self.fullSupplyTonnes, self.leaguetable_wSupply], ignore_index=True)
        
        condition = self.conditionTable.iloc[0,2]
        if condition == 'Yes':
            self.fullSupplyTonnes.iloc[24,5:]=0
            self.fullSupplyTonnes.iloc[32,5:]=self.fullSupplyTonnes.iloc[32,5:] + fullSuppInp.iloc[24,5:]
        elif condition == 'NO':
            pass
        self.fullSupplyTonnes.iloc[41,5:] = 0
        
        
    def calc_3(self):
        self.cal_Moisture = self.cal_x1(self.Moisture, self.leaguetable)
        self.cal_cashCost = self.cal_x1(self.cashCost, self.leaguetable )
        self.cal_sustCapital  = self.cal_x1(self.sustCapital, self.leaguetable)
        self.cal_cashFOB  = self.cal_x1(self.cashFOB, self.leaguetable)
        self.cal_capital  = self.cal_x1(self.capital, self.leaguetable)
        self.cal_fullFOB  = self.cal_x1(self.fullFOB, self.leaguetable)
        self.cal_freight  = self.cal_x1(self.freight, self.leaguetable)
        self.cal_CashPrice  = self.cal_x1(self.CashPrice, self.leaguetable)
        self.cal_fullPrice  = self.cal_x1(self.fullPrice, self.leaguetable)
        self.cal_fullCostCFR  = self.cal_x1(self.fullCostCFR, self.leaguetable)
        self.cal_viuCashCost  = self.cal_x1(self.viuCashCost, self.leaguetable)
        self.cal_viuCashSustCapital  = self.cal_x1(self.viuCashSustCapital, self.leaguetable)
        self.cal_viufullCost  = self.cal_x1(self.viufullCost, self.leaguetable)
        self.cal_cashSustCapital  = self.cal_x1(self.cashSustCapital, self.leaguetable)
        
    
    def calc_4(self): #final name
        self.finalName1 = pd.DataFrame({'Final Name':self.leaguetable.Project, '':''})
        self.finalName1['Final Name'][27] = 'Vietnam'
        self.finalName1['Final Name'][28] = 'SMB Malapouya-Boké'
        self.finalName1['Final Name'][47] = 'SMB Malapouya-Boké'
        
        inpYear = self.conditionTable.iloc[1,2]
        lbdr = self.leaguetable.copy()
        lbdr.rename(columns={'2016':'2016.0','2017':'2017.0','2018':'2018.0','2019':'2019.0'}, inplace =True)
        columns_pos = lbdr.columns.get_loc(str(float(int(inpYear)))) 
        #columns_pos is general
        
        match_table = self.dataReAssembled
        self.finalName1['Forecast Supply tonnes'] = match_table.loc[:,match_table.columns[columns_pos]]
        
        name = self.cols5
        for k in range(len(name)):
           self.finalName1 = self.finaltable(name[k], self.finalName1, columns_pos)
                
    def calc_5(self):
        #cc = self.thirdPartyInput
        #tp = self.thirdParty
        self.thirdParty['Name'] = self.finalName1['Final Name']
        self.thirdParty['3rd Party Market'] = self.thirdPartyInput[self.thirdPartyInput.columns[1]]
        self.thirdParty['Integrated'] = self.thirdPartyInput[self.thirdPartyInput.columns[2]]
        self.thirdParty['Forward sold or HT 3rd Party Market'] = self.thirdPartyInput[self.thirdPartyInput.columns[3]]
        for v in range( len(self.thirdParty)):
            if self.thirdPartyInput.iloc[v,1:].sum() == 0:
                self.thirdParty.loc[v, 'Error Check'] = 'Error'
            elif self.thirdPartyInput.iloc[v,1:].sum() == 1:
                 self.thirdParty.loc[v,'Error Check']= 0
            else:
                raise Exception('Values are either 1 or 0')
        
        #cash FoB's
        self.cashfb = self.thirdPartTables.copy()
        self.cashfb['Name'] = self.finalName1['Final Name']
        if self.conditionTable.iloc[2,2]  == 'Full':
            self.cashfb['tonnes']=self.finalName1['Full Supply Tonnes Mt']
            
        else:
            self.cashfb['tonnes']=self.finalName1['Forecast Supply tonnes']
            
        for k in range(len(self.cashfb)):
            if self.thirdParty.loc[k,'3rd Party Market'] ==1:
                self.cashfb.loc[k,'3rd Party Market'] =self.finalName1.loc[k,'Cash FOB']
            else:
                self.cashfb.loc[k,'3rd Party Market'] =0
                
        for k in range(len(self.cashfb)):
            if self.thirdParty.loc[k,'Integrated'] ==1:
                self.cashfb.loc[k,'Integrated'] =self.finalName1.loc[k,'Cash FOB']
            else:
                self.cashfb.loc[k,'Integrated'] =0
                
        for k in range(len(self.cashfb)):
            if self.thirdParty.loc[k,'Forward sold or HT 3rd Party Market'] ==1:
                self.cashfb.loc[k,'Forward sold or HT 3rd Party Market'] =self.finalName1.loc[k,'Cash FOB']
            else:
                self.cashfb.loc[k,'Forward sold or HT 3rd Party Market'] = 0
        
    def calc_6(self):
        
        self.tp_cashSustCapFOB = self.func_parties('Cash + Sustain Capital FOB', self.finalName1,  self.thirdParty)
        self.tp_fullFOB = self.func_parties('Full FOB', self.finalName1, self.thirdParty)
        self.tp_cashCost = self.func_parties('Cash Cost CFR', self.finalName1, self.thirdParty)
        self.tp_cashSustCap = self.func_parties('Cash + Sustaining Capital CFR', self.finalName1, self.thirdParty)
        self.tp_fullCost = self.func_parties('Full Cost CFR', self.finalName1, self.thirdParty)
        self.tp_viuCashCost = self.func_parties('ViU Cash Cost CFR', self.finalName1,  self.thirdParty)
        self.tp_viuCashSust = self.func_parties('ViU Cash Cost + Sustaining Capital CFR', self.finalName1,  self.thirdParty)
        self.tp_viuFullCost = self.func_parties('ViU Full Cost CFR', self.finalName1,  self.thirdParty)
        self.tp_cashPrice = self.func_parties('Cash Price', self.finalName1,  self.thirdParty)
        self.tp_fullPrice = self.func_parties('Full Price', self.finalName1,  self.thirdParty)
        self.tp_cashMargin = self.func_parties('Cash-Cash Margin', self.finalName1,  self.thirdParty)
        self.tp_fullMargin = self.func_parties('Full-Full Margin', self.finalName1, self.thirdParty)
        self.tp_freight = self.func_parties('Freight', self.finalName1,  self.thirdParty)
        
        
        
    def calc_7(self):
        lt = self.Minelisting.copy()
        lt.columns=lt.iloc[0]
        lt.drop(lt.index[0], inplace=True ,axis=0)
        lt.reset_index(drop=True, inplace=True)
        
        ftable = self.finalName1
        cc =pd.Series(0 ,index = [a for a in range(len(lt))], dtype='float')
        for i in range(len(lt)):
            cc[i] = ftable.loc[ftable['Final Name']== lt.iloc[i,0], 'Forecast Supply tonnes'].sum()
        lt['Forecast Supply tonnes']  = cc  
        #lt.rename(columns={'Forecast Supply tonnes':' '}, inplace=True)
        
        c_name =['Cash FOB','Sustaining Capital','Capital','Cash + Sustain Capital FOB','Full FOB', 'Freight',
                 'Cash Cost CFR','Cash + Sustaining Capital CFR', 'Full Cost CFR','ViU Cash Cost CFR', 'ViU Cash Cost + Sustaining Capital CFR',
                 'ViU Full Cost CFR','Cash Price', 'Full Price','Cash-Cash Margin','Full-Full Margin','Moisture']
       
        for k in range(len(c_name)):
            lt[c_name[k]] = self.func_ftable(lt,c_name[k], ftable)
            
        #lats man
        cc =pd.Series(0 ,index = [a for a in range(len(lt))], dtype='float')
        for i in range(len(lt)):
            cc[i] = ftable.loc[ftable['Final Name']== lt.iloc[i,0], 'Full Supply Tonnes Mt'].sum()
        lt['Full Supply Tonnes Mt']  = cc  
        #lt.rename(columns={'Full Supply Tonnes Mt':'  '}, inplace=True)
        self.oneListing = lt        
        
        self.ranking_cashFOB = self.rankings( self.cashfb)
        self.ranking_cashSustCapFOB = self.rankings( self.tp_cashSustCapFOB)
        self.ranking_fullFOB= self.rankings(self.tp_fullFOB)
        self.ranking_cashSustCap = self.rankings(self.tp_cashSustCap)
        self.ranking_fullCost = self.rankings(self.tp_fullCost)
        self.ranking_viuCashCost = self.rankings(self.tp_viuCashCost)
        self.ranking_viuCashSust = self.rankings(self.tp_viuCashSust)
        self.ranking_viuFullCost = self.rankings(self.tp_viuFullCost)
        self.ranking_cashPrice = self.rankings(self.tp_cashPrice)
        self.ranking_fullPrice = self.rankings( self.tp_fullPrice)
        self.ranking_cashMargin = self.rankings(self.tp_cashMargin)
        self.ranking_fullMargin = self.rankings(self.tp_fullMargin)
        self.ranking_freight = self.rankings( self.tp_freight)
        self.ranking_cashCost = self.rankings( self.tp_cashCost)
            
            
            
   # general functions
        
    def rankings(self, fb):
        lb = pd.Series(0, index=[a for a in range(len(fb))], dtype='float') # sort by actual supply
        for v in range(len(fb)):
            if fb.loc[v,'tonnes'] <= 0:
                lb[v] = 1000000
            else:
                lb[v] = fb.iloc[v,2:].sum()
                
        la =  pd.Series(0, index=[a for a in range(len(fb))], dtype='float') # sort by full supply
        for v in range(len(fb)):
            if fb.loc[v,'tonnes'] <= 0:
                la[v]=1000000
            else:
                if fb.loc[v,'Integrated'] >0:
                    la[v] = 1
                else:
                    if fb.loc[v,'Forward sold or HT 3rd Party Market'] >0:
                        la[v] = 10
                    else:
                        if fb.loc[v,'3rd Party Market'] >0:
                            la[v] = 100
            if la[v] != 1000000:
                la[v] = la[v]* lb[v]
                
        kz =  pd.Series(0, index=[a for a in range(len(fb))], dtype='float') # proxy actual
        row_no = 68
        for v in range(len(fb)):
            kz[v] = ((row_no+v)/1000000) + lb[v]
        
        ky =  pd.Series(0, index=[a for a in range(len(fb))], dtype='float') # proxy full
        row_no = 68
        for v in range(len(fb)):
            ky[v] = ((row_no+v)/1000000) + la[v]
            
        kx = kz.rank() # rank actual
       
        kw = ky.rank() # rank full
       
        outputs = pd.DataFrame({'Rank Full':kw, 'Rank Actual':kx, 'proxy Full': ky,'proxy Actual':kz,
                                'Sort by Full Supply':la,'Sort by Actual Supply':lb} )
        outputs = outputs.join(fb)
        #outputs.to_csv('outputs\\calculations\\sorted --Cash FOB.csv', index=False)
        outputs.drop(outputs.columns[[2,3,4,5]], axis=1, inplace =True)
        #rank table
        rank_tab = pd.DataFrame({'Rank': [_ for _ in range(1,len(fb)+1)]})
        
        if self.conditionTable.iloc[2,2] == 'Full':
            print('Full')
            outputs.rename(columns={'Rank Full':'Rank'}, inplace=True)
            rank_tab = rank_tab.merge(outputs, on='Rank', how='left')
            rank_tab.drop(rank_tab.columns[1], axis=1, inplace =True)
            
                
        elif self.conditionTable.iloc[2,2] =='Successful':
            print('Succesfull')
            outputs.rename(columns={'Rank Actual':'Rank'}, inplace=True)
            rank_tab = rank_tab.merge(outputs, on='Rank', how='left')
            rank_tab.drop(rank_tab.columns[1], axis=1, inplace =True)
            
        return rank_tab
      
        
        
        
        
        
    def func_1(self, year, table, projects):
        cc =pd.Series(0 ,index = [a for a in range(44)], dtype='float')
        for _ in range(44):
            cc[_] = table.loc[table.Project==projects[_], year].sum()
        return cc
    def func_ftable(self, lt, c_name, ftable ):
         cc =pd.Series(0 ,index = [a for a in range(len(lt))], dtype='float')
         for i in range(len(lt)):
             cc[i] = ftable.loc[ftable['Final Name']== lt.iloc[i,0], c_name].max()
         return cc
     
  
    def cal_x1(self, inpname,fs,):
        sam = inpname.copy()
        
        tt = pd.DataFrame({'Project':fs.Project, 'Country':fs.Country })
        sam.drop(sam.columns[1], axis=1, inplace=True)
        sam.rename(columns = {'Name for Lookup': 'Project'}, inplace=True)
        tt = tt.merge(sam, on='Project', how='left')
        tt.iloc[41,2:] = 0
        #the average
        tt.iloc[36,2:] = 0
        noColumns = len(tt.columns)
        for m in range(2,noColumns):
            tt.iloc[36, m] = (tt.iloc[:-8,m].sum())/44.0
        return tt
 
        
    def finaltable(self, name, fn,columns_pos ):
        if name =='Cash + Sustain Capital FOB':
            fn['Cash + Sustain Capital FOB'] = fn['Cash FOB']+fn['Sustaining Capital']
        elif name == 'Cash-Cash Margin':
            fn['Cash-Cash Margin']=  fn['Cash Price'] - fn['Cash Cost CFR'] 
        elif name =='Full-Full Margin':
            fn['Full-Full Margin'] = fn['Full Price']- fn['Full Cost CFR']
        elif name == 'Full Supply Tonnes Mt':
            table = self.fullSupplyTonnes
            fn[name] = table.loc[:,table.columns[columns_pos]]
            
        elif name == 'Full Price':
            table = self.cal_fullPrice
            fn[name] = table.loc[:,table.columns[columns_pos+1]]
        
        elif name == 'ViU Cash Cost CFR':
            table = self.cal_viuCashCost
            fn[name] = table.loc[:,table.columns[columns_pos+1]]
        
        elif name == 'ViU Cash Cost + Sustaining Capital CFR':
            table = self.cal_viuCashSustCapital
            fn[name] = table.loc[:,table.columns[columns_pos+1]]
            
        elif name == 'ViU Full Cost CFR':
            table = self.cal_viufullCost
            fn[name] = table.loc[:,table.columns[columns_pos+1]]
            
        elif name == 'Cash Price' :
            table = self.cal_CashPrice
            fn[name] = table.loc[:,table.columns[columns_pos+1]]
        
            
        elif name == 'Cash + Sustaining Capital CFR':
            table =self.cal_cashSustCapital
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Cash FOB':
            table = self.cal_cashFOB
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Sustaining Capital':
            table = self.cal_sustCapital
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Capital':
            table = self.cal_capital
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Full FOB':
            table = self.cal_fullFOB
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Freight':
            table = self.cal_freight
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Cash Cost CFR':
            table = self.cal_cashCost
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Full Cost CFR':
            table = self.cal_fullCostCFR
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        elif name == 'Moisture':
            table = self.cal_Moisture
            fn[name] = table.loc[:,table.columns[columns_pos]]
        
        else:
            print(name)
        
        return fn
    
    def func_parties(self, name_p, final_table, tp):
        cs = self.thirdPartTables.copy()
        cs['Name'] = self.finalName1['Final Name']
        if self.conditionTable.iloc[2,2]  == 'Full':
            cs['tonnes']=self.finalName1['Full Supply Tonnes Mt']
            
        else:
            cs['tonnes']=self.finalName1['Forecast Supply tonnes']
            
        for k in range(len(cs)):
            if tp.loc[k,'3rd Party Market'] ==1:
                cs.loc[k,'3rd Party Market'] =final_table.loc[k,name_p]
            else:
                cs.loc[k,'3rd Party Market'] =0
                
        for k in range(len(cs)):
            if tp.loc[k,'Integrated'] ==1:
                cs.loc[k,'Integrated'] =final_table.loc[k,name_p]
            else:
                cs.loc[k,'Integrated'] =0
                
        for k in range(len(cs)):
            if tp.loc[k,'Forward sold or HT 3rd Party Market'] ==1:
                cs.loc[k,'Forward sold or HT 3rd Party Market'] =final_table.loc[k,name_p]
            else:
                cs.loc[k,'Forward sold or HT 3rd Party Market'] = 0
        return cs
    
        
        
        
        
        
    def calc_all_1(self):
        for i in (self.cols1):
            marginAnalysis.calc_1(self, i)
        marginAnalysis.calc_2(self)
        marginAnalysis.calc_3(self)
        marginAnalysis.calc_4(self)
        marginAnalysis.calc_5(self)
        marginAnalysis.calc_6(self)
        marginAnalysis.calc_7(self)
        return self.cashSustCapital
            
#Forecast Supply tonnes
#Full Supply Tonnes Mt
w = marginAnalysis()
t = w.calc_all_1()


w.fullSupply.to_csv('outputs\\Full Supply Output table.csv', index=False, encoding='ISO-8859-1')
#w.fullSupplyTonnes.to_csv('outputs\\Full Supply Stream Tonnes.csv', index=False,  encoding='ISO-8859-1')

w.cal_Moisture.to_csv('outputs\\calculations\\Moisture table.csv', index=False,  encoding='ISO-8859-1')
w.cal_cashCost.to_csv('outputs\\calculations\\Cash Cost CFR table.csv', index=False,  encoding='ISO-8859-1')
w.cal_sustCapital.to_csv('outputs\\calculations\\Sustainig Capital table.csv', index=False,  encoding='ISO-8859-1')
w.cal_cashFOB.to_csv('outputs\\calculations\\Cash FOB table.csv', index=False,  encoding='ISO-8859-1')
w.cal_capital .to_csv('outputs\\calculations\\Capital table.csv', index=False,  encoding='ISO-8859-1')
w.cal_fullFOB.to_csv('outputs\\calculations\\Full FOB table.csv', index=False,  encoding='ISO-8859-1')
w.cal_freight.to_csv('outputs\\calculations\\Freight table.csv', index=False,  encoding='ISO-8859-1')
w.cal_CashPrice.to_csv('outputs\\calculations\\Cash Price table.csv', index=False,  encoding='ISO-8859-1')
w.cal_fullPrice.to_csv('outputs\\calculations\\Full Price table.csv', index=False,  encoding='ISO-8859-1')
w.cal_fullCostCFR.to_csv('outputs\\calculations\\Full Cost CFR table.csv', index=False,  encoding='ISO-8859-1')
w.cal_viuCashCost.to_csv('outputs\\calculations\\ViU Cash Cost CFR table.csv', index=False,  encoding='ISO-8859-1')
w.cal_viuCashSustCapital.to_csv('outputs\\calculations\\ViU Cash Cost + Sustaining Capital table.csv', index=False,  encoding='ISO-8859-1')
w.cal_viufullCost.to_csv('outputs\\calculations\\ViU Full Cost CFR table.csv', index=False,  encoding='ISO-8859-1')
w.cal_cashSustCapital.to_csv('outputs\\calculations\\Cash + Sustaining Capital CFR table.csv', index=False,  encoding='ISO-8859-1')


w.finalName1.to_csv('outputs\\Final Name table.csv', index=False,  encoding='ISO-8859-1')
w.oneListing.to_csv('outputs\\Final Name table 2.csv', index=False,  encoding='ISO-8859-1')

w.cashfb.to_csv("outputs\\Cash FOB's table.csv", index=False,  encoding='ISO-8859-1')
w.tp_cashSustCapFOB.to_csv("outputs\\Cash + Sustain Capital FOB's table.csv",index=False,  encoding='ISO-8859-1')
w.tp_fullFOB.to_csv("outputs\\ Full FOB's table.csv", index=False,encoding='ISO-8859-1')
w.tp_cashCost.to_csv("outputs\\Cash Cost CFR's table.csv", index=False,encoding='ISO-8859-1')
w.tp_cashSustCap.to_csv("outputs\\Cash + Sustaining Capital CFR's table.csv", index=False,encoding='ISO-8859-1')
w.tp_fullCost .to_csv("outputs\\Full Cost CFR's table.csv", index=False,encoding='ISO-8859-1')
w.tp_viuCashCost.to_csv("outputs\\ViU Cash Cost CFR's table.csv", index=False,encoding='ISO-8859-1')
w.tp_viuCashSust.to_csv("outputs\\ViU Cash Cost + Sustaining Capital CFR's table.csv", index=False,encoding='ISO-8859-1')
w.tp_viuFullCost.to_csv("outputs\\ViU Full Cost CFR's table.csv", index=False,encoding='ISO-8859-1')
w.tp_cashPrice.to_csv("outputs\\Cash Price's table.csv", index=False,encoding='ISO-8859-1')
w.tp_fullPrice.to_csv("outputs\\Full Price's table.csv", index=False,encoding='ISO-8859-1')
w.tp_cashMargin.to_csv("outputs\\Cash-Cash Margin's table.csv", index=False,encoding='ISO-8859-1')
w.tp_fullMargin.to_csv("outputs\\Full-Full Margin's table.csv", index=False,encoding='ISO-8859-1')
w.tp_freight.to_csv("outputs\\Freight's table.csv", index=False,encoding='ISO-8859-1')

w.ranking_cashFOB.to_csv("outputs\\Cash FOB's ranking table.csv", index=False,  encoding='ISO-8859-1')
w.ranking_cashSustCapFOB.to_csv("outputs\\Cash + Sustain Capital FOB's ranking table.csv",index=False,  encoding='ISO-8859-1')
w.ranking_fullFOB.to_csv("outputs\\ Full FOB's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_cashCost.to_csv("outputs\\Cash Cost CFR's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_cashSustCap.to_csv("outputs\\Cash + Sustaining Capital CFR's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_fullCost .to_csv("outputs\\Full Cost CFR's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_viuCashCost.to_csv("outputs\\ViU Cash Cost CFR's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_viuCashSust.to_csv("outputs\\ViU Cash Cost + Sustaining Capital CFR's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_viuFullCost.to_csv("outputs\\ViU Full Cost CFR's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_cashPrice.to_csv("outputs\\Cash Price's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_fullPrice.to_csv("outputs\\Full Price's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_cashMargin.to_csv("outputs\\Cash-Cash Margin's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_fullMargin.to_csv("outputs\\Full-Full Margin's ranking table.csv", index=False,encoding='ISO-8859-1')
w.ranking_freight.to_csv("outputs\\Freight's ranking table.csv", index=False,encoding='ISO-8859-1')

maflat_time = time.perf_counter()

w_fullSupply = ma_conv.mult_year_single_output(w.fullSupply, "Full Supply")
w_cal_Moisture = ma_conv.mult_year_single_output(w.cal_Moisture, "Moisture")
w_cal_cashCost = ma_conv.mult_year_single_output(w.cal_cashCost, "Cash cost CFR")
w_cal_sustCapital = ma_conv.mult_year_single_output(w.cal_sustCapital, "Sustaining Capital")
w_cal_cashFOB = ma_conv.mult_year_single_output(w.cal_cashFOB, "Cash FOB")
w_cal_capital = ma_conv.mult_year_single_output(w.cal_capital, "Capital")
w_cal_fullFOB = ma_conv.mult_year_single_output(w.cal_fullFOB, "Full FOB")
w_cal_freight = ma_conv.mult_year_single_output(w.cal_freight, "Freight")
w_cal_CashPrice = ma_conv.mult_year_single_output(w.cal_CashPrice, "Cash price")
w_cal_fullPrice = ma_conv.mult_year_single_output(w.cal_fullPrice, "Full Price")
w_cal_fullCostCFR = ma_conv.mult_year_single_output(w.cal_fullCostCFR, "Full Cost CFR")
w_cal_viuCashCost = ma_conv.mult_year_single_output(w.cal_viuCashCost, "Viu cash cost")
w_cal_viuCashSustCapital = ma_conv.mult_year_single_output(w.cal_viuCashSustCapital, "Viu Cash Sustaining Capital")
w_cal_viufullCost = ma_conv.mult_year_single_output(w.cal_viufullCost, "Viu full cost")
w_cal_cashSustCapital = ma_conv.mult_year_single_output(w.cal_cashSustCapital, "Cash sustaining Capital")
w_finalName1 = ma_conv.single_year_mult_out(w.finalName1, "Final Name")
w_oneListing = ma_conv.single_year_mult_out(w.oneListing, "Final Name 2")
w_cashfb = ma_conv.single_year_mult_out(w.cashfb, "Cash FOB's")
w_tp_cashSustCapFOB = ma_conv.single_year_mult_out(w.tp_cashSustCapFOB, "Cash + sustaining capital FOB")
w_tp_fullFOB = ma_conv.single_year_mult_out(w.tp_fullFOB, "Full FOB")
w_tp_cashCost = ma_conv.single_year_mult_out(w.tp_cashCost, "Cash Cost CFR's")
w_tp_cashSustCap = ma_conv.single_year_mult_out(w.tp_cashSustCap, "Cash sustaining capital")
w_tp_fullCost = ma_conv.single_year_mult_out(w.tp_fullCost, "Full cost CFR's")
w_tp_viuCashCost = ma_conv.single_year_mult_out(w.tp_viuCashCost, "ViU Cash Cost CFR's")
w_tp_viuCashSust = ma_conv.single_year_mult_out(w.tp_viuCashSust, "ViU Cash Cost + Sustaining Capital CFR's")
w_tp_viuFullCost = ma_conv.single_year_mult_out(w.tp_viuFullCost, "ViU Full Cost CFR's")
w_tp_cashPrice = ma_conv.single_year_mult_out(w.tp_cashPrice, "Cash Price's")
w_tp_fullPrice = ma_conv.single_year_mult_out(w.tp_fullPrice, "Full Price's")
w_tp_cashMargin = ma_conv.single_year_mult_out(w.tp_cashMargin, "Cash-Cash Margin's")
w_tp_fullMargin = ma_conv.single_year_mult_out(w.tp_fullMargin, "Full Margin's")
w_tp_freight = ma_conv.single_year_mult_out(w.tp_freight, "Freight's")
w_ranking_cashFOB = ma_conv.single_year_mult_out(w.ranking_cashFOB, "Cash FOB ranking")
w_ranking_cashSustCapFOB = ma_conv.single_year_mult_out(w.ranking_cashSustCapFOB, "Cash sustain Capacity FOB ranking")
w_ranking_fullFOB = ma_conv.single_year_mult_out(w.ranking_fullFOB, "Full FOB ranking")
w_ranking_cashCost = ma_conv.single_year_mult_out(w.ranking_cashCost, "Cash cost ranking")
w_ranking_cashSustCap = ma_conv.single_year_mult_out(w.ranking_cashSustCap, "Cash sustain capacity ranking")
w_ranking_fullCost = ma_conv.single_year_mult_out(w.ranking_fullCost, "Full cost ranking")
w_ranking_viuCashCost = ma_conv.single_year_mult_out(w.ranking_viuCashCost, "ViU cash cost ranking")
w_ranking_viuCashSust = ma_conv.single_year_mult_out(w.ranking_viuCashSust, "ViU cash cost ranking")
w_ranking_viuFullCost = ma_conv.single_year_mult_out(w.ranking_viuFullCost, "ViU full cost ranking")
w_ranking_cashPrice = ma_conv.single_year_mult_out(w.ranking_cashPrice, "Cash price ranking")
w_ranking_fullPrice = ma_conv.single_year_mult_out(w.ranking_fullPrice, "Full price ranking")
w_ranking_cashMargin = ma_conv.single_year_mult_out(w.ranking_cashMargin, "Cash margin ranking")
w_ranking_fullMargin = ma_conv.single_year_mult_out(w.ranking_fullMargin, "Full margin ranking")
w_ranking_freight = ma_conv.single_year_mult_out(w.ranking_freight, "Freight ranking")

snapshot_output_data = pd.DataFrame(columns=ma_conv.out_col)
db_list = [
    snapshot_output_data,
    w_fullSupply,
    w_cal_Moisture,
    w_cal_cashCost,
    w_cal_sustCapital,
    w_cal_cashFOB,
    w_cal_capital,
    w_cal_fullFOB,
    w_cal_freight,
    w_cal_CashPrice,
    w_cal_fullPrice,
    w_cal_fullCostCFR,
    w_cal_viuCashCost,
    w_cal_viuCashSustCapital,
    w_cal_viufullCost,
    w_cal_cashSustCapital,
    w_finalName1,
    w_oneListing,
    w_cashfb,
    w_tp_cashSustCapFOB,
    w_tp_fullFOB,
    w_tp_cashCost,
    w_tp_cashSustCap,
    w_tp_fullCost,
    w_tp_viuCashCost,
    w_tp_viuCashSust,
    w_tp_viuFullCost,
    w_tp_cashPrice,
    w_tp_fullPrice,
    w_tp_cashMargin,
    w_tp_fullMargin,
    w_tp_freight,
    w_ranking_cashFOB,
    w_ranking_cashSustCapFOB,
    w_ranking_fullFOB,
    w_ranking_cashCost,
    w_ranking_cashSustCap,
    w_ranking_fullCost,
    w_ranking_viuCashCost,
    w_ranking_viuCashSust,
    w_ranking_viuFullCost,
    w_ranking_cashPrice,
    w_ranking_fullPrice,
    w_ranking_cashMargin,
    w_ranking_fullMargin,
    w_ranking_freight,
]

snapshot_output_data = pd.concat(db_list, ignore_index=True)
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)

print("Time taken to convet to flat db: {0} ".format(time.perf_counter() - maflat_time))


'''

print('uploading to output db')
snapshot_output_data = snapshot_output_data

upload(snapshot_output_data)

'''


# print(db_list[45])