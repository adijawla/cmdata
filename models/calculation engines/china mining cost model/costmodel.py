import pandas as pd
import warnings
import extension as et
from outputdb import uploadtodb
from flatdb.flatdbconverter import Flatdbconverter

db_conv = Flatdbconverter("China mining cost model")

warnings.filterwarnings("ignore")
ids = ['county','expence','mine_status','Province','mine','Year']

inputs = ['costinputs','lookup','rsdatabase','taxtrance']
idname = ['county_id','expence_id','status_id','province_id','mine_id','year_id']
x = et.inputprocess(ids,inputs,idname)
print(x.input_db)
x.mergeall()
x.pivot('lookup','year','lookup',['province','expence'])
#[dbo].[rsdatabasemine]
#renaming
rs_renames = {'province':'Province','county':'County','mine':'Mine','strippingratioopenpit':'StrippingRatioOpenPit'}
ct_renames = {'diesel_usage':'Diesel Usage','open_pit_washing_cost_automation_labour_productivity_low':'open pit Washing Cost automation & labour productivity low','open_pit_washing_cost_automation_labour_productivity_medium':'open pit Washing Cost automation & labour productivity medium',
              'open_pit_washing_cost_automation_labour_productivity_high':'open pit Washing Cost automation & labour productivity high',
              'water_consumption':'Water Consumption',
              'resouce_tax':'Resouce tax',
              'other_fixed':'Other Fixed'}
tx_renames = {'to_refinery':'To refinery','in_mine':'In Mine','royalty':'Royalty',
              'other':'Other','county':'County'}
lp_renames = {'county':'County'}
######
#x.rename('rsdatabase',rs_renames)
x.rename('costinputs',ct_renames)
x.rename('taxtrance',tx_renames)
x.rename('rsdatabase',rs_renames)
#x.rename('lookup',lp_renames)
x.export()

rsdatabase = x.input_db[2]
costinputs = x.input_db[0]


taxtrans = x.input_db[3]
fxrate = 6.8
h = taxtrans["To refinery"][0]
m = taxtrans["In Mine"][0]
costinputs.to_csv("costinputs_copy.csv")
for i in range(13):
    v = taxtrans["Royalty"][i]+taxtrans["Other"][i]
    taxtrans.at[i,"Total"] = v
    y = taxtrans["To refinery"][i]*m/h
    taxtrans.at[i,"In Mine"] = y
taxtrans.at[0,"In Mine"] = m
empty = rsdatabase["empty"].tolist()
print(list(rsdatabase.columns))
data = {"province":rsdatabase['Province'].tolist(),
        "County":rsdatabase['County'].tolist(),
        "Mine":rsdatabase['Mine'].tolist(),
        "Status":rsdatabase["mine_status"].tolist(),
        "Tonnes Produced(kt)":rsdatabase["tonnesproduced"].tolist(),
        "Grade (%Al2O3)":rsdatabase["gradeal2o3"].tolist(),
        "A/S":rsdatabase["asa"].tolist(),
        "Mining Scale":rsdatabase["miningscale"].tolist(),
        "Mine Autonomous":rsdatabase["dressingautolevel"].tolist(),
        "%OpenPit":rsdatabase["minetypeopenpit"].tolist(),
        "%Underground":rsdatabase["minetypeunderground"].tolist(),
        "Depth Open Pit":rsdatabase["depthopenpit"].tolist(),
        "Depth Underground":rsdatabase["depthunderground"].tolist(),
        "Stripping ratio Open Pit":rsdatabase['StrippingRatioOpenPit'].tolist(),
        "Stripping ratio Underground":rsdatabase["strippingratiounderground"].tolist(),
        "Dressing Automation Level":rsdatabase["dressingautolevel"].tolist(),
        "Distance Open Pit":rsdatabase["distanceopenpit"].tolist(),
        "Distance Underground":rsdatabase["distanceunderground"].tolist(),
        "Local Tax and fees":rsdatabase["localtaxandfees"].tolist(),
        "Distance(to Refinery)":rsdatabase["distance"].tolist(),
        "Dressing Ratio":rsdatabase["dressingratio"].tolist(),
        #input data field ends here
        "Mine Automation level":rsdatabase["mineautolevel"].tolist() ,
        "Open Pit":rsdatabase["depthopenpit"].tolist(),
        "Underground":rsdatabase["depthunderground"].tolist(),
        "Dressing automation level":rsdatabase["dressingautolevel"].tolist(),
        "Production":rsdatabase["tonnesproduced"].tolist(),
        "Mine Type open":rsdatabase["minetypeopenpit"].tolist(),
        "Mine Type underground":rsdatabase["minetypeunderground"].tolist(),
        "Stripping ratio open pit":rsdatabase['StrippingRatioOpenPit'].tolist(),
        "Stripping ratio underground":rsdatabase["strippingratiounderground"].tolist(),
        "total tonnes mined open pit":empty,
        "total tonnes mined underground":empty,
        #starts here
        "open pit mining cost electricity consumption":empty,
        "open pit mining cost electricity price":empty,
        "open pit mining cost electricity cost1":empty,
        "open pit mining cost electricity cost2":empty,
        "open pit mining cost diesel usage":empty,
        "open pit mining cost diesel price":empty,
        "open pit mining cost diesel cost1":empty,
        "open pit mining cost diesel cost2":empty,
        "open pit mining cost labour productivity":empty,
        "open pit mining cost labour rate":empty,
        "open pit mining cost labour cost1":empty,
        "open pit mining cost labour cost2":empty,
        "open pit mining cost distance":rsdatabase["distanceopenpit"].tolist(),
        "open pit mining cost unit price":empty,
        "open pit mining cost freight cost1":empty,
        "open pit mining cost freight cost2":empty,
        "open pit mining cost total mining cost":empty,
        "open pit mining cost stripping ratio":rsdatabase['StrippingRatioOpenPit'].tolist(),
        "open pit mining cost ":empty,
        "open pit Washing Cost dressing ratio":rsdatabase["dressingratio"].tolist(),
        "open pit Washing Cost automation & electricity usage low":empty,
        "open pit Washing Cost automation & electricity usage medium":empty,
        "open pit Washing Cost automation & electricity usage high":empty,
        "open pit Washing Cost electricity consumption":empty,
        "open pit Washing Cost electricity price":empty,
        "open pit Washing Cost electricity cost":empty,
        "open pit Washing Cost diesel usage":[ costinputs.loc[costinputs.province==i]["Diesel Usage"].sum() for i in rsdatabase['Province'] ],
        "open pit Washing Cost diesel price":empty,
        "open pit Washing Cost diesel cost":empty,
        "open pit Washing Cost automation & labour productivity low":[ costinputs.loc[costinputs.province==i]["open pit Washing Cost automation & labour productivity low"].sum() for i in rsdatabase['Province'] ],
        "open pit Washing Cost automation & labour productivity medium":[ costinputs.loc[costinputs.province==i]["open pit Washing Cost automation & labour productivity medium"].sum() for i in rsdatabase['Province'] ],
        "open pit Washing Cost automation & labour productivity high":[ costinputs.loc[costinputs.province==i]["open pit Washing Cost automation & labour productivity high"].sum() for i in rsdatabase['Province'] ] ,
        "open pit Washing Cost labour productivity":empty,
        "open pit Washing Cost labour rate":empty,
        "open pit Washing Cost labour cost":empty,
        "open pit Washing Cost material usage":empty,
        "open pit Washing Cost material price":empty,
        "open pit Washing Cost material cost":empty,
        "open pit Washing Cost water consumption":[ costinputs.loc[costinputs.province==i]["Water Consumption"].sum() for i in rsdatabase['Province'] ],
        "open pit Washing Cost water price":empty,
        "open pit Washing Cost water cost":empty,
        "open pit Washing Cost total dressing cost1":empty,
        "open pit Washing Cost total dressing cost2":empty,

        

        "open pit Capital & Equipment Charges mine establishment":empty,
        "open pit Capital & Equipment Charges mine capital cost":empty,
        "open pit Capital & Equipment Charges mine equipment hire1":empty,
        "open pit Capital & Equipment Charges % of equipment maintenance cost":empty,
        "open pit Capital & Equipment Charges % of equipment sustaining cost":empty,
        "open pit Capital & Equipment Charges mine equipment ownership":empty,
        "open pit Capital & Equipment Charges mine equipment cost":empty,
        "open pit Capital & Equipment Charges mine equipment hire2":empty,
        "open pit Capital & Equipment Charges maintenance & equipment charge":empty,
        "open pit Capital & Equipment Charges sustaining capital charge":empty,

        

        #ends here
        "underground mining cost automation & electricity usage low":empty,
        "underground mining cost automation & electricity usage medium":empty,
        "underground mining cost automation & electricity usage high":empty,
        "underground mining cost size factor":empty,
        "underground mining cost depth factor":empty,
        "underground mining cost electricity consumption":empty,
        "underground mining cost electricity price":empty,
        "underground mining cost electricity cost1":empty,
        "underground mining cost electricity cost2":empty,
        "underground mining cost automation & labour productivity low":empty,
        "underground mining cost automation & labour productivity medium":empty,
        "underground mining cost automation & labour productivity high":empty,
        "underground mining cost automation depth factor":empty,
        "underground mining cost automation factor":empty,
        "underground mining cost diesel usage":empty,
        "underground mining cost diesel price":empty,
        "underground mining cost diesel cost1":empty,
        "underground mining cost diesel cost2":empty,
        "underground mining cost labour productivity":empty,
        "underground mining cost labour rate":empty,
        "underground mining cost labour cost1":empty,
        "underground mining cost labour cost2":empty,
        "underground mining cost distance":rsdatabase["distanceunderground"].tolist(),
        "underground mining cost unit price":empty,
        "underground mining cost freight cost1":empty,
        "underground mining cost freight cost2":empty,
        "underground explosives":empty,
        "underground mining cost total mining cost":empty,
        "underground mining cost stripping ratio":rsdatabase["strippingratiounderground"].tolist(),
        "underground mining cost ":empty,
        "underground Washing Cost dressing ratio":rsdatabase["dressingratio"].tolist(),
        "underground Washing Cost automation & electricity usage low":empty,
        "underground Washing Cost automation & electricity usage medium":empty,
        "underground Washing Cost automation & electricity usage high":empty,
        "underground Washing Cost electricity consumption":empty,
        "underground Washing Cost electricity price":empty,
        "underground Washing Cost electricity cost":empty,
        "underground Washing Cost diesel usage":[ costinputs.loc[costinputs.province==i]["Diesel Usage"].sum() for i in rsdatabase['Province'] ],
        "underground Washing Cost diesel price":empty,
        "underground Washing Cost diesel cost":empty,
        "underground Washing Cost automation & labour productivity low":empty,
        "underground Washing Cost automation & labour productivity medium":empty,
        "underground Washing Cost automation & labour productivity high":empty,
        "underground Washing Cost labour productivity":empty,
        "underground Washing Cost labour rate":empty,
        "underground Washing Cost labour cost":empty,
        "underground Washing Cost material usage":empty,
        "underground Washing Cost material price":empty,
        "underground Washing Cost material cost":empty,
        "underground Washing Cost water consumption":[ costinputs.loc[costinputs.province==i]["Water Consumption"].sum() for i in rsdatabase['Province'] ],
        "underground Washing Cost water price":empty,
        "underground Washing Cost water cost":empty,
        "underground Washing Cost total dressing cost1":empty,
        "underground Washing Cost total dressing cost2":empty,

        

        "underground Capital & Equipment Charges mine establishment":empty,
        "underground Capital & Equipment Charges mine capital cost":empty,
        "underground Capital & Equipment Charges mine equipment hire1":empty,
        "underground Capital & Equipment Charges % of equipment maintenance cost":empty,
        "underground Capital & Equipment Charges % of equipment sustaining cost":empty,
        "underground Capital & Equipment Charges mine equipment ownership":empty,
        "underground Capital & Equipment Charges mine equipment cost":empty,
        "underground Capital & Equipment Charges mine equipment hire2":empty,
        "underground Capital & Equipment Charges maintenance & equipment charge":empty,
        "underground Capital & Equipment Charges sustaining capital charge":empty,

        

        "other tax and fees":rsdatabase["localtaxandfees"].tolist(),
        "resource tax":[ costinputs.loc[costinputs.province==i]["Resouce tax"].sum() for i in rsdatabase['Province'] ],
        "total tax and fees":empty,
        "other proportional":empty,
        "other fixed":[ costinputs.loc[costinputs.province==i]["Other Fixed"].sum() for i in rsdatabase['Province'] ] ,
        "other cost":empty,
        "summary labour":empty,
        "summary energy":empty,
        "summary tax and fees":empty,
        "summary water":empty,
        "summary explosives":empty,
        "summary other":empty,
        "fsumlabour":empty,
        "fsumenergy":empty,
        "fsumwater":empty,
        "fsumtaxandfees":empty,
        "fsumexplosives":empty,
        "fsumother":empty,
        
}


inputdatadb = pd.DataFrame(data)
#inputdatadb = inputdatadb.transpose()
for i in inputdatadb.columns:
    try:
        inputdatadb[i] = inputdatadb[i].astype(float)
    except:
        pass
        

class CostModel():
    def __init__(self,db,taxtrans):
        self.db = db
        self.costinputs = x.input_db[0]
        self.lookup = x.input_db[1]
        self.rsdatabase = x.input_db[2]
        self.taxtrans = taxtrans
        #pd.read_sql("select * from snapshot_output_data",cnxn)
    def ttmop(self,index):
        d = [
        self.db.loc[index,"Production"],
            self.db.loc[index,"Mine Type open"],
            self.db.loc[index,"Stripping ratio open pit"],
        ]
        #print(type(d[0]),type(d[1]),type(d[2]))
        value = d[0]*d[1]*(1+d[2])
            
        self.db.at[index,"total tonnes mined open pit"] = value
                
    def ttmug(self,index):
        
        d = [
                self.db.loc[index,"Production"],
                self.db.loc[index,"Mine Type underground"],
                self.db.loc[index,"Stripping ratio underground"],
        ]
        value = d[0]*d[1]*(1+d[2] )
        self.db.at[index,"total tonnes mined underground"] = value
        
    def elcons(self,index):
        d = [1.5,
                 -0.129,
                 2.7,
                 self.db.loc[index,"total tonnes mined open pit"]]
            
        value = d[0]*pow(d[3],d[1])/d[2] if d[3] > 0 else 0
        self.db.at[index,"open pit mining cost electricity consumption"] = value
            
    def elprice(self,index):
        value = self.lookup.loc[self.lookup.province==self.db["province"][index]][self.lookup.expence=="electicity"]["2018"].sum()/1.17
            
        self.db.at[index,"open pit mining cost electricity price"] = value
        self.db.at[index,"open pit Washing Cost electricity price"] = value
        self.db.at[index,"underground mining cost electricity price"] = value
        self.db.at[index,"underground Washing Cost electricity price"] = value
        
    
    def elcost1(self,index):
        d = [
            self.db.loc[index,"open pit mining cost electricity consumption"],
            self.db.loc[index,"open pit mining cost electricity price"],
                ]
        value = d[0]*d[1]
        self.db.at[index,"open pit mining cost electricity cost1"] = value
        
    def elcost2(self,index):
        
        d = [
            self.db.loc[index,"Stripping ratio open pit"],
            self.db.loc[index,"total tonnes mined open pit"],
            self.db.loc[index,"open pit mining cost electricity cost1"],
                ]
        value = d[2]*(1+d[0]) if d[1] > 0 else 0
        self.db.at[index,"open pit mining cost electricity cost2"] = value
        
    
    def unitprice(self,index):
        value = self.taxtrans.loc[self.taxtrans.province==self.db["province"][index]]["In Mine"].sum()
        self.db.at[index,"open pit mining cost unit price"] = value
        self.db.at[index,"underground mining cost unit price"] = value
        
    def dieselusg(self,index):
        d = [
                0.005,
                1,
                -0.129,
                2.7,
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"Open Pit"]
                ]
        value = 0 if d[4] <= 0 else (d[0]*d[5]+d[1]+d[1]*pow(d[4],d[2]))/2.7
        self.db.at[index,"open pit mining cost diesel usage"] = value

    def dieselprice(self,index):
        value = self.lookup.loc[self.lookup.province==self.db["province"][index]][self.lookup.expence=="dissel"]["2018"].sum()/1.17/1000
        self.db.at[index,"open pit mining cost diesel price"] = value
        self.db.at[index,"open pit Washing Cost diesel price"] = value
        self.db.at[index,"underground mining cost diesel price"] = value
        self.db.at[index,"underground Washing Cost diesel price"] = value
                
        
    def dieselcost1(self,index):
        
            
            d = [
                self.db.loc[index,"open pit mining cost diesel usage"],
                self.db.loc[index,"open pit mining cost diesel price"],
                ]
            value = d[0]*d[1]
            self.db.at[index,"open pit mining cost diesel cost1"] = value
        
    def dieselcost2(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio open pit"],
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"open pit mining cost diesel cost1"],
                ]
            value = d[2]*(1+d[0]) if d[1] > 0 else 0
            
            self.db.at[index,"open pit mining cost diesel cost2"] = value
        
    def labpro(self,index):
        
            
            d = [
                0.0022,
                0.44,
                2.7,
                self.db.loc[index,"Open Pit"]
                ]
            value = (d[0]*d[3]+d[1])/d[2]
            self.db.at[index,"open pit mining cost labour productivity"] = value
        
    def labrate(self,index):
        
            value = self.lookup.loc[self.lookup.province==self.db["province"][index]][self.lookup.expence=="labor"]["2018"].sum()/300/8
            self.db.at[index,"open pit mining cost labour rate"] = value
            self.db.at[index,"open pit Washing Cost labour rate"] = value
            self.db.at[index,"underground mining cost labour rate"] = value
            self.db.at[index,"underground Washing Cost labour rate"] = value
        
    
    
    def labourcost1(self,index):
        
            
            d = [
                self.db.loc[index,"open pit mining cost labour productivity"],
                self.db.loc[index,"open pit mining cost labour rate"],
                ]
            value = d[0]*d[1]
            self.db.at[index,"open pit mining cost labour cost1"] = value
        
    def labourcost2(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio open pit"],
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"open pit mining cost labour cost1"],
                ]
            value = d[2]*(1+d[0]) if d[1] > 0 else 0
            self.db.at[index,"open pit mining cost labour cost2"] = value
        
    def freightcost1(self,index):
        
            
            d = [
                self.db.loc[index,"open pit mining cost distance"],
                self.db.loc[index,"open pit mining cost unit price"],
                ]
            value = d[0]*d[1]
            self.db.at[index,"open pit mining cost freight cost1"] = value
        
    def freightcost2(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio open pit"],
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"open pit mining cost freight cost1"]
                ]
            value = d[2]*(d[0]+1) if d[1] > 0 else 0
            self.db.at[index,"open pit mining cost freight cost2"] = value
        
    def totalminingcost(self,index):
        
            
            d = [
                self.db.loc[index,"open pit mining cost electricity cost1"],
                self.db.loc[index,"open pit mining cost diesel cost1"],
                self.db.loc[index,"open pit mining cost labour cost1"],
                0,#self.db.loc[index,"open pit mining cost unit price"]
                self.db.loc[index,"open pit mining cost freight cost1"]
                ]
            value = d[0]+d[1]+d[2]+d[3]+d[4]
            self.db.at[index,"open pit mining cost total mining cost"] = value
        
    
    def strippingratio(self,index):
        
            
            value = rsdatabase["StrippingRatioOpenPit"].tolist()[index]
            self.db.at[index,"open pit mining cost stripping ratio"] = value
        
    
    def openpitminingcost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"open pit mining cost total mining cost"],
                self.db.loc[index,"open pit mining cost stripping ratio"]
                ]
            value = d[1]*(d[2]+1) if d[0] > 0 else 0
            self.db.at[index,"open pit mining cost "] = value
        

    def autolow(self,index):
        
            
            d = [
                8,
                -0.07,
                self.db.loc[index,"total tonnes mined open pit"]
                ]
            value = 0 if d[2] <= 0 else d[0]*pow(d[2],-0.07)
            self.db.at[index,"open pit Washing Cost automation & electricity usage low"] = value
        
    def automed(self,index):
        
            
            d = [
                9,
                -0.07,
                self.db.loc[index,"total tonnes mined open pit"]
                ]
            value = 0 if d[2] <= 0 else d[0]*pow(d[2],-0.07)
            self.db.at[index,"open pit Washing Cost automation & electricity usage medium"] = value
        
    def autohigh(self,index):
        
            
            d = [
                10,
                -0.07,
                self.db.loc[index,"total tonnes mined open pit"]
                ]
            value = 0 if d[2] <= 0 else d[0]*pow(d[2],-0.07)
            self.db.at[index,"open pit Washing Cost automation & electricity usage high"] = value
        
    def washingelecons(self,index):
        
            
            d = [
                self.db.loc[index,"Dressing automation level"],
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"open pit Washing Cost automation & electricity usage low"],
                self.db.loc[index,"open pit Washing Cost automation & electricity usage medium"],
                self.db.loc[index,"open pit Washing Cost automation & electricity usage high"],
                ]
            value =  0 if d[1] <= 0 else d[2] if d[0] == 1 else d[3] if d[0] == 2 else d[4]
            self.db.at[index,"open pit Washing Cost electricity consumption"] = value
        
    def washingelecost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"open pit Washing Cost electricity consumption"],
                self.db.loc[index,"open pit Washing Cost electricity price"],
                ]
            value =  d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"open pit Washing Cost electricity cost"] = value
        
    def washingdieselcost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"open pit Washing Cost diesel usage"],
                self.db.loc[index,"open pit Washing Cost diesel price"],
                ]
            value =  d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"open pit Washing Cost diesel cost"] = value
        
    def washinglabourproductivity(self,index):
        
            
            d = [
                self.db.loc[index,"Dressing automation level"],
                self.db.loc[index,"open pit Washing Cost automation & labour productivity low"],
                self.db.loc[index,"open pit Washing Cost automation & labour productivity medium"],
                self.db.loc[index,"open pit Washing Cost automation & labour productivity high"],
                ]
            value = d[1] if d[0] == 1 else d[2] if d[0] == 2 else d[3] if d[0] == 3 else 0
            self.db.at[index,"open pit Washing Cost labour productivity"] = value
        
    def labourcost(self,index):
        
            

            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"open pit Washing Cost labour productivity"],
                self.db.loc[index,"open pit Washing Cost labour rate"],
                ]
            value = d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"open pit Washing Cost labour cost"] = value
        
    def materialprice(self,index):
        
            value = self.lookup.loc[self.lookup.province==self.db["province"][index]][self.lookup.expence=="water"]["2018"].sum()
            self.db.at[index,"open pit Washing Cost material price"] = value
            self.db.at[index,"open pit Washing Cost water price"] = value
            self.db.at[index,"underground Washing Cost water price"] = value
            self.db.at[index,"underground Washing Cost material price"] = 0
        
            
    def materialcost(self,index):
        
            
            d = [
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"open pit Washing Cost material price"],
                self.db.loc[index,"open pit Washing Cost material usage"],
                ]
            value = d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"open pit Washing Cost material cost"] = value
        
    
    def watercost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"open pit Washing Cost water consumption"],
                self.db.loc[index,"open pit Washing Cost water price"],
    
                ]
            value = d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"open pit Washing Cost water cost"] = value
        
    def totaldressingcost1(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"open pit Washing Cost electricity cost"],
                self.db.loc[index,"open pit Washing Cost diesel cost"],
                self.db.loc[index,"open pit Washing Cost labour cost"],
                self.db.loc[index,"open pit Washing Cost material cost"],
                self.db.loc[index,"open pit Washing Cost water cost"],
                ]
            value = d[2]+d[3]+d[4]+d[5]+d[6] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"open pit Washing Cost total dressing cost1"] = value
        

    def totaldressingcost2(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"open pit Washing Cost total dressing cost1"],
                ]
            value = d[2]*d[1] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"open pit Washing Cost total dressing cost2"] = value
        
    def totalequipcost(self,index):
        
            
            d = [
            self.db.loc[index,"Stripping ratio open pit"],
            self.db.loc[index,"open pit Capital & Equipment Charges mine capital cost"]
            ]
            value = d[1]*(1+d[0])
            self.db.at[index,"open pit Capital & Equipment Charges mine equipment cost"] = value
        
    def totalequiphire(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio Open Pit"],
                self.db.loc[index,"open pit Capital & Equipment Charges mine equipment hire1"]
                ]
            value = d[1]*(1+d[0])
            self.db.at[index,"open pit Capital & Equipment Charges mine equipment hire2"] = value
        
    def maintancecost(self,index):
        
            
            d = [
                self.db.loc[index,"open pit Capital & Equipment Charges % of equipment maintenance cost"],
                self.db.loc[index,"open pit Capital & Equipment Charges mine equipment ownership"],
                self.db.loc[index,"open pit Capital & Equipment Charges mine equipment cost"],
                self.db.loc[index,"open pit Capital & Equipment Charges mine equipment hire2"]
                ]
            value = d[2]*d[0]*d[1]+d[3]*(1-d[1])
            self.db.at[index,"open pit Capital & Equipment Charges maintenance & equipment charge"] = value
        
    def capitalcharge(self,index):
        
            
            d = [
                self.db.loc[index,"open pit Capital & Equipment Charges mine establishment"],
                self.db.loc[index,"open pit Capital & Equipment Charges % of equipment sustaining cost"],
                self.db.loc[index,"open pit Capital & Equipment Charges mine equipment ownership"],
                self.db.loc[index,"open pit Capital & Equipment Charges mine equipment cost"]
                ]
            value = d[3]*d[1]*d[2]+d[0]*d[1]
            self.db.at[index,"open pit Capital & Equipment Charges sustaining capital charge"] = value
        

   #underground starts here

    def ulow(self,index):
        
            
            d = [3.5,
            self.db.loc[index,"total tonnes mined underground"],
             -0.129,
             24
                 ]
            value = 0 if d[1]<=0 else  d[0]*d[3]*pow(d[1],d[2])
            self.db.at[index,"underground mining cost automation & electricity usage low"] = value
        
    def umed(self,index):
        #
            d = [3.5,
            self.db["total tonnes mined underground"][index],
             -0.14,
             45
                 ]
            value = 0 if d[1]<=0 else  d[0]*d[3]*pow(d[1],d[2])
            self.db.at[index,"underground mining cost automation & electricity usage medium"] = value
        #except Exception as e:
            #print(e)
            #self.db.at[index,"underground mining cost automation & electricity usage medium"] = 0
    def uhigh(self,index):
        
            
            d = [3.5,
            self.db.loc[index,"total tonnes mined underground"],
             -0.17,
             75
                 ]
            value = 0 if d[1]<=0 else  d[0]*d[3]*pow(d[1],d[2])
            self.db.at[index,"underground mining cost automation & electricity usage high"] = value
        

    

    def usizefactor(self,index):
        
            
            d = [self.db.loc[index,"Mine Automation level"],
                self.db.loc[index,"underground mining cost automation & electricity usage low"],
                 self.db.loc[index,"underground mining cost automation & electricity usage medium"],
                 self.db.loc[index,"underground mining cost automation & electricity usage high"],
                 ]
            value = d[1] if d[0] == 1 else d[2] if d[0] == 2 else d[3] if d[0] == 3 else 0
            self.db.at[index,"underground mining cost size factor"] = value
    def udepthfactor(self,index):
        
            
            d = [0.002,
                 0.796,
                 self.db.loc[index,"Underground"],
            ]
            value = d[0]*d[2]+d[1]
            self.db.at[index,"underground mining cost depth factor"] = value
        
        

    def uelecons(self,index):
        
            
            d = [self.db.loc[index,"underground mining cost size factor"],
                 self.db.loc[index,"underground mining cost depth factor"],
                 
            ]
            value = d[0]*d[1]
            self.db.at[index,"underground mining cost electricity consumption"] = value
        

    def uelccost1(self,index):
        
            
            d = [self.db.loc[index,"underground mining cost electricity consumption"],
                 self.db.loc[index,"underground mining cost electricity price"],
                 self.db.loc[index,"total tonnes mined underground"]
            ]
            value = d[0]*d[1] if d[2] > 0 else 0
            
            self.db.at[index,"underground mining cost electricity cost1"] = value
        

    def uelccost2(self,index):
        
            
            d = [self.db.loc[index,"underground mining cost electricity cost1"],
                 self.db.loc[index,"underground mining cost electricity price"],
                 self.db.loc[index,"Stripping ratio Underground"],
                 self.db.loc[index,"total tonnes mined underground"],
            ]
            value = d[0]*(1+d[2]) if d[3] > 0 else 0
            self.db.at[index,"underground mining cost electricity cost2"] = value
        
    def ulabprolow(self,index):
        
            
            d = [100,
                 1.015,
                 1.4,
                 -0.07,
                 self.db.loc[index,"total tonnes mined underground"],
            ]
            value = d[1] if d[4] < d[0] else d[2]*pow(d[4],d[3])
            self.db.at[index,"underground mining cost automation & labour productivity low"] = value
        
    def ulabpromed(self,index):
        
            d = [200,
                 0.936,
                 3,
                 -0.22,
                 self.db.loc[index,"total tonnes mined underground"],
            ]
            value = d[1] if d[4] < d[0] else d[2]*pow(d[4],d[3])
            self.db.at[index,"underground mining cost automation & labour productivity medium"] = value
        
    def ulabprohigh(self,index):
        
            
            d = [300,
                 0.785,
                 8.6,
                 -0.42,
                 self.db.loc[index,"total tonnes mined underground"],
            ]
            value = d[1] if d[4] < d[0] else d[2]*pow(d[4],d[3])
            self.db.at[index,"underground mining cost automation & labour productivity high"] = value
        

    def uautodepth(self,index):
        
            
            d = [125,
                 0.7,
                 450,
                 0.01,
                 0.5,
                 4,
                 self.db.loc[index,"Underground"],
                 450
                 ]
            
            value = 0.7 if d[6]<d[0] else d[3]*d[6]-d[4] if d[6]<d[7] else d[5] 
            self.db.at[index,"underground mining cost automation depth factor"] = value
        

    def uautofactor(self,index):
        
            
            d = [
                self.db.loc[index,"Mine Automation level"],
                self.db.loc[index,"underground mining cost automation & labour productivity low"],
                self.db.loc[index,"underground mining cost automation & labour productivity medium"],
                self.db.loc[index,"underground mining cost automation & labour productivity high"],
                ]

            value = d[1] if d[0] == 1 else d[2] if d[0] == 2 else d[3] if d[0] == 3 else 0
            self.db.at[index,"underground mining cost automation factor"] = value
        

    def ulabpro(self,index):
        
            
            d = [
                0.2,
                0.7,
                self.db.loc[index,"underground mining cost automation factor"],
                self.db.loc[index,"underground mining cost automation depth factor"],
                ]

            value = (d[2]*d[3]+d[0])*d[1]
            self.db.at[index,"underground mining cost labour productivity"] = value
        
    def ulabrate(self,index):
        
            
            d = [self.db.loc[index,"open pit mining cost labour rate"]]
            value = 1.2*d[0]
            self.db.at[index,"underground mining cost labour rate"] = value
        
            
    def ulabourcost1(self,index):
        
            
            d = [
                self.db.loc[index,"underground mining cost labour productivity"],
                self.db.loc[index,"underground mining cost labour rate"],
                self.db.loc[index,"total tonnes mined underground"]
                ]
            value = d[0]*d[1] if d[2] > 0 else 0
            self.db.at[index,"underground mining cost labour cost1"] = value
        
    def ulabourcost2(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio underground"],
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"underground mining cost labour cost1"],
                ]
            value = d[2]*(1+d[0]) if d[1] > 0 else 0
            self.db.at[index,"underground mining cost labour cost2"] = value
        
    def ufreightcost1(self,index):
        
            
            d = [
                self.db.loc[index,"underground mining cost distance"],
                self.db.loc[index,"underground mining cost unit price"],
                ]
            value = d[0]*d[1]
            self.db.at[index,"underground mining cost freight cost1"] = value
        
    def ufreightcost2(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio underground"],
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"underground mining cost freight cost1"]
                ]
            value = d[2]*(d[0]+1) if d[1] > 0 else 0
            self.db.at[index,"underground mining cost freight cost2"] = value
        
    def uexplosives(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined underground"]
            ]
            value = 16 if d[0] > 0 else 0
            self.db.at[index,"underground explosives"] = value
        

    def udieselusage(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined underground"],
                3.5,
                0,
                1.5,
                -0.129
                ]
            value = 0 if d[0] <= 0 else d[1]*d[3]*pow(d[0],d[4])
            self.db.at[index,"underground mining cost diesel usage"] = value
        

    def udieselcost1(self,index):
        
            
            d = [
                self.db.loc[index,"underground mining cost diesel price"],
                self.db.loc[index,"underground mining cost diesel usage"],
                self.db.loc[index,"total tonnes mined underground"],
            ]
            value = d[0]*d[1] if d[2] > 0 else 0
            self.db.at[index,"underground mining cost diesel cost1"] = value
        

    def udieselcost2(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio underground"],
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"underground mining cost diesel cost1"]
                ]
            value = d[2]*(1+d[0]) if d[1] > 0 else 0
            self.db.at[index,"underground mining cost diesel cost2"] = value
        
            
    def utotalminingcost(self,index):
        
            
            d = [
                self.db.loc[index,"underground mining cost electricity cost1"],
                self.db.loc[index,"underground mining cost diesel cost1"],
                self.db.loc[index,"underground mining cost labour cost1"],
                self.db.loc[index,"underground explosives"],
                self.db.loc[index,"underground mining cost freight cost1"]
                ]
            value = d[0]+d[1]+d[2]+d[3]+d[4]
            self.db.at[index,"underground mining cost total mining cost"] = value
        
    def ustripingratio(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"underground mining cost total mining cost"],
                self.db.loc[index,"underground mining cost stripping ratio"]
                ]
            value = d[1]*(d[2]+1) if d[0] > 0 else 0
            self.db.at[index,"underground mining cost "] = value
        
    def ueleclow(self,index):
        
            
            d = [18,
                self.db.loc[index,"total tonnes mined underground"],
                 -0.07,
                 2.25
                ]
            value = d[0]*pow(d[1],d[2])/d[3] if d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost automation & electricity usage low"] = value
        

    def uelecmed(self,index):
        
            
            d = [19,
                self.db.loc[index,"total tonnes mined underground"],
                 -0.07,
                 2.25
                ]
            value = d[0]*pow(d[1],d[2])/d[3] if d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost automation & electricity usage medium"] = value
        

    def uelechigh(self,index):
        
            
            d = [20,
                self.db.loc[index,"total tonnes mined underground"],
                 -0.07,
                 2.25
                ]
            value = d[0]*pow(d[1],d[2])/d[3] if d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost automation & electricity usage high"] = value
        

    
    def uelecusagecons(self,index):
        
            
            d = [
                self.db.loc[index,"Dressing automation level"],
                self.db.loc[index,"underground Washing Cost automation & electricity usage low"],
                self.db.loc[index,"underground Washing Cost automation & electricity usage medium"],
                self.db.loc[index,"underground Washing Cost automation & electricity usage high"],
                self.db.loc[index,"total tonnes mined underground"],
                ]
            value =  d[1] if d[0] == 1 and d[4] > 0 else d[2] if d[0] == 2 and d[4] > 0 else d[3] if d[0] == 3 and d[4] > 0 else 0  
            self.db.at[index,"underground Washing Cost electricity consumption"] = value
        
    def uwashingelecost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"underground Washing Cost electricity consumption"],
                self.db.loc[index,"underground Washing Cost electricity price"],
                ]
            value =  d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost electricity cost"] = value
            self.db.at[index,"underground Washing Cost automation & labour productivity low"] = 0.08
            self.db.at[index,"underground Washing Cost automation & labour productivity medium"] = 0.07
            self.db.at[index,"underground Washing Cost automation & labour productivity high"] = 0.06
        
    def uwashingdieselcost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"underground Washing Cost diesel usage"],
                self.db.loc[index,"underground Washing Cost diesel price"],
                ]
            value =  d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost diesel cost"] = value
        
    def uwashinglabourproductivity(self,index):
        
            
            d = [
                self.db.loc[index,"Dressing automation level"],
                self.db.loc[index,"underground Washing Cost automation & labour productivity low"],
                self.db.loc[index,"underground Washing Cost automation & labour productivity medium"],
                self.db.loc[index,"underground Washing Cost automation & labour productivity high"],
                ]

            value = d[1] if d[0] == 1 else d[2] if d[0] == 2 else d[3] if d[0] == 3 else 0
            self.db.at[index,"underground Washing Cost labour productivity"] = value
        
    def ulabourcost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"underground Washing Cost labour productivity"],
                self.db.loc[index,"underground Washing Cost labour rate"],
                ]
            value = d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost labour cost"] = value
        
    
    def umaterialcost(self,index):
        
            
            d = [
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"underground Washing Cost material price"],
                self.db.loc[index,"underground Washing Cost material usage"],
                ]
            value = d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost material cost"] = value
        
    def uwaterconsumption(self,index):
        
            
            d = [
                self.db.loc[index,"province"]
                ]
            value = 3 if d[0] == "Gunagxi" else 0
            self.db.at[index,"underground Washing Cost water consumption"] = value
        
    def uwatercost(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"underground Washing Cost water consumption"],
                self.db.loc[index,"underground Washing Cost water price"],
    
                ]
            value = d[2]*d[3] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost water cost"] = value
        
    def utotaldressingcost1(self,index):
        
            
            d = [
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"Dressing Ratio"],
                self.db.loc[index,"underground Washing Cost electricity cost"],
                self.db.loc[index,"underground Washing Cost diesel cost"],
                self.db.loc[index,"underground Washing Cost labour cost"],
                self.db.loc[index,"underground Washing Cost material cost"],
                self.db.loc[index,"underground Washing Cost water cost"],
                ]
            value = d[2]+d[3]+d[4]+d[5]+d[6] if d[0] > 0 and d[1] > 0 else 0
            self.db.at[index,"underground Washing Cost total dressing cost1"] = value
        
    
    
    def utotalequipcost(self,index):
        
            
            d = [
            self.db.loc[index,"Stripping ratio underground"],
            self.db.loc[index,"underground Capital & Equipment Charges mine capital cost"]
            ]
            value = d[1]*(1+d[0])
            self.db.at[index,"underground Capital & Equipment Charges mine equipment cost"] = value
        
    def utotalequiphire(self,index):
        
            
            d = [
                self.db.loc[index,"Stripping ratio underground"],
                self.db.loc[index,"underground Capital & Equipment Charges mine equipment hire1"]
                ]
            value = d[1]*(1+d[0])
            self.db.at[index,"underground Capital & Equipment Charges mine equipment hire2"] = value
        
    def umaintancecost(self,index):
        
            
            d = [
                self.db.loc[index,"underground Capital & Equipment Charges % of equipment maintenance cost"],
                self.db.loc[index,"underground Capital & Equipment Charges mine equipment ownership"],
                self.db.loc[index,"underground Capital & Equipment Charges mine equipment cost"],
                self.db.loc[index,"underground Capital & Equipment Charges mine equipment hire2"]
                ]
            value = d[2]*d[0]*d[1]+d[3]*(1-d[1])
            self.db.at[index,"underground Capital & Equipment Charges maintenance & equipment charge"] = value
        
    def ucapitalcharge(self,index):
        
            
            d = [
                self.db.loc[index,"underground Capital & Equipment Charges mine establishment"],
                self.db.loc[index,"underground Capital & Equipment Charges % of equipment sustaining cost"],
                self.db.loc[index,"underground Capital & Equipment Charges mine equipment ownership"],
                self.db.loc[index,"underground Capital & Equipment Charges mine equipment cost"]
                ]
            value = d[3]*d[1]*d[2]+d[0]*d[1]
            self.db.at[index,"underground Capital & Equipment Charges sustaining capital charge"] = value
      
    def totaltaxandfees(self,index):
        
        tax = "on"
        d = [
                self.db.loc[index,"total tonnes mined open pit"],
                self.db.loc[index,"total tonnes mined underground"],
                self.db.loc[index,"resource tax"],
                self.db.loc[index,"other tax and fees"],
                self.db.loc[index,"open pit Washing Cost dressing ratio"]
                ]
        value = ((d[2]+d[3]) if (d[0]+d[1]) > 0 else 0)*(1 if tax == "on" else 0)*(1 if d[4]==0 else 0)
        self.db.at[index,"total tax and fees"] = value
            
    def sumlabour(self,index):
        
            
            d = [
                self.db.loc[index,"open pit mining cost labour cost2"],
                self.db.loc[index,"open pit Washing Cost labour cost"],
                self.db.loc[index,"open pit Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type open"],
                self.db.loc[index,"underground mining cost labour cost2"],
                self.db.loc[index,"underground Washing Cost labour cost"],
                self.db.loc[index,"underground Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type underground"]
                ]
            value = (d[0]+d[1])*max(1,d[2])*d[3]+(d[4]+d[5])*max(1,d[6])*d[7]
            self.db.at[index,"summary labour"] = value
        
    def sumenergy(self,index):
        
        d = [
                self.db.loc[index,"open pit mining cost electricity cost2"],
                self.db.loc[index,"open pit mining cost diesel cost2"],
                self.db.loc[index,"open pit Washing Cost electricity cost"],
                self.db.loc[index,"open pit Washing Cost diesel cost"],
                self.db.loc[index,"open pit Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type open"],
                self.db.loc[index,"underground mining cost electricity cost2"],
                self.db.loc[index,"underground mining cost diesel cost2"],
                self.db.loc[index,"underground Washing Cost electricity cost"],
                self.db.loc[index,"underground Washing Cost diesel cost"],
                self.db.loc[index,"underground Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type underground"]
                ]
        value = (d[0]+d[1]+d[2]+d[3])*max(1,d[4])*d[5]+(d[6]+d[7]+d[8]+d[9])*max(1,d[10])*d[11]           
        self.db.at[index,"summary energy"] = value

    def sumwater(self,index):
        
            
            d = [
                self.db.loc[index,"open pit Washing Cost water cost"],
                self.db.loc[index,"open pit Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type open"],
                self.db.loc[index,"underground Washing Cost water cost"],
                self.db.loc[index,"underground Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type underground"]
                ]
            value = d[0]*max(1,d[1])*d[2]+d[3]*max(1,d[4])*d[5]
            self.db.at[index,"summary water"] = value
        

    def sumtax(self,index):
        
            
            d = [
                self.db.loc[index,"total tax and fees"]
                ]
            value = d[0]
            self.db.at[index,"summary tax and fees"] = value
        
    def sumexplosives(self,index):
        
            
            d = [
                self.db.loc[index,"underground explosives"],
                self.db.loc[index,"Mine Type underground"],
                ]
            value = d[0]*d[1]
            self.db.at[index,"summary explosives"] = value
        
    def otherproportional(self,index):
        
        d = [
                self.db.loc[index,"summary labour"],
                self.db.loc[index,"summary energy"],
                self.db.loc[index,"summary water"],
                self.db.loc[index,"summary tax and fees"],
                0.1
                ]
        value = (d[0]+d[1]+d[2]+d[3])*d[4]
        self.db.at[index,"other proportional"] = value 
    def othercost(self,index):
        
       # 
        d = [
            self.db.loc[index,"total tonnes mined open pit"],
            self.db.loc[index,"total tonnes mined underground"],
            self.db.loc[index,"other proportional"],
            self.db.loc[index,"other fixed"]
            ]
        value = d[2]+d[3] if (d[0]+d[1]) > 0 else 0
       # print('---'+str(d[2]))
        
        self.db.at[index,"other cost"] = value
     #   except:
    #        self.db.at[index,"other cost"] = 0
    def sumother(self,index):
        
            
            d = [
                self.db.loc[index,"open pit Washing Cost material cost"],
                self.db.loc[index,"open pit mining cost freight cost2"],
                self.db.loc[index,"open pit Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type open"],
                self.db.loc[index,"other cost"],
                self.db.loc[index,"underground mining cost freight cost2"],
                self.db.loc[index,"underground Washing Cost material cost"],
                self.db.loc[index,"underground Washing Cost dressing ratio"],
                self.db.loc[index,"Mine Type underground"]
                ]
            value = (d[0]+d[1])*max(1,d[2])*d[3]+d[4]+(d[5]+d[6])*max(1,d[7])*d[8]
            self.db.at[index,"summary other"] = value
        
    def summarylabour(self,index):
        
        d = [
               self.db.loc[index,"summary labour"] 
               ]   
        value = d[0]/fxrate
        self.db.at[index,"fsumlabour"] = value
    def summaryenergy(self,index):
        
        d = [
            self.db.loc[index,"summary energy"] 
            ]   
        value = d[0]/fxrate
        self.db.at[index,"fsumenergy"] = value
    def summarywater(self,index):
        
        d = [
               self.db.loc[index,"summary water"] 
               ]   
        value = d[0]/fxrate
        self.db.at[index,"fsumwater"] = value
    def summarytaxandfees(self,index):
        
        d = [
            self.db.loc[index,"summary tax and fees"] 
            ]   
        value = d[0]/fxrate
        self.db.at[index,"fsumtaxandfees"] = value
    def summaryexplosives(self,index):
        
        d = [
               self.db.loc[index,"summary explosives"] 
               ]   
        value = d[0]/fxrate
        self.db.at[index,"fsumexplosives"] = value 
    def summaryother(self,index):
        
        d = [
               self.db.loc[index,"summary other"] 
               ]   
        value = d[0]/fxrate
        self.db.at[index,"fsumother"] = value      
    def calcall(self,index):
        CostModel.ttmop(self,index)
        CostModel.ttmug(self,index)
        CostModel.elcons(self,index)
        CostModel.elprice(self,index)
        CostModel.elcost1(self,index)
        CostModel.elcost2(self,index)
        CostModel.unitprice(self,index)
        CostModel.dieselusg(self,index)
        CostModel.dieselprice(self,index)
        CostModel.dieselcost1(self,index)
        CostModel.dieselcost2(self,index)
        CostModel.labpro(self,index)
        CostModel.labrate(self,index)
        CostModel.labourcost1(self,index)
        CostModel.labourcost2(self,index)
        CostModel.freightcost1(self,index)
        CostModel.freightcost2(self,index)
        CostModel.totalminingcost(self,index)
        CostModel.strippingratio(self,index)
        CostModel.openpitminingcost(self,index)
        CostModel.autolow(self,index)
        CostModel.automed(self,index)
        CostModel.autohigh(self,index)
        CostModel.washingelecons(self,index)
        CostModel.washingelecost(self,index)
        CostModel.washingdieselcost(self,index)
        CostModel.washinglabourproductivity(self,index)
        CostModel.labourcost(self,index)
        CostModel.materialprice(self,index)
        CostModel.materialcost(self,index)
        CostModel.watercost(self,index)
        CostModel.totaldressingcost1(self,index)
        CostModel.totaldressingcost2(self,index)
        CostModel.totalequipcost(self,index)
        CostModel.totalequiphire(self,index)
        CostModel.maintancecost(self,index)
        CostModel.capitalcharge(self,index)

        CostModel.ulow(self,index)
        CostModel.umed(self,index)
        CostModel.uhigh(self,index)
        CostModel.usizefactor(self,index)
        CostModel.udepthfactor(self,index)
        CostModel.uelecons(self,index)
        CostModel.uelccost1(self,index)
        CostModel.uelccost2(self,index)
        CostModel.ulabprolow(self,index)
        CostModel.ulabpromed(self,index)
        CostModel.ulabprohigh(self,index)
        CostModel.uautodepth(self,index)
        CostModel.uautofactor(self,index)
        CostModel.ulabpro(self,index)
        CostModel.ulabrate(self,index)
        CostModel.ulabourcost1(self,index)
        CostModel.ulabourcost2(self,index)
        CostModel.ufreightcost1(self,index)
        CostModel.ufreightcost2(self,index)
        CostModel.uexplosives(self,index)
        CostModel.udieselusage(self,index)
        CostModel.udieselcost1(self,index)
        CostModel.udieselcost2(self,index)
        CostModel.utotalminingcost(self,index)
        CostModel.ustripingratio(self,index)
        CostModel.ueleclow(self,index)
        CostModel.uelecmed(self,index)
        CostModel.uelechigh(self,index)
        CostModel.uelecusagecons(self,index)
        
        CostModel.uwashingelecost(self,index)
        CostModel.uwashingdieselcost(self,index)
        CostModel.uwashinglabourproductivity(self,index)
        CostModel.ulabourcost(self,index)
        CostModel.umaterialcost(self,index)
        CostModel.utotaldressingcost1(self,index)
        CostModel.utotalequipcost(self,index)
        CostModel.utotalequiphire(self,index)
        CostModel.umaintancecost(self,index)
        CostModel.ucapitalcharge(self,index)
        CostModel.totaltaxandfees(self,index)
        
        
        CostModel.sumlabour(self,index)
        CostModel.sumenergy(self,index)
        CostModel.sumwater(self,index)
        CostModel.sumtax(self,index)
        CostModel.sumexplosives(self,index)
        CostModel.otherproportional(self,index)
        CostModel.othercost(self,index)
        CostModel.sumother(self,index)
        
        CostModel.summarylabour(self,index)
        CostModel.summaryenergy(self,index)
        CostModel.summarywater(self,index)
        CostModel.summarytaxandfees(self,index)
        CostModel.summaryexplosives(self,index)
        CostModel.summaryother(self,index)



            
dbi = CostModel(inputdatadb,taxtrans)


for i in range(114):
    dbi.calcall(i)
#ldb = reltoflat(dbi.db,ocnxn)

a = dbi.db.transpose()
b = pd.DataFrame(dbi.db['province'])
b['County'] = dbi.db['County']
b['Mine'] = dbi.db['Mine']
b['Tonnes Produced(kt)'] = dbi.db['Tonnes Produced(kt)']
b['fsumlabour'] = dbi.db['fsumlabour']
b['fsumenergy'] = dbi.db['fsumenergy']
b['fsumwater'] = dbi.db['fsumwater']
b['fsumtaxandfees'] = dbi.db['fsumtaxandfees']
b['fsumexplosives'] = dbi.db['fsumexplosives']
b['fsumother'] = dbi.db['fsumother']
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #ldb.to_csv('outputdata/snapshot_output_data.csv',index=False)
    a = a.transpose()
    a.to_csv("outputdata/costmodeloutput.csv")
    
    snapshot = db_conv.mult_year_single_output(a, "cost model output", idx_of_index=[[0,4]], idx_of_values=[[4,]],label="Field")
    snapshot.to_csv("snapshot_output_data.csv", index =False)
    uploadtodb.upload(snapshot)
