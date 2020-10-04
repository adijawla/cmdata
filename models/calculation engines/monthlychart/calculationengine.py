import os
import numpy as np
import pandas as pd
from pathlib import Path
import warnings
from datetime import datetime
import xlrd
from outputdb import uploadtodb
import datetime as dt
from flatdb.flatdbconverter import Flatdbconverter
from monthlyscript import restruct
warnings.filterwarnings("ignore")
from test import interrelationship_rest

db_conv = Flatdbconverter("Monthly Chart Pack")
dblist = []

inter = interrelationship_rest()

rest = restruct()

def excel_date(date1):
    temp = dt.datetime(1899, 12, 30)
    delta = date1 - temp
    return float(delta.days) + (float(delta.seconds) / 86400)

def read_date(date):
    return xlrd.xldate.xldate_as_datetime(excel_date(date), 0)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

print(BASE_DIR)

class lookup():
    def __init__(self):
        # self.fxrate = pd.read_excel(os.path.join(BASE_DIR,'fxrate.xlsx'))
        # self.fxrate = rest["monthly_fxrates"]
        self.fxrate = inter["fxrate"]

    def fxratemonth(self,indx):
        d = [self.fxrate['Date'][indx]]
        value = str(d[0].month)+" "+str(d[0].year)
        self.fxrate.loc[indx,"MONTH"] = value
    def fxratermb(self,indx):
        d = [self.fxrate["MONTH"].to_list()]
        value = self.fxrate.loc[self.fxrate.MONTH==self.fxrate["MONTH"][indx]]["RMB_per_US"].sum()/d[0].count(self.fxrate["MONTH"][indx])
        self.fxrate.at[indx,"RMB_per_US1"] = value
    def fxrateauperus(self,indx):
        d = [self.fxrate["MONTH"].to_list()]
        value = self.fxrate.loc[self.fxrate.MONTH==self.fxrate["MONTH"][indx]]["AU_per_US"].sum()/d[0].count(self.fxrate["MONTH"][indx])
        self.fxrate.at[indx,"AU_per_US1"] = value
    def calcall(self):
        for i in range(self.fxrate.shape[0]):
            lookup.fxratemonth(self,i)
        for i in range(self.fxrate.shape[0]):
            lookup.fxratermb(self,i)
            lookup.fxrateauperus(self,i)

c = lookup()
c.calcall()
c.fxrate.to_csv(os.path.join(BASE_DIR, "outputdata/fxrateoutput.csv"))


"""
self.fxrate = rest["monthly_fxrates"]
self.rawdata = rest["monthly_rawdata"]
self.cbixchart = rest["monthly_cbixchart"]
self.tradedata  = rest["monthly_tradedata"]
self.insuranceratelookup   = rest["monthly_insurance_rate_in_cif"]
self.bauxite_reqrmt_supply   = rest["monthly_summary_bauxite_requirement_supply"]
self.alumina_by_brovince     = rest["monthly_alumina_by_province"]
self.monthly_domestic_price_dmt      = rest["monthly_domestic_price_dmt"]
self.monthly_import_by_country_dmt   = rest["monthly_import_by_country_dmt"]
self.imported_bauxite_stocks = rest["imported_bauxite_stocks"]
self.MONTHLY_AVERAGE_CFR_PRICE      = rest["monthly_avg_cfr_price"]
self.MONTHLY_AVERAGE_VIU_CFR_PRICE  = rest["monthly_average_viu_cfr_price"]
"""

class weeklydatasummury():
    def __init__(self,look):
        # self.rawdata = pd.read_excel(os.path.join(BASE_DIR,'naohrawdata.xlsx'))
        # print(self.rawdata["date"])
        # self.rawdata = rest["monthly_rawdata"]
        self.rawdata = inter["noah"]
        han = self.rawdata["date"]
        for a in han:
            print(read_date(a))
        self.rawdata["date"] = pd.to_datetime(self.rawdata['date'].map(read_date))
        self.domestic = pd.DataFrame(columns=["Shanxi4.5 - 5.0","Henan4.0 - 5.0","Guizhou 5.5 - 6.5","Guangxi 7.0 - 8.0"])
        self.domesticmonthly = pd.DataFrame(columns=["month","Shanxi4.5 - 5.0","Henan4.0 - 5.0","Guizhou 5.5 - 6.5","Guangxi 7.0 - 8.0"])
        self.liquid = pd.DataFrame(columns=["shandong","Henan","Shanxi"])
        self.lookupkey = pd.DataFrame(columns=["lookup key","2","3","4","5","6","7","8","9"])
        self.look = look
        # self.cbixchart = pd.read_excel(os.path.join(BASE_DIR,'cbixchart.xlsx'))
        # self.cbixchart = rest["monthly_cbixchart"]
        self.cbixchart = inter["cbixchart"]
        self.cbixchart["chartdots"] = self.cbixchart["CBIX Weekly"]
    def shanxicalc(self,indx):
        d = [self.rawdata["shanxi1"][indx],self.rawdata["shanxi2"][indx]]
        v = (d[0]+d[1])/2
        self.domestic.at[indx,"Shanxi4.5 - 5.0"] = v
    def henancalc(self,indx):
        d = [self.rawdata["henan1"][indx],self.rawdata["henan2"][indx]]
        v = (d[0]+d[1])/2
        self.domestic.at[indx,"Henan4.0 - 5.0"] = v
    def guizhoucalc(self,indx):
        d = [self.rawdata["guizhou1"][indx],self.rawdata["guizhou2"][indx]]
        v = (d[0]+d[1])/2
        self.domestic.at[indx,"Guizhou 5.5 - 6.5"] = v
    def guangxicalc(self,indx):
        d = [self.rawdata["guangxi1"][indx],self.rawdata["guangxi2"][indx]]
        v = (d[0]+d[1])/2
        self.domestic.at[indx,"Guangxi 7.0 - 8.0"] = v
    def shandongcalc(self,indx):
        d = [self.rawdata["shandog l1"][indx],self.rawdata["shandog l2"][indx]]
        v = (d[0]+d[1])/2
        self.liquid.at[indx,"shandong"] = v
    def henancalc2(self,indx):
        d = [self.rawdata["henan l1"][indx],self.rawdata["henan l2"][indx]]
        v = (d[0]+d[1])/2
        self.liquid.at[indx,"henan"] = v
    def shanxicalc2(self,indx):
        d = [self.rawdata["shanxi l1"][indx],self.rawdata["shanxi l2"][indx]]
        v = (d[0]+d[1])/2
        self.liquid.at[indx,"shanxi"] = v

    def monthlymonth(self,indx):
        d = [self.rawdata['date'][indx],self.rawdata['date'][indx+1]]
        v = str(d[0].month)+"-"+str(d[0].year)+'-1' if (d[0].month-d[1].month) == -1 or (d[0].month-d[1].month) == 11 else "-"
        if v != '-':
            v = datetime.strptime(v, '%m-%Y-%d')
        self.domesticmonthly.loc[indx,"month"] = v

    def monthlyshanxi(self,indx):
        d = [self.domesticmonthly['month'][indx]]
        v = "-" if d[0] == "-" else self.domestic["Shanxi4.5 - 5.0"][indx]
        self.domesticmonthly.loc[indx,"Shanxi4.5 - 5.0"] = v
    def monthlyhenan(self,indx):
        d = [self.domesticmonthly['month'][indx]]
        v = "-" if d[0] == "-" else self.domestic["Henan4.0 - 5.0"][indx]
        self.domesticmonthly.loc[indx,"Henan4.0 - 5.0"] = v
    def monthlyguizhou(self,indx):
        d = [self.domesticmonthly['month'][indx]]
        v = "-" if d[0] == "-" else self.domestic["Guizhou 5.5 - 6.5"][indx]
        self.domesticmonthly.loc[indx,"Guizhou 5.5 - 6.5"] = v
    def monthlyguangxi(self,indx):
        d = [self.domesticmonthly['month'][indx]]
        v = "-" if d[0] == "-" else self.domestic["Guangxi 7.0 - 8.0"][indx]
        self.domesticmonthly.loc[indx,"Guangxi 7.0 - 8.0"] = v
    def lookupmonth(self):
        self.lookupkey["lookup key"] = self.domesticmonthly["month"]

    def lookup2(self,indx, prov, idx1):
        d = [self.lookupkey["lookup key"][indx],self.rawdata[prov][indx]]
        if d[0] != '-':
            d[0] = f"{d[0].month} {d[0].year}"
        try:
            v = "-" if d[0] == "-" else round(d[1]/(self.look.loc[self.look.MONTH.astype(str) == d[0]]["RMB_per_US"].values[0]), 1)
        except IndexError:
            v = 0
        self.lookupkey.at[indx,idx1] = v


    def calcall(self):
        for i in range(self.rawdata.shape[0]-1):
            weeklydatasummury.shanxicalc(self,i)
            weeklydatasummury.henancalc(self,i)
            weeklydatasummury.guizhoucalc(self,i)
            weeklydatasummury.guangxicalc(self,i)
            weeklydatasummury.shandongcalc(self,i)
            weeklydatasummury.henancalc2(self,i)
            weeklydatasummury.shanxicalc2(self,i)
        for i in range(self.rawdata.shape[0]-1):
            weeklydatasummury.monthlymonth(self,i)
            weeklydatasummury.monthlyshanxi(self,i)
            weeklydatasummury.monthlyhenan(self,i)
            weeklydatasummury.monthlyguizhou(self,i)
            weeklydatasummury.monthlyguangxi(self,i)
        weeklydatasummury.lookupmonth(self)
        provs = [("shanxi1", '2'), ("shanxi2", '3'), ("henan1", '4'), ("henan2", '5'), ("guizhou1", '6'), ("guizhou2", '7'), ("guangxi1", '8'), ("guangxi2", '9')]
        self.domesticmonthly.to_csv('test.csv')
        for i in range(self.rawdata.shape[0]-1):
            for a in provs:
                weeklydatasummury.lookup2(self,i, a[0], a[1])
          


class TradeWithProcessing():
    def __init__(self):
        # self.tradedata          = pd.read_excel(os.path.join(BASE_DIR,'traderawdata.xlsx'))
        self.tradedata  = rest["monthly_tradedata"]
        self.regionkeylookup    = pd.read_csv(os.path.join(BASE_DIR,'regionkeylookup.csv'))
        self.customerkeylookup  = pd.read_csv(os.path.join(BASE_DIR,'customerkeylookup.csv'))
        self.analysedregionfilter  = pd.read_csv(os.path.join(BASE_DIR,'analysedregionfilter.csv'))
        # self.insuranceratelookup   = pd.read_excel(os.path.join(BASE_DIR,'insuranceRateInCIF_Price.xlsx'))
        self.insuranceratelookup   = rest["monthly_insurance_rate_in_cif"]
        self.quotationdata      = pd.DataFrame(columns=['Date','COUNTRY','REGION','CUSTOMER','MONTH KEY','REGION KEY','CUSTOMER KEY','CIF PRICE','CFR PRICE','VIU CFR PRICE','IMPORT TONNAGE','CIF LANDED TON*PRICE','CFR LANDED TON*PRICE','VIU CFR TON*PRICE'])
        self.avgcalcdata        = pd.DataFrame(columns=['MONTH KEY','ANALYZED REGION KEY','MONTHLY AVERAGE CIF PRICE','MONTHLY AVERAGE CFR PRICE','MONTHLY AVERAGE VIU CFR PRICE'])
        self.customerdata       = pd.DataFrame(columns=['regioncustomerkey','lookupkey','monthly average cfr price','import tonnage'])
        self.workingsdata       = pd.DataFrame(columns=['lookup key','country','monthly average cif price by country'])
        self.scatterchart       = pd.DataFrame(columns=['180','import tonnage','australia','guinea','indonesia','soloman islands','vietnam','ghana','brazil','jamaica','montenegro','malaysia','turkey','fiji','india','greece','sierra leone'])
        self.db                 = {}


    def lookup(self, search, lookup, target):
        v = lookup.map(lambda x: x.lower() == search.lower())
        try:
            return target[v].tolist()[0]
        except Exception:
            return 0

    def qoutation(self):
        def dt_lookup(search, lookup, target):
            v = lookup.map(lambda x: x.date() <= search.date())
            try:
                return target[v].tolist()[0]
            except Exception:
                return 0

        for i in range(self.tradedata.shape[0]):
            self.quotationdata.at[i, "Date"]        = self.tradedata.loc[i, "Date"]
            self.quotationdata.at[i, "COUNTRY"]     = self.tradedata.loc[i, "Country"]
            self.quotationdata.at[i, "REGION"]      = self.tradedata.loc[i, "Region"]
            self.quotationdata.at[i, "CUSTOMER"]    = self.tradedata.loc[i, "Customer"]
            self.quotationdata.at[i, "CIF PRICE"]   = self.tradedata.loc[i, "DeclaredPrice"]
            self.quotationdata.at[i, "VIU CFR PRICE"]   = self.tradedata.loc[i, "CBIXEquivalentPrice"]
            self.quotationdata.at[i, "IMPORT TONNAGE"]  = self.tradedata.loc[i, "Tonnage"]

            self.quotationdata.at[i,"MONTH KEY"]    = f'{self.tradedata.loc[i, "Date"].month} {self.tradedata.loc[i, "Date"].year}'
            self.quotationdata.at[i,"REGION KEY"]   = self.lookup(self.quotationdata.loc[i, "REGION"], self.regionkeylookup.loc[:, "Mine"], self.regionkeylookup.loc[:, "Country"])
            self.quotationdata.at[i,"CUSTOMER KEY"] = self.lookup(self.quotationdata.loc[i, "CUSTOMER"], self.customerkeylookup.loc[:, "a"], self.customerkeylookup.loc[:, "b"])
            self.quotationdata.at[i,"CFR PRICE"]    = self.quotationdata.loc[i, "CIF PRICE"].astype(float) / (1 + dt_lookup(self.quotationdata.loc[i, "Date"], self.insuranceratelookup.loc[:, "date"], self.insuranceratelookup.loc[:, "rate"]))
            self.quotationdata.at[i,"CIF LANDED TON*PRICE"] = self.quotationdata.loc[i, "CIF PRICE"] * self.quotationdata.loc[i, "IMPORT TONNAGE"]
            self.quotationdata.at[i,"CFR LANDED TON*PRICE"] = self.quotationdata.loc[i, "CFR PRICE"] * self.quotationdata.loc[i, "IMPORT TONNAGE"]
            self.quotationdata.at[i,"VIU CFR TON*PRICE"]    = self.quotationdata.loc[i, "VIU CFR PRICE"] * self.quotationdata.loc[i, "IMPORT TONNAGE"]

        self.db[Path(os.path.join(BASE_DIR,"outputdata/tradedata/qoutation.csv"))] = self.quotationdata

    def monthly_avg(self):
        def sumifs(sum_range1, sum_range2, range1, criteria1, range2, criteria2):
            ind = range1 == criteria1
            idx = range2 == criteria2
            v = ind & idx

            try:
                return sum_range1[v].sum() / sum_range2[v].sum()
            except Exception:
                return 0

        for i in range(self.tradedata.shape[0]):
            self.avgcalcdata.at[i, "ANALYZED REGION KEY"]           = self.lookup(self.quotationdata.loc[i,"REGION KEY"], self.analysedregionfilter.loc[:, "a"], self.analysedregionfilter.loc[:, "b"])
            self.avgcalcdata.at[i, "MONTH KEY"]                     = f'{self.quotationdata.loc[i,"MONTH KEY"]}{self.avgcalcdata.loc[i, "ANALYZED REGION KEY"]}'
            self.avgcalcdata.loc[i, "MONTHLY AVERAGE CIF PRICE"]    = '-' if self.avgcalcdata.loc[i, "ANALYZED REGION KEY"] == '-' else sumifs(
                self.quotationdata.loc[:,"CIF LANDED TON*PRICE"],   # sum_range1
                self.quotationdata.loc[:, "IMPORT TONNAGE"],        # sum_range2
                self.quotationdata.loc[:,"MONTH KEY"],              # range1
                self.quotationdata.loc[i,"MONTH KEY"],              # criteria1
                self.quotationdata.loc[:,"REGION KEY"],             # range2
                self.avgcalcdata.loc[i, "ANALYZED REGION KEY"]      # criteria2
            )
            self.avgcalcdata.loc[i, "MONTHLY AVERAGE CFR PRICE"]    = '-' if self.avgcalcdata.loc[i, "ANALYZED REGION KEY"] == '-' else sumifs(
                self.quotationdata.loc[:,"CFR LANDED TON*PRICE"],   # sum_range1
                self.quotationdata.loc[:, "IMPORT TONNAGE"],        # sum_range2
                self.quotationdata.loc[:,"MONTH KEY"],              # range1
                self.quotationdata.loc[i,"MONTH KEY"],              # criteria1
                self.quotationdata.loc[:,"REGION KEY"],             # range2
                self.avgcalcdata.loc[i, "ANALYZED REGION KEY"]      # criteria2
            )
            self.avgcalcdata.loc[i, "MONTHLY AVERAGE VIU CFR PRICE"]   = '-' if self.avgcalcdata.loc[i, "ANALYZED REGION KEY"] == '-' else sumifs(
                self.quotationdata.loc[:,"VIU CFR TON*PRICE"],      # sum_range1
                self.quotationdata.loc[:, "IMPORT TONNAGE"],        # sum_range2
                self.quotationdata.loc[:,"MONTH KEY"],              # range1
                self.quotationdata.loc[i,"MONTH KEY"],              # criteria1
                self.quotationdata.loc[:,"REGION KEY"],             # range2
                self.avgcalcdata.loc[i, "ANALYZED REGION KEY"]      # criteria2
            )

        self.db[Path(os.path.join(BASE_DIR,"outputdata/tradedata/avgcalcdata.csv"))] = self.avgcalcdata

    def customer_calcs(self):
        def sumifs(sum_range1=None, sum_range2=None, range1=None, criteria1=None, range2=None, criteria2=None):
            ind = range1 == criteria1
            idx = range2 == criteria2
            v = ind & idx
            try:
                if not sum_range2 is None:
                    return sum_range1[v].sum() / sum_range2[v].sum()
                else:
                    return sum_range1[v].sum()
            except Exception:
                return 0

        for i in range(self.tradedata.shape[0]):
            self.customerdata.at[i, 'regioncustomerkey']    = f'{self.quotationdata.loc[i, "REGION"]}-{self.quotationdata.loc[i,"CUSTOMER KEY"]}'
            self.customerdata.at[i, 'lookupkey']            = f'{self.quotationdata.loc[i,"MONTH KEY"]}{self.customerdata.loc[i, "regioncustomerkey"]}'
        for i in range(self.tradedata.shape[0]):
            self.customerdata.at[i, 'monthly average cfr price']    = sumifs(
                sum_range1  =   self.quotationdata.loc[:,"CFR LANDED TON*PRICE"],   # sum_range1
                sum_range2  =   self.quotationdata.loc[:, "IMPORT TONNAGE"],        # sum_range2
                range1      =   self.customerdata.loc[:,"regioncustomerkey"],       # range1
                criteria1   =   self.customerdata.loc[i,"regioncustomerkey"],       # criteria1
                range2      =   self.quotationdata.loc[:,"MONTH KEY"],                # range2
                criteria2   =   self.quotationdata.loc[i, "MONTH KEY"]                # criteria2
            )
            self.customerdata.at[i, 'import tonnage']       = sumifs(
                sum_range1  =   self.quotationdata.loc[:,"IMPORT TONNAGE"],         # sum_range1
                range1      =   self.customerdata.loc[:,"regioncustomerkey"],       # range1
                criteria1   =   self.customerdata.loc[i,"regioncustomerkey"],       # criteria1
                range2      =   self.quotationdata.loc[:,"MONTH KEY"],                # range2
                criteria2   =   self.quotationdata.loc[i, "MONTH KEY"]                # criteria2
            )

        self.db[Path(os.path.join(BASE_DIR,"outputdata/tradedata/customerdata.csv"))] = self.customerdata

    def workings(self):
        def sumifs(sum_range1, sum_range2, range1, criteria1, range2, criteria2):
            ind = range1.map(lambda x: x.lower() == criteria1.lower())
            idx = range2.map(lambda x: x.lower() == criteria2.lower())
            v = ind * idx
            
            try:
                return sum_range1[v].sum() / sum_range2[v].sum()
            except Exception:
                return 0

        for i in range(self.tradedata.shape[0]):
            self.workingsdata.at[i, 'lookup key']   = f'{self.quotationdata.loc[i,"MONTH KEY"]}{self.quotationdata.loc[i, "COUNTRY"]}'
            self.workingsdata.at[i, 'country']      = self.quotationdata.loc[i, "COUNTRY"]
            self.workingsdata.at[i, 'monthly average cif price by country'] = sumifs(
                self.quotationdata.loc[:,"CIF LANDED TON*PRICE"],   # sum_range1
                self.quotationdata.loc[:, "IMPORT TONNAGE"],        # sum_range2
                self.quotationdata.loc[:,"MONTH KEY"],              # range1
                self.quotationdata.loc[i,"MONTH KEY"],              # criteria1
                self.quotationdata.loc[:,"COUNTRY"],             # range2
                self.workingsdata.loc[i, "country"]                 # criteria2
            )
        self.db[Path(os.path.join(BASE_DIR,"outputdata/tradedata/workingsdata.csv"))] = self.workingsdata

    def scatter_chart(self):
        max_date = self.tradedata.loc[:, "Date"].max().date()
        col_num = self.scatterchart.columns[0]

        def excel_date(yyyy, mm, dd):
            date = datetime(yyyy, mm, dd)
            temp = datetime(1899, 12, 30)
            delta = date - temp
            return float(delta.days) + (float(delta.seconds) / 86400)


        for i in range(self.tradedata.shape[0]):
            self.scatterchart.at[i, col_num]              = 0 if int(excel_date(max_date.year,max_date.month,max_date.day) - excel_date(self.quotationdata.loc[i, "Date"].year,self.quotationdata.loc[i, "Date"].month,self.quotationdata.loc[i, "Date"].day)) > int(col_num) else 1
            self.scatterchart.at[i, "import tonnage"]     = np.nan if self.scatterchart.loc[i, col_num] == 0 else self.quotationdata.loc[i, "IMPORT TONNAGE"]
            for col in self.scatterchart.columns[2:]:
                self.scatterchart.at[i, col]              = np.nan if self.scatterchart.loc[i, col_num] == 0 else round(self.tradedata.loc[i, "DeclaredPrice"],1) if self.tradedata.loc[i, "Country"].lower() == col.lower() else np.nan

        self.db[Path(os.path.join(BASE_DIR,"outputdata/tradedata/scatterchart.csv"))]  = self.scatterchart


    def calcall(self):
        self.qoutation()
        self.monthly_avg()
        self.customer_calcs()
        self.workings()
        self.scatter_chart()

        #Save files
    
        if Path(os.path.join(BASE_DIR,"outputdata")).exists():
            pass
        else:
            os.mkdir(Path(os.path.join(BASE_DIR,"outputdata")))
        for filepath, file in self.db.items():
            dirname = os.path.dirname(filepath)

            if os.path.exists(dirname):
                pass
            else:
                os.mkdir(dirname)
            filepath_s = str(filepath).replace('\\', '/')
            print(filepath_s)
            name = f"{filepath_s.split('/')[-2]} {filepath_s.split('/')[-1][:-4]}"
            print(name)
            dblist.append(db_conv.single_year_mult_out(file, name))
            print(filepath.stem)
            file.to_csv(filepath, index=False)
        





class MonthlyDataSummary:
    def __init__(self): #, trade
        self.fxrates                         = pd.read_csv(Path(os.path.join(BASE_DIR, "outputdata/fxrateoutput.csv")))
        self.weeklydatasumm                  = weeklydatasummury(self.fxrates)
        self.weeklydatasumm.calcall()
        self.lookup_sheet                    = pd.read_csv(Path(os.path.join(BASE_DIR,"outputdata/fxrateoutput.csv")))
        # self.bauxite_reqrmt_supply           = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="Bauxite Requirement & Supply, M") #dexter: changed sheet name
        self.bauxite_reqrmt_supply           = rest["monthly_summary_bauxite_requirement_supply"]
        # self.alumina_by_brovince             = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="Alumina by Province, Mt Aa")
        self.alumina_by_brovince     = rest["monthly_alumina_by_province"]
        # self.monthly_domestic_price_dmt      = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY DOMESTIC PRICE (RMB per")##
        self.monthly_domestic_price_dmt      = rest["monthly_domestic_price_dmt"]
        # self.monthly_import_by_country_dmt   = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY IMPORT BY COUNTRY (dmt)")
        self.monthly_import_by_country_dmt   = rest["monthly_import_by_country_dmt"]
        # self.imported_bauxite_stocks         = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="Imported Bauxite Stocks")
        self.imported_bauxite_stocks = rest["imported_bauxite_stocks"]
        self.db                              = {}
        # Output files inputs
        self.MONTHLY_IMPORTED_TONNAGE_dmt = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY IMPORTED TONNAGE")
        self.MONTHLY_IMPORTED_TONNAGE_dmt['MONTHLY IMPORTED TONNAGE (dmt)'] = self.MONTHLY_IMPORTED_TONNAGE_dmt['MONTHLY IMPORTED TONNAGE (dmt)'].apply(lambda x: x.replace(" - ", "-"))
        self.MONTHLY_IMPORT_BY_REGION_dmt   = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY IMPORT BY REGION (dmt)")
        self.MONTHLY_IMPORT_BY_CUSTOMER_dmt = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY IMPORT BY CUSTOMER (dmt")#
        self.MONTHLY_IMPORT_BY_COUNTRY_dmt2 = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY IMPORT BY COUNTRY 2 (dm")#
        self.MONTHLY_DOMESTIC_PRICE_US      = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY DOMESTIC PRICE (US$ per")#
        new_df = pd.DataFrame(columns=self.monthly_domestic_price_dmt.columns[1:])
        self.MONTHLY_DOMESTIC_PRICE_US = self.MONTHLY_DOMESTIC_PRICE_US.join(new_df)

        #dexter
        # self.MONTHLY_AVERAGE_CFR_PRICE      = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name='Monthly Average CFR Price')
        self.MONTHLY_AVERAGE_CFR_PRICE      = rest["monthly_avg_cfr_price"]
        # self.MONTHLY_AVERAGE_VIU_CFR_PRICE  = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="Monthly Average VIU CFR Price")
        self.MONTHLY_AVERAGE_VIU_CFR_PRICE  = rest["monthly_average_viu_cfr_price"]
        self.MONTHLY_AVERAGE_CIF_PRICE_COUNTRY = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY AVERAGE CIF PRICE_CONTR")
        self.MONTHLY_AVERAGE_CFR_PRICE2     = pd.read_excel(Path(os.path.join(BASE_DIR,"Monthly Data Summary inputs.xlsx")), sheet_name="MONTHLY AVERAGE CFR PRICE 2")

        #dexter
        self.avgcalcdata  = pd.read_csv(Path(os.path.join(BASE_DIR, "outputdata/tradedata/avgcalcdata.csv")))     # trade.avgcalcdata
        self.workingsdata = pd.read_csv(Path(os.path.join(BASE_DIR, "outputdata/tradedata/workingsdata.csv")))    # trade.workingsdata
        self.customerdata = pd.read_csv(Path(os.path.join(BASE_DIR, "outputdata/tradedata/customerdata.csv")))    # trade.customerdata
        self.monthly_average_cif_price = pd.DataFrame(columns=[])
        self.monthly_average_cfr_price = pd.DataFrame(columns=[])
        self.monthly_average_viu_cfr_price = pd.DataFrame(columns=[])
        self.monthly_viu_cfr_rank = pd.DataFrame(columns=[])
        self.monthly_average_cif_price_by_country = pd.DataFrame(columns=[])
        self.monthly_average_cfr_price2 = pd.DataFrame(columns=[])

        self.sum_key = []
        self.years   = []

    def lookup(self, search, lookup, target):
        v = lookup ==  search
        
        try:
            print(target[v])
            return target[v].tolist()[0]
        except Exception as e:
            print(e)
            return np.nan

    def sumifs(self, sum_range1=None, sum_range2=None, range1=None, criteria1=None, range2=None, criteria2=None):
        ind = range1 == criteria1
        if range2 is None:
            v = ind
        else:
            idx = range2 == criteria2
            v = ind & idx

        try:
            if sum_range2 is None:
                return sum_range1[v].sum()
            else:
                return sum_range1[v].sum() / sum_range2[v].sum()
        except Exception:
            return 0

    def monthly_average_cif_price_func(self):
        years = self.imported_bauxite_stocks.columns[1:]
        self.sum_key = [ (str(years[i].month) + ' '+ str(years[i].year) ) for i in range(len(years)) ]
        self.monthly_average_cif_price['Month']     = years
        self.monthly_average_cif_price['AUS HT']    = [self.func_1('MONTHLY AVERAGE CIF PRICE', self.sum_key[i]+'AUS HT') for i in range(len(years))]
        self.monthly_average_cif_price['AUS LT']    = [self.func_1('MONTHLY AVERAGE CIF PRICE', self.sum_key[i]+'AUS LT') for i in range(len(years))]
        self.monthly_average_cif_price['SMB']       = [self.func_1('MONTHLY AVERAGE CIF PRICE', self.sum_key[i]+'SMB') for i in range(len(years))]
        self.monthly_average_cif_price['Indonesia'] = [self.func_1('MONTHLY AVERAGE CIF PRICE', self.sum_key[i]+'Indonesia') for i in range(len(years))]
        self.monthly_average_cif_price['Brazil']    = [self.func_1('MONTHLY AVERAGE CIF PRICE', self.sum_key[i]+'Brazil') for i in range(len(years))]
        self.years=years
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cif price.csv"))] = self.monthly_average_cif_price

    def monthly_average_cfr_price_func(self):
        self.monthly_average_cfr_price['Month']     = self.years
        self.monthly_average_cfr_price['AUS HT']    = [self.func_1('MONTHLY AVERAGE CFR PRICE', self.sum_key[i]+'AUS HT') for i in range(len(self.years))]
        self.monthly_average_cfr_price['AUS LT']    = [self.func_1('MONTHLY AVERAGE CFR PRICE', self.sum_key[i]+'AUS LT') for i in range(len(self.years))]
        self.monthly_average_cfr_price['SMB']       = [self.func_1('MONTHLY AVERAGE CFR PRICE', self.sum_key[i]+'SMB') for i in range(len(self.years))]
        self.monthly_average_cfr_price['Indonesia'] = [self.func_1('MONTHLY AVERAGE CFR PRICE', self.sum_key[i]+'Indonesia') for i in range(len(self.years))]
        self.monthly_average_cfr_price['Brazil']    = [self.func_1('MONTHLY AVERAGE CFR PRICE', self.sum_key[i]+'Brazil') for i in range(len(self.years))]
        data = self.MONTHLY_AVERAGE_CFR_PRICE.copy()
        data = data.set_index('MONTHLY AVERAGE CFR PRICE (US$/dmt)').T.reset_index()
        for i in self.monthly_average_cfr_price.index:
            self.monthly_average_cfr_price.loc[i,'AUS HT']    =self.monthly_average_cfr_price.loc[i,'AUS HT'] if  pd.isna(data.loc[i,'AUS HT']) else data.loc[i,'AUS HT']
            self.monthly_average_cfr_price.loc[i,'AUS LT']    =self.monthly_average_cfr_price.loc[i,'AUS LT'] if  pd.isna(data.loc[i,'AUS LT']) else data.loc[i,'AUS LT']
            self.monthly_average_cfr_price.loc[i,'SMB']       =self.monthly_average_cfr_price.loc[i,'SMB'] if  pd.isna(data.loc[i,'SMB']) else data.loc[i,'SMB']
            self.monthly_average_cfr_price.loc[i,'Indonesia'] =self.monthly_average_cfr_price.loc[i,'Indonesia'] if  pd.isna(data.loc[i,'Indonesia']) else data.loc[i,'Indonesia']
            self.monthly_average_cfr_price.loc[i,'Brazil']    =self.monthly_average_cfr_price.loc[i,'Brazil'] if  pd.isna(data.loc[i,'Brazil']) else data.loc[i,'Brazil']
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cfr price.csv"))] = self.monthly_average_cfr_price

    def monthly_average_viu_cfr_price_func(self):
        self.monthly_average_viu_cfr_price['Month']     = self.years
        self.monthly_average_viu_cfr_price['AUS HT']    = [float(self.func_1('MONTHLY AVERAGE VIU CFR PRICE', self.sum_key[i]+'AUS HT')) for i in range(len(self.years))]
        self.monthly_average_viu_cfr_price['AUS LT']    = [float(self.func_1('MONTHLY AVERAGE VIU CFR PRICE', self.sum_key[i]+'AUS LT')) for i in range(len(self.years))]
        self.monthly_average_viu_cfr_price['SMB']       = [float(self.func_1('MONTHLY AVERAGE VIU CFR PRICE', self.sum_key[i]+'SMB')) for i in range(len(self.years))]
        self.monthly_average_viu_cfr_price['Indonesia'] = [float(self.func_1('MONTHLY AVERAGE VIU CFR PRICE', self.sum_key[i]+'Indonesia')) for i in range(len(self.years))]
        self.monthly_average_viu_cfr_price['Brazil']    = [float(self.func_1('MONTHLY AVERAGE VIU CFR PRICE', self.sum_key[i]+'Brazil')) for i in range(len(self.years))]
        data = self.MONTHLY_AVERAGE_VIU_CFR_PRICE.copy()
        data = data.set_index('MONTHLY AVERAGE VIU CFR PRICE (US$/dmt)').T.reset_index()
        for i in self.monthly_average_viu_cfr_price.index:
            self.monthly_average_viu_cfr_price.loc[i,'AUS HT']      = self.monthly_average_viu_cfr_price.loc[i,'AUS HT'] if pd.isna(data.loc[i,'AUS HT']) else data.loc[i,'AUS HT']
            self.monthly_average_viu_cfr_price.loc[i,'AUS LT']      = self.monthly_average_viu_cfr_price.loc[i,'AUS LT'] if  pd.isna(data.loc[i,'AUS LT']) else data.loc[i,'AUS LT']
            self.monthly_average_viu_cfr_price.loc[i,'SMB']         = self.monthly_average_viu_cfr_price.loc[i,'SMB'] if  pd.isna(data.loc[i,'SMB']) else data.loc[i,'SMB']
            self.monthly_average_viu_cfr_price.loc[i,'Indonesia']   = self.monthly_average_viu_cfr_price.loc[i,'Indonesia'] if  pd.isna(data.loc[i,'Indonesia']) else data.loc[i,'Indonesia']
            self.monthly_average_viu_cfr_price.loc[i,'Brazil']      = self.monthly_average_viu_cfr_price.loc[i,'Brazil'] if  pd.isna(data.loc[i,'Brazil']) else data.loc[i,'Brazil']
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average viu cfr price.csv"))] = self.monthly_average_viu_cfr_price

    def monthly_viu_cfr_rank_func(self):
        self.monthly_viu_cfr_rank['Month']      = self.years
        self.monthly_viu_cfr_rank['AUS HT']     = [self.func_2(self.monthly_average_viu_cfr_price.loc[i,'AUS HT':], self.monthly_average_viu_cfr_price.loc[i,'AUS HT']) for i in range(len(self.years))]
        self.monthly_viu_cfr_rank['AUS LT']     = [self.func_2(self.monthly_average_viu_cfr_price.loc[i,'AUS HT':], self.monthly_average_viu_cfr_price.loc[i,'AUS LT']) for i in range(len(self.years))]
        self.monthly_viu_cfr_rank['SMB']        = [self.func_2(self.monthly_average_viu_cfr_price.loc[i,'AUS HT':], self.monthly_average_viu_cfr_price.loc[i,'SMB']) for i in range(len(self.years))]
        self.monthly_viu_cfr_rank['Indonesia']  = [self.func_2(self.monthly_average_viu_cfr_price.loc[i,'AUS HT':], self.monthly_average_viu_cfr_price.loc[i,'Indonesia']) for i in range(len(self.years))]
        self.monthly_viu_cfr_rank['Brazil']     = [self.func_2(self.monthly_average_viu_cfr_price.loc[i,'AUS HT':], self.monthly_average_viu_cfr_price.loc[i,'Brazil']) for i in range(len(self.years))]
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average viu cfr rank.csv"))] =self.monthly_viu_cfr_rank

    def monthly_average_cif_price_by_country_func(self):
        #self.MONTHLY_AVERAGE_CIF_PRICE_COUNTRY
        self.monthly_average_cif_price_by_country['Month']      = self.years
        for col in self.MONTHLY_AVERAGE_CIF_PRICE_COUNTRY['MONTHLY AVERAGE CIF PRICE BY COUNTRY (US$/dmt)']:
            self.monthly_average_cif_price_by_country[col] = [self.func_3( self.sum_key[i]+str(col)) for i in range(len(self.years))]
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cif price by country.csv"))] = self.monthly_average_cif_price_by_country

    def monthly_average_cfr_price_2_func(self):
        self.monthly_average_cfr_price2['Month']      = self.years
        for col in self.MONTHLY_AVERAGE_CFR_PRICE2['MONTHLY AVERAGE CFR PRICE (US$/dmt)']:
            col = col.replace(' ','')
            self.monthly_average_cfr_price2[col] = [self.func_4( self.sum_key[i]+str(col)) for i in range(len(self.years))]
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cfr price 2.csv"))]  = self.monthly_average_cfr_price2

        self.monthly_average_cif_price  = self.monthly_average_cif_price.set_index('Month').T.reset_index().rename(columns={'index':'Month'}).fillna(0)
        self.monthly_average_cfr_price  = self.monthly_average_cfr_price.set_index('Month').T.reset_index().rename(columns={'index':'Month'}).fillna(0)
        self.monthly_average_viu_cfr_price  = self.monthly_average_viu_cfr_price.set_index('Month').T.reset_index().rename(columns={'index':'Month'}).fillna(0)
        self.monthly_viu_cfr_rank   = self.monthly_viu_cfr_rank.set_index('Month').T.reset_index().rename(columns={'index':'Month'}).fillna(0)
        self.monthly_average_cif_price_by_country   = self.monthly_average_cif_price_by_country.set_index('Month').T.reset_index().rename(columns={'index':'Month'}).fillna(0)
        self.monthly_average_cfr_price2 = self.monthly_average_cfr_price2.set_index('Month').T.reset_index().rename(columns={'index':'Month'})

        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cif price.csv"))]      = self.monthly_average_cif_price
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cfr price.csv"))]      = self.monthly_average_cfr_price
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average viu cfr price.csv"))]  = self.monthly_average_viu_cfr_price
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average viu cfr rank.csv"))]   = self.monthly_viu_cfr_rank
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cif price by country.csv"))] = self.monthly_average_cif_price_by_country
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/Monthly average cfr price 2.csv"))]    = self.monthly_average_cfr_price2

    def func_1(self,col_name, sum_key):
        value = 0
        d = {self.avgcalcdata['MONTH KEY'][x]:self.avgcalcdata[col_name][x]  for x in range(len(self.avgcalcdata))}
        try:
            value = d[sum_key]
        except:
            value = np.nan
        return value

    def func_2(self, arry, value):
        result = 0
        rank = sorted(arry,reverse=True)
        try:
            result = rank.index(value) + 1
        except:
            result =np.nan
        return result

    def func_3(self, sum_key):
        value = 0
        d = {self.workingsdata['lookup key'][x]:self.workingsdata['monthly average cif price by country'][x]  for x in range(len(self.workingsdata))}
        try:
            value = d[sum_key]
        except:
            value = np.nan
        value = 0 if pd.isna(value) else value
        return value

    def func_4(self, sum_key):
        value = 0
        d = {self.customerdata['lookupkey'][x]:self.customerdata['monthly average cfr price'][x]  for x in range(len(self.customerdata))}
        try:
            value = d[sum_key]
        except:
            value = np.nan
        value = 0 if pd.isna(value) else value
        return value

        
    def monthly_import_by_tonnage_dmt(self):
        df = self.MONTHLY_IMPORTED_TONNAGE_dmt.copy()
        total_index = self.totals_index(df, 1)
        
        for i in range(df.shape[0]):
            for col in df.columns[3:]:
                v = self.lookup(
                        f'{col.month} {col.year}{self.MONTHLY_IMPORTED_TONNAGE_dmt.loc[i, "MONTHLY IMPORTED TONNAGE (dmt)"]}',
                        self.customerdata.loc[:, 'lookupkey'],
                        self.customerdata.loc[:, 'import tonnage']
                    )
                if pd.isna(v):
                    self.MONTHLY_IMPORTED_TONNAGE_dmt.at[i, col]       = 0
                else:
                    self.MONTHLY_IMPORTED_TONNAGE_dmt.at[i, col]       = v

        for col in df.columns[3:]:
            self.MONTHLY_IMPORTED_TONNAGE_dmt.at[total_index, col] = self.MONTHLY_IMPORTED_TONNAGE_dmt.loc[:total_index, col].sum()
            self.MONTHLY_IMPORTED_TONNAGE_dmt.at[total_index+1, col] = self.MONTHLY_IMPORTED_TONNAGE_dmt.loc[total_index, col] * 12/10**6

        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_by_tonnage_dmt.csv"))] = self.MONTHLY_IMPORTED_TONNAGE_dmt


    def monthly_import_by_region_dmt(self):
        df = self.MONTHLY_IMPORT_BY_REGION_dmt.copy()
        total_index = self.totals_index(df, 1)

        for i in range(df.shape[0]):
            for col in df.columns[2:]:
                self.MONTHLY_IMPORT_BY_REGION_dmt.at[i, col]       = self.sumifs(
                    sum_range1  =   self.MONTHLY_IMPORTED_TONNAGE_dmt.loc[:, col],            # sum_range1
                    range1      =   self.MONTHLY_IMPORTED_TONNAGE_dmt.loc[:, 'Region Key'],        # range1
                    criteria1   =   self.MONTHLY_IMPORT_BY_REGION_dmt.loc[i, 'MONTHLY IMPORT BY REGION (dmt)']                # criteria1
                )
        
        for col in df.columns[2:]:
            self.MONTHLY_IMPORT_BY_REGION_dmt.at[total_index, col] = self.MONTHLY_IMPORT_BY_REGION_dmt.loc[:total_index-2, col].sum()

        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_by_region_dmt.csv"))] = self.MONTHLY_IMPORT_BY_REGION_dmt

    def monthly_import_by_customer(self):
        df = self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.copy()
        total_index = self.totals_index(df, 1)
        df2 = self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.copy()
        for i in range(df.shape[0]):
            for col in df.columns[1:]:
                self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.at[i, col]       = self.sumifs(
                    sum_range1  =   self.MONTHLY_IMPORTED_TONNAGE_dmt.loc[:, col],         # sum_range1
                    range1      =   self.MONTHLY_IMPORTED_TONNAGE_dmt.loc[:, 'Customer Key'],       # range1
                    criteria1   =   self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.loc[i, 'MONTHLY IMPORT BY CUSTOMER (dmt)']                # criteria1
                )
        df2.iloc[:3,1:] = self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.iloc[:3,1:]
        df2.iloc[3,1:] = self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.iloc[3:15,1:].sum()

        for col in df.columns[1:]:
            self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.at[total_index, col] = self.MONTHLY_IMPORT_BY_CUSTOMER_dmt.loc[:total_index-1, col].sum()
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_by_customer.csv"))] = self.MONTHLY_IMPORT_BY_CUSTOMER_dmt
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_by_customer2.csv"))] = df2



    def bauxite_reqrmt_supply_func(self):
        df = self.bauxite_reqrmt_supply.copy()
        total_index = self.totals_index(df, 0)

        for col in df.columns[1:]:
            df.at[total_index, col] = df.loc[:total_index-2, col].sum()

        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/bauxite_requirement_supply.csv"))] = df

    def alumina_by_brovince_func(self):
        df = self.alumina_by_brovince.copy()
        total_index = self.totals_index(df, 0)

        for col in df.columns[1:]:
            df.at[total_index, col] = df.loc[:total_index-1, col].sum()

        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/alumina_by_brovince.csv"))] = df


    def imported_bauxite_stocks_func(self):
        df = self.imported_bauxite_stocks.copy()
        total_index = self.totals_index(df, 0)

        for col in df.columns[1:]:
            df.at[total_index, col] = df.loc[:total_index-2, col].sum()

        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/imported_bauxite_stocks.csv"))] = df

    def totals_index(self, df, lookup_col):
        try:
            total_index = df[df.iloc[:,lookup_col].map(lambda x: x.lower() == "total")].index.to_numpy()[-1]
        except Exception:
            total_index = int(df.shape[0]-1)
        return total_index


    def calc_month_import_by_country(self):
        # self.monthly_import_by_country_dmt
        mibr = self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_by_region_dmt.csv"))]
        cols = range(15, len(mibr.columns))
        cols = [mibr.columns[a] for a in cols]
        result = [mibr.loc[mibr["Country Key"] == self.monthly_import_by_country_dmt.loc[i, "MONTHLY IMPORT BY COUNTRY (dmt)"], cols].sum() for i in range(self.monthly_import_by_country_dmt.shape[0])]
        self.monthly_import_by_country_dmt.loc[:, 14:] = result

        monthly_import_dmt_1 = pd.DataFrame(columns=self.monthly_import_by_country_dmt.columns)
        monthly_import_dmt_1["MONTHLY IMPORT BY COUNTRY (dmt)"] = ["Guinea","Australia","Indonesia","Brazil","Malaysia","India","Others","TOTAL"]
        self.MONTHLY_IMPORT_BY_COUNTRY_dmt2 = monthly_import_dmt_1.copy()
        cols1 = self.monthly_import_by_country_dmt.columns[1:]
        x = self.monthly_import_by_country_dmt.loc[self.monthly_import_by_country_dmt["MONTHLY IMPORT BY COUNTRY (dmt)"].isin(["Guinea","Australia","Indonesia","Brazil","Malaysia","India"]), cols1] / (10**6)
        x = x.reset_index()
        print(x)
        monthly_import_dmt_1.iloc[:6, 1:] = x
        monthly_import_dmt_1.iloc[6, 1:] =  self.monthly_import_by_country_dmt.loc[~self.monthly_import_by_country_dmt["MONTHLY IMPORT BY COUNTRY (dmt)"].isin(["Guinea","Australia","Indonesia","Brazil","Malaysia","India"]), cols1].sum() / (10**6)
        monthly_import_dmt_1.iloc[7, 1:] = monthly_import_dmt_1.iloc[:7, 1:].sum()
        print("monthly import")
        print(self.monthly_import_by_country_dmt.loc[9])
        print(monthly_import_dmt_1.loc[5])

        self.temp_mon = monthly_import_dmt_1
        temp_denom = monthly_import_dmt_1.iloc[7, 1:][1:].replace(0, np.nan)
        self.MONTHLY_IMPORT_BY_COUNTRY_dmt2.iloc[:6, 1:] = monthly_import_dmt_1.iloc[:6, 1:] / temp_denom
        self.MONTHLY_IMPORT_BY_COUNTRY_dmt2.iloc[:6, 1:] = self.MONTHLY_IMPORT_BY_COUNTRY_dmt2.iloc[:6, 1:].replace(np.nan, 0)
        self.MONTHLY_IMPORT_BY_COUNTRY_dmt2.iloc[7, 1:]  = self.MONTHLY_IMPORT_BY_COUNTRY_dmt2.iloc[:6, 1:].sum()
        self.MONTHLY_IMPORT_BY_COUNTRY_dmt2.iloc[6, 1:] = monthly_import_dmt_1.iloc[6, 1:]/monthly_import_dmt_1.iloc[7, 1:]
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_by_country_dmt.csv"))] = self.monthly_import_by_country_dmt
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_dmt_1.csv"))] = monthly_import_dmt_1
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_import_dmt_2.csv"))] = self.MONTHLY_IMPORT_BY_COUNTRY_dmt2


        # print(result)

    def calc_monthly_domestic_price(self):
        result = np.array([self.weeklydatasumm.domesticmonthly.loc[self.weeklydatasumm.domesticmonthly["month"].astype(str) == str(self.monthly_domestic_price_dmt.columns[i]), ['Shanxi4.5 - 5.0', 'Henan4.0 - 5.0', 'Guizhou 5.5 - 6.5','Guangxi 7.0 - 8.0']].values for i in range(17,self.monthly_domestic_price_dmt.shape[1])]).transpose()
        result = list(map(lambda x: x[0], result))
        self.monthly_domestic_price_dmt.iloc[:, 17:] = result
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_domestic_price_dmt.csv"))] = self.monthly_domestic_price_dmt

    def calc_monthly_domestic_price_us(self):
        print(self.sum_key)
        looks = [self.lookup_sheet.loc[self.lookup_sheet["MONTH"] == self.sum_key[i], "RMB_per_US1"].values[0] for i in range(4,len(self.sum_key))]
        self.MONTHLY_DOMESTIC_PRICE_US.iloc[0, 5:] = looks

        calc = np.array([list(map('-'.join, np.array(list(map(str, self.weeklydatasumm.lookupkey.loc[self.weeklydatasumm.lookupkey["lookup key"].astype(str) == str(self.MONTHLY_DOMESTIC_PRICE_US.columns[i]), ['2', '3', '4', '5', '6', '7', '8', '9']].values[0]))).reshape(-1, 2).tolist())) for i in range(5,self.MONTHLY_DOMESTIC_PRICE_US.shape[1])]).transpose()
        self.MONTHLY_DOMESTIC_PRICE_US.iloc[1:, 5:] = calc
        self.db[Path(os.path.join(BASE_DIR,"outputdata/monthly/monthly_domestic_price_us.csv"))] = self.MONTHLY_DOMESTIC_PRICE_US



    def calc_all(self):
        #dexter
        self.monthly_average_cif_price_func()
        self.monthly_average_cfr_price_func()
        self.monthly_average_viu_cfr_price_func()
        self.monthly_viu_cfr_rank_func()
        self.monthly_average_cif_price_by_country_func()
        self.monthly_average_cfr_price_2_func()

        #dunfred
        self.monthly_import_by_tonnage_dmt()
        self.monthly_import_by_region_dmt()
        self.monthly_import_by_customer()
        self.bauxite_reqrmt_supply_func()
        self.alumina_by_brovince_func()
        self.imported_bauxite_stocks_func()
        self.calc_month_import_by_country()
        self.calc_monthly_domestic_price()
        self.calc_monthly_domestic_price_us()
        #Save files
        for filepath, file in self.db.items():
            dirname = os.path.dirname(filepath)
            if os.path.exists(dirname):
                pass
            else:
                os.mkdir(dirname)

            print(filepath)
            filepath_s = str(filepath).replace('\\', '/')
            name = f"{filepath_s.split('/')[-2]} {filepath_s.split('/')[-1][:-4]}"
            print(name)
            if name == "monthly monthly_import_by_region_dmt":
                dblist.append(db_conv.mult_year_single_output(file, name, idx_of_index=[[0,2]], idx_of_values=[[2,]], label="Date"))
            else:
                dblist.append(db_conv.mult_year_single_output(file, name, idx_of_index=[[0,1]], idx_of_values=[[1,]], label="Date"))
            file.to_csv(filepath, index=True)

trade = TradeWithProcessing()
trade.calcall()

# monthly = MonthlyDataSummary(trade)
monthly = MonthlyDataSummary()
monthly.calc_all()


snapshot = pd.concat(dblist, ignore_index=True)
uploadtodb.upload(snapshot)
snapshot = snapshot.to_csv("snapshot_output_data", index=False)
