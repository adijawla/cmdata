import weekly_charts_db
import pandas as pd
import datetime as dt
import warnings
from outputdb import uploadtodb
from flatdb.flatdbconverter import Flatdbconverter

warnings.filterwarnings("ignore")

db_conv = Flatdbconverter("Weekly Chart Pack")


dblist = []


class Causticprice():
    def __init__(self):
        self.cpdata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\causticpricedata.csv")
        self.ddata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\domesticpricerawdata1.csv")
        self.caustic_price_data = pd.DataFrame(
            columns=["month", "data", "Shandong", "Henan", "Shanxi"])
        self.caustic_check = pd.DataFrame(
            columns=["check1", "check2", "check3"])
        self.domesticpricedata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\domesticpricedata1.csv")
        self.caustic_check1 = pd.DataFrame(
            columns=["check1", "check2", "check3"])

    def liquidcausticshandong(self, indx):
        d = [self.cpdata["Shandong L1"][indx],
             self.cpdata["Shandong L2"][indx]]
        v = (d[0]+d[1])/2
        self.caustic_price_data.at[indx, "Shandong"] = v

    def liquidcausticmonth(self, indx):
        d = [self.cpdata["date"][indx]]
        # dexter remove unit of datetime
        v = pd.TimedeltaIndex(d, unit='d') + dt.datetime(1900, 1, 1)
        self.caustic_price_data.at[indx, "month"] = v[0].date()

    def liquidcaustichenan(self, indx):
        d = [self.cpdata["Henan L1"][indx], self.cpdata["Henan L2"][indx]]
        v = (d[0]+d[1])/2
        self.caustic_price_data.at[indx, "Henan"] = v

    def liquidcausticshanxi(self, indx):
        d = [self.cpdata["Shanxi L1"][indx], self.cpdata["Shanxi L2"][indx]]
        v = (d[0]+d[1])/2
        self.caustic_price_data.at[indx, "Shanxi"] = v

    def causticcheck1(self, indx):
        d = [self.cpdata["Shandong L1"][indx], self.cpdata["Shandong L2"]
             [indx], self.caustic_price_data["Shandong"][indx]]
        v = ((d[0]+d[1])/2)-d[0]
        self.caustic_check.at[indx, "check1"] = v

    def causticcheck2(self, indx):
        d = [self.cpdata["Shanxi L1"][indx], self.cpdata["Shanxi L2"]
             [indx], self.caustic_price_data["Shanxi"][indx]]
        v = ((d[0]+d[1])/2)-d[2]
        self.caustic_check.at[indx, "check2"] = v

    def causticcheck3(self, indx):
        d = [self.cpdata["Henan L1"][indx], self.cpdata["Henan L2"]
             [indx], self.caustic_price_data["Henan"][indx]]
        v = ((d[0]+d[1])/2)-d[2]
        self.caustic_check.at[indx, "check3"] = v

    def domesticshanxi(self, indx):
        if self.domesticpricedata["Shanxi 4.5 - 5.0"][indx] == 0:
            d = [self.ddata["shanxi4.5"][indx], self.ddata["shanxi5"][indx]]
            v = (d[0]+d[1])/2
            self.domesticpricedata.at[indx, "Shanxi 4.5 - 5.0"] = v

    def domestichenan(self, indx):
        if self.domesticpricedata["Henan 4.0 - 5.0"][indx] == 0:
            d = [self.ddata["henan4"][indx], self.ddata["henan5"][indx]]
            v = (d[0]+d[1])/2
            self.domesticpricedata.at[indx, "Henan 4.0 - 5.0"] = v

    def domesticguizhou(self, indx):
        if self.domesticpricedata["Guizhou 5.5 - 6.5"][indx] == 0:
            d = [self.ddata["guizhou5.5"][indx],
                 self.ddata["guizhou6.5"][indx]]
            v = (d[0]+d[1])/2
            self.domesticpricedata.at[indx, "Guizhou 5.5 - 6.5"] = v

    def domesticcheck1(self, indx):
        d = [self.ddata["shanxi4.5"][indx], self.ddata["shanxi5"]
             [indx], self.domesticpricedata["Shanxi 4.5 - 5.0"][indx]]
        v = ((d[0]+d[1])/2)-d[2]
        self.caustic_check1.at[indx, "check 1"] = v

    def domesticcheck2(self, indx):
        d = [self.ddata["henan4"][indx], self.ddata["henan5"]
             [indx], self.domesticpricedata["Henan 4.0 - 5.0"][indx]]
        v = ((d[0]+d[1])/2)-d[2]
        self.caustic_check1.at[indx, "check 2"] = v

    def domesticcheck3(self, indx):
        d = [self.ddata["guizhou5.5"][indx], self.ddata["guizhou6.5"]
             [indx], self.domesticpricedata["Guizhou 5.5 - 6.5"][indx]]
        v = ((d[0]+d[1])/2)-d[2]
        self.caustic_check1.at[indx, "check 3"] = v

    def calcall(self):
        for i in range(self.cpdata.shape[0]):
            Causticprice.liquidcausticshandong(self, i)
            Causticprice.liquidcaustichenan(self, i)
            Causticprice.liquidcausticshanxi(self, i)
            Causticprice.liquidcausticmonth(self, i)
            Causticprice.causticcheck1(self, i)
            Causticprice.causticcheck2(self, i)
            Causticprice.causticcheck3(self, i)
        for i in range(self.ddata.shape[0]):
            Causticprice.domesticshanxi(self, i)
            Causticprice.domestichenan(self, i)
            Causticprice.domesticguizhou(self, i)
            Causticprice.domesticcheck1(self, i)
            Causticprice.domesticcheck2(self, i)
            Causticprice.domesticcheck3(self, i)


class Frieght():
    def __init__(self):
        self.frieghtdata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\frieghtdata.csv")
        self.dailypricedata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\dailypricedata.csv")
        self.WeeklyChartDataFrame = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\cbixHitoricalDailyPrice.csv")
        self.cmaaxrollingdataFrame = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\cmaaxrollingdata1.csv")
        self.cmaxrollingYearDataFrame = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\cmaxrollingYearData.csv")
        self.cockpit = 43980
        self.frieghtrollingdata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\frieghtrollingdata.csv")
        self.cbixrollingdata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\cbixrollingdata.csv")
        self.cmaaxdata1 = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\cmaaxdata1.csv")
        self.cmaaxdata2 = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\cmaaxdata2.csv")
        self.cbixRollingYearData = pd.DataFrame(
            columns=["day", "date", "CBIX1", "CBIX2"])
        self.cmaaxrollingdata = pd.read_csv(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\Inputs\\cmaaxrollingdata.csv")
        self.cmaxrollingYearData = pd.DataFrame(
            columns=["date", "northprice", "southprice", "wacif"])

    # no such column as 'week ending' error
    def weekbeforecbx(self, indx):
        d = [self.frieghtdata["week"][indx]]
        v = (self.cockpit-d[0])//7
        self.frieghtdata.at[indx, "weekbeforecbx"] = v

    def daybeforecbx(self, indx):
        d = [self.dailypricedata["date"][indx]]
        v = self.cockpit-d[0]
        self.dailypricedata.at[indx, "daybeforecbx"] = v

    def frieghtrollingcapesize(self, indx):
        d = [self.frieghtdata.loc[self.frieghtdata.weekbeforecbx ==
                                  self.frieghtrollingdata["week"][indx]]["guinea,capesize"].sum()]
        v = d[0]
        self.frieghtrollingdata.at[indx, "capesize"] = v

    def frieghtrollingpanamax(self, indx):
        d = [self.frieghtdata.loc[self.frieghtdata.weekbeforecbx ==
                                  self.frieghtrollingdata["week"][indx]]["australia,panamax"].sum()]
        v = d[0]
        self.frieghtrollingdata.at[indx, "panamax"] = v

    def frieghtrollingbci(self, indx):
        d = [self.frieghtdata.loc[self.frieghtdata.weekbeforecbx ==
                                  self.frieghtrollingdata["week"][indx]]["brazil,capesize"].sum()]
        v = d[0]
        self.frieghtrollingdata.at[indx, "BCI"] = v

    def frieghtrollingbpi(self, indx):
        d = [self.frieghtdata.loc[self.frieghtdata.weekbeforecbx ==
                                  self.frieghtrollingdata["week"][indx]]["brazil,capesize"].sum()]
        v = 0
        self.frieghtrollingdata.at[indx, "BPI"] = v

    def frieghtrollingdate(self, indx):
        d = [self.frieghtdata.loc[self.frieghtdata.weekbeforecbx ==
                                  self.frieghtrollingdata["week"][indx]]["brazil,capesize"].sum()]
        v = d[0]
        self.frieghtrollingdata.at[indx, "Date"] = v

    def cbxrollingdate(self, indx):
        d = [self.dailypricedata.loc[self.dailypricedata.daybeforecbx ==
                                     self.cbixrollingdata["Day"][indx]]["date"].sum()]
        v = d[0]
        self.cbixrollingdata.at[indx, "Date"] = v

    def cbxrollingcbx1(self, indx):
        d = [self.dailypricedata.loc[self.dailypricedata.daybeforecbx ==
                                     self.cbixrollingdata["Day"][indx]]["cbixusd"].sum()]
        v = d[0]
        self.cbixrollingdata.at[indx, "CBIX1"] = v

    def cbxrollingcbx2(self, indx):
        d = [self.cbixrollingdata["CBIX1"][indx]]
        v = d[0]
        self.cbixrollingdata.at[indx, "CBIX2"] = v

    def cmaax1day(self, indx):
        d = []
        v = self.cockpit - self.cmaaxdata1["date"][indx]
        self.cmaaxdata1.at[indx, "day"] = v

    def cmaax1wacif(self, indx):
        d = []
        try:
            check = int(self.cmaaxdata1["date"][indx])
        except:
            check = 0
        v = self.cmaaxdata2.loc[self.cmaaxdata2["date"]
                                == check]["wacifrmb"].sum()
        self.cmaaxdata1.at[indx, "wacif"] = v

    def cmaax2wacifrmb(self, indx):
        d = []
        v = self.cmaaxdata2["wacif"][indx]*self.cmaaxdata2["usdrmb"][indx]
        self.cmaaxdata2.at[indx, "wacifrmb"] = v

    def weeklychartdata(self, indx):
        d = [pd.DatetimeIndex([self.WeeklyChartDataFrame["date"][indx]]),
             self.WeeklyChartDataFrame["cbix"][indx]]
        v1 = d[0][0].day_name()
        v2 = str(d[0][0].date())
        v3 = d[1]
        v4 = d[1] if d[0][0].day_name() == "Friday" else 0
        self.cbixRollingYearData.at[indx, "day"] = v1
        self.cbixRollingYearData.at[indx, "date"] = v2
        self.cbixRollingYearData.at[indx, "CBIX1"] = v3
        self.cbixRollingYearData.at[indx, "CBIX2"] = v4

    def cmaaxrollingdata1(self, indx):
        d = [self.cmaaxrollingdataFrame["Date"][indx],
             self.cmaaxrollingdataFrame["NAX"][indx],
             self.cmaaxrollingdataFrame["SAX"][indx],
             self.cmaaxrollingdataFrame["WA CIF RMB/t"][indx]]
        v1 = d[0]
        v2 = str(d[1]).replace(",", "")
        v3 = str(d[2]).replace(",", "") if not pd.isnull(d[2]) else 0
        v4 = str(d[3]).replace(",", "").replace(" ", "")
        self.cmaaxrollingdata.at[indx, "Date"] = str(v1)
        self.cmaaxrollingdata.at[indx, "NAX"] = v2
        self.cmaaxrollingdata.at[indx, "SAX"] = v3
        self.cmaaxrollingdata.at[indx, "wacif"] = float(v4)

    def cmaaxrollingdata2(self, indx):
        d = [self.cmaxrollingYearDataFrame["Date.1"][indx],
             self.cmaxrollingYearDataFrame["Northern Price"][indx],
             self.cmaxrollingYearDataFrame["Southern Price"][indx],
             self.cmaxrollingYearDataFrame["WA CIF VAT incl."][indx],
             ]
        v1 = d[0]
        v2 = d[1]
        v3 = d[2]
        if not pd.isnull(d[1]):
            if pd.isnull(d[3]):
                if indx == 0:
                    v4 = 0
                else:
                    tmp = indx
                    while pd.isnull(self.cmaxrollingYearDataFrame["WA CIF VAT incl."][tmp]):
                        tmp -= 1
                    v4 = self.cmaxrollingYearDataFrame["WA CIF VAT incl."][tmp]
            else:
                v4 = d[3]
        else:
            v4 = 0
        self.cmaxrollingYearData.at[indx, "date"] = v1
        self.cmaxrollingYearData.at[indx, "northprice"] = v2
        self.cmaxrollingYearData.at[indx, "southprice"] = v3
        self.cmaxrollingYearData.at[indx, "wacif"] = v4

    def calcall(self):
        for i in range(self.frieghtdata.shape[0]):
            Frieght.weekbeforecbx(self, i)
        for i in range(self.dailypricedata.shape[0]):
            Frieght.daybeforecbx(self, i)
        for i in range(self.frieghtrollingdata.shape[0]):
            Frieght.frieghtrollingcapesize(self, i)
            Frieght.frieghtrollingpanamax(self, i)
            Frieght.frieghtrollingbci(self, i)
            Frieght.frieghtrollingbpi(self, i)
            Frieght.frieghtrollingdate(self, i)

        for i in range(self.cbixrollingdata.shape[0]):
            Frieght.cbxrollingdate(self, i)
            Frieght.cbxrollingcbx1(self, i)
            Frieght.cbxrollingcbx2(self, i)

        for i in range(self.cmaaxdata1.shape[0]):
            # Frieght.cmaax1day(self,i)
            Frieght.cmaax1wacif(self, i)
            Frieght.cmaax2wacifrmb(self, i)

        for i in range(self.cmaaxrollingdata.shape[0]):
            Frieght.cmaaxrollingdata1(self, i)
            Frieght.cmaaxrollingdata2(self, i)

        for i in range(self.WeeklyChartDataFrame.shape[0]):
            Frieght.weeklychartdata(self, i)


def main():
    caustic_price = Causticprice()
    freight = Frieght()
    caustic_price.calcall()
    freight.calcall()

    caustic_data_frames = ['caustic_price.caustic_price_data',
                           'caustic_price.caustic_check',
                           'caustic_price.domesticpricedata',
                           'caustic_price.caustic_check1']

    freight_data_frames = ['freight.frieghtdata',
                           'freight.dailypricedata',
                           'freight.frieghtrollingdata',
                           'freight.cbixrollingdata',
                           'freight.cbixRollingYearData',
                           'freight.cmaaxrollingdata',
                           'freight.cmaxrollingYearData',
                           'freight.cmaaxdata1',
                           'freight.cmaaxdata2']

    for frame in caustic_data_frames:
        dblist.append(db_conv.single_year_mult_out(
            eval(frame), (str(frame).split('.'))[1]))
        eval(frame).to_excel(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\output\{(str(frame).split('.'))[1]}.xlsx", float_format='%.3f')

    for frame in freight_data_frames:
        dblist.append(db_conv.single_year_mult_out(
            eval(frame), (str(frame).split('.'))[1]))
        eval(frame).to_excel(
            r"c:\Users\magmarkd1\Desktop\cmdata\models\calculation engines\Weekly Chart Pack\output\{(str(frame).split('.'))[1]}.xlsx", float_format='%.3f')

    ''' 
    same output with multiple sheets
    using OPENPYXL module to create single xlsx file with multiple sheets as output
    [optional]
    '''
    # with pd.ExcelWriter('output/caustic_output.xlsx') as writer:
    #     caustic_price.outputdata1.to_excel(writer, sheet_name='outputdata1')
    #     caustic_price.outputdata2.to_excel(writer, sheet_name='outputdata2')
    #     caustic_price.outputdata3.to_excel(writer, sheet_name='outputdata3')
    #     caustic_price.outputdata4.to_excel(writer, sheet_name='outputdata4')
    # with pd.ExcelWriter('output/frieght_output.xlsx') as writer:
    #     freight.fdata.to_excel(writer, sheet_name='fdata')
    #     freight.ddata.to_excel(writer, sheet_name='ddata')
    #     freight.outputdata3.to_excel(writer, sheet_name='outputdata3')
    #     freight.outputdata4.to_excel(writer, sheet_name='outputdata4')
    #     freight.outputdata5.to_excel(writer, sheet_name='outputdata5')
    #     freight.outputdata6.to_excel(writer, sheet_name='outputdata6')
    #     freight.outputdata7.to_excel(writer, sheet_name='outputdata7')
    #     freight.cmaaxdata1.to_excel(writer, sheet_name='cmmaxdata1')
    #     freight.cmaaxdata2.to_excel(writer, sheet_name='cmmaxdata2')


if __name__ == '__main__':
    main()

snapshot_output_data = pd.concat(dblist, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, db_conv.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
uploadtodb.upload(snapshot_output_data)
