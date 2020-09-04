import pandas as pd
import numpy as np
from flatdb.flatdbconverter import Flatdbconverter
import pathlib
from outputdb import uploadtodb

print(pathlib.Path().absolute())
curr_path = pathlib.Path().absolute()
pf_flat = Flatdbconverter("Price forecast model")
override_store = {}
try:
    snaps = pd.read_csv("snapshot_output_data.csv")
    override_rows = snaps.loc[snaps["override_value"] == 1, pf_flat.out_col]
    print(override_rows)
    print(override_rows.values)
    for v in override_rows.values:
        override_store[f"{v[2]}_{v[4]}_{v[5]}"] = float(v[6])
    print(override_store)
except :
    pass

dblist = []


class PriceForecastModel:
    def __init__(self):
        self.Drwdwn = pd.read_csv("inputs/EM_Drw Dwn Model.csv")
        self.projects2 = pd.read_csv("inputs/EM_Sheet1.csv")
        self.dg113 = pd.read_csv("inputs/EM_Supply.csv")
        self.dg112 = self.dg113.copy()
        self.projects4 = self.dg112.drop(["Name1"], axis=1)
        self.dg114 = pd.read_csv("inputs/EM_Supply with Countries.csv")
        self.projects = pd.read_csv("inputs/EM_Project.csv")
        self.projects1 = pd.read_csv("inputs/EM_Rank.csv")
        self.DDM = pd.read_csv("inputs/EM_DDM.csv")
        self.DDM2 = pd.read_csv("inputs/EM_1710.csv")
        self.BARactual = pd.read_excel(
            "inputs/EM_BAR for actual supply.xlsx",
            sheet_name="EM_BAR for actual supply",
        )
        self.B1817 = pd.read_csv("inputs/EM_1817.csv")
        self.D1710 = pd.read_csv("inputs/EM_Supply.csv")
        self.DDMHardCoded = pd.read_csv("inputs/EM_DDM.csv")
        self.B2 = pd.read_csv("inputs/EM_BAR.csv")
        self.LBT = pd.read_csv("inputs/EM_LBT.csv")
        self.LBT1 = pd.read_csv("inputs/EM_LBT PROJECT.csv")
        self.df8 = pd.read_csv("inputs/EM_LBT Name.csv")
        self.J278 = pd.read_csv("inputs/EM_Africa-BARS-Chalco Boffa-Guinea.csv")
        self.Australia = pd.read_csv("inputs/EM_Region.csv")
        self.AMR = pd.read_csv("inputs/EM_AMR.csv")
        self.AMR_supply = pd.read_csv("inputs/AMR_supply.csv")
        self.D171 = self.D1710.copy()

    def calculations(self):
        PriceForecastModel.Rank(self)
        PriceForecastModel.MaxCapacity(self)
        PriceForecastModel.CostofSupply(self)
        PriceForecastModel.PriceDetermination(self)
        PriceForecastModel.StockDemandandClosingStocks(self)
        PriceForecastModel.SupplyCapacityandLimitofDemand(self)
        PriceForecastModel.CostofSupplyCBIX(self)
        PriceForecastModel.CostofSupplyOnlythoseSupplying(self)
        PriceForecastModel.cbixforecast(self)
        PriceForecastModel.puredemand(self)
        PriceForecastModel.stockbuildingdemand(self)
        PriceForecastModel.totaldemand(self)
        PriceForecastModel.demandflow(self)
        PriceForecastModel.openstock(self)
        PriceForecastModel.closestock(self)
        PriceForecastModel.closingstockcurrent(self)
        PriceForecastModel.openingstocks(self)
        PriceForecastModel.australia(self)
        PriceForecastModel.chinastockpile(self)
        PriceForecastModel.tonnesbycost(self)
        PriceForecastModel.LBT(self)
        PriceForecastModel.Tonnes(self)
        PriceForecastModel.AfricaChalco(self)
        PriceForecastModel.AfricaBARS(self)
        PriceForecastModel.Total(self)
        PriceForecastModel.Australia(self)
        PriceForecastModel.OverallBAr(self)

        PriceForecastModel.Whoisatmargin255(self)
        PriceForecastModel.EngineMain1811(self)
        PriceForecastModel.Enginemain1735(self)
        PriceForecastModel.Enginemain1736(self)
        PriceForecastModel.EngineMain1815and1160(self)
        PriceForecastModel.EngineMain1718(self)
        PriceForecastModel.leagueTableBuilder(self)

    """Rank Names of the project"""

    def Rank(self):
        global dg2, Result, join3, D, D1

        projects1 = self.projects1.copy()
        projects = self.projects.copy()
        projects2 = self.projects2.copy()
        projects4 = self.projects4.copy()

        join3 = projects1.merge(projects, on="Name", how="right")

        Result1 = join3.drop(["Name"], axis=1)

        Result = Result1.rank(method="first")
        Result2 = Result.copy()

        D = projects.copy()
        Result1 = Result.copy()
        D1 = pd.concat([D, Result1], axis=1)
        D1 = D1.sort_values(D1.columns[1])

        dg1 = projects2.merge(projects4, on="Name", how="left")

        dg2 = dg1.copy()
        dg2 = dg2.drop(["Name"], axis=1)

        dblist.append(pf_flat.mult_year_single_output(dg1, "51"))
        dg1.to_csv("51.csv", encoding="utf-8", index=False)

        Result2 = pd.concat([D, Result1], axis=1)

        dblist.append(pf_flat.mult_year_single_output(Result2, "467"))
        Result2.to_csv("467.csv", encoding="utf-8", index=False)
        self.dg1 = dg1
        dg114 = self.dg114.copy()
        dg114 = dg114.iloc[:, 0:3]
        dg32 = pd.DataFrame()
        dg32 = dg1.merge(dg114, on="Name", how="left")

        self.dgref = dg32

    """Engine Main 121"""

    def MaxCapacity(self):
        global D4, R51, dg12, D2, D3
        R51 = dg2.iloc[0:1, :]
        dg12 = dg2.copy()
        dg12 = dg12.fillna(0)

        dg12 = dg12.fillna(0)

        for i in range(dg2.shape[1]):
            Sum = 0
            for j in range(dg2.shape[0]):
                Sum = Sum + dg2.iloc[j, i]

                j = j + 1

            R51.iloc[0, i] = Sum

        i = i + 1

        D4 = pd.DataFrame(columns=self.projects2.columns)
        D4 = D4.iloc[:, 0:1]

        for i in range(1, D1.shape[1]):

            D2 = D1.sort_values(D1.columns[i])
            D3 = D2.iloc[:, 0]
            i = i + 1
            D3 = D3.reset_index()

            D3 = D3.iloc[:, 1]

            D4 = pd.concat([D4, D3], axis=1, ignore_index=True)

        D4 = D4.iloc[:, 1:]

        R51 = R51.reset_index()
        R51.iloc[0, 0] = "Identified Supply"
        dg12 = self.dg1.iloc[:, 1:]

        D4.columns = dg12.columns

        dblist.append(pf_flat.mult_year_single_output(D4, "542"))
        D4.to_csv("542.csv", encoding="utf-8", index=False)
        dblist.append(pf_flat.mult_year_single_output(R51, "121 and 1723"))
        R51.to_csv("121 and 1723.csv", encoding="utf-8", index=False)
        self.dg12 = dg12

    """709"""

    def CostofSupply(self):
        global D15
        projects1 = self.projects1

        D15 = pd.DataFrame()
        for i in range(D4.shape[1]):
            DB10 = D4.iloc[:, i]
            DB10 = DB10.to_frame()

            DB10 = DB10.rename(columns={DB10.columns[0]: "Name"})

            Join15 = DB10.merge(projects1, on="Name", how="left")

            D17 = Join15.iloc[:, i + 1]

            D15 = pd.concat([D15, D17], axis=1)
            i = i + 1
        G17 = DB10[["Name"]]
        D15 = pd.concat([G17, D15], axis=1)

        dblist.append(pf_flat.mult_year_single_output(D15, "709"))
        D15.to_csv("709.csv", encoding="utf-8", index=False)

    """1709"""

    def PriceDetermination(self):
        global DM1
        DM1 = self.DDM.copy()

        for i in range(1, DM1.shape[1]):
            Sum = 0
            for j in range(DM1.shape[0]):

                Sum = Sum + (DM1.iloc[j, i])

                j = j + 1

            DM1.iloc[0, i] = Sum

            i = i + 1

        """DM1=DM1.iloc[0,:]
        DM1=DM1.to_numpy()
        DM1=DM1.transpose()
        DM1=pd.DataFrame(DM1)
        DM1=DM1.transpose()"""
        # DM1.iloc[0,4]=25.12339216

        self.DM1 = DM1

        DM1.iloc[0, 0] = "Pure Demand"
        DM2 = DM1.iloc[0:1, :]
        dblist.append(pf_flat.mult_year_single_output(DM2, "1709 & 25"))
        DM2.to_csv("1709 & 25.csv", encoding="utf-8", index=False)

    """1710"""

    def StockDemandandClosingStocks(self):
        DDM2 = self.DDM2
        BARactual = self.BARactual
        global DDM3, D8

        A1 = BARactual.iloc[0, 1]
        A = 36.520084519 / A1
        B1 = BARactual.iloc[0, 2]
        B = 56.099999178 / B1
        C1 = BARactual.iloc[0, 3]
        C = 52.050471544 / C1
        D1 = BARactual.iloc[0, 4]
        D = 68.76 / D1

        DDM2.iloc[0, 4] = 25.12339216
        DDM3 = DDM2.copy()

        for i in range(DM1.shape[1]):
            Sum = 0
            for j in range(DDM2.shape[0]):
                Sum = DM1.iloc[j, i] + DDM2.iloc[j, i]
                j = j + 1
                DDM3.iloc[0, i] = Sum
            i = i + 1
        DDM3.iloc[0, 0] = "Total Demand"
        DDM3.iloc[0, 1] = A
        DDM3.iloc[0, 2] = B
        DDM3.iloc[0, 3] = C
        DDM3.iloc[0, 4] = D

        print("removed")

        C11 = 33.4 / A1
        C12 = 21.1 / A1
        C13 = 32.5 / B1
        C14 = 30.1 / C1
        C15 = 23.78 / D1
        C16 = 38.95 / 2.641907649

        DDM4 = DDM2.copy()

        DDM4.iloc[0, 0] = ""
        DDM4.iloc[0, 1] = C12
        DDM4.iloc[0, 2] = C13
        DDM4.iloc[0, 3] = C14
        DDM4.iloc[0, 4] = C15
        DDM4.iloc[0, 5] = C16

        D5 = pd.DataFrame()
        projects2 = self.projects2
        projects4 = self.projects4
        dg11 = projects2.merge(projects4, on="Name", how="left")

        dg11 = dg11.replace(np.NaN, 0)
        for i in range(D4.shape[1]):

            D6 = D4.iloc[:, i]

            D6 = D6.to_frame()

            D6 = D6.rename(columns={D6.columns[0]: "Name"})

            Join5 = D6.merge(dg11, on="Name", how="right")
            D7 = Join5.iloc[:, i + 1]
            D5 = pd.concat([D5, D7], axis=1, ignore_index=True)
            i = i + 1

        D8 = D5.copy()
        D17 = D6[["Name"]]
        D8 = pd.concat([D17, D5], axis=1, ignore_index=True)
        D8 = D8.replace(" ", 0).bfill()

        for i in range(1, D8.shape[1]):
            Sum = 0
            for j in range(D8.shape[0]):
                Sum = Sum + D8.iloc[j, i]
                D8.iloc[j, i] = Sum
                j = j + 1
            i = i + 1
        D8.columns = self.dg1.columns

        dblist.append(pf_flat.mult_year_single_output(D8, "864"))
        D8.to_csv("864.csv", encoding="utf-8", index=False)
        self.C11 = C11
        self.DDM4 = DDM4

    """937"""

    def SupplyCapacityandLimitofDemand(self):

        D9 = D8.copy()
        for i in range(1, D9.shape[1]):
            for j in range(D9.shape[0]):
                if D9.iloc[j, i] > DDM3.iloc[0, i]:
                    D9.iloc[j, i] = DDM3.iloc[0, i]
                j = j + 1
            i = i + 1

        dblist.append(pf_flat.mult_year_single_output(D9, "937"))
        D9.to_csv("937.csv", encoding="utf-8", index=False)
        self.D9 = D9

    def CostofSupplyCBIX(self):

        global D19
        D19 = self.D9.copy()
        D9 = self.D9.copy()

        for i in range(1, D19.shape[1]):
            for j in range(1, D19.shape[0]):
                D19.iloc[j, i] = D9.iloc[j, i] - D9.iloc[j - 1, i]
                j = j + 1
            i = i + 1

        dblist.append(pf_flat.mult_year_single_output(D19, "1012"))
        D19.to_csv("1012.csv", encoding="utf-8", index=False)
        self.D19 = D19
        self.L1012 = D19

    """1012"""
    """1088"""

    def CostofSupplyOnlythoseSupplying(self):
        global D11
        D9 = self.D19.copy()
        D11 = self.D19.copy()

        for i in range(1, D9.shape[1]):
            for j in range(D9.shape[0]):
                if D9.iloc[j, i] > 0:
                    D11.iloc[j, i] = D15.iloc[j, i]
                else:

                    D11.iloc[j, i] = 0
                j = j + 1
            i = i + 1
        D12 = D11.copy()

        dblist.append(pf_flat.mult_year_single_output(D12, "1088"))
        D12.to_csv("1088.csv", encoding="utf-8", index=False)
        self.D12 = D12

        """D15 is 709 Table"""

        """1165"""
        E11 = D9.copy()

        for i in range(1, D11.shape[1]):
            for j in range(D11.shape[0]):
                if D11.iloc[j, i] > 0:
                    E11.iloc[j, i] = D4.iloc[j, i - 1]
                else:
                    E11.iloc[j, i] = 0
                j = j + 1
            i = i + 1
        self.E11 = E11
        E11 = E11.drop(["Name"], axis=1)
        E11 = E11.reset_index()
        E11 = E11.rename(columns={E11.columns[0]: "Rank"})

        dblist.append(
            pf_flat.mult_year_single_output(E11, "1165 and Who ia at margin R10")
        )
        E11.to_csv("1165 and Who ia at margin R10.csv", encoding="utf-8", index=False)

    def cbixforecast(self):
        """1160,1815"""

        D12 = self.D12.max()
        D13 = D12.to_frame()
        D13 = D13.transpose()

        """1818"""
        D1818 = D13.copy()
        for i in range(D1818.shape[1]):
            for j in range(D1818.shape[0]):
                D1818.iloc[j, i] = 0
                j = j + 1
            i = i + 1
        print(self.B1817)
        B1817 = self.B1817.copy()
        D1818.iloc[0, 5] = B1817.iloc[0, 5]

        for i in range(6, D1818.shape[1]):
            for j in range(1):
                D1818.iloc[j, i] = D13.iloc[j, i] * (1.1)
                j = j + 1
            i = i + 1

        D1818.iloc[0, 0] = "cbixforecast"

        dblist.append(pf_flat.mult_year_single_output(D1818, "1818"))
        D1818.to_csv("1818.csv", encoding="utf-8", index=False)

        """1688"""

    def puredemand(self):

        B3 = self.B2.copy()

        for i in range(1, self.B2.shape[1]):
            for j in range(self.B2.shape[0]):
                Product = self.B2.iloc[j, i] * DM1.iloc[j, i]

                B3.iloc[0, i] = Product

                j = j + 1
            i = i + 1
        B3.iloc[0, 0] = "Pure Demand"
        dblist.append(pf_flat.mult_year_single_output(B3, "1688"))
        B3.to_csv("1688.csv", encoding="utf-8", index=False)

    def stockbuildingdemand(self):
        """1689"""
        B4 = self.B2.copy()
        for i in range(1, self.DDM2.shape[1]):
            for j in range(self.DDM2.shape[0]):
                Product = self.B2.iloc[j, i] * self.DDM2.iloc[j, i]

                B4.iloc[0, i] = Product

                j = j + 1
            i = i + 1
        B4.iloc[0, 0] = "Stock Building Demand - do this every year"
        dblist.append(pf_flat.mult_year_single_output(B4, "1689"))
        B4.to_csv("1689.csv", encoding="utf-8", index=False)

    """1690"""

    def totaldemand(self):
        B5 = self.B2.copy()
        B2 = self.B2.copy()
        for i in range(1, DDM3.shape[1]):
            for j in range(DDM3.shape[0]):
                Product = B2.iloc[j, i] * DDM3.iloc[j, i]

                B5.iloc[0, i] = Product

                j = j + 1
            i = i + 1

        B5.iloc[0, 0] = "Total Demand"
        dblist.append(pf_flat.mult_year_single_output(B5, "1690"))
        B5.to_csv("1690.csv", encoding="utf-8", index=False)

    """MIN(1723,1711)"""

    def demandflow(self):
        minimum = R51.copy()
        for i in range(R51.shape[1]):
            for j in range(R51.shape[0]):
                A = R51.iloc[j, i]
                B = DDM3.iloc[j, i]
                C = min(A, B)
                minimum.iloc[j, i] = C
                j = j + 1
            i = i + 1
        self.DDM3 = DDM3
        dblist.append(pf_flat.mult_year_single_output(DDM3, "1711"))
        DDM3.to_csv("1711.csv", encoding="utf-8", index=False)

    """P1733"""

    def openstock(self):
        DDM4 = self.DM1.copy()
        DDM1 = self.DM1.copy()
        DDM4 = DDM4.fillna(0)
        DDM1 = DDM1.fillna(0)
        minimum = R51.copy()
        min1 = self.DDM3.copy()
        for i in range(1, minimum.shape[1]):
            if min1.iloc[0, i] < minimum.iloc[0, i]:
                minimum.iloc[0, i] = min1.iloc[0, i]

        for i in range(5, DDM4.shape[1]):

            A = i - 1

            Value = DDM4.iloc[0, A] - DDM1.iloc[0, i]

            Value1 = max(0, Value + minimum.iloc[0, i])
            DDM4.iloc[0, i] = Value1

            i = i + 1
        self.DDM4 = DDM4
        DDM4.iloc[0, 0] = "Closing Stocks"
        DDM14 = DDM4.iloc[0:1, :]

        dblist.append(pf_flat.mult_year_single_output(DDM14, "1733Output"))
        DDM14.to_csv("1733Output.csv", encoding="utf-8", index=False)

    """1732"""

    def closestock(self):
        DDM4 = self.DDM4.copy()
        DDM1 = DM1.copy()
        DDM4 = DDM4.fillna(0)
        DDM1 = DDM1.fillna(0)
        DDM5 = DDM4.copy()

        for i in range(1, DDM5.shape[1]):
            j = 0

            A = i - 1

            Value = DDM4.iloc[j, A]
            DDM5.iloc[j, i] = Value

            i = i + 1
        DDM5.iloc[0, 1] = self.C11
        DDM5.iloc[0, 0] = "Opening Stocks"
        DDM15 = DDM5.iloc[0:1, :]

        dblist.append(pf_flat.mult_year_single_output(DDM15, "1732"))
        DDM15.to_csv("1732.csv", encoding="utf-8", index=False)
        self.DDM5 = DDM5

    def closingstockcurrent(self):
        """1732-1733"""
        DDM6 = self.DDM4.copy()
        for i in range(1, self.DDM5.shape[1]):
            for j in range(self.DDM5.shape[0]):

                Value = self.DDM5.iloc[j, i] - self.DDM4.iloc[j, i]
                DDM6.iloc[j, i] = Value

                j = j + 1
            i = i + 1
        DDM6.iloc[0, 0] = "Difference between opening and closing stocks"
        DDM16 = DDM6.iloc[0:1, :]

        dblist.append(pf_flat.mult_year_single_output(DDM16, "1732-1733"))
        DDM16.to_csv("1732-1733.csv", encoding="utf-8", index=False)
        self.DDM6 = DDM6

    """1755,1756,1757"""

    """1755-1857"""

    def openingstocks(self):
        Change = self.DDM5.copy()
        DDM4 = self.DDM4.copy()
        DDM5 = self.DDM5.copy()
        B2 = self.B2.copy()

        for i in range(1, self.DDM5.shape[1]):
            j = 0
            Product = self.DDM5.iloc[j, i] * self.B2.iloc[j, i]

            Change.iloc[0, i] = Product

            i = i + 1
            Change1 = DDM5.copy()
        for i in range(1, DDM5.shape[1]):
            j = 0
            Product = DDM4.iloc[j, i] * B2.iloc[j, i]

            Change1.iloc[0, i] = Product

            i = i + 1

        Change2 = DDM5.copy()
        for i in range(1, DDM5.shape[1]):
            j = 0
            Product = Change1.iloc[j, i] - Change.iloc[j, i]

            Change2.iloc[0, i] = Product

            i = i + 1
        Change.iloc[0, 0] = "Change"
        Change = Change2.iloc[0:1, :]
        Change1.iloc[0, 0] = "Closing Stocks"
        Change1 = Change1.iloc[0:1, :]
        Change2.iloc[0, 0] = "Opening Stocks"
        Change2 = Change2.iloc[0:1, :]

        Change2.iloc[0, 0] = "Open"
        Change.iloc[0, 0] = "Close"
        Change1.iloc[0, 0] = "Change"
        dblist.append(pf_flat.mult_year_single_output(Change2, "1755"))
        Change2.to_csv("1755.csv", encoding="utf-8", index=False)
        dblist.append(pf_flat.mult_year_single_output(Change, "1756"))
        Change.to_csv("1756.csv", encoding="utf-8", index=False)
        dblist.append(pf_flat.mult_year_single_output(Change1, "1757"))
        Change1.to_csv("1757.csv", encoding="utf-8", index=False)

    """1798-1802"""

    def australia(self):
        EM1165 = self.E11.copy()
        EM51 = self.dgref.copy()
        D115 = pd.DataFrame()
        EM1165 = EM1165.drop(["Name"], axis=1)

        cols1 = list(EM51.columns)
        cols1 = [cols1[-1]] + cols1[:-1]
        EM51 = EM51[cols1]

        for i in range(0, EM1165.shape[1]):

            D110 = EM1165.iloc[:, i]
            D110 = D110.to_frame()

            D110 = D110.rename(columns={D110.columns[0]: "Name"})

            Join115 = D110.merge(EM51, on="Name", how="left")

            Join115 = Join115.groupby("Name3").sum()

            D117 = Join115.iloc[:, i : i + 1]

            D115 = pd.concat([D115, D117], axis=1)
            D115 = D115.rename(columns={"Name": i})
            D110 = D110.rename(columns={"Name": i})
            i = i + 1
        self.D115 = D115
        D116 = D115.copy()
        D116 = D116.reset_index()
        D116 = D116.rename(columns={"Name3": "Region"})

        dblist.append(pf_flat.mult_year_single_output(D116, "1798-1802"))
        D116.to_csv("1798-1802.csv", encoding="utf-8", index=False)

    """1810"""

    def chinastockpile(self):
        DDM6 = self.DDM6.copy()
        DDM7 = DDM6.copy()
        for i in range(1, self.DDM5.shape[1]):
            for j in range(self.DDM5.shape[0]):
                if DDM7.iloc[j, i] < 0:
                    DDM7.iloc[j, i] = 0
                    j = j + 1
            i = i + 1

        DDM7.iloc[0, 0] = "China Stockpile"
        DDM17 = DDM7.iloc[0:1, :]
        dblist.append(pf_flat.mult_year_single_output(DDM17, "1810"))
        DDM17.to_csv("1810.csv", encoding="utf-8", index=False)

    def tonnesbycost(self):
        """Whoisatmargin J256"""
        D9 = self.D9
        J256 = D9.round(decimals=2)

        """Marginal Cost 254"""

        J254 = D11.round(decimals=1)

        dblist.append(pf_flat.mult_year_single_output(J256, "Who is at margin"))
        J256.to_csv("Who is at margin.csv", encoding="utf-8", index=False)
        dblist.append(pf_flat.mult_year_single_output(J254, "254"))
        J254.to_csv("254.csv", encoding="utf-8", index=False)

        """LBT"""

    def LBT(self):
        E11 = self.E11.copy()

        d2 = E11.dropna(how="all", axis=1)
        df1 = d2.copy()
        df2 = d2.copy()
        df2 = df2.replace(0, np.NaN)
        i = 0
        df2 = df2.iloc[:, 1:]
        Final = self.L1012.iloc[:, 1:3]
        df2 = df2.replace(0, np.NaN)

        for i in range(df2.shape[1]):

            df4 = df2.iloc[:, i].dropna(how="all", axis=0)
            df4 = df4.reset_index(drop=True)

            Final = pd.concat([Final, df4], axis=1, ignore_index=True)

            i = i + 1
        Final1 = Final
        Final1 = Final.iloc[:, 2:]

        Final1.columns = dg12.columns
        final2 = Final1.copy()
        final2 = final2.reset_index()
        final2 = final2.rename(columns={"index": "Rank"})

        dblist.append(pf_flat.mult_year_single_output(final2, "J9 LBT"))
        final2.to_csv("J9 LBT.csv", encoding="utf-8", index=False)
        self.Final1 = Final1.copy()

    """Tonnes"""

    def Tonnes(self):
        D9 = self.D9.copy()
        Final1 = self.Final1.copy()
        de2 = self.L1012.dropna(how="all", axis=1)
        df1 = de2.copy()
        df2 = de2.copy()
        Final = self.L1012.iloc[:, 1:3]

        df2 = df2.replace(0, np.NaN)

        for i in range(1, df2.shape[1]):

            df4 = df2.iloc[:, i].dropna(how="all", axis=0)
            df4 = df4.reset_index(drop=True)

            Final = pd.concat([Final, df4], axis=1, ignore_index=True)

            i = i + 1

        Final2 = Final.iloc[:, 1:]

        Final2 = Final2.iloc[:, 1:]
        Final1 = Final1.iloc[:, 1:]
        Final2.columns = dg12.columns
        final12 = Final2.copy()
        final12 = Final2.reset_index()
        final12 = final12.rename(columns={"index": "Rank"})
        dblist.append(pf_flat.mult_year_single_output(final12, "J58"))
        final12.to_csv("J58.csv", encoding="utf-8", index=False)

        self.Final2 = Final2.copy()

    """Africa-Chalco Boffa-Guine"""

    """Africa-Chalco Boffa-Guine"""

    def AfricaChalco(self):
        df = self.Final1.copy()
        df1 = self.Final2.copy()

        d1 = df.dropna(how="all", axis=1)
        d2 = df1.dropna(how="all", axis=1)

        C = d1.shape[1]

        FinalDataframe = pd.DataFrame()
        for i in range(C):
            df4 = pd.DataFrame()
            data = d1.iloc[:, i]
            df4 = pd.concat([df4, data], axis=1)
            df4.columns = ["name"]
            data1 = d2.iloc[:, i]
            df4 = pd.concat([df4, data1], axis=1)
            join3 = pd.DataFrame()

            join3 = self.df8.merge(df4, on="name", how="left")
            Final_dframe = join3.iloc[:, 1]

            FinalDataframe = FinalDataframe.append(Final_dframe)
            i = i + 1
        FinalDataframe = FinalDataframe.transpose()
        FinalDataframe = FinalDataframe.fillna(0)
        dg113 = self.dg113.iloc[:, 0]
        FinalDataframe = pd.concat([dg113, FinalDataframe], axis=1)
        dblist.append(
            pf_flat.mult_year_single_output(
                FinalDataframe, "Africa-Chalco Boffa-Guine J 278.csv"
            )
        )
        FinalDataframe.to_csv(
            "Africa-Chalco Boffa-Guine J 278.csv", encoding="utf-8", index=False
        )
        dblist.append(pf_flat.mult_year_single_output(FinalDataframe, "J228"))
        FinalDataframe.to_csv("J228.csv", encoding="utf-8", index=False)

        self.FinalDataframe = FinalDataframe.copy()

    """Africa-BARS-Chalco Boffa-Guinea"""
    """327"""

    def AfricaBARS(self):
        FinalDataframe = self.FinalDataframe.copy()
        J327 = FinalDataframe.copy()

        for j in range(1, FinalDataframe.shape[1]):
            cols = self.J278.columns
            for k in range(FinalDataframe.shape[0]):
                l = FinalDataframe.iloc[k, j]

                m = self.J278.iloc[k, j]
                p = l * m

                if f"Lbt 327_{cols[j]}_{k}" in override_store:
                    print(True)
                    J327.iloc[k, j] = override_store[f"Lbt 327_{cols[j]}_{k}"]
                else:
                    J327.iloc[k, j] = p
                k = k + 1
            j = j + 1

        # make changes here
        dblist.append(pf_flat.single_year_mult_out(J327, "LBT 327"))
        J327.to_csv("LBT 327.csv", encoding="utf-8", index=False)
        dblist.append(pf_flat.mult_year_single_output(J327, "LBT 394"))
        J327.to_csv("LBT 394.csv", encoding="utf-8", index=False)
        for h in override_store.keys():
            # print(h)
            sp = h.split("_")
            if sp[0] == "Lbt 451":
                # print(sp)
                i, c = sp[-1], sp[-2]
                J327.loc[int(i), c] = override_store[h]
        dblist.append(pf_flat.single_year_mult_out(J327, "LBT 451"))
        J327.to_csv("LBT 451.csv", encoding="utf-8", index=False)
        self.J327 = J327

    """J374"""

    def Total(self):
        J374 = self.J327.copy()
        J327 = self.J327.copy()
        for i in range(1, self.J327.shape[1]):
            Sum = 0
            for j in range(self.J327.shape[0]):
                Sum = Sum + self.J327.iloc[j, i]
                j = j + 1
                J374.iloc[0, i] = Sum
            i = i + 1

        J374 = J374.iloc[0, :]
        J374 = J374.to_numpy()
        J374 = J374.transpose()
        J374 = pd.DataFrame(J374)
        J374 = J374.transpose()

        self.J374 = J374

        J374 = J374.iloc[0:1, :]
        J375 = J374.iloc[:, 1:]
        J375.columns = dg12.columns
        J375 = J375.reset_index()
        J375.iloc[0, 0] = "LBT Total 327 to 372"
        dblist.append(pf_flat.mult_year_single_output(J375, "J374"))
        J375.to_csv("J374.csv", encoding="utf-8", index=False)

    """J376"""

    def Australia(self):

        RegionM = pd.concat([self.Australia, self.J327], axis=1)

        RegionM = RegionM.rename(columns={"0": "Region"})

        RegionG = RegionM.groupby("Region").sum()

        dblist.append(pf_flat.mult_year_single_output(RegionG, "J 376"))
        RegionG.to_csv("J 376.csv", encoding="utf-8")

    def OverallBAr(self):
        """J384"""

        FinalDataframe = self.FinalDataframe.copy()
        J327 = self.J327.copy()
        J384 = self.J327.copy()

        FinalDataframe = self.FinalDataframe.copy()

        for i in range(1, J327.shape[1]):
            Sum = 0
            Sum1 = 0
            for j in range(J384.shape[0]):
                Sum1 = Sum1 + FinalDataframe.iloc[j, i]
                Sum = Sum + J327.iloc[j, i]
                j = j + 1

            J384.iloc[0, i] = Sum / Sum1
            i = i + 1

        J384.iloc[0, 0] = "Overall BAR"

        J384 = J384.iloc[0:1, :]

        dblist.append(pf_flat.mult_year_single_output(J384, "J384"))
        J384.to_csv("J384.csv", encoding="utf-8", index=False)

    def Whoisatmargin255(self):
        D19 = self.D19.copy()
        D12 = self.D12.copy()
        E11 = self.E11.copy()
        W255 = self.E11.copy()

        for i in range(1, E11.shape[1]):
            for j in range(E11.shape[0]):
                if D19.iloc[j, i] > 0:
                    D12.iloc[j, i] = round(D12.iloc[j, i], 1)
                    D19.iloc[j, i] = round(D19.iloc[j, i], 2)

                    W255.iloc[j, i] = (
                        str(E11.iloc[j, i])
                        + ",[$/dmt "
                        + str(D12.iloc[j, i])
                        + ",["
                        + str(D19.iloc[j, i])
                        + " mln dmt]"
                    )

                else:
                    W255.iloc[j, i] = " "
                j = j + 1
            i = i + 1
        W255 = W255.iloc[:, 1:]
        W226 = W255.copy()
        W226 = W226.reset_index()

        dblist.append(pf_flat.mult_year_single_output(W226, "Who is at margin 255"))
        W226.to_csv("Who is at margin 255.csv", encoding="utf-8", index=False)

    def EngineMain1811(self):
        D115 = self.D115.copy()
        D116 = D115.copy()
        D115 = D115.replace(np.NaN, 0)
        for i in range(D115.shape[1]):
            sum = 0
            for j in range(D115.shape[0]):
                sum = sum + D115.iloc[j, i]
                j = j + 1
            D116.iloc[0, i] = sum
            i = i + 1

        D116 = D116.reset_index()
        D116.iloc[0, 0] = "ChinaDemand"
        D116 = D116.iloc[0:1, :]

        self.D116 = D116
        dblist.append(pf_flat.mult_year_single_output(D116, "Engine Main1811"))
        D116.to_csv("Engine Main1811.csv", encoding="utf-8", index=False)

    def Enginemain1735(self):
        EM1732 = self.DDM5.copy()
        EM1709 = self.DM1.copy()
        EM1735 = self.D116.copy()
        for i in range(1, EM1732.shape[1]):
            EM1735.iloc[0, i] = (EM1732.iloc[0, i] / EM1709.iloc[0, i]) * 52
            i = i + 1

        EM1735.iloc[0, 0] = "Weeks Supply Opening"
        dblist.append(pf_flat.mult_year_single_output(EM1735, "Engine Main1735"))
        EM1735.to_csv("Engine Main1735.csv", encoding="utf-8", index=False)

    def Enginemain1736(self):
        EM1733 = self.DDM4.copy()
        EM1709 = self.DM1.copy()
        EM1736 = self.D116.copy()
        for i in range(1, EM1733.shape[1]):
            EM1736.iloc[0, i] = (EM1733.iloc[0, i] / EM1709.iloc[0, i]) * 52
            i = i + 1
        EM1736.iloc[0, 0] = "Weeks Supply Closing"
        self.EM1736 = EM1736
        dblist.append(pf_flat.mult_year_single_output(EM1736, "Engine Main1736"))
        EM1736.to_csv("Engine Main1736.csv", encoding="utf-8", index=False)

    def EngineMain1815and1160(self):
        D12 = self.D12

        EM1815 = self.EM1736.copy()
        for i in range(1, D12.shape[1]):
            max = 0
            for j in range(D12.shape[0]):
                if D12.iloc[j, i] > max:
                    max = D12.iloc[j, i]

                j = j + 1
                EM1815.iloc[0, i] = max

            i = i + 1
        self.EM1815 = EM1815
        EM1815.iloc[0, 0] = "CBIX - Marginal Tonne excl. Weiqiao Self Supply"
        dblist.append(
            pf_flat.mult_year_single_output(EM1815, "Engine Main1815 and 1160")
        )
        EM1815.to_csv("Engine Main1815 and 1160.csv", encoding="utf-8", index=False)

    def EngineMain1718(self):
        EM1733 = self.DDM4.copy()
        DRW = self.Drwdwn.copy()
        EM1713 = self.EM1815.copy()
        EM171 = self.EM1815.copy()
        EM1718 = self.EM1815.copy()
        A = 16
        B = 39

        for i in range(1, DRW.shape[1]):
            EM1713.iloc[0, i] = (16 / 52) * DRW.iloc[0, i]
            i = i + 1
        for i in range(1, DRW.shape[1]):
            C = EM1713.iloc[0, i] / A
            EM171.iloc[0, i] = EM1733.iloc[0, i] - (C * B)

            if EM171.iloc[0, i] > 0:
                EM1718.iloc[0, i] = 0
            else:
                EM1718.iloc[0, i] = -1 * EM171.iloc[0, i]

            i = i + 1

        EM1718.iloc[0, 0] = "extra tonnes to get to 39.1 weeks closing stocks"
        dblist.append(pf_flat.mult_year_single_output(EM1718, "Engine Main1718"))
        EM1718.to_csv("Engine Main1718.csv", encoding="utf-8", index=False)

    def leagueTableBuilder(self):
        Supply_with_Countries = self.dg114.copy()  # EM_Supply_with Countries
        African_Bars = self.J278.copy()  # EM African Bars
        Supply = self.dg113.copy()  # EM Supply
        Amrr_supply = self.AMR_supply.copy()
        Supply_Columns = [
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
            "2022",
            "2023",
            "2024",
            "2025",
            "2026",
            "2027",
            "2028",
            "2029",
            "2030",
            "2031",
            "2032",
            "2033",
            "2034",
            "2035",
            "2036",
            "2037",
            "2038",
            "2039",
            "2040",
        ]
        current_Columns = Supply_Columns.copy()
        myDF = pd.DataFrame()
        myDF["Name3"] = Supply_with_Countries["Name3"]
        myDF["Name1"] = Supply_with_Countries["Name1"]
        myDF["Name"] = Supply_with_Countries["Name"]

        def createDF():
            toChange = [4, 7, 28, 37, 38, 39, 40, 44]
            # toChange = [3,6,27,37,38,39,40,44]
            for i in current_Columns:
                a = str(i)
                currentState = African_Bars[a] * Supply[a]
                if int(a) >= 2018:
                    for o in toChange:
                        if o == 4:
                            currentState[o] = (
                                African_Bars[a][o] * Supply[a][o]
                            ) + Amrr_supply[a][3]
                        elif o == 7:
                            currentState[o] = (
                                African_Bars[a][o] * Supply[a][o]
                            ) + Amrr_supply[a][1]
                        elif o == 28:
                            currentState[o] = (
                                African_Bars[a][15] * Supply[a][28]
                            ) + Amrr_supply[a][2]
                        elif o == 37:
                            currentState[o] = (
                                African_Bars[a][37] * Supply[a][37]
                            ) + Amrr_supply[a][4]
                        elif o == 38:
                            currentState[o] = currentState[o] = (
                                African_Bars[a][38] * Supply[a][38]
                            ) + Amrr_supply[a][5]
                        elif o == 39:
                            currentState[o] = currentState[o] = (
                                African_Bars[a][39] * Supply[a][39]
                            ) + Amrr_supply[a][6]
                        elif o == 40:
                            currentState[o] = (
                                African_Bars[a][40] * Supply[a][40]
                            ) + Amrr_supply[a][7]
                            currentState[o + 1] = 0
                        elif o == 44:
                            currentState[o] = currentState[o] = (
                                African_Bars[a][44] * Supply[a][44]
                            ) + Amrr_supply[a][0]

                myDF[a] = currentState
            return myDF

        leagueDF = createDF()

        leagueDF.to_csv("Output Files/leagTableA451_U496.csv", index=False)
        dblist.append(pf_flat.mult_year_single_output(leagueDF, "leagTableA451_U496"))

        print(leagueDF)


pf = PriceForecastModel()
pf.calculations()


snapshot_output_data = pd.concat(dblist, ignore_index=True)

try:
    override_res = override_rows.values
    for i, v in enumerate(override_rows.index):
        print(snapshot_output_data.loc[v],)
        set_it = snapshot_output_data.loc[v].values
        print(override_res[i][-2:])
        set_it[-2:] = override_res[i][-2:]
        snapshot_output_data.loc[v] = set_it
except:
    pass

snapshot_output_data = snapshot_output_data.loc[:, pf_flat.out_col]
# print(len(dblist))
snapshot_output_data.to_csv('snapshot_output_data.csv', index=False)
uploadtodb.upload(snapshot_output_data)
