import pandas as pd
import numpy as np
import pyodbc


class priceForecastRestructure:
    def __init__(self):
        server = "magdb.database.windows.net"
        database = "input_db"
        username = "letmetry"
        password = "T@lst0y50"
        driver = "{ODBC Driver 17 for SQL Server}"

        self.conn = pyodbc.connect(
            "DRIVER="
            + driver
            + ";PORT=1433;SERVER="
            + server
            + ";PORT=1443;DATABASE="
            + database
            + ";UID="
            + username
            + ";PWD="
            + password
        )
        self.EM_Supply1 = pd.read_sql_query(
            "SELECT * from [dbo].[em_supply]", self.conn
        )
        self.MINE_Table = pd.read_sql_query("SELECT * from [dbo].[mine]", self.conn)
        self.COUNTRY = pd.read_sql_query("SELECT * from [dbo].[country]", self.conn)
        self.YEAR_Table = pd.read_sql_query("SELECT * from [dbo].[year]", self.conn)
        self.REGION_Table = pd.read_sql_query("SELECT * from [dbo].[region]", self.conn)
        self.EM_Supply_Countries = pd.read_sql_query(
            "SELECT * from [dbo].[em_supply_with_countries]", self.conn
        )
        self.AFRICAN_BarsTable = pd.read_sql_query(
            "SELECT * from [dbo].[EM_Africa_Guinea]", self.conn
        )
        self.EM_RANK = pd.read_sql_query("SELECT * from [dbo].[EM_Rank]", self.conn)
        self.RegionTable = pd.read_sql_query("SELECT * from [dbo].[region]", self.conn)
        self.RegionTable = self.RegionTable["region"]
        self.em_lbt = pd.read_sql_query("SELECT * from [dbo].[em_lbt]", self.conn)
        self.lbt = pd.read_sql_query("SELECT * from [dbo].[lbt]", self.conn)

        # self.tochange = pd.read_sql_query("SELECT * from [dbo].[tochange]", self.conn)
        # Changing tochange  to YEar
        c = 0
        for i in self.em_lbt["year_id"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.em_lbt["year_id"][c] = self.YEAR_Table["year"][yearIndex]
            c = c + 1
        # print(self.em_lbt)
        self.AMR_SUPPLY = pd.read_sql_query(
            "SELECT * from [dbo].[amr_supply]", self.conn
        )

    def EM_Supply_Restructure(self):
        # EM_Supply1 = pd.read_sql_query("SELECT * from [dbo].[EM_Supply]", self.conn)
        # MINE_Table = pd.read_sql_query("SELECT * from [dbo].[mine]", self.conn)
        # COUNTRY = pd.read_sql_query("SELECT * from [dbo].[country]", self.conn)
        # YEAR_Table = pd.read_sql_query("SELECT * from [dbo].[year]", self.conn)
        a = b = c = 0
        for i in self.EM_Supply1["country_id"]:
            countryIndex = self.COUNTRY[self.COUNTRY["country_id"] == i].index.values
            countryIndex = countryIndex[0]
            self.EM_Supply1["country_id"][b] = self.COUNTRY["country"][countryIndex]
            b = b + 1
        for i in self.EM_Supply1["mine_id"]:
            currentIndex = self.MINE_Table[self.MINE_Table["mine_id"] == i].index.values
            currentIndex = currentIndex[0]
            self.EM_Supply1["mine_id"][a] = self.MINE_Table["mine"][currentIndex]
            a = a + 1
        for i in self.EM_Supply1["year_id"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.EM_Supply1["year_id"][c] = self.YEAR_Table["year"][yearIndex]
            c = c + 1
        # self.EM_Supply1.drop(["creation_date", "updation_date"], axis=1, inplace=True)
        self.EM_Supply1.columns = ["EM_Supply_id", "Name", "Name1", "Name3", "value"]
        # print(self.EM_Supply1)
        Supply = pd.DataFrame()
        Supply["Name"] = self.EM_Supply1.Name.unique()
        Supply["Name1"] = np.nan
        me = []
        for i in self.EM_Supply1.Name.unique():
            now = self.EM_Supply1[self.EM_Supply1["Name"] == i].index.values
            for j in now:
                if (self.EM_Supply1["Name3"][j]) == "2014":
                    me.append(j)
        for i in range(len(Supply["Name1"])):
            a = me[i]
            Supply["Name1"][i] = self.EM_Supply1["Name1"][a]
        for i in self.EM_Supply1.Name3.unique():
            Supply[i] = np.nan
        for i in self.EM_Supply1.Name3.unique():
            cur = self.EM_Supply1[self.EM_Supply1["Name3"] == i].index.values
            update = []
            for j in cur:
                update.append(self.EM_Supply1["value"][j])
            for w in range(len(Supply[i]) - 1):
                Supply[i][w] = update[w]
        Supply.to_csv("EM_Supply.csv", index=False)
        print(Supply)
        return Supply

    def EM_SupplyCountries_Restructure(self):
        a = b = c = d = 0
        # for i in self.EM_Supply_Countries["attribute"]:
        #     yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
        #     yearIndex = yearIndex[0]
        #     self.EM_Supply_Countries["attribute"][c] = self.YEAR_Table["year"][
        #         yearIndex
        #     ]
        #     c = c + 1
        for i in self.EM_Supply_Countries["year_id"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.EM_Supply_Countries["year_id"][c] = self.YEAR_Table["year"][yearIndex]
            c = c + 1
        for i in self.EM_Supply_Countries["country_id"]:
            countryIndex = self.COUNTRY[self.COUNTRY["country_id"] == i].index.values
            countryIndex = countryIndex[0]
            self.EM_Supply_Countries["country_id"][b] = self.COUNTRY["country"][
                countryIndex
            ]
            b = b + 1
        for i in self.EM_Supply_Countries["mine_id"]:
            currentIndex = self.MINE_Table[self.MINE_Table["mine_id"] == i].index.values
            currentIndex = currentIndex[0]
            self.EM_Supply_Countries["mine_id"][a] = self.MINE_Table["mine"][
                currentIndex
            ]
            a = a + 1
        for i in self.EM_Supply_Countries["region_id"]:
            regionIndex = self.REGION_Table[
                self.REGION_Table["region_id"] == i
            ].index.values
            regionIndex = regionIndex[0]
            self.EM_Supply_Countries["region_id"][d] = self.REGION_Table["region"][
                regionIndex
            ]
            d = d + 1
        # self.EM_Supply_Countries.drop(
        #     ["creation_date", "updation_date"], axis=1, inplace=True
        # )
        self.EM_Supply_Countries.columns = [
            "EM_Supply_Countries_id",
            "Name",
            "Name1",
            "Name3",
            "Year",
            "value",
        ]
        Supply_Countries = pd.DataFrame()
        Supply_Countries["Name"] = self.EM_Supply_Countries["Name"].unique()
        Supply_Countries["Name1"] = np.nan
        Supply_Countries["Name3"] = np.nan
        me = []
        for i in self.EM_Supply_Countries.Name.unique():
            update = self.EM_Supply_Countries[
                self.EM_Supply_Countries["Name"] == i
            ].index.values
            for j in update:
                if (self.EM_Supply_Countries["Year"][j]) == "2014":
                    me.append(j)
        for i in range(len(Supply_Countries["Name1"])):
            a = me[i]
            Supply_Countries["Name1"][i] = self.EM_Supply_Countries["Name1"][a]
            Supply_Countries["Name3"][i] = self.EM_Supply_Countries["Name3"][a]
        for i in self.EM_Supply_Countries.Year.unique():
            Supply_Countries[i] = np.nan
        for i in self.EM_Supply_Countries.Year.unique():
            cur = self.EM_Supply_Countries[
                self.EM_Supply_Countries["Year"] == i
            ].index.values
            me = []
            for j in cur:
                me.append(self.EM_Supply_Countries["value"][j])
            for w in range(len(Supply_Countries[i]) - 1):
                Supply_Countries[i][w] = me[w]

        Supply_Countries.to_csv("EM_Supply_WithCountries.csv", index=False)
        print(Supply_Countries)
        return Supply_Countries

    def EM_AfricanBArs_Resconstruct(self):
        a = c = 0
        for i in self.AFRICAN_BarsTable["mine_id"]:
            currentIndex = self.MINE_Table[self.MINE_Table["mine_id"] == i].index.values
            currentIndex = currentIndex[0]
            self.AFRICAN_BarsTable["mine_id"][a] = self.MINE_Table["mine"][currentIndex]
            a = a + 1
        for i in self.AFRICAN_BarsTable["year_id"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.AFRICAN_BarsTable["year_id"][c] = self.YEAR_Table["year"][yearIndex]
            c = c + 1
        # self.AFRICAN_BarsTable.drop(
        #     ["creation_date", "updation_date"], axis=1, inplace=True
        # )
        self.AFRICAN_BarsTable.columns = ["EM_Africa_Guinea_id", "Name", "Years", "Bar"]
        AfricaBars = pd.DataFrame()
        AfricaBars["Name"] = self.AFRICAN_BarsTable.Name.unique()
        for i in self.AFRICAN_BarsTable.Years.unique():
            AfricaBars[i] = np.nan
        for j in self.AFRICAN_BarsTable.Years.unique():
            cur = self.AFRICAN_BarsTable[
                self.AFRICAN_BarsTable["Years"] == j
            ].index.values
            me = []
            for i in cur:
                me.append(self.AFRICAN_BarsTable["Bar"][i])
            for w in range(len(AfricaBars[j])):
                AfricaBars[j][w] = me[w]
        AfricaBars.to_csv("EM_Africa_BARS_Chalco_Boffa_Guinea.csv", index=False)
        return AfricaBars

    def EM_Rank_Structure(self):
        a = c = 0
        for i in self.EM_RANK["mine_id"]:
            currentIndex = self.MINE_Table[self.MINE_Table["mine_id"] == i].index.values
            currentIndex = currentIndex[0]
            self.EM_RANK["mine_id"][a] = self.MINE_Table["mine"][currentIndex]
            a = a + 1
        for i in self.EM_RANK["year_id"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.EM_RANK["year_id"][c] = self.YEAR_Table["year"][yearIndex]
            c = c + 1

        self.EM_RANK.drop(["creation_date", "updation_date"], axis=1, inplace=True)
        self.EM_RANK.columns = ["Name", "Years", "Rank"]
        Rank = pd.DataFrame()
        Rank["Name"] = self.EM_RANK.Name.unique()
        for i in self.EM_RANK.Years.unique():
            Rank[i] = np.nan
        for j in self.EM_RANK.Years.unique():
            cur = self.EM_RANK[self.EM_RANK["Years"] == j].index.values
            me = []
            for i in cur:
                me.append(self.EM_RANK["Rank"][i])
            for w in range(len(Rank[j])):
                Rank[j][w] = me[w]
        Rank.to_csv("EM_Rank.csv", index=False)
        # print(Rank)
        return Rank

    # Start here
    def DRW_DWN(self):
        DRW = pd.DataFrame(columns=["Years"])
        DRW.loc[0] = "Demand flowign into rest of model"
        for i in self.em_lbt.year_id.unique():
            DRW[i] = np.nan
        for j in self.em_lbt.year_id.unique():
            cur = self.em_lbt[self.em_lbt["year_id"] == j].index.values
            me = []
            for i in cur:
                me.append(self.em_lbt["Demand_flowign_into_rest_of_model"][i])
            for w in range(len(DRW[j])):
                DRW[j][w] = me[w]
        DRW.to_csv("EM_Drw Dwn Model.csv", index=False)
        # print(DRW)
        return DRW
        # print(DRW)

    def DDM(self):
        ddm = pd.DataFrame(columns=["Years"])
        ddm.loc[0] = "Demand from existing and planned merchant refiners"
        for i in self.em_lbt.year_id.unique():
            ddm[i] = np.nan
        for j in self.em_lbt.year_id.unique():
            cur = self.em_lbt[self.em_lbt["year_id"] == j].index.values
            me = []
            for i in cur:
                me.append(
                    self.em_lbt["Demand_from_existing_and_planned_merchant_refiners"][i]
                )
            for w in range(len(ddm[j])):
                ddm[j][w] = me[w]

        aaa = pd.DataFrame(columns=["Years"])
        aaa.loc[0] = "Most likely uptake of imports BX by inland refineries"

        for i in self.em_lbt.year_id.unique():
            aaa[i] = np.nan
        for j in self.em_lbt.year_id.unique():
            cur = self.em_lbt[self.em_lbt["year_id"] == j].index.values
            me = []
            for i in cur:
                me.append(
                    self.em_lbt[
                        "Most_likely_uptake_of_imports_BX_by_inland_refineries"
                    ][i]
                )
            for w in range(len(ddm[j])):
                aaa[j][w] = me[w]
        ddm.loc[1] = aaa.loc[0].copy()
        ddm.to_csv("EM_DDM.csv", index=False)
        print(ddm)
        return ddm

    def total_Demand(self):

        TD = pd.DataFrame(columns=["Years"])
        TD.loc[0] = "Total Demand"
        for i in self.em_lbt.year_id.unique():
            TD[i] = np.nan
        for j in self.em_lbt.year_id.unique():
            cur = self.em_lbt[self.em_lbt["year_id"] == j].index.values
            me = []
            for i in cur:
                me.append(self.em_lbt["Total_Demand"][i])
            for w in range(len(TD[j])):
                TD[j][w] = me[w]
        print(TD)
        TD.to_csv("Total Demand.scv", index=False)
        return TD

    def Bar_For_Act_Supply(self):

        BFAS = pd.DataFrame(columns=["Years"])
        BFAS.loc[0] = "BAR for Actual Supply "
        for i in self.em_lbt.year_id.unique():
            BFAS[i] = 0

        for j in self.em_lbt.year_id.unique():
            cur = self.em_lbt[self.em_lbt["year_id"] == j].index.values
            me = []
            for i in cur:
                me.append(self.em_lbt["BAR_for_Actual_Supply"][i])
            for w in range(len(BFAS[j])):
                BFAS[j][w] = me[w]
        # print(BFAS.iloc[0, 1])
        # BFAS.columns = [""]
        # print(BFAS)
        BFAS.to_csv("Bars for Actual supply.csv", index=False)
        BFAS.fillna(value=0, inplace=True)
        return BFAS

    def CBIX_Actual(self):

        CBIX = pd.DataFrame(columns=["Years"])
        CBIX.loc[0] = "CBIX - Actual"
        for i in self.em_lbt.year_id.unique():
            CBIX[i] = np.nan
        for j in self.em_lbt.year_id.unique():
            cur = self.em_lbt[self.em_lbt["year_id"] == j].index.values
            me = []
            for i in cur:
                me.append(self.em_lbt["CBIX_Actual"][i])
            for w in range(len(CBIX[j])):
                CBIX[j][w] = me[w]
        # print(CBIX)
        CBIX.to_csv("CBIX Actual.csv", index=False)
        return CBIX

    def Bars_IN_Use_Weiqiao(self):

        BIUW = pd.DataFrame(columns=["Years"])
        BIUW.loc[0] = "BARs in use for overall Weiqiao self Supply"

        for i in self.em_lbt.year_id.unique():
            BIUW[i] = np.nan
        for j in self.em_lbt.year_id.unique():
            cur = self.em_lbt[self.em_lbt["year_id"] == j].index.values
            me = []
            for i in cur:
                me.append(self.em_lbt["bars_in_use_for_overall_weiqiao_self_supply"][i])
            for w in range(len(BIUW[j])):
                BIUW[j][w] = me[w]
        # print(BIUW)
        BIUW.to_csv("Bars In Use Weiqiao.csv", index=False)
        return BIUW

    # Ends

    # done
    def AMR_supply(self):
        a = c = 0

        for i in self.AMR_SUPPLY["mine_id"]:
            currentIndex = self.MINE_Table[self.MINE_Table["mine_id"] == i].index.values
            currentIndex = currentIndex[0]
            self.AMR_SUPPLY["mine_id"][a] = self.MINE_Table["mine"][currentIndex]
            a = a + 1
        for i in self.AMR_SUPPLY["year_id"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.AMR_SUPPLY["year_id"][c] = self.YEAR_Table["year"][yearIndex]
            c = c + 1
        # self.AMR_SUPPLY.drop(
        #     ["creation_date", "updation_date"], axis=1, inplace=True
        # )
        self.AMR_SUPPLY.columns = ["ID", "Self_Supply", "Years", "value"]
        amr_sup = pd.DataFrame()
        amr_sup["Self Supply"] = self.AMR_SUPPLY.Self_Supply.unique()
        for i in self.AMR_SUPPLY.Years.unique():
            amr_sup[i] = np.nan
        for j in self.AMR_SUPPLY.Years.unique():
            cur = self.AMR_SUPPLY[self.AMR_SUPPLY["Years"] == j].index.values
            me = []
            for i in cur:
                me.append(self.AMR_SUPPLY["value"][i])
            for w in range(len(amr_sup[j])):
                amr_sup[j][w] = me[w]
        amr_sup.to_csv("AMR_Supply.csv", index=False)
        print(amr_sup)
        return amr_sup

    def lbt_em(self):
        d = 0
        lbt_em = pd.DataFrame()
        lbt_em["Name"] = self.RegionTable.copy()
        for i in self.lbt["region_id"]:
            regionIndex = self.REGION_Table[
                self.REGION_Table["region_id"] == i
            ].index.values
            regionIndex = regionIndex[0]
            self.lbt["region_id"][d] = self.REGION_Table["region"][regionIndex]
            d += 1
            # print(self.lbt)
        c = 0
        for i in self.lbt["year_id"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.lbt["year_id"][c] = self.YEAR_Table["year"][yearIndex]
            c = c + 1
        for i in self.lbt.year_id.unique():
            lbt_em[i] = 0
        aus = self.lbt[self.lbt["region_id"] == "Australasia"].index.values
        afr = self.lbt[self.lbt["region_id"] == "Africa"].index.values
        asi = self.lbt[self.lbt["region_id"] == "Asia ex-China"].index.values
        ame = self.lbt[self.lbt["region_id"] == "Americas"].index.values
        oth = self.lbt[self.lbt["region_id"] == "Other"].index.values
        aa = []
        bb = []
        cc = []
        dd = []
        ee = []
        for i in aus:
            aa.append(self.lbt["Value"][i])
        for i in afr:
            bb.append(self.lbt["Value"][i])
        for i in asi:
            cc.append(self.lbt["Value"][i])
        for i in ame:
            dd.append(self.lbt["Value"][i])
        for i in oth:
            ee.append(self.lbt["Value"][i])

        a = 0
        for j in self.lbt.year_id.unique():
            lbt_em[j][0] = float(aa[a])
            lbt_em[j][1] = float(bb[a])
            lbt_em[j][2] = float(cc[a])
            lbt_em[j][3] = float(dd[a])
            lbt_em[j][4] = float(ee[a])
            a += 1
        return lbt_em


tab = priceForecastRestructure()
# tab.total_Demand()
# tab.DRW_DWN()
# tab.DDM()
# tab.CBIX_Actual()

# tab.Bars_IN_Use_Weiqiao()


# tab.AMR_supply()
# print(tab.RegionTable)
# tab.lbt_em()

# tab.EM_Supply_Restructure()
# tab.EM_SupplyCountries_Restructure()
# tab.EM_Rank_Structure()


tab.Bar_For_Act_Supply()
# tab.EM_AfricanBArs_Resconstruct()
