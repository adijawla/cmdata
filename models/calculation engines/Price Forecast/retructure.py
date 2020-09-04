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
            "SELECT * from [dbo].[EM_Supply]", self.conn
        )
        self.MINE_Table = pd.read_sql_query("SELECT * from [dbo].[mine]", self.conn)
        self.COUNTRY = pd.read_sql_query("SELECT * from [dbo].[country]", self.conn)
        self.YEAR_Table = pd.read_sql_query("SELECT * from [dbo].[year]", self.conn)
        self.REGION_Table = pd.read_sql_query("SELECT * from [dbo].[region]", self.conn)
        self.EM_Supply_Countries = pd.read_sql_query(
            "SELECT * from [dbo].[EM_Supply_Countries]", self.conn
        )
        self.AFRICAN_BarsTable = pd.read_sql_query(
            "SELECT * from [dbo].[EM_Africa_Guinea]", self.conn
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
        self.EM_Supply1.drop(["creation_date", "updation_date"], axis=1, inplace=True)
        self.EM_Supply1.columns = ["EM_Supply_id", "Name", "Country", "Year", "Supply"]
        print(self.EM_Supply1)
        self.EM_Supply1.to_csv("EM_Supply.csv", index=False)

    def EM_SupplyCountries_Restructure(self):
        a = b = c = d = 0
        for i in self.EM_Supply_Countries["attribute"]:
            yearIndex = self.YEAR_Table[self.YEAR_Table["year_id"] == i].index.values
            yearIndex = yearIndex[0]
            self.EM_Supply_Countries["attribute"][c] = self.YEAR_Table["year"][
                yearIndex
            ]
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
        self.EM_Supply_Countries.drop(
            ["creation_date", "updation_date"], axis=1, inplace=True
        )
        self.EM_Supply_Countries.columns = [
            "EM_Supply_Countries_id",
            "Name",
            "Name1",
            "Name3",
            "Year",
            "value",
        ]
        self.EM_Supply_Countries.to_csv("EM_Supply_WithCountries.csv", index=False)

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
        self.AFRICAN_BarsTable.drop(
            ["creation_date", "updation_date"], axis=1, inplace=True
        )
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

    def printTable(self):
        cur = self.conn.cursor()
        # for i in cur.tables():
        #     print(i.table_name)
        # print(cur.execute("select * from EM_Supply"))
        EM_Supply1 = pd.read_sql_query("SELECT * from [dbo].[EM_Supply]", self.conn)
        print(EM_Supply1)
        print(EM_Supply1.columns)

        # err = cur.execute("select * from EM_Supply")
        # print(err)


tab = priceForecastRestructure()

tab.EM_Supply_Restructure()
tab.EM_SupplyCountries_Restructure()
tab.EM_AfricanBArs_Resconstruct()
