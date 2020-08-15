from flatdbconverter import Flatdbconverter
import pandas as pd
from flatdbconverter import read_output_database

db_conv = Flatdbconverter("Supply Demand Costs n Prices Alcoa")

# pr = pd.read_csv("price_forecast_snapshot.csv")
pr = read_output_database(5)
print(pr)
# row = pd.read_csv('row_mining_snapshot.csv')
# all_rev = db_conv.reverse(row, "ROW minind model", ["Collector Full_Cost_Cfr_(Us$-Dmt)", "Collector Freight_(Us$-Dmt)", "Collector Organics", "Collector Moisture", "Collector Bauxite_Style", "Collector Total_Alumina", "Collector Lt_Avail_Alumina", "Collector Vessels", "Collector Monohydrate", "Collector Total_Silica", "Collector Lt_R._Silica"])
pr_rev = db_conv.reverse(pr, "Price forecast model", ["Lbt 451", "1688" , "1689", "1690", "1818"])
print(pr_rev)

# print(pr)