from flatdbconverter import Flatdbconverter
import pandas as pd

db_conv = Flatdbconverter("Margin Analysis preparation sheets")
row = pd.read_csv('row_mining_snapshot.csv')
all_rev = db_conv.reverse(row, "ROW minind model", ["Collector Cash_Fob_(Us$-Dmt)", "Collector Full_Fob_(Us$-Dmt)", "Collector Cash_Cost_Cfr_(Us$-Dmt)", "Collector Full_Cost_Cfr_(Us$-Dmt)", "Collector Sustaining_Capital_(Us$-Dmt)", "Collector Capital_(Us$-Dmt)", "Collector Freight_(Us$-Dmt)", "Collector Moisture"])

print(all_rev)