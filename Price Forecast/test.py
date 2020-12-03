import pandas as pd
from flatdb.flatdbconverter import Flatdbconverter, read_output_database, reverse

# cbix_snap = pd.read_csv('cbix2_snapshot_output_data.csv')

# cbix_snap = read_output_database(191, ["Default Data_Tables Viu_Cost_Data_Table"])
# print(cbix_snap)
# data = reverse(cbix_snap, model="CBIX machine version used in price forecasts", output_set=["Default Data_Tables Viu_Cost_Data_Table"])

ddm_cbix_set = ["Cockpit Import Bx Demand Summary As Aa "]
ddm_cbix = read_output_database(207, ddm_cbix_set)
cockpit = reverse(ddm_cbix,model="Draw Down Model CBIX 55", output_set=ddm_cbix_set)
print(cockpit)
