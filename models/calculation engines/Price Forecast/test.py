import pandas as pd
from flatdbconverter import Flatdbconverter
db_conv = Flatdbconverter('Price forecast model')

cbix_snap = pd.read_csv('cbix2_snapshot_output_data.csv')

data = db_conv.reverse(cbix_snap, model="CBIX machine version used in price forecasts", output_set=["Default Data_Tables Viu_Cost_Data_Table"])
print(data)
