import pandas as pd
from flatdbconverter import Flatdbconverter
db_conv = Flatdbconverter('CBIX machine version used in price forecasts')

cbix_snap = pd.read_csv('row_mining_snapshot.csv')

data = db_conv.reverse(cbix_snap, model="ROW minind model", output_set=["Chosen Mines"])
data = data["ROW minind model"]
cm_rev = data["Chosen Mines"].transpose()
print(cm_rev)
