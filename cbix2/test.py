import pandas as pd
from flatdb.flatdbconverter import reverse, read_output_database

# cbix_snap = pd.read_csv('row_mining_snapshot.csv')
# 2014-2030
cbix_out_set = ["2014 Chosen Mines"]
cbix_snap = read_output_database(262)

data = reverse(cbix_snap, model="Row mining model", output_set=["Chosen Mines"])
data = data["Row mining model"]
cm_rev = data["Chosen Mines"].transpose()
print(cm_rev)
