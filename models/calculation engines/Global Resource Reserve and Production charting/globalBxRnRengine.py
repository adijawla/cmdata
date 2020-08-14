import pandas as pd
from flatdbconverter import Flatdbconverter, read_from_database, upload

country = pd.read_csv('country.csv')
country_lk = country.loc[:, ['country_Id', 'Country']].values
country_lk = dict(country_lk)
print(country_lk)
model = Flatdbconverter('Global Resource Reserve and Production charting')
class global_bx():
    def __init__(self):
        self.global_bx_input = read_from_database("Globalbxdata").replace(country_lk)
    def calc(self):
        self.global_bx_input = self.global_bx_input.fillna(0)
        self.global_bx_input.to_csv('Global Bx R&R outputs.csv', index=False)
g = global_bx()
g.calc()

db_list = [model.single_year_mult_out(g.global_bx_input, 'Main table'),]

snapshot_output_data = pd.concat(db_list, ignore_index=True)
snapshot_output_data = snapshot_output_data.loc[:, model.out_col]
snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
upload(snapshot_output_data)
reversed_model = model.reverse()
print(reversed_model)