import pandas as pd
import numpy as np
from flatdb.flatdbconverter import Flatdbconverter, read_from_database , read_output_database
from outputdb import uploadtodb

db_conv = Flatdbconverter("Bauxite grades in use field study results")
# snapshot_table = pd.read_sql_query('SELECT * FROM snapshot_table')
coll = read_output_database(126, ["Collector 2391"])
print(coll)
rev_col = db_conv.reverse(coll, 'Draw Down Model', ["Collector 2391"])
rev_col = rev_col["Draw Down Model"]
collector = rev_col["Collector 2391"]

def make_unique(data, enc):
    data = list(map(str, data))
    cols=pd.Series(data)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [dup + enc + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    return cols.values

# collector = pd.read_csv("collector_2391.csv")
# bx = pd.read_csv('input/as_ratio_bauxite_grades.csv')
bx = read_from_database('as_ratio_bauxite_grades')
bx = bx.loc[:, ['Date', 'Shanxi','Henan','Guizhou','Guangxi']]
print(bx)
bx["Date"] = make_unique(bx["Date"], '.')

col_names = ['Date', 'Shanxi','Henan','Guizhou','Guangxi', 'Shanxi_1','Henan_1','Guizhou_1' ,'Guangxi_1', 'Shanxi Forecast','Henan Forecast','Guizhou Forecast','Guangxi Forecast']
# col_names = make_unique(col_names, '_')


result_df = pd.DataFrame(columns=[])
result_df["Date"] = bx["Date"]
result_df["Shanxi"] = bx["Shanxi"]
result_df['Henan'] = bx['Henan']
result_df["Guizhou"] = bx['Guizhou']
result_df["Guangxi"] = bx['Guangxi']
# result_df[['Shanxi','Henan','Guizhou','Guangxi']] = pd.DataFrame([bx["Shanxi"], bx["Henan"], bx["Guizhou"], bx["Guangxi"]])
# result_df.loc[:, ['Date', 'Shanxi','Henan','Guizhou','Guangxi']] = result_df.loc[:, ['Date', 'Shanxi','Henan','Guizhou','Guangxi']].values

data1_df = pd.DataFrame(columns=[])
data1_df["Date"] = [a + '.8' for a in  list(map(str, range(2006, 2032)))]
data1_df[['Shanxi_1','Henan_1','Guizhou_1' ,'Guangxi_1', 'Shanxi Forecast', 'Henan Forecast', 'Guizhou Forecast', 'Guangxi Forecast']] = pd.DataFrame([np.full(8, np.nan)])


def cust_set_values(r, r_1, c, c_1):
    # print(collector.loc[collector['category'] == c, r].values)
    data1_df.loc[data1_df['Date'].isin(r), c_1] = collector.loc[collector['category'] == c, r_1].values.flatten()

first_years = [a + '.8' for a in  list(map(str, range(2008, 2018)))]
first_years_1 = list(map(str, range(2008, 2018)))
cust_set_values(first_years, first_years_1, 'Shanxi', 'Shanxi_1')
cust_set_values(first_years, first_years_1, 'Henan', 'Henan_1')
cust_set_values(first_years, first_years_1, 'Guizhou', 'Guizhou_1')
cust_set_values(first_years, first_years_1, 'Guangxi', 'Guangxi_1')

sec_years = [a + '.8' for a in  list(map(str, range(2017, 2032)))]
sec_years_1 = list(map(str, range(2017, 2032)))
cust_set_values(sec_years, sec_years_1, 'Shanxi', 'Shanxi Forecast')
cust_set_values(sec_years, sec_years_1, 'Henan', 'Henan Forecast')
cust_set_values(sec_years, sec_years_1, 'Guizhou', 'Guizhou Forecast')
cust_set_values(sec_years, sec_years_1, 'Guangxi', 'Guangxi Forecast')
# print(data1_df)

# print(col_names)


final_result = pd.DataFrame(columns=col_names)
print(result_df)
# final_result = final_result.merge(result_df, on='Date', how='outer')
final_result = pd.concat([final_result, result_df, data1_df])
final_result["Date"] = [a.split('.')[0] for a in final_result["Date"]]
final_result.to_csv('final.csv', index=False)
print(final_result)
snapshot = db_conv.single_year_mult_out(final_result, "Bauxite Grades")
uploadtodb.upload(snapshot)
