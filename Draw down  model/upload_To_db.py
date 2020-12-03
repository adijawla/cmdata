import connect
import time
import pandas as pd

snapshot_output_data = pd.read_csv(r"C:\Users\magmarkd\Desktop\timer\snapshot_output_data.csv")



a = connect.to_db()
conn = a.outputstart()
cursor = conn.cursor()
cursor.fast_executemany = True
output_db_upload_time = time.perf_counter()
for i in range(snapshot_output_data.shape[0]):
    x = snapshot_output_data.loc[i]
    cursor.execute("INSERT INTO dbo.snapshot_output_data([model_id],[output_set],[output_lebel],[outrow],[output_value],[override_value],[actual_value]) values (?,?,?,?,?,?,?)",str(x[1]),str(x[2]),str(x[5]),str(x[4]),str(x[6]),'' if pd.isna(x[7]) else str(x[7]),'' if pd.isna(x[8]) else str(x[8]))
print("Time taken to upload to output db: {0} ".format(time.perf_counter() - output_db_upload_time))
'''
for i in range(snapshot_output_data.shape[0]):
    chunk = snapshot_output_data.iloc[i:i+1,:].values.tolist()
    t =  tuple(tuple(x) for x in chunk)
    cursor.executemany("INSERT INTO dbo.snapshot_output_data([model_id],[output_set],[output_lebel],[outrow],[output_value],[override_value],[actual_value]) values (?,?,?,?,?,?,?)",t)
    
    '''
cursor.close()
print("done")