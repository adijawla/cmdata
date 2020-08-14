import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import time

class upload():
    def __init__(self,snapshot_output_data):
        #snapshot_output_data = pd.read_csv(r"C:\Users\magmarkd\Desktop\Production code with Flat DB\row\snapshot_output_data.csv")
        snapshot_output_data.rename(columns = {'output_label':'output_lebel','output_row':'outrow'}, inplace = True)
        snapshot_output_data.reindex(columns=["snapshot_id","model_id","output_set", "output_id", "output_lebel","outrow", "output_value", "override_value", "actual_value"])
        snapshot_output_data = snapshot_output_data.drop(['snapshot_id','output_id'],axis=1)
        engine = create_engine("mssql+pyodbc://letmetry:Ins201799@magdb.database.windows.net:1433/outputdb_updated?driver=ODBC+Driver+17+for+SQL+Server")
        output_db_upload_time = time.perf_counter()
        print("started")
        snapshot_output_data.to_sql("snapshot_output_data",index=False, con=engine,if_exists='append')
        print("Time taken to upload to output db: {0} ".format(time.perf_counter() - output_db_upload_time))
