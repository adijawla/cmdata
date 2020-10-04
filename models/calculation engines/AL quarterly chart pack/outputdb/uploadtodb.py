import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import time
from datetime import datetime


class snapshot():
    def __init__(self):
        engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/outputdb_updated?driver=ODBC+Driver+17+for+SQL+Server", encoding ='utf-8')
        q = 'SELECT TOP (1) shapshot_id FROM [dbo].[snapshot_table] ORDER BY shapshot_id DESC;'
        try:
            self.id = pd.read_sql_query(q, engine)['shapshot_id'][0]+1
            
        except:
            self.id = 1
        
        

class upload():
    def __init__(self,snapshot_output_data):
        engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/outputdb_updated?driver=ODBC+Driver+17+for+SQL+Server", encoding ="utf-8", fast_executemany=True)
        #snapshot_output_data = pd.read_csv(r"C:\Users\magmarkd\Desktop\Production code with Flat DB\row\snapshot_output_data.csv")
        snapshot_output_data.rename(columns = {'output_label':'output_lebel','output_row':'outrow'}, inplace = True)
        snapshot_output_data.reindex(columns=["snapshot_id","model_id","output_set", "output_id", "output_lebel","outrow", "output_value", "override_value", "actual_value"])
        
        query_l = 'SELECT * FROM output_def_table'
        uni_lable = snapshot_output_data['output_lebel'].unique()
        db_label = pd.read_sql_query(query_l,engine)['output_lebel']
        #labels = list(uni_lable.values)
        #labels = labels.exten
        uni_lable = list(set([str(a).title() for a in uni_lable]))
        db_label = list(set([str(a).title() for a in db_label]))
        diff = list(set(uni_lable) - set(db_label))
        diff = list(diff)
        
        
        #print(diff)
        if len(diff) != 0:
            out_lable = pd.DataFrame(columns=["output_lebel"])
            out_lable["output_lebel"] = diff
            out_lable.to_csv('unique_label.csv', index=False)
            out_def_q =  out_lable.to_sql("output_def_table",index=False, con=engine,if_exists='append')
            
            

        db_lb_oi = pd.read_sql_query(query_l,engine)
        print(db_lb_oi)

        snapshot_output_data = snapshot_output_data.drop(['output_id'],axis=1)

        query1 = "SET IDENTITY_INSERT snapshot_table ON"
        engine.execute(query1)
        cur = datetime.now()
        time1 = cur.strftime('%H:%M:%S')
        date = cur.strftime('%Y-%m-%d')
        v = snapshot()
        print(snapshot_output_data["snapshot_id"][0])
        model_id = snapshot_output_data['model_id'][0]
        
        #mn_query = 'select distinct  model_name from model_info where model_id = '+str(model_id)+';'
        mn_query = 'select distinct  model_name from model_details where model_id = '+str(model_id)+';'
        print("name" ,pd.read_sql_query(mn_query, engine)['model_name'].values[0])
        m_name = pd.read_sql_query(mn_query, engine)['model_name'][0]
        # query = "INSERT INTO snapshot_table (shapshot_id,model_id,snapshot_run_date,snapshot_run_time,create_user_id,,model_name) VALUES ("+str(v.id)+","+str(snapshot_output_data['model_id'][0])+",{d'"+str(date)+"'},{t'"+str(time1)+"'},0,"+"'"+str(m_name)+"'")"
        query = f'INSERT INTO snapshot_table (shapshot_id,model_id,snapshot_run_date,snapshot_run_time,create_user_id,model_name) VALUES ({v.id},{snapshot_output_data["model_id"][0]},' + "{d'" + f'{date}' + "'},{t'" + f'{time1}' + "'},0," + f"'{m_name}');"
        #query = 'INSERT INTO snapshot_table (shapshot_id,model_id,snapshot_run_date,snapshot_run_time,create_user_id) VALUES ("+str(v.id)+","+str(snapshot_output_data['model_id'][0])+",{d'"+str(date)+"'},{t'"+str(time1)+"'},0,"+"'"+str(m_name)+"'"+")'"
        print(query)
        engine.execute(query)
        output_db_upload_time = time.perf_counter()
        #q = 'SELECT TOP (1) shapshot_id FROM [dbo].[snapshot_table] ORDER BY shapshot_id DESC;'
        #snapshot_id = pd.read_sql_query(q, engine)['shapshot_id'][0]+1
      
        
        print("started")
        snapshot_output_data["output_value"] = snapshot_output_data["output_value"].astype(str)
        snapshot_output_data["snapshot_id"] = v.id
        snapshot_output_data = snapshot_output_data.merge(db_lb_oi.loc[:, ["output_lebel", "output_id"]], on="output_lebel", how="left")
        #snapshot_output_data.to_csv("snapshot_output_data.csv")
        snapshot_output_data.to_sql("snapshot_output_data",index=False, con=engine,if_exists='append', chunksize=1000)
        
        model_info = snapshot_output_data.drop_duplicates(["output_id", "output_lebel", "output_set", "model_id"])
        #model_info.to_csv("model_info.csv")
        mi_time = f"{date} {time1}"
        model_info["update_date_time"] = mi_time
        model_info["model_name"] = pd.read_sql_query(mn_query, engine)['model_name'].values[0]
        model_info = model_info.loc[:, ["model_id", "model_name", "output_set", "output_id", "update_date_time"] ]
        model_info.to_sql("model_info",index=False, con=engine,if_exists='append')
        print("Time taken to upload to output db: {0} ".format(time.perf_counter() - output_db_upload_time))
