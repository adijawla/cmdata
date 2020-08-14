import pandas as pd
import pyodbc

def func():
    server = 'magdb.database.windows.net'
    database = 'input_db'
    username = 'letmetry'
    password = 'Ins201799'
    driver= '{ODBC Driver 17 for SQL Server}'
           
    conn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
    mine = pd.read_sql_query("SELECT * FROM [dbo].[mine]", conn)
    mine.drop(columns =["creation_date","updation_date"], inplace = True)
    #mine = pd.read_csv('mine.V6.0.csv')
    cc_data = pd.read_sql_query("SELECT * FROM [dbo].[cc_other_data]", conn)
    cc_data.drop(columns =["creation_date","updation_date"], inplace = True)
    #cc_data = pd.read_csv('cc_other_data.csv')
    cc_line = pd.read_sql_query("SELECT * FROM [dbo].[cc_other_lines]", conn)
    cc_line.drop(columns =["creation_date","updation_date"], inplace = True)
    #cc_line = pd.read_csv('cc_other_lines.csv')

    d= {mine['mine_id'][x]:mine['mine'][x] for x in mine.index}

    #cc data
    cc_data.rename(columns={
        'label_regardless':'Label regardless',
        'linear_order':'Linear order',
        'rank':'Rank',
        'mine_id':'Name',
        'prodn_capacity':'Prodn / Capacity',
        'series_1':'Series 1',
        'series_2':'Series 2',
        'series_3':'Series 3',
        'series_4':'Series 4',
        'series_5':'Series 5',
        'series_6':'Series 6',
        'series_7':'Series 7',
        'series_8':'Series 8',
        'series_9':'Series 9',
        'series_10':'Series 10',
        'total_cost':'Total Cost'
            },inplace =True)

    cc_data['Name'] = [d[cc_data['Name'][x]] for x in cc_data.index]

    #cc lines
    cc_line.rename(columns={
        'extra_l1_x_1':'Extra L1 X 1',
        'extra_l1_y_1':'Extra L1 Y 1',
        'extra_l2_x_2':'Extra L2 X 2',
        'extra_l2_y_2':'Extra L2 Y 2',
        'extra_l3_x_3':'Extra L3 X 3',
        'extra_l3_y_3':'Extra L3 Y 3',
        'extra_l4_x_4':'Extra L4 X 4',
        'extra_l4_y_4':'Extra L4 Y 4'
        },inplace =True)
    return cc_data,cc_line


 

