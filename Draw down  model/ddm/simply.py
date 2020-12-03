import pandas as pd

def put(db,index1,index2,index3,column,value):
    idx = pd.IndexSlice
    db.at[idx[index1,index2,index3],column] = value
def get(db,index1,index2,index3,column):
    idx = pd.IndexSlice
    return db.loc[index1,index2,index3][column].sum()
