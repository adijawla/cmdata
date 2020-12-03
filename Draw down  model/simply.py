import pandas as pd

def put(db,index1,index2,index3,column=None,value=None, s ='2004', e = '2031'):
    if column == None:
        db.loc[(index1,index2,index3), s:e] = value
    else:
        # idx = pd.IndexSlice
        # db.at[idx[index1,index2,index3], column] = value
        db.loc[(index1,index2,index3), column] = value
def get(db,index1,index2,index3,column=None, s ='2004', e = '2031'):
    if column == None:
        idx = pd.IndexSlice
        val = db.loc[(index1,index2,index3), s:e ].values
        return val
    else:
        idx = pd.IndexSlice
        try:
            return db.loc[(index1,index2,index3)][column].values[0]
        except AttributeError:
            return db.loc[(index1,index2,index3)][column]