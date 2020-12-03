import pandas as pd
import io
import json
from restruc import *
all = {}
print("temp started")
ph = x.data('proddatah')
QuickSummarySwitches = x.data('quicksummaryswitches')
cockpitAllow3rdPartyTrade = x.data('cockpitallow3rdpartytrade')
cockpit3rdPartySwitching = x.data('cockpit3rdpartyswitching')
cockpitdatae = cockpitAllow3rdPartyTrade.copy()
proddatae = ph.copy()
depdatae = QuickSummarySwitches.copy()
st = '2005'
while True:
    if st in ph.columns:
        proddatae[st] = 0
        depdatae[st] = 0
        cockpitdatae[st] = 0
        st = str(int(st)+1)
    else:
        break
depdatae2 = depdatae.copy()
ind = depdatae.index.tolist()
depdatae2 = depdatae2.drop([ind[-1],ind[-2],ind[-3]])
bauxitedatae = cockpitdatae.copy()
cat = bauxitedatae['category'].tolist()
print(cat)
print(bauxitedatae.columns)
bauxitedatae = bauxitedatae.drop(['category'],axis=1)
bauxitedatae = bauxitedatae.drop(range(4,bauxitedatae.shape[0]))
bauxitedatae = bauxitedatae.assign(ind = ['Existing and Planned Merchant Refiners',
                                                 'Maximum Potential bauxite imports by inland refinery reserves depletion',
                                                 'Most likely uptake of imports BX by inland refineries',
                                                 'Demand flowign into rest of model']) 

proddatae2 = pd.DataFrame(columns=proddatae.columns)
for i in range(0,proddatae.shape[0]):
    x = len(cat)
    try:
        for j in range(x):
            proddatae2.loc[i*x+j,"bauxite"] = proddatae.loc[i,"bauxite"]
            proddatae2.loc[i*x+j,"owner"] = proddatae.loc[i,"owner"]
            proddatae2.loc[i*x+j,"ownership"] = proddatae.loc[i,"ownership"]
            proddatae2.loc[i*x+j,"technology"] = proddatae.loc[i,"technology"]
            proddatae2.loc[i*x+j,"category"] = cat[j]
            proddatae2.loc[i*x+j,"province"] = proddatae.loc[i,"province"]
            proddatae2.loc[i*x+j,"refinery"] = proddatae.loc[i,"refinery"]
    except:
        break




#depdatae = pd.read_csv("ddm/depdatae.csv")
#depdatae2 = pd.read_csv("ddm/depdatae2.csv")
#proddatae = pd.read_csv("ddm/proddatae.csv")
#proddatae2 = pd.read_csv("ddm/proddatae2.csv")
#pd.read_csv("ddm/QuickSummarySwitches.csv")
#bauxitedatae = pd.read_csv("ddm/bauxitedatae.csv")
#pd.read_csv("ddm/cockpit3rdPartySwitching.csv")
plotlink = proddatae2.copy()
plotlink["Factor X"]  = 0
plotlink["Alumina Grade"] = 0
plotlink["A/S"] = 0
plotlink["open stock"] = 0
#proddatae2.rename(columns = {'province':'Province'}, inplace = True)
#cockpitdatae = pd.read_csv("ddm/cockpitdatae.csv")
#pd.read_csv("ddm/cockpitAllow3rdPartyTrade.csv")
# print(a.to_json())


# depdatae = pd.read_csv("depdatae.csv")
# depdatae2 = pd.read_csv("depdatae2.csv")
# proddatae = pd.read_csv("proddatae.csv")
# proddatae2 = pd.read_csv("proddatae2.csv")
# QuickSummarySwitches = pd.read_csv("QuickSummarySwitches.csv")
# bauxitedatae = pd.read_csv("bauxitedatae.csv")
# cockpit3rdPartySwitching = pd.read_csv("cockpit3rdPartySwitching.csv")
# plotlink = pd.read_csv("plotlink.csv")
# cockpitdatae = pd.read_csv("cockpitdatae.csv")
# cockpitAllow3rdPartyTrade = pd.read_csv("cockpitAllow3rdPartyTrade.csv")
# print(a.to_json())
print("templ done")

varss = ["depdatae", "depdatae2", "proddatae", "proddatae2", "QuickSummarySwitches", "bauxitedatae", "cockpit3rdPartySwitching", "plotlink", "cockpitdatae", "cockpitAllow3rdPartyTrade"] 
with io.open("templates.json", 'w') as db_file:
    for a in varss:
        d = {a: eval(a).to_dict(orient='list')}
        all.update(d)
    try:
        db_file.write(json.dumps(all))
    except:
        print(a)


def load_data():
    filename = "templates.json"
    with open(filename) as f:
        data = json.load(f)
    return data

all_json_data = load_data()

def get_templates():
    data = {}
    # for key in all_json_data.keys():
    #     data[key] = pd.DataFrame(all_json_data[key])
    for key in varss:
        data[key] = eval(key)

    return data

# print(get_templates())
