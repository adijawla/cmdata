import pandas as pd
import io
import json

all = {}
depdatae = pd.read_csv("ddm/depdatae.csv")
depdatae2 = pd.read_csv("ddm/depdatae2.csv")
proddatae = pd.read_csv("ddm/proddatae.csv")
proddatae2 = pd.read_csv("ddm/proddatae2.csv")
QuickSummarySwitches = pd.read_csv("ddm/QuickSummarySwitches.csv")
bauxitedatae = pd.read_csv("ddm/bauxitedatae.csv")
cockpit3rdPartySwitching = pd.read_csv("ddm/cockpit3rdPartySwitching.csv")
plotlink = pd.read_csv("ddm/plotlink.csv")
cockpitdatae = pd.read_csv("ddm/cockpitdatae.csv")
cockpitAllow3rdPartyTrade = pd.read_csv("ddm/cockpitAllow3rdPartyTrade.csv")
# print(a.to_json())


varss = ["depdatae", "depdatae2", "proddatae", "proddatae2", "QuickSummarySwitches", "bauxitedatae", "cockpit3rdPartySwitching", "plotlink", "cockpitdatae", "cockpitAllow3rdPartyTrade"] 
with io.open("templates.json", 'w') as db_file:
    for a in varss:
        d = {a: eval(a).to_dict(orient='list')}
        all.update(d)
    db_file.write(json.dumps(all)) 


def load_data():
    filename = "templates.json"
    with open(filename) as f:
        data = json.load(f)
    return data

all_json_data = load_data()

def get_templates():
    data = {}
    for key in all_json_data.keys():
        data[key] = pd.DataFrame(all_json_data[key])
    return data

# print(get_templates())