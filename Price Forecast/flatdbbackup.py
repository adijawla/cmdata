import pandas as pd
import numpy as np
import re
import time
import warnings
import os
import json
import io
from outputdb import uploadtodb
import pyodbc
from sqlalchemy import create_engine

def startupCheck():
    filename = "../flatdbconfig.json"
    if os.path.isfile(filename) and os.access(filename, os.R_OK):
        # checks if file exists
        print ("File exists and is readable")
    else:
        print ("Either file is missing or is not readable, creating file...")
        with io.open(os.path.join('.', filename), 'w') as db_file:
            db_file.write(json.dumps({}))

startupCheck()



models = {
	"Draw Down Model": 1,
	"China mining cost model": 2,
	"Economic overlay": 3,
	"Row mining model": 4,
	"Price forecast model": 5,
	"Margin Analysis preparation sheets": 6,
	"World trade data processing": 7,
	"ViU/Freight(CBIX)": 8,
	"CBIX machine version used in price forecasts": 9,
	"Quarterly Chart Pack": 10,
	"Aa Price Forecast (CHN)": 11,
	"Al Cost Model": 12,
	"Qrtly mining model": 13,
	"Aa Cost Model (CHN)": 14,
	"Guinea FOB Cost Model": 15,
	"CM Alcoa update": 16,
	"DDM Collector for Alcoa": 17,
	"Cost curve charting sheet": 18,
	"Watermelon charts": 19,
	"China bauxite data base": 20,
	"Weekly Chart Pack": 21,
	"Monthly Chart Pack": 22,
	"Trade Data Tiding": 23,
	"Trade Data Term Sheet": 24,
	"Supply Demand Costs n Prices Alcoa": 25,
	"Bauxite grades in use field study results": 26,
	"Global Resource Reserve and Production charting": 27,
    "Draw Down Model CBIX 45": 28,
    "Draw Down Model CBIX 55": 29,
    "Draw Down Model CBIX 65": 30,
}

def read_from_database(table):
        engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/input_db?driver=ODBC+Driver+17+for+SQL+Server")
        query = f'SELECT * FROM {table}'
        data = pd.read_sql(sql=query, con=engine)
        # converts number strings to numeric
        return data.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce')).fillna(data)

def read_output_database(snapshot_id, out_set=None):
        engine = create_engine("mssql+pyodbc://letmetry:T@lst0y50@magdb.database.windows.net:1433/outputdb_updated?driver=ODBC+Driver+17+for+SQL+Server")
        if out_set is None :
            query = f'SELECT * FROM snapshot_output_data WHERE snapshot_id = {snapshot_id}'
        else:
            query = f'SELECT * FROM snapshot_output_data WHERE snapshot_id = {snapshot_id} AND output_set IN {str(tuple(out_set))[:-2]})'
        data = pd.read_sql(sql=query, con=engine)
        data = data.rename(columns={"outrow" : "output_row", "output_lebel": "output_label" })
        # converts number strings to numeric
        return data

class Flatdbconverter():
    def __init__(self, model):
        self.out_structure = pd.DataFrame(columns=[])
        self.model = model
        self.output_labels = {}
        self.snapshot_ids = {}
        self.out_col = ['snapshot_id','model_id','output_set','output_id','output_label', 'output_row','output_value', 'override_value', 'actual_value', 'outputrow_pwb']
        self.v = uploadtodb.snapshot()

        if self.model not in models:
            raise Exception("Name doesnt exist in models list, please check the right name in model to instantiate Flatdbconverter")


    # helper function
    def get_label_id(self, labels):
            lookup = {**self.output_labels}
            max_id =  max(lookup.values()) if len(lookup.values()) > 0 else 0
            for i in range(len(labels)):
                labels[i] = str(labels[i])
                if labels[i] in lookup:
                    labels[i] = lookup[labels[i]]
                else:
                    max_id += 1
                    lookup[labels[i]] = max_id
                    labels[i] = max_id
            self.output_labels = lookup
            return labels

    def get_snapshot_id(self, snapshot):
            return int(self.v.id)


    def get_model_ids(self, model):
            if model in models:
                return models[model]

    # supports dfdb.csv, db.csv
    def single_year_mult_out(self, filepath, output_set_name, model=None, path=False):
        output_set_name = ' '.join(output_set_name.split('_'))
        output_set_name = output_set_name.title()
        if model is None:
            model = self.model
         # store setups
        setups = load_setups()
        if model not in setups.keys():
          setups[model] = {}
        setups[model] = {
            **setups[model],
            output_set_name:
            {
            "db_type": "single year multiple output",
            "model": model
            }
            }
        dump_configs(setups)
        if path :
            data = pd.read_csv(filepath).dropna(how='all')
        else:
            data = filepath.copy().dropna(how='all').dropna(how='all', axis=1).reset_index(drop=True)
            if data.columns[0] == 'index':
                data = data.drop(['index'], axis=1)
        output = pd.DataFrame(columns=self.out_col)
        columns = list(data.columns)
        output_values = np.array(data.loc[:, columns].values)
        output_values = np.array([a for a in output_values if not pd.isna(a).all()]).flatten()
        cols = data.shape[1]
        total_rows = len(output_values)
        rows = total_rows // cols
        output_rows = np.array([[a]*cols for a in range(rows)]).flatten()
        output_id = self.get_label_id([*columns])
        output_set = [output_set_name]*total_rows
        snapshot_id = np.full(total_rows, self.get_snapshot_id(model))
        output['snapshot_id'] = snapshot_id
        output['output_set'] = output_set
        output['output_row'] = output_rows
        output['output_id'] = output_id * rows
        output['output_label'] = columns * rows
        output['output_value'] = np.nan_to_num(output_values)
        output['model_id'] = np.full(total_rows, self.get_model_ids(model))
        output['override_value'] = np.nan
        output['actual_value'] = np.nan
        output['outputrow_pwb'] = np.nan
        if output.empty:
            warnings.warn(f"Output set: {output_set_name}\nEmpty result, please make sure you're using the right params for the right table")
        return output

    def multi_year_multi_out(self, filepath, output_set_name, label="Year", idx_of_index=[], idx_of_values=[], model=None, path=False, col_params=[], make_multi=None, not_num_indexed=False):
        output_set_name = ' '.join(output_set_name.split('_'))
        output_set_name = output_set_name.title()
        if model is None:
            model = self.model
        setups = load_setups()
        if model not in setups.keys():
          setups[model] = {}
        setups[model] = {
            **setups[model],
            output_set_name:
            {
                "db_type": "multiple year multiple output",
                "config": {
                    "label": label
                }
            }
            }
        dump_configs(setups)
        if path :
            data = pd.read_csv(filepath).dropna(how='all', axis=1)
        else:
            if not_num_indexed:
                data = filepath.copy().dropna(how='all', axis=1).reset_index()
            else:
                data = filepath.copy().dropna(how='all', axis=1).reset_index(drop=True)
            if data.columns[0] == 'index' and len(col_params) == 0:
                data = data.drop(['index'], axis=1)
            columns = list(data.columns)
            for param in col_params:
                columns[param[0]] = param[1]
            data.columns = columns

        if len(idx_of_index) == 0 and len(idx_of_values) != 0:
            raise Exception("Can only set both idx_of_index and idx_of_values or None")
        if len(idx_of_index) != 0 and len(idx_of_values)== 0:
            raise Exception("Can only set both idx_of_index and idx_of_values or None")

        output = pd.DataFrame(columns=[])
        rows = data.shape[0]
        columns = data.columns

        if len(idx_of_index) > 0 and len(idx_of_values) > 0 :
            col_ex_years = get_val_from_idx(idx_of_index, columns)
            years = get_val_from_idx(idx_of_values, columns)
        else:
            col_ex_years = exclude_years(columns)
            #col_ex_years_values = data.loc[:, col_ex_years].values
            years = list(set(map(str,columns)) - set(map(str, col_ex_years)))
            #print(years)
            years = sorted(years)

        # col_ex_years = exclude_years(columns)
        # print(col_ex_years)
        # years = list(set(columns) - set(col_ex_years))
        # years = sorted(years)
        row_ind = col_ex_years[:-1]
        field = col_ex_years[-1]
        row_ind_values = data.loc[:, row_ind].drop_duplicates().values
        field_values = data.loc[:, field].unique()
        if make_multi is not None:
            data[make_multi] = output_set_name
            # print(row_ind)
            row_ind = [make_multi]
            if len(col_ex_years) == 1:
                field = col_ex_years[0]
            # field = col_ex_years
            # print(field)
            # print(row_ind)
        data.set_index(row_ind, inplace=True)
        # data = data.sort_index()
        row_ind_values = data.index.unique()
        #print(row_ind_values)
        row_ind_values = [a if isinstance(a, (tuple, list, set)) else [a] for a in row_ind_values]
        #print(field)
        output_label = np.array([[[*row_ind, label, *data.loc[tuple(ind), field].values] for year in years] for ind in row_ind_values])
        output_label = output_label.reshape(-1)
        if isinstance(output_label[0], (list, tuple)):
            output_label = np.hstack(output_label)
        total_rows = len(output_label)
        # print(total_rows, len(output_rows))
        # print(row_ind_values)
        output_value = np.array([[[*ind , year, *data.loc[tuple(ind), year].values] for year in years] for ind in row_ind_values])
        output_rows = np.hstack(np.array([[len([*row_ind, label, *data.loc[tuple(ind), year].values]) for year in years] for ind in row_ind_values]).reshape(-1))
        # print(len(output_rows))
        output_rows = np.hstack(np.array([[i]*output_rows[i] for i in range(len(output_rows))]))
        # print(len(output_rows), total_rows)
        # print(output_value)
        output_value = output_value.reshape(-1)
        if isinstance(output_value[0], (list, tuple)):
            output_value = np.hstack(output_value)
        output_value = np.hstack(output_value)
        output_set = [output_set_name] * total_rows
        snapshot_id = np.full(total_rows, self.get_snapshot_id(model))
        output_id = self.get_label_id([*output_label])
        output['snapshot_id'] = snapshot_id
        output['output_set'] = output_set
        output['output_row'] = output_rows
        output['output_id'] = output_id
        output['output_label'] = output_label
        output['output_value'] = np.nan_to_num(output_value)
        output['model_id'] = np.full(total_rows, self.get_model_ids(model) )
        output['override_value'] = np.nan
        output['actual_value'] = np.nan
        output["outputrow_pwb"] = np.nan
        if output.empty:
            warnings.warn(f"Output set: {output_set_name}\nEmpty result, please make sure you're using the right params for the right table")

        return output

    # supports alumina grade output
    def mult_year_single_output(self, filepath, output_set_name, idx_of_index=[], idx_of_values=[], label="Year", model=None, path=False, col_params=[]):
        output_set_name = ' '.join(output_set_name.split('_'))
        output_set_name = output_set_name.title()
        if model is None:
            model = self.model
            setups = load_setups()
        if model not in setups.keys():
          setups[model] = {}
        setups[model] = {
            **setups[model],
             output_set_name:
            {
                "db_type": "multiple year single output",
                "config": {
                    "label": label
                }
            }
            }
        dump_configs(setups)
        if path :
            data = pd.read_csv(filepath).dropna(how='all')
        else:
            data = filepath.copy().dropna(how='all').dropna(how='all', axis=1).reset_index(drop=True)
            # if data.columns[0] == 'index':
            #     data = data.drop(['index'], axis=1)
            columns = list(data.columns)
            for param in col_params:
                columns[param[0]] = param[1]
            data.columns = columns
        # print(data)
        output = pd.DataFrame(columns=[])
        if len(idx_of_index) == 0 and len(idx_of_values) != 0:
            raise Exception("Can only set both idx_of_index and idx_of_values or None")
        if len(idx_of_index) != 0 and len(idx_of_values)== 0:
            raise Exception("Can only set both idx_of_index and idx_of_values or None")

        rows = data.shape[0]
        columns = data.columns
        if len(idx_of_index) > 0 and len(idx_of_values) > 0 :
            col_ex_years = get_val_from_idx(idx_of_index, columns)
            years = get_val_from_idx(idx_of_values, columns)
        else:
            col_ex_years = exclude_years(columns)
            col_ex_years_values = data.loc[:, col_ex_years].values
            years = list(set(columns) - set(col_ex_years))
            years = sorted(years)

        col_ex_years_values = data.loc[:, col_ex_years].values
        years_values = data.loc[:, years].values
        # print(col_ex_years_values.shape, years_values.shape)
        col_ex_years_cols = col_ex_years_values.shape[1]
        years_cols = years_values.shape[1]
        total_rows = rows * years_cols * (col_ex_years_cols+2)
        all_values = [[[*col_ex_years_values[i],years[j], years_values[i][j]] for j in range(len(years_values[i]))] for i in range(len(years_values))]
        output_rows = [([i]*(total_rows // rows)) for i in range(rows)]
        outputrow_pwb = np.array([[i]*((total_rows // (rows)) // (len(years))) for i in range(rows * len(years))]).flatten()
        # print(output_rows, rows)
        output_set = [output_set_name]*total_rows
        snapshot_id = np.full(total_rows, self.get_snapshot_id(model))
        # output_id = np.arange(1,total_rows+1)
        output_label = [*col_ex_years, label, output_set_name]
        output_id = self.get_label_id([*output_label])
        populate_output_label = output_label * (rows * years_cols)
        populate_output_id = output_id *(rows * years_cols)
        output['snapshot_id'] = snapshot_id
        output['output_set'] = output_set
        output['output_row'] = np.array(output_rows).flatten()
        output['output_id'] = populate_output_id
        output['output_label'] = populate_output_label
        output['output_value'] = np.nan_to_num(np.array(all_values).flatten())
        output['model_id'] = np.full(total_rows, self.get_model_ids(model))
        output['override_value'] = np.nan
        output['actual_value'] = np.nan
        output["outputrow_pwb"] = outputrow_pwb
        if output.empty:
            warnings.warn(f"Output set: {output_set_name}\nEmpty result, please make sure you're using the right params for the right table")
        # print(all_values[1])
        return output


    def reverse(self, snapshot=None, model=None, output_set=None):
        if snapshot is not None:
            flatdb = snapshot
        else:
            flatdb = pd.read_csv("snapshot_output_data.csv")
        flatdb_config = load_setups()
        if model is not None:
            # print(flatdb_config.keys())
            if model not in flatdb_config.keys():
                raise RuntimeError("Model name not in flatdbconfig.json, please rerun this model or ensure there's not typo error")
            flatdb_config = {model: flatdb_config[model]}
            if output_set is not None:
                flatdb_config = {model: {k: flatdb_config[model][k] for k in output_set}}


        result = {}
        # print(flatdb_config)
        for v in flatdb_config.keys():
            result[v] = {}
            for s in flatdb_config[v]:
                print(s)
                if flatdb_config[v][s]["db_type"] == 'multiple year single output':
                    res = self.reverse_2(s, flatdb, flatdb_config[v][s]["config"])
                    result[v][s] = res

                elif flatdb_config[v][s]["db_type"] == 'multiple year multiple output':
                    # continue
                    res = self.reverse_3(s, flatdb, flatdb_config[v][s]["config"])
                    result[v][s] = res

                elif flatdb_config[v][s]["db_type"] == 'single year multiple output':
                    res = self.reverse_1(s, flatdb)
                    result[v][s] = res
        return result

    # reverse single year multiple out
    def reverse_1(self, output_set, df):
        output_set = output_set.title()
        data = df.loc[df["output_set"] == output_set, ["output_row", "output_value", "output_label"]]
        labels = data.loc[:, "output_label"].unique()
        res = pd.DataFrame(columns=[])
        for l in labels:
            res[l] = data.loc[df["output_label"] == l, "output_value"].values
        df2 = res.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce'))
        res = df2.fillna(res)
        return res


    def reverse_2(self, output_set, df, params):
        output_set = output_set.title()
        mult_label = params["label"]
        data = df.loc[df["output_set"] == output_set, ["output_row", "output_value", "output_label"]]
        labels = list(data.loc[:, "output_label"].unique())
        if len(labels) == 0:
            raise Exception("Output_set not found in snapshot output, improper use of dataframe for the right flatdbconverter method can cause this")
        labels.remove(output_set)
        labels.remove(mult_label)
        res = pd.DataFrame(columns=[])
        for l in labels:
            vals = data.loc[(df["output_label"] == l) , ["output_value", "output_row"]].drop_duplicates()
            vals = vals["output_value"].values
            res[l] = vals
        all_mult_label_col = data.loc[data['output_label'] == mult_label, "output_value"].unique()
        all_mult_values = data.loc[data['output_label'] == output_set, "output_value"].values.reshape(-1, len(all_mult_label_col)).transpose()
        # print(res, all_mult_label_col)
        for i in range(len(all_mult_label_col)):
            # print(len(all_mult_values[i]))
            #print(all_mult_label_col[i], all_mult_values[i])
            res[all_mult_label_col[i]] = all_mult_values[i]
        df2 = res.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce'))
        res = df2.fillna(res)
        return res

    def reverse_3(self, output_set, df, params):
        output_set = output_set.title()
        mult_label = params["label"]
        data = df.loc[df["output_set"] == output_set, ["output_row", "output_value", "output_label"]]

        if data.empty:
            raise Exception("Output_set not found in snapshot output, improper use of dataframe for the right flatdbconverter method can cause this")
        get_multi_labels = data.loc[data["output_label"] == mult_label, "output_value"].unique()
        get_fields = data.loc[data["output_row"] == 0, 'output_label'].unique().tolist()
        stop_idx = get_fields.index(mult_label)
        get_fields = get_fields[:stop_idx]
        get_fields.append(params["field"])
        max_row = data["output_row"].max()
        curr_min = 0
        curr_max = len(get_multi_labels)
        # print(max_row)
        result = pd.DataFrame(columns=[*get_fields, *get_multi_labels])
        # print(output_set)
        while curr_max < max_row + len(get_multi_labels):
            print(curr_min, curr_max)
            df = pd.DataFrame(columns=[*get_fields, *get_multi_labels])
            all_labels = data.loc[~data["output_label"].isin([*get_fields, mult_label ]), "output_label"].unique()
            values = data.loc[(~data["output_label"].isin([*get_fields, mult_label ]) & ((data["output_row"] < curr_max) & (data["output_row"] >= curr_min) )), "output_value"].values.reshape(-1, len(all_labels)).transpose()
            # print(len(values[0]))
            # print(values)
            df[params['field']] = all_labels
            df[get_multi_labels] = values
            for a in get_fields[:-1]:
                df[a] = data.loc[(data["output_row"] == curr_min) & (data["output_label"] == a), "output_value"].values[0]
            curr_min = curr_max
            curr_max = curr_max + len(get_multi_labels)
            result = pd.concat([result, df], ignore_index=True)
        # print(result)
        if params["make_multi"] is not None:
            result.drop(params["make_multi"], inplace=True, axis=1)

        df2 = result.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce'))
        result = df2.fillna(result)
        return result

    # def auto_decide(*args):
    #     if
def load_setups():
    filename = "../flatdbconfig.json"
    with open(filename) as f:
        data = json.load(f)
    return data

def dump_configs(data):
    filename = "../flatdbconfig.json"
    with io.open(os.path.join('.', filename), 'w') as db_file:
        db_file.write(json.dumps(data))

def get_val_from_idx(idx, values):
    result = []
    for t in idx:
        if len(t) == 1:
            result.extend(values[t[0]:])
        elif len(t) == 2:
            result.extend(values[t[0]:t[1]])
        else:
            raise Exception("Length of index range cant be more than two")
    return result


def exclude_years(column):
    column = list(map(str, column))
    reg = re.compile(r'(?!(19|20)[0-9]{2})')
    res = [bool(reg.match(a)) for a in column]
    res = [column[i] for i in range(len(column)) if res[i]]
    return res
