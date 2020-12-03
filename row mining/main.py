import time, os
import numpy as np
import pandas as pd
from numpy_financial import pmt
from flatdb.flatdbconverter import Flatdbconverter
from outputdb import uploadtodb
from extension import DB_TO_FILE

r_flat = Flatdbconverter('Row mining model')
snapshot_output_data = pd.DataFrame(columns=r_flat.out_col)
db_list = [snapshot_output_data]

# cbix_snap = pd.read_csv('cbix2_snapshot_output_data.csv')

# cbix_rev = r_flat.reverse(cbix_snap, model="CBIX machine version used in price forecasts", output_set=["Capesize Data_Tables Freight_Inputs_For_Row_Mining_Model"])
# cbix_rev = cbix_rev["CBIX machine version used in price forecasts"]

override_store = {}
try:
    snaps = pd.read_csv('snapshot_output_data.csv')
    override_rows = snaps.loc[snaps['override_value'] == 1]
    # print(override_rows.values)
    for v in override_rows.values:
        override_store[f'{v[2]}_{v[4]}_{v[5]}'] = float(v[6])
    # print(override_store)
except FileNotFoundError:
    pass


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
class RestOfWorld:
    def __init__(self):
        # Generate inputs directly from db
        self.ext = DB_TO_FILE()

        # List of all inputs are declared here
        rows = [] # list of all seperate mines first few inputs
        mine_names = [] # respective mine name for all inputs in rows list
        mine_infos = [] # contains extra information about mine like location, build structure, etc
        costinputs = [] # contains 2015 C1 mining cost, Duties and royalties, 2015 C1 transp. cost inc ship loading

        m_infos       = self.ext.mineinfo() # pd.read_excel("mineinfo.xlsx")
        # print(m_infos)
        row_inputs    = self.ext.cost_spec_input1() # pd.read_excel("cost_n_spec_input.xlsx", sheet_name="Sheet1")
        # print(row_inputs)
        costinputs_df = self.ext.cost_spec_input2() # pd.read_excel("cost_n_spec_input.xlsx", sheet_name="Sheet2")
        # print(costinputs_df)




        #mine_names
        MINE_NAMES = m_infos.columns[1:]
        # size = row_inputs.shape[0]
        # step = int(size / MINE_NAMES.shape[0])
        # cost_sz  = costinputs_df.shape[0]
        # cost_stp = int(cost_sz / MINE_NAMES.shape[0])


        for mine in MINE_NAMES:
            # if mine == "Weipa/Amrun Generic":
            #     pass
            # else:
            df = row_inputs[row_inputs['mine'] == mine].reset_index().drop(['index'], axis=1)
            mf = pd.concat([m_infos.loc[:,"Field"], m_infos.loc[:,mine]], axis=1)
            cost_df = costinputs_df[costinputs_df['mine'] == mine].reset_index().drop(['index'], axis=1)
            
            mine_names.append(mine)
            rows.append(df)
            mine_infos.append(mf)
            costinputs.append(cost_df)

        self.choosenmines = pd.read_excel(os.path.join(BASE_DIR, "choosenmineoutput.xlsx"))

        self.CHOOSEN_YEAR = 2030 # Enter chosen year here NB: 2014 - 2030

        self.COLLECTOR_MINES = self.ext.collector_mines.loc[:,'mine'].tolist()        

        self.cost_breakdown_columns = [
            "country",
            "mine",
            "FOB Cash Cost",
            "Full Cost CFR",
            "Sust. Cap",
            "Cap Chrg",
            "Other Chgs",
            "Freight",
            "BAR LT",
            "BAR HT-D",
            "BAR HT-B",
            "T.AA",
            "LT A.AA",
            "Mon AA",
            "T.SiO2",
            "LT R.SiO2",
            "Organic C",
            "Bx Style",
            "Moisture as Shipped",
            "Processing Penalties (extra crushing, high goethite, high phos, etc) (US$/dmt bx)",
            "Specific Capital (applicable to project as modelled - US$/dmt bx output)",
            "Project Life (yr)",
            "Construction Period (yr)",
            "Minimum IRR to proceed (%)",
            "Build Quality",
            "Infrastucture Covered by Capital",
            "Percentage of Specific Capital used to Calculate Annual Sustaining Capital Charge",
            "Vessel Class",
            "Capital US$/dmt"
        ]

        self.other_tables_columns = ["country", "mine"]
        [self.other_tables_columns.append(str(year)) for year in range(2016,2031)]

        self.other_tables_info = [
            [r"Cash FOB (US$-dmt)",              89],
            [r"Full FOB (US$-dmt)",              74],
            [r"Cash Cost CFR (US$-dmt)",         71],
            [r"Full Cost CFR (US$-dmt)",         75],
            [r"Freight (US$-dmt)",               60],
            [r"Moisture as decimal",             5],
            [r"Sustaining Capital (US$-dmt)",    65],
            [r"Capital (US$-dmt)",               69],
            [r"Vessels",                         8],
            [r"Total Alumina",                   1],
            [r"LT Avail Alumina",                0],
            [r"Monohydrate",                     4],
            [r"Total Silica",                    3],
            [r"LT R. Silica",                    2],
            [r"Moisture",                        5],
            [r"Organics",                        7],
            [r"Bauxite Style",                   6]

        ]

        self.mine_infos_cols_tab = self.ext.mineinfo() # pd.read_excel(os.path.join(BASE_DIR, "mineinfo.xlsx"))
        
        [self.mine_infos_cols_tab.loc[3, mine] for mine in self.COLLECTOR_MINES]
        # here
        self.capefreight  = self.ext.capefreight() # pd.read_excel(os.path.join(BASE_DIR, "capefreight.xlsx"))
        # self.capefreight = cbix_rev["Capesize Data_Tables Freight_Inputs_For_Row_Mining_Model"]
        self.capefreight.columns = ["country", "mine", *range(2014, 2031)]
        # print(self.capefreight)
        self.diesel       = self.ext.diesel_index() # pd.read_excel(os.path.join(BASE_DIR, "diesel_index.xlsx"))
        self.diesel.set_index("year", drop=True, inplace=True)
        # print(self.diesel)

        self.fxrates      = self.ext.fxratestomines() # pd.read_excel(os.path.join(BASE_DIR, "fxratestominesasindex.xlsx"))
        # print(self.fxrates)
        # here
        self.panamax      = self.ext.panamax() # pd.read_excel(os.path.join(BASE_DIR, "panamax.xlsx"))
        self.split_factor = self.ext.splittingfactor() # pd.read_excel(os.path.join(BASE_DIR, "splittingfactor.xlsx"))
        self.rows         = rows
        self.mine_names   = mine_names
        self.mine_infos   = mine_infos
        self.costinputs   = costinputs
        self.db = {}
        self.colA_db = {}

    def fxratestominesasindex(self):
        df = self.ext.fxrates() # pd.read_excel(os.path.join(BASE_DIR, "fxrates.xlsx"))        
        new_df = pd.DataFrame(columns=df.columns)
        new_df.at[:, "country"] = df.loc[:, "country"]
        for i in range(df.shape[0]):
            for year in new_df.columns[1:]:
                if str(year) == '2015' and float(new_df.loc[i, "country"] == "Malaysia"):
                    new_df.at[i, str(year)] = np.average([df.loc[i, '2014'], df.loc[i, '2015']]) / df.loc[i, str(year)]
                else:
                    new_df.at[i, str(year)] = 1.0 if year in range(2014,2016) else np.average([df.loc[i, '2014'], df.loc[i, '2015']]) / df.loc[i, str(year)]

        # new_df.to_excel("fxratestominesasindex.xlsx", index=False)

        db_list.append(r_flat.mult_year_single_output(new_df, "fx rates to mines as index"))
        self.db["fxratestominesasindex.xlsx"] = new_df


    def mine_function(self, row_inputs, mine, mine_info, cost_inputs):
        new_df = pd.DataFrame(columns=row_inputs.columns)
        tmp_row_inputs = row_inputs.set_index('field') #.drop(['index'], axis=1)
        new_df.at[0, :] = tmp_row_inputs.loc['LT Avail AA', :]
        new_df.at[1, :] = tmp_row_inputs.loc['Total Alumina', :]
        new_df.at[2, :] = tmp_row_inputs.loc['LT React. Silica', :]
        new_df.at[3, :] = tmp_row_inputs.loc['Total Silica', :]
        new_df.at[4, :] = tmp_row_inputs.loc['Monhydrate', :]
        new_df.at[5, :] = tmp_row_inputs.loc['Moisture', :]
        new_df.at[6, :] = tmp_row_inputs.loc['Bauxite style', :]
        new_df.at[7, :] = tmp_row_inputs.loc['Total Organic Carbon', :]
        new_df.at[8, :] = tmp_row_inputs.loc['Vessel size', :]

        new_df.at[0, 'field'] = 'LT Avail AA'
        new_df.at[1, 'field'] = 'Total Alumina'
        new_df.at[2, 'field'] = 'LT React. Silica'
        new_df.at[3, 'field'] = 'Total Silica'
        new_df.at[4, 'field'] = 'Monhydrate'
        new_df.at[5, 'field'] = 'Moisture'
        new_df.at[6, 'field'] = 'Bauxite style'
        new_df.at[7, 'field'] = 'Total Organic Carbon'
        new_df.at[8, 'field'] = 'Vessel size'

        # new_df.set_index('field', inplace=True)
        colA_df = pd.DataFrame(columns=["Name", "Value"])


        # new_df.at[:, "mine"] = mine
        # new_df.at[:, "country"] = row_inputs.loc[0, "country"]

        def vlp(search, target_col, result_col):
            v = target_col == search
            try:
                return result_col[v].tolist()[0]
            except Exception:
                return np.nan
                
        colA_df.at[0, "Value"] = 0.35 if int(str(mine_info.loc[0, mine])[0]) * 1 == 1 else 0.55 if int(str(mine_info.loc[0, mine])[0]) * 1 == 2 else 0.75

        colA_df.at[1, "Name"] = "portion capital to port"
        colA_df.at[1, "Value"] = 0.0 if int(str(mine_info.loc[1, mine])[0]) * 1 == 1 else 0.0 if int(str(mine_info.loc[1, mine])[0]) * 1 == 2 else 0.50

        colA_df.at[2, "Name"] = "portion capital to roads n rail"
        colA_df.at[2, "Value"] = 0.0 if int(str(mine_info.loc[1, mine])[0]) * 1 == 1 else 0.50 if int(str(mine_info.loc[1, mine])[0]) * 1 == 2 else 0.25

        colA_df.at[3, "Name"] = "portion capital to mine"
        colA_df.at[3, "Value"] = 1.0 if int(str(mine_info.loc[1, mine])[0]) * 1 == 1 else 0.50 if int(str(mine_info.loc[1, mine])[0]) * 1 == 2 else 0.25

        colA_df.at[4, "Name"] = "Port Life (years)"
        colA_df.at[4, "Value"] = 50 if int(str(mine_info.loc[0, mine])[0]) * 1 == 1 else 35 if int(str(mine_info.loc[0, mine])[0]) * 1 == 2 else 20

        colA_df.at[5, "Name"] = "Road Life (Years)"
        colA_df.at[5, "Value"] = 15 if int(str(mine_info.loc[0, mine])[0]) * 1 == 1 else 10 if int(str(mine_info.loc[0, mine])[0]) * 1 == 2 else 5

        colA_df.at[6, "Name"] = "Mine Equip Life (Years)"
        colA_df.at[6, "Value"] = 10 if int(str(mine_info.loc[0, mine])[0]) * 1 == 1 else 7.5 if int(str(mine_info.loc[0, mine])[0]) * 1 == 2 else 5

        colA_df.at[7, "Name"] = "Sust. Capital Factor"
        colA_df.at[7, "Value"] = colA_df.loc[0, "Value"] * (colA_df.loc[1, "Value"]/colA_df.loc[4, "Value"] + colA_df.loc[2, "Value"]/colA_df.loc[5, "Value"] + colA_df.loc[3, "Value"]/colA_df.loc[6, "Value"])

        colA_df.at[8, "Name"] = "Build Quality"
        colA_df.at[8, "Value"] = mine_info.loc[0, mine]

        colA_df.at[9, "Name"] = "Infrastructure Included"
        colA_df.at[9, "Value"] = mine_info.loc[1, mine]


        db_list.append(r_flat.single_year_mult_out(colA_df, mine.lower()))
        self.colA_db[f"outputs/mines_column_A_calcs/{mine.lower()}_col_A.csv"] = colA_df
        colA_df = self.colA_db[f"outputs/mines_column_A_calcs/{mine.lower()}_col_A.csv"]

        # data = []
        for year in range(2014,2031):
            # Bauxite to Alumina Ratio (BAR) (dry)
            new_df.at[9, "field"] = "quartz"
            new_df.at[9, str(year)] = float(new_df.loc[3, str(year)]) - float(new_df.loc[2, str(year)])

            new_df.at[10, "field"] = "LT processing"
            try:
                new_df.at[10, str(year)] = 1 / float(new_df.loc[0, str(year)]) / 0.90
            except ZeroDivisionError:
                new_df.at[10, str(year)] = 0
            new_df.at[11, "field"] = "HT-B processing"
            new_df.at[11, str(year)] = 1/(float(new_df.loc[1, str(year)])-float(new_df.loc[2, str(year)])-0.50 * float(new_df.loc[9, str(year)]))/0.90 # ht_b_processing

            new_df.at[12, "field"] = "HT-D processing"
            new_df.at[12, str(year)] = 1/(float(new_df.loc[1, str(year)])-1.23 * float(new_df.loc[3, str(year)])) / 0.90

            # NaOH use (as 100% NaOH)
            new_df.at[13, "field"] = "LT processing"
            new_df.at[13, str(year)] = float(new_df.loc[2, str(year)]) * 0.9 * float(new_df.loc[10, str(year)])

            new_df.at[14, "field"] = "HT-B processing"
            new_df.at[14, str(year)] = (float(new_df.loc[2, str(year)]) + 0.50 * float(new_df.loc[9, str(year)]) * 0.90 * float(new_df.loc[11, str(year)]))

            new_df.at[15, "field"] = "HT-D processing"
            new_df.at[15, str(year)] = float(new_df.loc[3, str(year)]) * 0.60 * float(new_df.loc[12, str(year)])

            # "Reacting" Values
            new_df.at[16, "field"] = "LT - Alumina"
            new_df.at[16, str(year)] = float(new_df.loc[0, str(year)]) + float(new_df.loc[2, str(year)])

            new_df.at[17, "field"] = "HT-B - Alumina"
            new_df.at[17, str(year)] = float(new_df.loc[1, str(year)])

            new_df.at[18, "field"] = "HT-D - Alumina"
            new_df.at[18, str(year)] = float(new_df.loc[1, str(year)])

            new_df.at[19, "field"] = "LT - Silica"
            new_df.at[19, str(year)] = float(new_df.loc[2, str(year)])

            new_df.at[20, "field"] = "HT-B - Silica"
            new_df.at[20, str(year)] = float(new_df.loc[2, str(year)]) + 0.50 * float(new_df.loc[9, str(year)])

            new_df.at[21, "field"] = "HT-D - Silica"
            new_df.at[21, str(year)] = float(new_df.loc[3, str(year)])

            new_df.at[22, "field"] = "Moisture"
            new_df.at[22, '2014'] = float(new_df.loc[5, '2014'])

            # Costs
            new_df.at[23, "field"] = "2015 C1 mining cost"
            new_df.at[23, str(year)] = cost_inputs.loc[0, str(year)] if  year in range(2014,2020) else 0

            new_df.at[24, "field"] = "Duties and royalties"
            new_df.at[24, str(year)] = cost_inputs.loc[1, str(year)] if year in range(2014,2020)  else 0

            new_df.at[25, "field"] = "2015 C1 transp. cost inc ship loading"
            new_df.at[25, str(year)] =  cost_inputs.loc[2, str(year)] if year in range(2014,2020) else 0



            # Adjusting for Oil and Local exchange rate changes
            new_df.at[26, "field"] = "diesel  price now  / diesel price 2015"
            new_df.at[26, str(year)] = float(self.diesel.loc[str(year), "dieselindex"])

            new_df.at[27, "field"] = "exchange rate local now / local beginning 2015"
            new_df.at[27, str(year)] = float(vlp(mine_info.loc[3, mine], self.fxrates.loc[:, "country"], self.fxrates.loc[:, str(year)]))

            new_df.at[28, "field"] = "% mining cost = diesel"
            new_df.at[28, '2014'] = float(vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "country"], self.split_factor.loc[:, "mining_diesel"]))

            new_df.at[29, "field"] = "% mining cost = local currency denominated"
            new_df.at[29, '2014'] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "country"], self.split_factor.loc[:, "mining_local_currency"])

            new_df.at[30, "field"] = "other mining cost %"
            new_df.at[30, '2014'] = 1 - sum([float(new_df.loc[28, '2014']), float(new_df.loc[29, '2014'])])

            new_df.at[31, "field"] = "% royalties n duties cost = diesel"
            new_df.at[31, '2014'] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "country"], self.split_factor.loc[:, "royalties_diesel"])

            new_df.at[32, "field"] = "% royalties n duties cost = local currency denominated"
            new_df.at[32, '2014'] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "country"], self.split_factor.loc[:, "royalty_local_currency"])

            new_df.at[33, "field"] = "other royalties n duties cost %"
            new_df.at[33, '2014'] = 1 - sum([float(new_df.loc[31, '2014']), float(new_df.loc[32, '2014'])])

            new_df.at[34, "field"] = "% transport cost = diesel"
            if mine == "Rondon do Para":
                new_df.at[34, '2014'] = 0.15
            else:
                new_df.at[34, '2014'] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "country"], self.split_factor.loc[:, "transport_diesel"])


            new_df.at[35, "field"] = "% transport cost = local currency denominated"
            if mine == "Rondon do Para":
                new_df.at[35, '2014'] = 0.7
            else:
                new_df.at[35, '2014'] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "country"], self.split_factor.loc[:, "transport_local_currency"])

            new_df.at[36, "field"] = "other transport cost %"
            new_df.at[36, '2014'] = 1 - sum([float(new_df.loc[34, '2014']), float(new_df.loc[35, '2014'])])

            new_df.at[37, "field"] = "mining cost broken down - diesel"
            new_df.at[37, '2014'] = float(new_df.loc[23, '2014']) * float(new_df.loc[28, '2014'])

            new_df.at[38, "field"] = "mining cost broken down - local denomination"
            new_df.at[38, '2014'] = float(new_df.loc[23, '2014']) * float(new_df.loc[29, '2014'])

            new_df.at[39, "field"] = "mining cost broken down - other"
            new_df.at[39, '2014'] = float(new_df.loc[23, '2014']) * float(new_df.loc[30, '2014'])

            new_df.at[40, "field"] = "new mining cost - diesel"
            new_df.at[40, str(year)] = float(new_df.loc[37, '2014']) * float(new_df.loc[26, str(year)])

            new_df.at[41, "field"] = "new mining cost - local denomination"
            new_df.at[41, str(year)] = float(new_df.loc[38, '2014']) * float(new_df.loc[27, str(year)])

            new_df.at[42, "field"] = "new mining cost - other"
            new_df.at[42, str(year)] = float(new_df.loc[39, '2014'])

            new_df.at[43, "field"] = "new mining cost - Total"
            new_df.at[43, str(year)] = sum([float(new_df.loc[40, str(year)]), float(new_df.loc[41, str(year)]), float(new_df.loc[42, str(year)])])


            new_df.at[44, "field"] = "royalties n duties cost broken down - diesel"
            new_df.at[44, '2014'] = float(new_df.loc[24, '2014']) * float(new_df.loc[31, '2014'])

            new_df.at[45, "field"] = "royalties n duties cost broken down - local denomination"
            new_df.at[45, '2014'] = float(new_df.loc[24, '2014']) * float(new_df.loc[32, '2014'])

            new_df.at[46, "field"] = "royalties n duties cost broken down - other"
            new_df.at[46, '2014'] = float(new_df.loc[24, '2014']) * float(new_df.loc[33, '2014'])


            new_df.at[47, "field"] = "new royalties n duties cost - diesel"
            new_df.at[47, str(year)] = float(new_df.loc[44, '2014']) * float(new_df.loc[27, str(year)])

            new_df.at[48, "field"] = "new royalties n duties cost - local denomination"
            new_df.at[48, str(year)] = float(new_df.loc[45, '2014']) * float(new_df.loc[27, str(year)])

            new_df.at[49, "field"] = "new royalties n duties cost - other"
            new_df.at[49, str(year)] = float(new_df.loc[46, '2014'])

            new_df.at[50, "field"] = "new royalties n duties cost - Total"
            new_df.at[50, str(year)] = sum([float(new_df.loc[47, str(year)]), float(new_df.loc[48, str(year)]), float(new_df.loc[49, '2014'])])


            new_df.at[51, "field"] = "transport cost broken down - diesel"
            new_df.at[51, '2014'] = float(new_df.loc[25, '2014']) * float(new_df.loc[34, '2014'])

            new_df.at[52, "field"] = "transport cost broken down - local denomination"
            new_df.at[52, '2014'] = float(new_df.loc[25, '2014']) * float(new_df.loc[35, '2014'])

            new_df.at[53, "field"] = "transport cost broken down - other"
            new_df.at[53, '2014'] = float(new_df.loc[25, '2014']) * float(new_df.loc[36, '2014'])

            new_df.at[54, "field"] = "new transport cost - diesel"
            new_df.at[54, str(year)] = float(new_df.loc[51, '2014']) * float(new_df.loc[26, str(year)])

            new_df.at[55, "field"] = "new transport cost - local denomination"
            new_df.at[55, str(year)] = float(new_df.loc[52, '2014']) * float(new_df.loc[27, str(year)])

            new_df.at[56, "field"] = "new transport cost - other"
            new_df.at[56, str(year)] = float(new_df.loc[53, '2014'])

            new_df.at[57, "field"] = "new transport cost - Total"
            new_df.at[57, str(year)] = sum([float(new_df.loc[54, str(year)]), float(new_df.loc[55, str(year)]), float(new_df.loc[56, str(year)])])

        #Adding rest of costs fields row values
        for year in range(2020,2031):
            new_df.at[23, str(year)] = float(new_df.loc[43, str(year)])
            if mine in ["Metro BH1", "Metro LT", "Metro Blend"]:
                if year == 2021:
                    new_df.at[23, 2021] = 6.8639
                if year > 2021:
                    new_df.at[23, str(year)] = float(new_df.loc[23, '2021'])

            if mine == "Kuantan" and year  >= 2020:
                new_df.at[23, str(year)] = float(new_df.loc[23, '2019'])


            new_df.at[24, str(year)] = float(new_df.loc[50, str(year)])
            if mine in ["Metro BH1", "Metro LT", "Metro Blend"]:
                if year == 2021:
                    new_df.at[24, '2021'] = 4.87
                if year > 2021:
                    new_df.at[24, str(year)] = float(new_df.loc[24, '2021'])

            if mine == "Kuantan" and year  >= 2020:
                new_df.at[24, str(year)] = float(new_df.loc[24, '2019'])


            new_df.at[25, str(year)] = float(new_df.loc[57, str(year)])
            if mine in ["Metro BH1", "Metro LT", "Metro Blend"]:
                if  year == 2021:
                    new_df.at[25, '2021'] = 5.76+1
                if  year > 2021:
                    new_df.at[25, year] = float(new_df.loc[25, '2021'])

            if mine == "Kuantan" and year  >= 2020:
                new_df.at[25, str(year)] = float(new_df.loc[25, '2019'])


        for year in range(2014,2031):
            new_df.at[58, "field"] = "Freight on Panamax"
            new_df.at[58, str(year)] = vlp(mine_info.loc[2, mine], self.panamax.loc[:, "mine"], self.panamax.loc[:, str(year)])
            if mine in ["Dom. Rep.", "GAC", "Alufer (Bel Air)", "AMR", "SMB Malapouya", "AMC", "Chalco Boffa", "SMB Santou-Houda", "SMB SH no rail capital", "SPIC"] and year == 2014:
                new_df.at[58, '2014'] = vlp(mine_info.loc[2, mine], self.panamax.loc[:, "mine"], self.panamax.loc[:, '2015'])
            if mine == "Kola Apatit":
                new_df.at[58, '2014'] = 47.6157578513243
            if mine == "Kuantan":
                new_df.at[58, '2014'] = 8.44234736164304


            new_df.at[59, "field"] = "Freight on Capesize"
            new_df.at[59, str(year)] = vlp(mine_info.loc[2, mine], self.capefreight.loc[:, "mine"], self.capefreight.loc[:, year])
            if mine in ["Dom. Rep.", "GAC", "Alufer (Bel Air)", "AMR", "SMB Malapouya", "AMC", "Chalco Boffa", "SMB Santou-Houda", "SMB SH no rail capital", "SPIC"]  and year == 2014:
                new_df.at[59, '2014'] = vlp(mine_info.loc[2, mine], self.capefreight.loc[:, "mine"], self.capefreight.loc[:, 2015])
            if mine == "Kola Apatit":
                new_df.at[59, '2014'] = 45.1246620019278
            if mine == "Kuantan":
                new_df.at[59, '2014'] = 8.24627232341848


            new_df.at[60, "field"] = "Freight for vessel choosen"
            new_df.at[60, str(year)] = new_df.loc[58, str(year)] if new_df.loc[8, str(year)] == "Panamax" else float(new_df.loc[59, str(year)]) / float(1 - float(new_df.loc[5, str(year)]))

            new_df.at[61, "field"] = "Capital cost of mine (inc. all infrasturure for export)"
            new_df.at[61, str(year)] = tmp_row_inputs.loc['Capital cost of mine (inc. all infrasturure for export)', str(year)]

            new_df.at[62, "field"] = "Life of mine"
            new_df.at[62, str(year)] = tmp_row_inputs.loc['Life of mine', str(year)]

            new_df.at[63, "field"] = "constructiuon period for mine & infrastructure"
            new_df.at[63, str(year)] = tmp_row_inputs.loc['constructiuon period for mine & infrastructure', str(year)]

            new_df.at[64, "field"] = "IRR required for project"
            new_df.at[64, str(year)] = tmp_row_inputs.loc['IRR', str(year)]

            # here
            new_df.at[65, "field"] = "Sustaining Capital Charge ($/t)"
            all_yrs = list(new_df.columns[3:])
            axis_1 = f'{mine.lower()}_Sustaining Capital Charge ($/t)_{all_yrs.index(str(year))}'
            # print(axis_1)
            # print(all_yrs.index(year))
            if axis_1 in override_store:
                print("entered")
                new_df.at[65, str(year)] = override_store[axis_1]
            else:
                if mine in ["Amrun", "Amrun HT"]:
                    new_df.at[65, str(year)] = 1.44174310639839 if year == 2014 else float(new_df.loc[65, str(year - 1)])
                elif mine in ["Metro BH1", "Metro Blend", "Metro LT"]:
                    new_df.at[65, str(year)] = 0.43*0.75 if year == 2014 else float(new_df.loc[65, str(year - 1)])
                else:
                    new_df.at[65, str(year)] = colA_df.loc[7, "Value"] * float(new_df.loc[61, str(year)]) if year == 2014 else float(new_df.loc[65, str(year - 1)])


            new_df.at[66, "field"] = "Partner & Other payments"
            if mine in ["Other Ghana", "Kurubuka", "Guyana Others", "Goa", "Bintan"]:
                new_df.at[66, str(year)] = 2 if year == 2014 else float(new_df.loc[66, str(year-1)])
            else:
                new_df.at[66, str(year)] = 0 if year == 2014 else float(new_df.loc[66, str(year-1)])


            new_df.at[67, "field"] = "capital payback charge - before adjustment"
            new_df.at[67, str(year)] = pmt(float(new_df.loc[64, str(year)]), float(new_df.loc[62, str(year)]), float(new_df.loc[61, str(year)])) * (float(new_df.loc[62, str(year)]) + float(new_df.loc[63, str(year)])/2) / float(new_df.loc[62, str(year)])

            new_df.at[68, "field"] = "adjustment (new project, expansion, old mine)"
            new_df.at[68, str(year)] = 1.0 if mine_info.loc[5, mine] == "New" else 0.20 if mine_info.loc[5, mine] == "Expansion" else 0

            # here
            axis_2 = f'{mine.lower()}_capital payback charge_{all_yrs.index(str(year))}'
            new_df.at[69, "field"] = "capital payback charge"
            if axis_2 in override_store:
                new_df.at[69, str(year)] = override_store[axis_2]
            else:
                new_df.at[69, str(year)] = float(new_df.loc[67, str(year)]) * float(new_df.loc[68, str(year)])

            # here
            new_df.at[70, "field"] = "C1 Cost FOB"
            axis_3 = f'{mine.lower()}_C1 Cost FOB_{all_yrs.index(str(year))}'
            if axis_3 in override_store:
                new_df.at[70, str(year)] = override_store[axis_3]
            else:
                new_df.at[70, str(year)] = sum([float(new_df.loc[23, str(year)]), float(new_df.loc[24, str(year)]), float(new_df.loc[25, str(year)])])

            new_df.at[71, "field"] = "C1 Cost CFR Shandong"
            new_df.at[71, str(year)] = float(new_df.loc[70, str(year)]) + float(new_df.loc[60, str(year)])

            new_df.at[72, "field"] = "Cost FOB inc. Sustaining Capital"
            new_df.at[72, str(year)] = float(new_df.loc[70, str(year)]) + float(new_df.loc[65, str(year)])

            new_df.at[73, "field"] = "Cost CFR inc. Sustaining Capital"
            new_df.at[73, str(year)] = float(new_df.loc[71, str(year)]) + float(new_df.loc[65, str(year)])

            new_df.at[74, "field"] = "Full cost FOB"
            new_df.at[74, str(year)] = float(new_df.loc[72, str(year)]) + float(new_df.loc[69, str(year)]) + float(new_df.loc[66, str(year)])

            new_df.at[75, "field"] = "Full cost CFR Shandong = Incentive price"
            new_df.at[75, str(year)] = float(new_df.loc[74, str(year)]) + float(new_df.loc[60, str(year)])

            ''' Please Ignore, Don't change anything here '''
            new_df.at[76, "field"] = "Price CFR Shandong - not in use"
            new_df.at[76, str(year)] = np.nan # tmp_row_inputs.loc[13, str(year)]

            new_df.at[77, "field"] = "Margin C1"
            new_df.at[77, str(year)] = np.nan # float(new_df.loc[76, str(year)] - float(new_df.loc[71, str(year)]

            new_df.at[78, "field"] = "Margin inc. Sustaining Capital"
            new_df.at[78, str(year)] = np.nan # float(new_df.loc[76, str(year)] - float(new_df.loc[73, str(year)]

            new_df.at[79, "field"] = "Margin at Full Cost"
            new_df.at[79, str(year)] = np.nan # float(new_df.loc[76, str(year)] - float(new_df.loc[75, str(year)]
            ''' Proceed '''


            new_df.at[80, "field"] = "Bauxite style"
            new_df.at[80, str(year)] = new_df.loc[6, str(year)]

            new_df.at[81, "field"] = "Alumina (%)"
            new_df.at[81, str(year)] = new_df.loc[16, str(year)] if new_df.loc[80, str(year)] == "LT" else new_df.loc[17, str(year)] if new_df.loc[80, str(year)] == "HT-B" else new_df.loc[18, str(year)] if new_df.loc[80, str(year)] == "HT-D" else "error"

            new_df.at[82, "field"] = "Silica (%)"
            new_df.at[82, str(year)] = new_df.loc[19, str(year)] if new_df.loc[80, str(year)] == "LT" else new_df.loc[20, str(year)] if new_df.loc[80, str(year)] == "HT-B" else new_df.loc[21, str(year)] if new_df.loc[80, str(year)] == "HT-D" else "error"

            new_df.at[83, "field"] = "Moisture (%)"
            new_df.at[83, str(year)] = float(new_df.loc[5, str(year)])

            new_df.at[84, "field"] = "t/t (dry bauxite/alumina)"
            new_df.at[84, str(year)] = float(new_df.loc[10, str(year)]) if new_df.loc[80, str(year)] == "LT" else new_df.loc[11, str(year)] if new_df.loc[80, str(year)] == "HT-B" else new_df.loc[12, str(year)] if new_df.loc[80, str(year)] == "HT-D" else "error"

            new_df.at[85, "field"] = "2015 C1 mining cost ($/t)"
            new_df.at[85, str(year)] = float(new_df.loc[23, str(year)])

            new_df.at[86, "field"] = "2015 C1 transp. cost ($/t)"
            new_df.at[86, str(year)] = float(new_df.loc[25, str(year)])

            new_df.at[87, "field"] = "ST Freight to China ($/dmt)"
            new_df.at[87, str(year)] = float(new_df.loc[60, str(year)])

            new_df.at[88, "field"] = "C1 Cost CFR"
            new_df.at[88, str(year)] = float(new_df.loc[71, str(year)])

            new_df.at[89, "field"] = "C1 Cost FOB"
            new_df.at[89, str(year)] = float(new_df.loc[70, str(year)])

            new_df.at[90, "field"] = "Capital Payback ($/t)Charge"
            new_df.at[90, str(year)] = float(new_df.loc[67, str(year)])

            new_df.at[91, "field"] = "Sustaining Capital Charge ($/t)"
            new_df.at[91, str(year)] = float(new_df.loc[65, str(year)])

            new_df.at[92, "field"] = "Cash + Sustaining Capital CFR"
            new_df.at[92, str(year)] = float(new_df.loc[88, str(year)]) + float(new_df.loc[91, str(year)])

            new_df.at[93, "field"] = "Full Cost"
            new_df.at[93, str(year)] = float(new_df.loc[75, str(year)])

            new_df.at[94, "field"] = "Mine Gate"
            new_df.at[94, str(year)] = float(new_df.loc[23, str(year)])

            new_df.at[95, "field"] = "Other processing penalties (US$/t_bx)"
            new_df.at[95, str(year)] = tmp_row_inputs.loc['Other penalties - extra processing costs', str(year)]


            new_df.at[9:, "mine"] = mine # set rest of mine cells to current mine name
            new_df.at[9:, "country"] = tmp_row_inputs.loc['Other penalties - extra processing costs', "country"]

            # if year == self.CHOOSEN_YEAR:
            self.choosenmines.at[1,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[81, str(year)]) # Alumina (%)
            self.choosenmines.at[2,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[82, str(year)]) # Silica (%)
            self.choosenmines.at[3,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[83, str(year)]) # Moisture (%)
            self.choosenmines.at[4,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[80, str(year)] # Bauxite style
            self.choosenmines.at[5,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[84, str(year)]) # t/t (dry bauxite/alumina)
            self.choosenmines.at[6,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[88, str(year)]) # C1 Cost CFR
            self.choosenmines.at[7,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[92, str(year)]) # Cash + Sustaining Capital CFR
            self.choosenmines.at[8,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[93, str(year)]) # Full Cost
            self.choosenmines.at[9,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[4,  str(year)])  # Monohydrate (%)
            self.choosenmines.at[10, self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[7,  str(year)] # Total Organic Carbon
            self.choosenmines.at[11, self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = float(new_df.loc[95, str(year)]) # Other processing penalties (US$/t_bx)
            self.choosenmines.at[12, self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[8,  str(year)] # Vessel Class choosen

            db_list.append(r_flat.single_year_mult_out(self.choosenmines, f"{year} Chosen mines"))
            self.db[f"outputs/choosenmines/{year}_choosenmines.csv"] = self.choosenmines

        db_list.append(r_flat.multi_year_multi_out(new_df, mine.lower()))
        _mine = '_'.join(mine.lower().split('/'))
        self.db[f"outputs/{_mine}.csv"] = new_df

    def calcall(self):
        self.fxratestominesasindex()
        for row_inps, col, mine_info, cost_inps in zip(self.rows, self.mine_names, self.mine_infos, self.costinputs):
            # if col == "Weipa/Amrun Generic":
            #     pass
            # else:
            print(row_inps)
            print(mine_info.columns)  
            print(cost_inps)
            print(f"Inputs belong to only {col}? - \t{row_inps.iloc[0,0] == col == mine_info.columns[-1]  == cost_inps.iloc[0,0]}")
            self.mine_function(row_inps, col, mine_info, cost_inps)


    def cost_breakdown(self):
        new_df = pd.DataFrame(columns=self.cost_breakdown_columns)
        mine_countries = [self.mine_infos_cols_tab.loc[3, mine] for mine in self.COLLECTOR_MINES]

        new_df.at[:, "country"] = mine_countries
        new_df.at[:, "mine"] = self.COLLECTOR_MINES


        for mine, ind in zip(new_df.loc[:, "mine"], range(len(new_df.loc[:, "mine"]))):
            # if mine == "Weipa/Amrun Generic":
            #     pass
            # else:
            _mine = '_'.join(mine.lower().split('/'))
            mine_df = self.db[f"outputs/{_mine}.csv"]
            mine_colA_df = self.colA_db[f"outputs/mines_column_A_calcs/{_mine}_col_A.csv"]

            # Start filling in the dataframe
            new_df.at[ind, "FOB Cash Cost"]              = mine_df.loc[70, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Full Cost CFR"]              = mine_df.loc[75, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Sust. Cap"]                  = mine_df.loc[65, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Cap Chrg"]                   = mine_df.loc[69, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Other Chgs"]                 = mine_df.loc[66, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Freight"]                    = mine_df.loc[60, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "BAR LT"]                     = mine_df.loc[10, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "BAR HT-D"]                   = mine_df.loc[12, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "BAR HT-B"]                   = mine_df.loc[11, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "T.AA"]                       = mine_df.loc[1, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "LT A.AA"]                    = mine_df.loc[0, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Mon AA"]                     = mine_df.loc[4, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "T.SiO2"]                     = mine_df.loc[3, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "LT R.SiO2"]                  = mine_df.loc[2, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Organic C"]                  = mine_df.loc[7, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Bx Style"]                   = mine_df.loc[6, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Moisture as Shipped"]        = mine_df.loc[5, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Processing Penalties (extra crushing, high goethite, high phos, etc) (US$/dmt bx)"]    = mine_df.loc[95, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Specific Capital (applicable to project as modelled - US$/dmt bx output)"]    = mine_df.loc[61, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Project Life (yr)"]           = mine_df.loc[62, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Construction Period (yr)"]    = mine_df.loc[63, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Minimum IRR to proceed (%)"]  = mine_df.loc[64, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Build Quality"]                     = mine_colA_df.loc[8, "Value"]
            new_df.at[ind, "Infrastucture Covered by Capital"]  = mine_colA_df.loc[9, "Value"]
            new_df.at[ind, "Percentage of Specific Capital used to Calculate Annual Sustaining Capital Charge"]  = mine_colA_df.loc[7, "Value"]
            new_df.at[ind, "Vessel Class"]                      = mine_df.loc[8, str(self.CHOOSEN_YEAR)]
            new_df.at[ind, "Capital US$/dmt"]                   = mine_df.loc[61, str(self.CHOOSEN_YEAR)]

        db_list.append(r_flat.single_year_mult_out(new_df, "Collector Cost breakdown"))
        self.db["outputs/collector_tab_tables/cost_breakdown.csv"] = new_df



    def other_collector_tables_functions(self, table_name, cell_ref):
        # print(table_name, cell_ref)
        new_df = pd.DataFrame(columns=self.other_tables_columns)
        mine_countries = [self.mine_infos_cols_tab.loc[3, mine] for mine in self.COLLECTOR_MINES]

        new_df.at[:, "country"] = mine_countries
        new_df.at[:, "mine"] = self.COLLECTOR_MINES

        for mine, ind in zip(new_df.loc[:, "mine"], range(len(new_df.loc[:, "mine"]))):
            mine_df = self.db[f"outputs/{mine.lower()}.csv"]
            for year in range(2016,2031):
                new_df.at[ind, str(year)] = mine_df.loc[cell_ref, str(year)]

        table_name = "_".join(table_name.split(" "))

        db_list.append(r_flat.mult_year_single_output(new_df, f'Collector {table_name.lower()}'))
        self.db[f"outputs/collector_tab_tables/{table_name.lower()}.csv"] = new_df



    def runall(self):
        self.cost_breakdown()
        for info in self.other_tables_info:
            self.other_collector_tables_functions(info[0], info[1])

        if os.path.exists("outputs"):
            pass
        else:
            os.mkdir("outputs")

        for filepath1, file1 in self.colA_db.items():
            dirname1 = os.path.dirname(filepath1)
            if os.path.exists(dirname1) or dirname1 == "":
                pass
            else:
                os.mkdir(dirname1)

            file1.to_csv(filepath1, index=False)

        for filepath2, file2 in self.db.items():
            dirname2 = os.path.dirname(filepath2)
            if os.path.exists(dirname2) or dirname2 == "":
                pass
            else:
                os.mkdir(dirname2)
            file2.to_csv(filepath2, index=False)


start = time.process_time() # Start time
# Rest Of  World Mines Engine
row = RestOfWorld()
row.calcall()

# Collector tab Tables Engine
row.runall()

end = time.process_time() - start
snapshot_output_data = pd.concat(db_list, ignore_index=True)
# overrides data
try:
    override_res = override_rows.values
    for i, v in enumerate(override_rows.index):
        print(snapshot_output_data.loc[v],)
        set_it = snapshot_output_data.loc[v].values
        print(override_res[i][-2:])
        set_it[-2:] = override_res[i][-2:]
        snapshot_output_data.loc[v] = set_it
except:
    pass

print(f"Execution time {end} secs")
# stops here
# snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)

uploadtodb.upload(snapshot_output_data)


