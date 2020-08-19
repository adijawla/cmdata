import time, os
import numpy as np 
import pandas as pd
from flatdbconverter import Flatdbconverter
from extension import DB_TO_FILE

r_flat = Flatdbconverter('ROW minind model')
snapshot_output_data = pd.DataFrame(columns=r_flat.out_col)
db_list = [snapshot_output_data]

cbix_snap = pd.read_csv('cbix2_snapshot_output_data.csv')

cbix_rev = r_flat.reverse(cbix_snap, model="CBIX machine version used in price forecasts", output_set=["Capesize Data_Tables Freight_Inputs_For_Row_Mining_Model"])
cbix_rev = cbix_rev["CBIX machine version used in price forecasts"]

override_store = {}
try:
    snaps = pd.read_csv('snapshot_output_data.csv')
    override_rows = snaps.loc[snaps['override_value'] == 1]
    # print(override_rows.values)
    for v in override_rows.values:
        override_store[f'{v[2]}_{v[4]}_{v[5]}'] = float(v[6])
    print(override_store)
except FileNotFoundError:
    pass


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
class RestOfWorld:
    def __init__(self):
        # Generate inputs directly from db
        # ext = DB_TO_FILE()
        # ext.bd_to_excel()
        #                 
        # List of all inputs are declared here
        rows = [] # list of all seperate mines first few inputs
        mine_names = [] # respective mine name for all inputs in rows list
        mine_infos = [] # contains extra information about mine like location, build structure, etc
        cost_2014  = [] # contains IRR value inputs
        # sust_chrgs = [] # contains Sustaining Capital Charge values for all mines
        costinputs = [] # contains 2015 C1 mining cost, Duties and royalties, 2015 C1 transp. cost inc ship loading
        
        m_infos       = pd.read_excel("mineinfo.xlsx")
        row_inputs    = pd.read_excel("cost_n_spec_input.xlsx", sheet_name="Sheet1")
        costinputs_df = pd.read_excel("cost_n_spec_input.xlsx", sheet_name="Sheet2")
        
        
        
        
        #mine_names
        MINE_NAMES = m_infos.columns[1:]
        
        size = row_inputs.shape[0]
        step = int(size / MINE_NAMES.shape[0])
        cost_sz  = costinputs_df.shape[0]
        cost_stp = int(cost_sz / MINE_NAMES.shape[0])
        
        
        for mine in MINE_NAMES:
            for ind in range(step-1,size,step):
                df = row_inputs.loc[ind-(step-1):ind, :]
                df.index = range(len(df))
                if df.iloc[0,0] == mine:
                    rows.append(df)
                    mine_names.append(mine) 
                    break
                
            mine_infos.append(pd.concat([m_infos.loc[:,"Field"], m_infos.loc[:,mine]], axis=1))
        
        
            for j in range(cost_stp-1, cost_sz, cost_stp):
                cost_df = costinputs_df.loc[j-(cost_stp-1):j, :]
                cost_df.index = range(len(cost_df))
                if cost_df.iloc[0,0] == mine:
                    costinputs.append(cost_df)
                    break
                
        self.choosenmines = pd.read_excel(os.path.join(BASE_DIR, "choosenmineoutput.xlsx"))
        
        self.CHOOSEN_YEAR = 2030 # Enter chosen year here NB: 2014 - 2030
        
        
        self.COLLECTOR_MINES = [
            "Chalco Boffa",
            "Alufer (Bel Air)",
            "Weipa",
            "Gove",
            "AMR",
            "Gujarat & Maharashtra",
            "West Kalimantan",
            "Henan-Chine",
            "Kuantan",
            "Aurum Xinfa-Fiji",
            "Dom. Rep.",
            "Discovery Bay",
            "Awaso",
            "CBG",
            "AMC",
            "GAC",
            "Solomon Islands",
            "Metallica",
            "Aurukun",
            "Metro LT",
            "MRN",
            "Juruti",
            "Mirai",
            "Metro BH1",
            "Amrun",
            "Rondon do Para",
            "Huntly",
            "Vietnam Others",
            "SMB Malapouya",
            "Rundi",
            "Metro Blend",
            "Dabiss N",
            "Amrun HT",
            "Dian Dian",
            "Dynamic",
            "GDM",
            "Ashapura Guinea",
            "SMB Kaboye",
            "SMB Dabiss S",
            "AGB2A",
            "Mokanji",
            "SMB Santou-Houda",
            "SPIC",
        ]

        self.cost_breakdown_columns = [
            "Country",
            "Mine",
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

        self.other_tables_columns = ["Country", "Mine"]
        [self.other_tables_columns.append(year) for year in range(2016,2031)]

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

        self.selected_mines = self.COLLECTOR_MINES
        self.mine_infos_cols_tab     = pd.read_excel(os.path.join(BASE_DIR, "mineinfo.xlsx"))

        # here
        # self.capefreight  = pd.read_excel(os.path.join(BASE_DIR, "capefreight.xlsx"))        

        self.capefreight = cbix_rev["Capesize Data_Tables Freight_Inputs_For_Row_Mining_Model"]
        self.capefreight.columns = ["Country", "Mine", *range(2014, 2031)]
        self.diesel       = pd.read_excel(os.path.join(BASE_DIR, "diesel_index.xlsx"))
        self.diesel.set_index("Year", drop=True, inplace=True)

        self.fxrates      = pd.read_excel(os.path.join(BASE_DIR, "fxratestominesasindex.xlsx"))

        # here
        self.panamax      = pd.read_excel(os.path.join(BASE_DIR, "panamax.xlsx"))
        self.split_factor = pd.read_excel(os.path.join(BASE_DIR, "splittingfactor.xlsx"))     
        self.rows         = rows
        self.mine_names   = mine_names
        self.mine_infos   = mine_infos
        self.costinputs   = costinputs
        self.db = {}
        self.colA_db = {}

        
    def fxratestominesasindex(self):
        df = pd.read_excel(os.path.join(BASE_DIR, "fxrates.xlsx"))
        new_df = pd.DataFrame(columns=df.columns)
        new_df.at[:, "Country"] = df.loc[:, "Country"]
        for i in range(df.shape[0]):
            for year in new_df.columns[1:]:
                
                if year == 2015 and new_df.loc[i, "Country"] == "Malaysia":                
                    new_df.at[i, year] = np.average([df.loc[i, '2014'], df.loc[i, '2015']]) / df.loc[i, str(year)]
                else:                
                    new_df.at[i, year] = 1.0 if year in range(2014,2016) else np.average([df.loc[i, '2014'], df.loc[i, '2015']]) / df.loc[i, str(year)]
    
        # new_df.to_excel("fxratestominesasindex.xlsx", index=False)

        db_list.append(r_flat.mult_year_single_output(new_df, "fx rates to mines as index"))
        self.db["fxratestominesasindex.xlsx"] = new_df
        

    def mine_function(self, row_inputs, mine, mine_info, cost_inputs):
        new_df = row_inputs.loc[1:9, :]
        new_df.index = range(len(new_df))

        colA_df = pd.DataFrame(columns=["Name", "Value"])
        

        new_df.at[:, "Mine"] = mine
        new_df.at[:, "Country"] = row_inputs.loc[0, "Country"]

        def vlp(search, target_col, result_col):
            for i in range(len(target_col)):
                if target_col.iloc[i] == search:
                    return result_col.iloc[i]

        colA_df.at[0, "Name"] = "Overall factoring"
        colA_df.at[0, "Value"] = 0.35 if int(mine_info.loc[0, mine][0]) * 1 == 1 else 0.55 if int(mine_info.loc[0, mine][0]) * 1 == 2 else 0.75
        
        colA_df.at[1, "Name"] = "portion capital to port"
        colA_df.at[1, "Value"] = 0.0 if int(mine_info.loc[1, mine][0]) * 1 == 1 else 0.0 if int(mine_info.loc[1, mine][0]) * 1 == 2 else 0.50
        
        colA_df.at[2, "Name"] = "portion capital to roads n rail"
        colA_df.at[2, "Value"] = 0.0 if int(mine_info.loc[1, mine][0]) * 1 == 1 else 0.50 if int(mine_info.loc[1, mine][0]) * 1 == 2 else 0.25
        
        colA_df.at[3, "Name"] = "portion capital to mine"
        colA_df.at[3, "Value"] = 1.0 if int(mine_info.loc[1, mine][0]) * 1 == 1 else 0.50 if int(mine_info.loc[1, mine][0]) * 1 == 2 else 0.25
        
        colA_df.at[4, "Name"] = "Port Life (years)"
        colA_df.at[4, "Value"] = 50 if int(mine_info.loc[0, mine][0]) * 1 == 1 else 35 if int(mine_info.loc[0, mine][0]) * 1 == 2 else 20
        
        colA_df.at[5, "Name"] = "Road Life (Years)"
        colA_df.at[5, "Value"] = 15 if int(mine_info.loc[0, mine][0]) * 1 == 1 else 10 if int(mine_info.loc[0, mine][0]) * 1 == 2 else 5
        
        colA_df.at[6, "Name"] = "Mine Equip Life (Years)"
        colA_df.at[6, "Value"] = 10 if int(mine_info.loc[0, mine][0]) * 1 == 1 else 7.5 if int(mine_info.loc[0, mine][0]) * 1 == 2 else 5
        
        colA_df.at[7, "Name"] = "Sust. Capital Factor"
        colA_df.at[7, "Value"] = colA_df.loc[0, "Value"] * (colA_df.loc[1, "Value"]/colA_df.loc[4, "Value"] + colA_df.loc[2, "Value"]/colA_df.loc[5, "Value"] + colA_df.loc[3, "Value"]/colA_df.loc[6, "Value"])

        colA_df.at[8, "Name"] = "Build Quality"
        colA_df.at[8, "Value"] = mine_info.loc[0, mine]
        
        colA_df.at[9, "Name"] = "Infrastructure Included"
        colA_df.at[9, "Value"] = mine_info.loc[1, mine]
        
        
        db_list.append(r_flat.single_year_mult_out(colA_df, mine.lower()))
        self.colA_db[f"outputs/mines_column_A_calcs/{mine.lower()}_col_A.csv"] = colA_df
        colA_df = self.colA_db[f"outputs/mines_column_A_calcs/{mine.lower()}_col_A.csv"]

        data = []
        for year in range(2014,2031):            
            # Bauxite to Alumina Ratio (BAR) (dry)
            new_df.at[9, "Field"] = "quartz"
            new_df.at[9, year] = float(new_df.loc[3, year]) - float(new_df.loc[2, year])

            new_df.at[10, "Field"] = "LT processing"
            try:
                new_df.at[10, year] = 1 / float(new_df.loc[0, year]) / 0.90
            except ZeroDivisionError:
                new_df.at[10, year] = 0
            
            new_df.at[11, "Field"] = "HT-B processing"
            new_df.at[11, year] = 1/(new_df.loc[1, year]-new_df.loc[2, year]-0.50 * new_df.loc[9, year])/0.90 # ht_b_processing
            
            new_df.at[12, "Field"] = "HT-D processing"
            new_df.at[12, year] = 1/(new_df.loc[1, year]-1.23 * new_df.loc[3, year]) / 0.90

            # NaOH use (as 100% NaOH)
            new_df.at[13, "Field"] = "LT processing"
            new_df.at[13, year] = new_df.loc[2, year] * 0.9 * new_df.loc[10, year]

            new_df.at[14, "Field"] = "HT-B processing"
            new_df.at[14, year] = (new_df.loc[2, year] + 0.50 * new_df.loc[9, year]) * 0.90 * new_df.loc[11, year]

            new_df.at[15, "Field"] = "HT-D processing"
            new_df.at[15, year] = new_df.loc[3, year] * 0.60 * new_df.loc[12, year]

            # "Reacting" Values
            new_df.at[16, "Field"] = "LT - Alumina"
            new_df.at[16, year] = new_df.loc[0, year] + new_df.loc[2, year]

            new_df.at[17, "Field"] = "HT-B - Alumina"
            new_df.at[17, year] = new_df.loc[1, year]

            new_df.at[18, "Field"] = "HT-D - Alumina"
            new_df.at[18, year] = new_df.loc[1, year]

            new_df.at[19, "Field"] = "LT - Silica"
            new_df.at[19, year] = new_df.loc[2, year]

            new_df.at[20, "Field"] = "HT-B - Silica"
            new_df.at[20, year] = new_df.loc[2, year] + 0.50 * new_df.loc[9, year]

            new_df.at[21, "Field"] = "HT-D - Silica"
            new_df.at[21, year] = new_df.loc[3, year]

            new_df.at[22, "Field"] = "Moisture"
            new_df.at[22, 2014] = new_df.loc[5, 2014]

            # Costs
            new_df.at[23, "Field"] = "2015 C1 mining cost"
            new_df.at[23, year] = cost_inputs.loc[0, year] if  year in range(2014,2020) else 0

            new_df.at[24, "Field"] = "Duties and royalties"
            new_df.at[24, year] = cost_inputs.loc[1, year] if year in range(2014,2020)  else 0

            new_df.at[25, "Field"] = "2015 C1 transp. cost inc ship loading"
            new_df.at[25, year] =  cost_inputs.loc[2, year] if year in range(2014,2020) else 0



            # Adjusting for Oil and Local exchange rate changes
            new_df.at[26, "Field"] = "diesel  price now  / diesel price 2015"
            new_df.at[26, year] = self.diesel.loc[year, "dieselindex"].astype(float)

            new_df.at[27, "Field"] = "exchange rate local now / local beginning 2015"
            new_df.at[27, year] = float(vlp(mine_info.loc[3, mine], self.fxrates.loc[:, "Country"], self.fxrates.loc[:, str(year)]))

            new_df.at[28, "Field"] = "% mining cost = diesel"
            new_df.at[28, 2014] = float(vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "Country"], self.split_factor.loc[:, "mining diesel"]))

            new_df.at[29, "Field"] = "% mining cost = local currency denominated"
            new_df.at[29, 2014] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "Country"], self.split_factor.loc[:, "Mining Local Currency"])

            new_df.at[30, "Field"] = "other mining cost %"
            new_df.at[30, 2014] = 1 - sum([new_df.loc[28, 2014], new_df.loc[29, 2014]])

            new_df.at[31, "Field"] = "% royalties n duties cost = diesel"
            new_df.at[31, 2014] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "Country"], self.split_factor.loc[:, "royalties diesel"])

            new_df.at[32, "Field"] = "% royalties n duties cost = local currency denominated"
            new_df.at[32, 2014] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "Country"], self.split_factor.loc[:, "royalty local currency"])

            new_df.at[33, "Field"] = "other royalties n duties cost %"
            new_df.at[33, 2014] = 1 - sum([new_df.loc[31, 2014], new_df.loc[32, 2014]])

            new_df.at[34, "Field"] = "% transport cost = diesel"
            if mine == "Rondon do Para":
                new_df.at[34, 2014] = 0.15
            else:
                new_df.at[34, 2014] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "Country"], self.split_factor.loc[:, "transport diesel"])


            new_df.at[35, "Field"] = "% transport cost = local currency denominated"
            if mine == "Rondon do Para":
                new_df.at[35, 2014] = 0.7
            else:
                new_df.at[35, 2014] = vlp(mine_info.loc[3, mine], self.split_factor.loc[:, "Country"], self.split_factor.loc[:, "transport local currency"])

            new_df.at[36, "Field"] = "other transport cost %"
            new_df.at[36, 2014] = 1 - sum([new_df.loc[34, 2014], new_df.loc[35, 2014]])

            new_df.at[37, "Field"] = "mining cost broken down - diesel"
            new_df.at[37, 2014] = new_df.loc[23, 2014] * new_df.loc[28, 2014]

            new_df.at[38, "Field"] = "mining cost broken down - local denomination"
            new_df.at[38, 2014] = new_df.loc[23, 2014] * new_df.loc[29, 2014]

            new_df.at[39, "Field"] = "mining cost broken down - other"
            new_df.at[39, 2014] = new_df.loc[23, 2014] * new_df.loc[30, 2014]

            new_df.at[40, "Field"] = "new mining cost - diesel"
            new_df.at[40, year] = new_df.loc[37, 2014] * new_df.loc[26, year]

            new_df.at[41, "Field"] = "new mining cost - local denomination"
            new_df.at[41, year] = new_df.loc[38, 2014] * new_df.loc[27, year]

            new_df.at[42, "Field"] = "new mining cost - other"
            new_df.at[42, year] = new_df.loc[39, 2014]

            new_df.at[43, "Field"] = "new mining cost - Total"
            new_df.at[43, year] = sum([new_df.loc[40, year], new_df.loc[41, year], new_df.loc[42, year]])


            new_df.at[44, "Field"] = "royalties n duties cost broken down - diesel"
            new_df.at[44, 2014] = new_df.loc[24, 2014] * new_df.loc[31, 2014]

            new_df.at[45, "Field"] = "royalties n duties cost broken down - local denomination"
            new_df.at[45, 2014] = new_df.loc[24, 2014] * new_df.loc[32, 2014]

            new_df.at[46, "Field"] = "royalties n duties cost broken down - other"
            new_df.at[46, 2014] = new_df.loc[24, 2014] * new_df.loc[33, 2014]


            new_df.at[47, "Field"] = "new royalties n duties cost - diesel"
            new_df.at[47, year] = new_df.loc[44, 2014] * new_df.loc[27, year]

            new_df.at[48, "Field"] = "new royalties n duties cost - local denomination"
            new_df.at[48, year] = new_df.loc[45, 2014] * new_df.loc[27, year]

            new_df.at[49, "Field"] = "new royalties n duties cost - other"
            new_df.at[49, year] = new_df.loc[46, 2014]

            new_df.at[50, "Field"] = "new royalties n duties cost - Total"
            new_df.at[50, year] = sum([new_df.loc[47, year], new_df.loc[48, year], new_df.loc[49, 2014]])


            new_df.at[51, "Field"] = "transport cost broken down - diesel"
            new_df.at[51, 2014] = new_df.loc[25, 2014] * new_df.loc[34, 2014]

            new_df.at[52, "Field"] = "transport cost broken down - local denomination"
            new_df.at[52, 2014] = new_df.loc[25, 2014] * new_df.loc[35, 2014]

            new_df.at[53, "Field"] = "transport cost broken down - other"
            new_df.at[53, 2014] = new_df.loc[25, 2014] * new_df.loc[36, 2014]

            new_df.at[54, "Field"] = "new transport cost - diesel"
            new_df.at[54, year] = new_df.loc[51, 2014] * new_df.loc[26, year]

            new_df.at[55, "Field"] = "new transport cost - local denomination"
            new_df.at[55, year] = new_df.loc[52, 2014] * new_df.loc[27, year]

            new_df.at[56, "Field"] = "new transport cost - other"
            new_df.at[56, year] = new_df.loc[53, 2014]

            new_df.at[57, "Field"] = "new transport cost - Total"
            new_df.at[57, year] = sum([new_df.loc[54, year], new_df.loc[55, year], new_df.loc[56, year]])
        
        #Adding rest of costs fields row values
        for year in range(2020,2031):
            new_df.at[23, year] = new_df.loc[43, year]
            if mine in ["Metro BH1", "Metro LT", "Metro Blend"]:
                if year == 2021:
                    new_df.at[23, 2021] = 6.8639
                if year > 2021:
                    new_df.at[23, year] = new_df.loc[23, 2021]

            if mine == "Kuantan" and year  >= 2020:
                new_df.at[23, year] = new_df.loc[23, 2019]


            new_df.at[24, year] = new_df.loc[50, year]
            if mine in ["Metro BH1", "Metro LT", "Metro Blend"]:
                if year == 2021:
                    new_df.at[24, 2021] = 4.87
                if year > 2021:
                    new_df.at[24, year] = new_df.loc[24, 2021]

            if mine == "Kuantan" and year  >= 2020:
                new_df.at[24, year] = new_df.loc[24, 2019]


            new_df.at[25, year] = new_df.loc[57, year]
            if mine in ["Metro BH1", "Metro LT", "Metro Blend"]:
                if  year == 2021:
                    new_df.at[25, 2021] = 5.76+1
                if  year > 2021:
                    new_df.at[25, year] = new_df.loc[25, 2021]

            if mine == "Kuantan" and year  >= 2020:
                new_df.at[25, year] = new_df.loc[25, 2019]


        for year in range(2014,2031):
            new_df.at[58, "Field"] = "Freight on Panamax"
            new_df.at[58, year] = vlp(mine_info.loc[2, mine], self.panamax.loc[:, "mine"], self.panamax.loc[:, year])
            if mine in ["Dom. Rep.", "GAC", "Alufer (Bel Air)", "AMR", "SMB Malapouya", "AMC", "Chalco Boffa", "SMB Santou-Houda", "SMB SH no rail capital", "SPIC"] and year == 2014:
                new_df.at[58, 2014] = vlp(mine_info.loc[2, mine], self.panamax.loc[:, "mine"], self.panamax.loc[:, 2015])
            if mine == "Kola Apatit":
                new_df.at[58, 2014] = 47.6157578513243
            if mine == "Kuantan":
                new_df.at[58, 2014] = 8.44234736164304


            new_df.at[59, "Field"] = "Freight on Capesize"
            new_df.at[59, year] = vlp(mine_info.loc[2, mine], self.capefreight.loc[:, "mine"], self.capefreight.loc[:, year])
            if mine in ["Dom. Rep.", "GAC", "Alufer (Bel Air)", "AMR", "SMB Malapouya", "AMC", "Chalco Boffa", "SMB Santou-Houda", "SMB SH no rail capital", "SPIC"]  and year == 2014:
                new_df.at[59, 2014] = vlp(mine_info.loc[2, mine], self.capefreight.loc[:, "mine"], self.capefreight.loc[:, 2015])
            if mine == "Kola Apatit":
                new_df.at[59, 2014] = 45.1246620019278
            if mine == "Kuantan":
                new_df.at[59, 2014] = 8.24627232341848
            

            new_df.at[60, "Field"] = "Freight for vessel choosen"
            new_df.at[60, year] = float(new_df.loc[58, year] if new_df.loc[8, year] == "Panamax" else new_df.loc[59, year]) / float(1 - new_df.loc[5, year])
            
            new_df.at[61, "Field"] = "Capital cost of mine (inc. all infrasturure for export)"
            new_df.at[61, year] = row_inputs.loc[10, year]
            
            new_df.at[62, "Field"] = "Life of mine"
            new_df.at[62, year] = row_inputs.loc[11, year]
            
            new_df.at[63, "Field"] = "constructiuon period for mine & infrastructure"
            new_df.at[63, year] = row_inputs.loc[12, year]

            new_df.at[64, "Field"] = "IRR required for project"
            new_df.at[64, year] = row_inputs.loc[13, year]
            
            # here
            new_df.at[65, "Field"] = "Sustaining Capital Charge ($/t)"
            all_yrs = list(new_df.columns[4:])
            axis_1 = f'{mine.lower()}_Sustaining Capital Charge ($/t)_{all_yrs.index(year)}'
            # print(axis_1)
            # print(all_yrs.index(year))
            if axis_1 in override_store:
                print("entered")
                new_df.at[65, year] = override_store[axis_1]
            else:
                if mine in ["Amrun", "Amrun HT"]:
                    new_df.at[65, year] = 1.44174310639839 if year == 2014 else new_df.loc[65, year - 1]
                elif mine in ["Metro BH1", "Metro Blend", "Metro LT"]:
                    new_df.at[65, year] = 0.43*0.75 if year == 2014 else new_df.loc[65, year - 1]                
                else:
                    new_df.at[65, year] = colA_df.loc[7, "Value"] * new_df.loc[61, year] if year == 2014 else new_df.loc[65, year - 1]


            new_df.at[66, "Field"] = "Partner & Other payments"    
            if mine in ["Other Ghana", "Kurubuka", "Guyana Others", "Goa", "Bintan"]:
                new_df.at[66, year] = 2 if year == 2014 else new_df.loc[66, year-1]
            else:
                new_df.at[66, year] = 0 if year == 2014 else new_df.loc[66, year-1]
            

            new_df.at[67, "Field"] = "capital payback charge - before adjustment"
            new_df.at[67, year] = -np.pmt(new_df.loc[64, year], new_df.loc[62, year], new_df.loc[61, year]) * (new_df.loc[62, year] + new_df.loc[63, year]/2) / new_df.loc[62, year]
            
            new_df.at[68, "Field"] = "adjustment (new project, expansion, old mine)"
            new_df.at[68, year] = 1.0 if mine_info.loc[5, mine] == "New" else 0.20 if mine_info.loc[5, mine] == "Expansion" else 0
            
            # here
            axis_2 = f'{mine.lower()}_capital payback charge_{all_yrs.index(year)}'
            new_df.at[69, "Field"] = "capital payback charge"
            if axis_2 in override_store:
                new_df.at[69, year] = override_store[axis_2]
            else:
                new_df.at[69, year] = new_df.loc[67, year] * new_df.loc[68, year]
            
            # here
            new_df.at[70, "Field"] = "C1 Cost FOB"
            axis_3 = f'{mine.lower()}_C1 Cost FOB_{all_yrs.index(year)}'
            if axis_3 in override_store:
                new_df.at[70, year] = override_store[axis_3]
            else:
                new_df.at[70, year] = sum([new_df.loc[23, year], new_df.loc[24, year], new_df.loc[25, year]])
            
            new_df.at[71, "Field"] = "C1 Cost CFR Shandong"
            new_df.at[71, year] = new_df.loc[70, year] + new_df.loc[60, year]

            new_df.at[72, "Field"] = "Cost FOB inc. Sustaining Capital"
            new_df.at[72, year] = new_df.loc[70, year] + new_df.loc[65, year]
            
            new_df.at[73, "Field"] = "Cost CFR inc. Sustaining Capital"
            new_df.at[73, year] = new_df.loc[71, year] + new_df.loc[65, year]
            
            new_df.at[74, "Field"] = "Full cost FOB"
            new_df.at[74, year] = new_df.loc[72, year] + new_df.loc[69, year] + new_df.loc[66, year]
            
            new_df.at[75, "Field"] = "Full cost CFR Shandong = Incentive price"
            new_df.at[75, year] = new_df.loc[74, year] + new_df.loc[60, year]

            ''' Please Ignore, Don't change anything here '''
            new_df.at[76, "Field"] = "Price CFR Shandong - not in use"
            new_df.at[76, year] = np.nan # row_inputs.loc[13, year]

            new_df.at[77, "Field"] = "Margin C1"
            new_df.at[77, year] = np.nan # new_df.loc[76, year] - new_df.loc[71, year]

            new_df.at[78, "Field"] = "Margin inc. Sustaining Capital"
            new_df.at[78, year] = np.nan # new_df.loc[76, year] - new_df.loc[73, year]

            new_df.at[79, "Field"] = "Margin at Full Cost"
            new_df.at[79, year] = np.nan # new_df.loc[76, year] - new_df.loc[75, year]
            ''' Proceed '''


            new_df.at[80, "Field"] = "Bauxite style"
            new_df.at[80, year] = new_df.loc[6, year]
            
            new_df.at[81, "Field"] = "Alumina (%)"
            new_df.at[81, year] = new_df.loc[16, year] if new_df.loc[80, year] == "LT" else new_df.loc[17, year] if new_df.loc[80, year] == "HT-B" else new_df.loc[18, year] if new_df.loc[80, year] == "HT-D" else "error"
            
            new_df.at[82, "Field"] = "Silica (%)"
            new_df.at[82, year] = new_df.loc[19, year] if new_df.loc[80, year] == "LT" else new_df.loc[20, year] if new_df.loc[80, year] == "HT-B" else new_df.loc[21, year] if new_df.loc[80, year] == "HT-D" else "error"

            new_df.at[83, "Field"] = "Moisture (%)"
            new_df.at[83, year] = new_df.loc[5, year]

            new_df.at[84, "Field"] = "t/t (dry bauxite/alumina)"
            new_df.at[84, year] = new_df.loc[10, year] if new_df.loc[80, year] == "LT" else new_df.loc[11, year] if new_df.loc[80, year] == "HT-B" else new_df.loc[12, year] if new_df.loc[80, year] == "HT-D" else "error"

            new_df.at[85, "Field"] = "2015 C1 mining cost ($/t)"
            new_df.at[85, year] = new_df.loc[23, year]

            new_df.at[86, "Field"] = "2015 C1 transp. cost ($/t)"
            new_df.at[86, year] = new_df.loc[25, year]

            new_df.at[87, "Field"] = "ST Freight to China ($/dmt)"
            new_df.at[87, year] = new_df.loc[60, year]

            new_df.at[88, "Field"] = "C1 Cost CFR"
            new_df.at[88, year] = new_df.loc[71, year]

            new_df.at[89, "Field"] = "C1 Cost FOB"
            new_df.at[89, year] = new_df.loc[70, year]

            new_df.at[90, "Field"] = "Capital Payback ($/t)Charge"
            new_df.at[90, year] = new_df.loc[67, year]

            new_df.at[91, "Field"] = "Sustaining Capital Charge ($/t)"
            new_df.at[91, year] = new_df.loc[65, year]

            new_df.at[92, "Field"] = "Cash + Sustaining Capital CFR"
            new_df.at[92, year] = new_df.loc[88, year] + new_df.loc[91, year]

            new_df.at[93, "Field"] = "Full Cost"
            new_df.at[93, year] = new_df.loc[75, year]

            new_df.at[94, "Field"] = "Mine Gate"
            new_df.at[94, year] = new_df.loc[23, year]

            new_df.at[95, "Field"] = "Other processing penalties (US$/t_bx)"
            new_df.at[95, year] = row_inputs.loc[0, year]
            

            new_df.at[9:, "Mine"] = mine # set rest of mine cells to current mine name
            new_df.at[9:, "Country"] = row_inputs.loc[0, "Country"]

            # if year == self.CHOOSEN_YEAR:
            self.choosenmines.at[1,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[81, year] # Alumina (%)
            self.choosenmines.at[2,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[82, year] # Silica (%)
            self.choosenmines.at[3,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[83, year] # Moisture (%)
            self.choosenmines.at[4,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[80, year] # Bauxite style
            self.choosenmines.at[5,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[84, year] # t/t (dry bauxite/alumina)
            self.choosenmines.at[6,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[88, year] # C1 Cost CFR
            self.choosenmines.at[7,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[92, year] # Cash + Sustaining Capital CFR
            self.choosenmines.at[8,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[93, year] # Full Cost
            self.choosenmines.at[9,  self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[4,  year]  # Monohydrate (%)
            self.choosenmines.at[10, self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[7,  year] # Total Organic Carbon
            self.choosenmines.at[11, self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[95,  year] # Other processing penalties (US$/t_bx)
            self.choosenmines.at[12, self.choosenmines.columns[self.choosenmines.iloc[0, :] == mine]] = new_df.loc[8,  year] # Vessel Class choosen

            db_list.append(r_flat.single_year_mult_out(self.choosenmines, "{year} Chosen mines"))
            self.db[f"outputs/choosenmines/{year}_choosenmines.csv"] = self.choosenmines

        db_list.append(r_flat.multi_year_multi_out(new_df, mine.lower()))
        self.db[f"outputs/{mine.lower()}.csv"] = new_df

    def calcall(self):
        self.fxratestominesasindex()
        for row_inps, col, mine_info, cost_inps in zip(self.rows, self.mine_names, self.mine_infos, self.costinputs):

            print(f"Inputs belong to only {col}? - \t{row_inps.iloc[0,0] == col == mine_info.columns[-1]  == cost_inps.iloc[0,0]}")
            self.mine_function(row_inps, col, mine_info, cost_inps)


    def cost_breakdown(self):
        new_df = pd.DataFrame(columns=self.cost_breakdown_columns)
        mine_countries = [self.mine_infos_cols_tab.loc[3, mine] for mine in self.selected_mines]

        new_df.at[:, "Country"] = mine_countries
        new_df.at[:, "Mine"] = self.selected_mines
        

        for mine, ind in zip(new_df.loc[:, "Mine"], range(len(new_df.loc[:, "Mine"]))):
            mine_df = self.db[f"outputs/{mine.lower()}.csv"]
            mine_colA_df = self.colA_db[f"outputs/mines_column_A_calcs/{mine.lower()}_col_A.csv"]

            # Start filling in the dataframe
            new_df.at[ind, "FOB Cash Cost"]              = mine_df.loc[70, self.CHOOSEN_YEAR]
            new_df.at[ind, "Full Cost CFR"]              = mine_df.loc[75, self.CHOOSEN_YEAR]
            new_df.at[ind, "Sust. Cap"]                  = mine_df.loc[65, self.CHOOSEN_YEAR]
            new_df.at[ind, "Cap Chrg"]                   = mine_df.loc[69, self.CHOOSEN_YEAR]
            new_df.at[ind, "Other Chgs"]                 = mine_df.loc[66, self.CHOOSEN_YEAR]
            new_df.at[ind, "Freight"]                    = mine_df.loc[60, self.CHOOSEN_YEAR]
            new_df.at[ind, "BAR LT"]                     = mine_df.loc[10, self.CHOOSEN_YEAR]
            new_df.at[ind, "BAR HT-D"]                   = mine_df.loc[12, self.CHOOSEN_YEAR]
            new_df.at[ind, "BAR HT-B"]                   = mine_df.loc[11, self.CHOOSEN_YEAR]
            new_df.at[ind, "T.AA"]                       = mine_df.loc[1, self.CHOOSEN_YEAR]
            new_df.at[ind, "LT A.AA"]                    = mine_df.loc[0, self.CHOOSEN_YEAR]
            new_df.at[ind, "Mon AA"]                     = mine_df.loc[4, self.CHOOSEN_YEAR]
            new_df.at[ind, "T.SiO2"]                     = mine_df.loc[3, self.CHOOSEN_YEAR]
            new_df.at[ind, "LT R.SiO2"]                  = mine_df.loc[2, self.CHOOSEN_YEAR]
            new_df.at[ind, "Organic C"]                  = mine_df.loc[7, self.CHOOSEN_YEAR]
            new_df.at[ind, "Bx Style"]                   = mine_df.loc[6, self.CHOOSEN_YEAR]
            new_df.at[ind, "Moisture as Shipped"]        = mine_df.loc[5, self.CHOOSEN_YEAR]
            new_df.at[ind, "Processing Penalties (extra crushing, high goethite, high phos, etc) (US$/dmt bx)"]    = mine_df.loc[95, self.CHOOSEN_YEAR]
            new_df.at[ind, "Specific Capital (applicable to project as modelled - US$/dmt bx output)"]    = mine_df.loc[61, self.CHOOSEN_YEAR]
            new_df.at[ind, "Project Life (yr)"]           = mine_df.loc[62, self.CHOOSEN_YEAR]
            new_df.at[ind, "Construction Period (yr)"]    = mine_df.loc[63, self.CHOOSEN_YEAR]
            new_df.at[ind, "Minimum IRR to proceed (%)"]  = mine_df.loc[64, self.CHOOSEN_YEAR]
            new_df.at[ind, "Build Quality"]                     = mine_colA_df.loc[8, "Value"]
            new_df.at[ind, "Infrastucture Covered by Capital"]  = mine_colA_df.loc[9, "Value"]
            new_df.at[ind, "Percentage of Specific Capital used to Calculate Annual Sustaining Capital Charge"]  = mine_colA_df.loc[7, "Value"]
            new_df.at[ind, "Vessel Class"]                      = mine_df.loc[8, self.CHOOSEN_YEAR]
            new_df.at[ind, "Capital US$/dmt"]                   = mine_df.loc[61, self.CHOOSEN_YEAR]

        db_list.append(r_flat.single_year_mult_out(new_df, "Collector Cost breakdown"))
        self.db["outputs/collector_tab_tables/cost_breakdown.csv"] = new_df
    


    def other_collector_tables_functions(self, table_name, cell_ref):
        # print(table_name, cell_ref)
        new_df = pd.DataFrame(columns=self.other_tables_columns)
        mine_countries = [self.mine_infos_cols_tab.loc[3, mine] for mine in self.selected_mines]

        new_df.at[:, "Country"] = mine_countries
        new_df.at[:, "Mine"] = self.selected_mines

        for mine, ind in zip(new_df.loc[:, "Mine"], range(len(new_df.loc[:, "Mine"]))):
            mine_df = self.db[f"outputs/{mine.lower()}.csv"]
            for year in range(2016,2031):
                new_df.at[ind, year] = mine_df.loc[cell_ref, year]
                
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
override_res = override_rows.values
for i, v in enumerate(override_rows.index):
    # print(snapshot_output_data.loc[v], override_res[i])
    override_res[i][7] = int(override_res[i][7])
    snapshot_output_data.loc[v] = override_res[i]
# stops here
# snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)


print(f"Done in {round(end/60, 2)} minutes")



