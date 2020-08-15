import time, os
import numpy as np
import pandas as pd
from flatdbconverter import Flatdbconverter, read_from_database

sdc_flat = Flatdbconverter("Supply Demand Costs n Prices Alcoa")

snapshot_output_data = pd.DataFrame(columns=sdc_flat.out_col)

pr = read_output_database(3)
row = pd.read_csv('row_mining_snapshot.csv')
row_rev = sdc_flat.reverse(row, "ROW minind model", ["Collector Full_Cost_Cfr_(Us$-Dmt)", "Collector Freight_(Us$-Dmt)", "Collector Organics", "Collector Moisture", "Collector Bauxite_Style", "Collector Total_Alumina", "Collector Lt_Avail_Alumina", "Collector Vessels", "Collector Monohydrate", "Collector Total_Silica", "Collector Lt_R._Silica"])
row_rev = row_rev["ROW minind model"]
pr_rev = sdc_flat.reverse(pr, "Price forecast model", ["Lbt 451", "1688" , "1689", "1690", "1818"])
pr_rev = pr_rev["Price forecast model"]

# convert row mining inputs to the rigth format

full_c_cfr_shadong_dataframe = row_rev["Collector Full_Cost_Cfr_(Us$-Dmt)"] 
full_c_cfr_shadong_dataframe.drop(list(map(str, range(2016, 2019))), inplace=True, axis=1)
full_c_cfr_shadong_dataframe.rename(columns={"Mine":"Project"}, inplace=True)
samp = pd.to_numeric(full_c_cfr_shadong_dataframe.columns, errors="coerce")
full_c_cfr_shadong_dataframe.columns = [full_c_cfr_shadong_dataframe.columns[i] if np.isnan(samp[i]) else samp[i] for i in np.arange(len(samp))]


# convert all price forecast inputs to the rigt format
demand_inps = pd.concat([pr_rev["1688"], pr_rev["1689"], pr_rev["1690"]], ignore_index=True)
demand_inps.drop(list(map(str, [*range(2014, 2019), *range(2032, 2041)])), inplace=True, axis=1)
demand_inps =  demand_inps.rename(columns={"2031":"Total"})
samp1 = pd.to_numeric(demand_inps.columns, errors="coerce")
demand_inps.columns = [demand_inps.columns[i] if np.isnan(samp1[i]) else samp1[i] for i in np.arange(len(samp1))]

cbix_price_forecast = pr_rev["1818"]
cbix_price_forecast.drop(list(map(str, [*range(2014, 2019), *range(2031, 2041)])), inplace=True, axis=1)
samp2 = pd.to_numeric(cbix_price_forecast.columns, errors="coerce")
cbix_price_forecast.columns = [cbix_price_forecast.columns[i] if np.isnan(samp2[i]) else samp2[i] for i in np.arange(len(samp2))]

f_supply_mt = pr_rev["Lbt 451"]
f_supply_mt.drop(list(map(str, [*range(2014, 2019), *range(2032, 2041)])), inplace=True, axis=1)
f_supply_mt = f_supply_mt.rename(columns={"Name":"Project"})

from_price_forecast_max = f_supply_mt.loc[(f_supply_mt['Project'] == 'Amrun') | (f_supply_mt['Project'] == "AMRUN HT")].reset_index(drop=True)
f_supply_mt.drop(['2019', '2031'], inplace=True, axis=1)
samp3 = pd.to_numeric(f_supply_mt.columns, errors="coerce")
f_supply_mt.columns = [f_supply_mt.columns[i] if np.isnan(samp3[i]) else int(samp3[i]) for i in np.arange(len(samp3))]
samp4 = pd.to_numeric(from_price_forecast_max.columns, errors="coerce")
from_price_forecast_max.columns = [from_price_forecast_max.columns[i] if np.isnan(samp4[i]) else int(samp4[i]) for i in np.arange(len(samp4))]
# print(from_price_forecast_max)

class SUPPLY_DEMAND_COST:
    def __init__(self):
        # self.demand_inps                        = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Demand (Mt)")
        self.demand_inps                        = demand_inps
        self.cbix_model_outputs                 = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="cbix model outputs")
        self.cbix_model_mine_names              = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="cbix model outputs mine names")
        # self.cbix_price_forecast_inps           = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="CBIX Price forecast")
        self.cbix_price_forecast_inps           = cbix_price_forecast
        # self.forecast_supply_inps               = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Forecast Supply (Mt)")
        self.forecast_supply_inps               = f_supply_mt
        self.potential_supply_inps              = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Potential Supply (Mt)")
        self.full_cost_shandong_inps            = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Full Costs CFR Shandong")
        self.market_seff_supply_inps            = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Off Market or Seff Supply")        
        self.price_forecasts_inps               = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Project or Mine Price Forecasts")
        self.viu_djusted_cost_shandong_inps     = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="ViU Adjusted Full Costs CFR Shandong")
        # self.amrun_max_out_demand               = pd.read_excel("supply demand cost inputs.xlsx", sheet_name='from price foreacst model - max out demand')
        self.amrun_max_out_demand               = from_price_forecast_max


        # self.full_cost_shandong_df              = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Full Costs CFR Shandong DataFrame")
        self.full_cost_shandong_df              = full_c_cfr_shadong_dataframe
        self.viu_djusted_cost_shandong_df       = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="ViU Adjusted Full Costs CFR Shandong DataFrame")
        self.price_forecasts_df                 = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Project or Mine Price Forecasts DataFrame")
        self.potential_supply_df                = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Potential Supply DataFrame")
        self.forecast_supply_df                 = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Forecast Supply DataFrame")

        self.other_extras_inps                  = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Other Extra's")
        self.switch_to_cape_inps                = pd.read_excel("supply demand cost inputs.xlsx", sheet_name="Switch to Capesize")

        self.db                                 = {}
        self.cols                               = ["Project", "Country", 2019,2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030]
        self.ctz_df                             = pd.DataFrame(columns=["ctz", 2019,2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030])
        self.inputs_total_df                    = pd.DataFrame(columns=["Total", 2019,2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030])
        self.contestable_df                     = pd.DataFrame(columns=["contestable", 2019,2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030])
        self.db_list = [snapshot_output_data]

        close = False
        for a in range(self.potential_supply_inps.shape[0]):
            for year in range(2019,2031):
                if self.potential_supply_inps.loc[a, "Project"] == "Amrun#":
                    self.potential_supply_inps.at[a, year] = self.amrun_max_out_demand.loc[:, year].sum()                    
                    close = True
            if close:
                break
            




    def vlookup(self, search, lookup, target):
        v = lookup.map(lambda x: x == search)
        try:
            return target[v].tolist()[0]
        except Exception:
            return np.nan

    def sumifs(self, search, lookup, target):
        v = lookup.map(lambda x: x == search)
        try:
            return target[v].sum()
        except Exception:
            return np.nan

    def full_cost_shandong(self):
        for i in range(self.full_cost_shandong_df.shape[0]):
            for year in range(2020,2031):
                self.full_cost_shandong_df.at[i, year] = self.vlookup(self.full_cost_shandong_df.loc[i, "Project"], self.full_cost_shandong_inps["Project"], self.full_cost_shandong_inps[year])

        self.db_list.append(sdc_flat.mult_year_single_output(self.full_cost_shandong_df,"Full Costs CFR Shandong"))
        self.db["outputs/costs_and_prices/Full_Costs_CFR_Shandong.xlsx"] = self.full_cost_shandong_df

    def viu_djusted_cost_shandong(self):
        for i in range(self.viu_djusted_cost_shandong_df.shape[0]):
            for year in range(2020,2031):
                self.viu_djusted_cost_shandong_df.at[i, year] = self.vlookup(self.viu_djusted_cost_shandong_df.loc[i, "Project"], self.viu_djusted_cost_shandong_inps["Project"], self.viu_djusted_cost_shandong_inps[year])

        self.db_list.append(sdc_flat.mult_year_single_output(self.viu_djusted_cost_shandong_df, "ViU Adjusted Full Costs CFR Shandong"))
        self.db["outputs/costs_and_prices/ViU_Adjusted_Full_Costs_CFR_Shandong.xlsx"] = self.viu_djusted_cost_shandong_df
    
    def price_forecasts(self):
        for i in range(self.price_forecasts_df.shape[0]):
            for year in range(2020,2031):
                self.price_forecasts_df.at[i, year] = self.vlookup(self.price_forecasts_df.loc[i, "Project"], self.price_forecasts_inps["Project"], self.price_forecasts_inps[year])

        self.db_list.append(sdc_flat.mult_year_single_output(self.price_forecasts_df, "Project or Mine Price Forecasts"))
        self.db["outputs/costs_and_prices/Project_or_Mine_Price_Forecasts-based_on_CBIX_price_forecast.xlsx"] = self.price_forecasts_df

    def potential_supply(self):
        l = self.potential_supply_df.shape[0]

        for i in range(self.potential_supply_df.shape[0]):
            for year in range(2019,2031):
                self.potential_supply_df.at[i, year] = self.vlookup(self.potential_supply_df.loc[i, "Project"], self.potential_supply_inps["Project"], self.potential_supply_inps[year])
                #Calculate total
                if i+1 == l:
                    self.potential_supply_df.at[l, 'Project'] = 'Total'
                    self.potential_supply_df.at[l, year] = self.potential_supply_df.loc[:, year].sum()

        self.db_list.append(sdc_flat.mult_year_single_output(self.potential_supply_df, "Potential supply"))
        self.db["outputs/supply/Potential_Supply.xlsx"] = self.potential_supply_df
        
        # Potential Supply inputs total
        self.inputs_total_df.at[0, "Total"] = "Potential Supply inputs total"
        self.ctz_df.at[0, 'ctz'] = "Potential Supply ctz"
        for year in range(2019,2031):
            self.inputs_total_df.at[0, year]   = round(self.potential_supply_inps.loc[:, year].sum(), 3)
            self.ctz_df.at[0, year]            = round(self.potential_supply_df.loc[l, year] - self.inputs_total_df.loc[0, year], 2)

    def forecast_supply(self):
        l = self.forecast_supply_df.shape[0]

        for i in range(self.forecast_supply_df.shape[0]):
            for year in range(2020,2031):
                self.forecast_supply_df.at[i, year] = self.sumifs(self.forecast_supply_df.loc[i, "Project"], self.forecast_supply_inps["Project"], self.forecast_supply_inps[year])
                #Calculate total
                if i+1 == l:
                    self.forecast_supply_df.at[l, 'Project'] = 'Total'
                    self.forecast_supply_df.at[l, year] = self.forecast_supply_df.loc[:, year].sum()
        self.forecast_supply_df.at[l, 2019] = self.forecast_supply_df.loc[:, 2019].sum()

        # Forecast Supply inputs total
        self.inputs_total_df.at[1, "Total"] = "Forecast Supply inputs total"
        self.ctz_df.at[1, 'ctz'] = "Forecast Supply ctz"
        self.ctz_df.at[2, 'ctz'] = "Forecast Supply inputs ctz"
        for year in range(2020,2031):
            self.inputs_total_df.at[1, year]   = self.forecast_supply_inps.loc[:, year].sum()
            self.ctz_df.at[1, year]            = self.forecast_supply_df.loc[l, year] - self.inputs_total_df.loc[1, year]
            self.ctz_df.at[2, year]            = self.inputs_total_df.loc[1, year] - self.demand_inps.loc[int(self.demand_inps.shape[0]-1), year]
        
        
        self.contestable_df.at[0, "contestable"] = "Contestable market (Mt)"
        self.contestable_df.at[1, "contestable"] = r"Contestable market (% total)"

        self.forecast_supply_df.at[l+1, 'Project'] = 'Contestable market (Mt)'
        self.forecast_supply_df.at[l+2, 'Project'] = r'Contestable market (% total)'

        for year in range(2019,2031):
            self.contestable_df.at[0, year] = self.forecast_supply_df.loc[l, year] - self.market_seff_supply_inps.loc[:, year].sum()
            self.contestable_df.at[1, year] = self.contestable_df.loc[0, year] / self.forecast_supply_df.loc[l, year]

            self.forecast_supply_df.at[l+1, year] = self.contestable_df.loc[0, year]

            self.forecast_supply_df.at[l+2, year] =self.contestable_df.loc[1, year]

        self.db_list.append(sdc_flat.mult_year_single_output(self.forecast_supply_df, "Forecast Supply"))
        self.db["outputs/supply/Forecast_Supply.xlsx"] = self.forecast_supply_df


    def market_supply(self):
        market_supply_df = self.db["outputs/supply/Forecast_Supply.xlsx"].copy().loc[:int(self.db["outputs/supply/Forecast_Supply.xlsx"].shape[0]-4), :]

        def seek(search, year):
            v = self.market_seff_supply_inps.loc[:, "Project"] == search
            target = self.market_seff_supply_inps.loc[:, year][v]
            try:
                return target.tolist()[0] if (len(target.tolist()) != 0 and not pd.isna(target.tolist()[0])) else 0
            except Exception:
                return 0

        for i in range(market_supply_df.shape[0]):
            for year in range(2019,2030):
                market_supply_df.at[i, year] = market_supply_df.loc[i, year] - seek(market_supply_df.loc[i, "Project"], year)

        self.db_list.append(sdc_flat.mult_year_single_output(market_supply_df, "On Market Supply"))
        self.db["outputs/supply/On_market_supply.xlsx"] = market_supply_df

    def viu_costs_for_on_market_supply(self):
        market_supply_df             = self.db["outputs/supply/On_market_supply.xlsx"].copy()
        viu_djusted_cost_shandong_df = self.db["outputs/costs_and_prices/ViU_Adjusted_Full_Costs_CFR_Shandong.xlsx"].copy()
        viu_costs_ms_df              = self.db["outputs/supply/Forecast_Supply.xlsx"].copy().loc[:int(self.db["outputs/supply/Forecast_Supply.xlsx"].shape[0]-4), :]        
        
        for year in viu_costs_ms_df.columns[2:]:
            viu_costs_ms_df.at[:, year] = np.nan
                
        
        for i in range(viu_costs_ms_df.shape[0]):
            for year in range(2019,2030):                
                viu_costs_ms_df.at[i, year] = 0 if (round(market_supply_df.loc[i, year], 6) <= 0) else self.vlookup(viu_costs_ms_df.loc[i, 'Project'], viu_djusted_cost_shandong_df.loc[:, "Project"], viu_djusted_cost_shandong_df.loc[:, year])
            if viu_costs_ms_df.loc[i, 'Project'] == "Metro BH1◇":
                viu_costs_ms_df.at[i, 2019] = 41.519826120328
                
        self.db_list.append(sdc_flat.mult_year_single_output(viu_costs_ms_df, "ViU Costs for on market supply"))
        self.db["outputs/supply/ViU_Costs_for_on_market_supply.xlsx"] = viu_costs_ms_df

    def supply_tab_final_outputs(self):
        viu_costs_ms_df = self.db["outputs/supply/ViU_Costs_for_on_market_supply.xlsx"].copy()
        df = pd.DataFrame(columns=["calcs",2019,2020,2021,2022,2023,2024,2025,2026,2027,2028,2029,2030])

        df.at[0, "calcs"]   = "Maximim"
        df.at[1, "calcs"]   = "Maximum + 10%"
        df.at[2, "calcs"]   = "CBIX Price Foreacst"
        df.at[3, "calcs"]   = "difference"

        for year in range(2019,2031):
            df.at[0, year] = viu_costs_ms_df.loc[:, year].max()
            df.at[1, year] = df.loc[0, year] * 1.10
            df.at[2, year] = self.cbix_price_forecast_inps.loc[0, year]
            if year != 2019:
                df.at[3, year] = df.loc[1, year] - df.loc[2, year]

        self.db_list.append(sdc_flat.mult_year_single_output(df, "Supply tab final calculations"))
        self.db["outputs/supply/supply_tab_final_calculations.xlsx"] = df

    def quality_freight_data(self):
        tables = ["Total Alumina","LT Avail Alumina","Monohydrate","Total Silica","LT R. Silica","Moisture","Organics","Bauxite Style","Vessel Used","Freight (US$ div dmt)"]
        tables_inputs = []
                
        step = int((self.cbix_model_outputs.shape[1]-1)/len(tables))        

        for i, tab in zip(range(1, (self.cbix_model_outputs.shape[1]-1), step), range(len(tables))):
            df = pd.concat([self.cbix_model_outputs.iloc[:, 0], self.cbix_model_outputs.iloc[:, i:i+step]], axis=1)            
            columns = ["Mine"]
            [columns.append(int(col)) for col in range(int(df.iloc[0, 1]),int(df.iloc[0, -1])+1)]
            df.columns = columns
            df = pd.DataFrame(data=np.array(df.loc[1:, :]), columns=columns)
            tables_inputs.append(df)

        for table_name, table_input in zip(tables, tables_inputs):
            dataframe = self.cbix_model_mine_names.copy()

            if table_name in ["Organics","Bauxite Style","Vessel Used"]:
                dataframe = dataframe.astype(str)

            for i in range(self.cbix_model_mine_names.shape[0]):
                for year in range(2019, 2031):
                    dataframe.at[i, year] = self.vlookup(dataframe.loc[i, "Mine"], table_input.loc[:, "Mine"], table_input.loc[:, year])

            self.db_list.append(sdc_flat.mult_year_single_output(dataframe, table_name))
            self.db[f"outputs/quality_and_freight_data/{table_name}.xlsx"] = dataframe

    def capesize_deductions(self):
        vessel_used_df = self.db[f"outputs/quality_and_freight_data/Vessel Used.xlsx"].copy()
        moisture_df    = self.db[f"outputs/quality_and_freight_data/Moisture.xlsx"].copy()
        df = self.cbix_model_mine_names.copy()
        
        for i in range(df.shape[0]):
            for year in range(2019, 2031):
                df.at[i, year] = self.switch_to_cape_inps.loc[i, "Value"] / (1 - moisture_df.loc[i, year]) if vessel_used_df.loc[i, year] == "Capesize" else 0

        self.db_list.append(sdc_flat.mult_year_single_output(df, "Capesize deductions"))
        self.db[f"outputs/quality_and_freight_data/deductions/capesize_deductions.xlsx"] = df

    def extra_deductions(self):
        moisture_df  = self.db[f"outputs/quality_and_freight_data/Moisture.xlsx"].copy()
        df           = self.cbix_model_mine_names.copy()
        
        for i in range(df.shape[0]):
            try:
                df.at[i, 2019] = 0 if pd.isna(self.other_extras_inps.loc[i, "Value"] / (1 - moisture_df.loc[i, 2019])) else self.other_extras_inps.loc[i, "Value"] / (1 - moisture_df.loc[i, 2019])
            except Exception:
                df.at[i, 2019] = 0

        self.db_list.append(sdc_flat.mult_year_single_output(df, "Extra deductions"))
        self.db[f"outputs/quality_and_freight_data/deductions/extra_deductions.xlsx"] = df


    def freight_after_deductions(self):
        freight_before_df       = self.db[f"outputs/quality_and_freight_data/Freight (US$ div dmt).xlsx"].copy()
        capesize_deductions_df  = self.db[f"outputs/quality_and_freight_data/deductions/capesize_deductions.xlsx"].copy()
        extra_deductions_df     = self.db[f"outputs/quality_and_freight_data/deductions/extra_deductions.xlsx"].copy()

        df = self.cbix_model_mine_names.copy()
        
        for i in range(df.shape[0]):
            for year in range(2019, 2031):                
                df.at[i, year] = float(freight_before_df.loc[i, year]) - float(capesize_deductions_df.loc[i, year]) - (0 if pd.isna(float(extra_deductions_df.loc[i, year])) else float(extra_deductions_df.loc[i, year]))
                
        self.db_list.append(sdc_flat.mult_year_single_output(df, "Freight after deductions"))
        self.db[f"outputs/quality_and_freight_data/deductions/Freight (US$ div dmt).xlsx"] = df




    def calcall(self):
        self.full_cost_shandong()
        self.viu_djusted_cost_shandong()
        self.price_forecasts()
        self.potential_supply()
        self.forecast_supply()
        self.market_supply()
        self.viu_costs_for_on_market_supply()
        self.supply_tab_final_outputs()
        self.quality_freight_data()
        self.capesize_deductions()
        self.extra_deductions()
        self.freight_after_deductions()

    def save(self):
        if os.path.exists("outputs"):
            pass
        else:
            os.mkdir("outputs")

        for filepath, file in self.db.items():
            dirname = os.path.dirname(filepath)
            if os.path.exists(dirname):
                pass
            else:
                os.mkdir(dirname)            
            file.to_excel(filepath, index=False)

        self.inputs_total_df.to_excel('outputs/inputs_total_outputs.xlsx', index=False)
        self.contestable_df.to_excel('outputs/contestable_outputs.xlsx', index=False)
        self.ctz_df.to_excel('outputs/ctz_outputs.xlsx', index=False)
        self.db_list.append(sdc_flat.mult_year_single_output(self.inputs_total_df, "Input Dataframe total"))
        self.db_list.append(sdc_flat.mult_year_single_output(self.inputs_total_df, "Contestable Dataframe"))
        self.db_list.append(sdc_flat.mult_year_single_output(self.inputs_total_df, "CTZ dataframe"))


db_list = [

]

if __name__ == "__main__":
    start = time.process_time()

    supply_cost = SUPPLY_DEMAND_COST()
    supply_cost.calcall()
    supply_cost.save()
    print(len(supply_cost.db_list))
    snapshot_output_data = pd.concat(supply_cost.db_list, ignore_index=True)
    print(snapshot_output_data)
    snapshot_output_data.to_csv("snapshot_output_data.csv", index=False)
    end = time.process_time() - start

    print(f"{round(end/60, 2)} minutes")