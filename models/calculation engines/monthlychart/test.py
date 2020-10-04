from flatdb.flatdbconverter import read_output_database, reverse, get_latest_o_set, read_from_database

noah_o_set_1 = {  
    "shandong_l1": "shandog l1",
    "shandong_l2": "shandog l2",
    "shanxi_l1": "shanxi l1",
    "shanxi_l2": "shanxi l2",
    "shanxi_s1": "shanxi s1",
    "shanxi_s2": "shanxi s2",
    "henan_l1": "henan l1",
    "henan_l2": "henan l2",
    "henan_s3": "henan s1",
    "henan_s4": "henan s2",
    "guangxi_l1": "guangxi l1",
    "guangxi_l2": "guangxi l2",
    "guangxi_s1": "guangxi s1",
    "guangxi_s2": "guangxi s2",
    "guizhou_s1": "guizhou s1",
    "guizhou_s2": "guizhou s2",
}

noah_o_set = {
    "shanxi4_5": "shanxi1",
    "shanxi5": "shanxi2",
    "henan4": "henan1",
    "henan5": "henan2",
    "guizhou5_5": "guizhou1",
    "guizhou6_5": "guizhou2",
 }
# domestic_o_set = ["shanxi4_5","shanxi5","henan4","henan5","guizhou5_5","guizhou6_5"]

def interrelationship_rest():
    # o_set = ["Caustic Price Data", "Domesticpricedata", "Cmaaxdata2", "Cbixrollingyeardata"]
    o_set = ["Cmaaxdata2", "Cbixrollingyeardata"]
    weekly = read_output_database(321, o_set)
    rev_weekly = reverse(weekly, "Weekly Chart Pack", o_set)["Weekly Chart Pack"]
    rev_weekly["Cmaaxdata2"] = rev_weekly["Cmaaxdata2"].drop(columns=["wacifrmb","cmaaxdata2_id","wacif","australia"])
    rev_weekly["Cmaaxdata2"] = rev_weekly["Cmaaxdata2"].rename(columns={"date": "Date", "usdrmb":"RMB_per_US"})
    rev_weekly["Cmaaxdata2"][["AU_per_US", "MONTH", "RMB_per_US1", "AU_per_US1"]] = 0
    
    rev_weekly["Cbixrollingyeardata"] = rev_weekly["Cbixrollingyeardata"].sort_values(by="date")
    rev_weekly["Cbixrollingyeardata"] = rev_weekly["Cbixrollingyeardata"].loc[rev_weekly["Cbixrollingyeardata"]["CBIX2"] != 0].drop(columns=["CBIX1", "day"]).rename(columns={"CBIX2": "CBIX Weekly", "date": "Date"})
    rev_weekly["Cbixrollingyeardata"]["Chart Dots"] = 0
    rev_weekly["Cbixrollingyeardata"]["Date"] = rev_weekly["Cbixrollingyeardata"]["Date"].astype("datetime64[ns]")
    for a  in rev_weekly:
        rev_weekly[a].to_csv(f"inter/{a}.csv", index=False)

    domestic_pd = read_from_database("domestic_price_raw_data").rename(columns=noah_o_set).drop(columns=['creation_date', 'updation_date', 'domestic_price_raw_data_id'])
    domestic_pd["guangxi1"] = 0
    domestic_pd["guangxi2"] = 0
    caustic_pd = read_from_database("checked_causticpricedata").rename(columns=noah_o_set_1).drop(columns=['creation_date', 'updation_date', 'date', 'checked_causticpricedata_id'])
    noah = domestic_pd.join(caustic_pd)
    noah.to_csv(f"inter/noah.csv", index=False)

    noah["date"] = noah["date"].astype("datetime64[ns]")
    rev_weekly["Cmaaxdata2"]["Date"] = rev_weekly["Cmaaxdata2"]["Date"].astype("datetime64[ns]")
    rev_weekly["Cbixrollingyeardata"]["Date"] = rev_weekly["Cbixrollingyeardata"]["Date"].astype("datetime64[ns]")
    # domestic_pd.to_csv(f"inter/domestic_pd.csv", index=False)
    # caustic_pd.to_csv(f"inter/caustic_pd.csv", index=False)
    return {
        "noah": noah,
        "fxrate": rev_weekly["Cmaaxdata2"],
        "cbixchart": rev_weekly["Cbixrollingyeardata"]
    }
    


# interrelationship_rest()