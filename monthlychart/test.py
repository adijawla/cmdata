from flatdb.flatdbconverter import read_output_database, reverse



def interrelationship_rest():
    o_set = ["Caustic Price Data", "Domesticpricedata", "Cmaaxdata2", "Cbixrollingyeardata"]
    weekly = read_output_database(314, o_set)
    rev_weekly = reverse(weekly, "Weekly Chart Pack", o_set)["Weekly Chart Pack"]
    rev_weekly["Cbixrollingyeardata"] = rev_weekly["Cbixrollingyeardata"].sort_values(by="date")
    # rev_weekly["Cbixrollingyeardata"] = rev_weekly["Cbixrollingyeardata"].loc[rev_weekly["Cbixrollingyeardata"]["CBIX2"] != 0].drop(columns=["CBIX1"]).rename(columns={"CBIX2": "CBIX Weekly", "date": "Date"})
    # rev_weekly["Cbixrollingyeardata"]["Date"] = rev_weekly["Cbixrollingyeardata"]["Date"].astype("datetime64[ns]")
    for a  in rev_weekly:
        rev_weekly[a].to_csv(f"inter/{a}.csv", index=False)

interrelationship_rest()