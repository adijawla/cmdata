from flatdb.flatdbconverter import Flatdbconverter, read_output_database, reverse


# row = pd.read_csv('row_mining_snapshot.csv')

def interrelationship_rest(np):
    row_sets = ["Collector Cash Fob (Us$-Dmt)", "Collector Full_Fob_(Us$-Dmt)", "Collector Cash_Cost_Cfr_(Us$-Dmt)", "Collector Full_Cost_Cfr_(Us$-Dmt)", "Collector Sustaining_Capital_(Us$-Dmt)", "Collector Capital_(Us$-Dmt)", "Collector Freight_(Us$-Dmt)", "Collector Moisture"]
    row_sets = [' '.join(a.split('_')).title() for a in row_sets]
    print(row_sets)
    row = read_output_database(244, row_sets)
    all_rev = reverse(row, "Row mining model", row_sets)["Row mining model"]
    for a in all_rev:
        all_rev[a] = all_rev[a].replace({np.nan: 0, 'nan': 0})
        all_rev[a].to_csv(f'inter/{a}.csv', index=False)
    return all_rev
