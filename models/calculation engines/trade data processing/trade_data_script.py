from flatdb.flatdbconverter import read_from_database


def restruct():
    td = read_from_database("raw_data").drop(columns=['raw_data_id', "updation_date", "creation_date"])
    tdl = read_from_database("raw_data_lookups").drop(columns=["raw_data_lookups_id", "updation_date", "creation_date"])
    td.columns = [a.title() for a in td.columns]
    td['Declaration Date'] = td['Declaration Date'].astype("datetime64[ns]")
    return {
        "raw": td,
        "lookup": tdl
    }

