import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tradedata.initialise.create_database import connect_to_postgres

def return_month_data(engine, datestring, threshold=50000):
    """
    Check tables don't have data in them already by loading # records and comparing them to threshold.
    Returns list of tables to load for the month passed in the argument.
    """

    def return_monthly_records(engine, table_name, datestring):
        """Query Database to return monthly records from Postgres"""
        with engine.connect() as conn:
            records = conn.execute(f"SELECT COUNT(date) FROM {table_name} WHERE date = '{datestring}'")
            records = records.fetchone()[0]
        return records

    table_records = {}
    for table in ["exports", "imports", "dispatches", "arrivals"]:
        records = return_monthly_records(engine, table, datestring)
        table_records[table] = records

    table_records["date"] = datestring

    # Check for tables to load in update according to threshold and return.
    return pd.DataFrame(table_records, index = [0])



yr = [str(x) for x in range(10, 21)]
mth = [str(x).rjust(2, "0") for x in range(1,13)]

engine = connect_to_postgres(username = "postgres", password = "postgres",
                                host = "localhost", database = "trade")

month_data_records = []
for year in yr:
    for month in mth:
        print("Testing " + datestring)
        datestring = f"20{year}{month}01"
        month_data_records.append(return_month_data(engine, datestring))

df_records = pd.concat(month_data_records)
df_records["date"] = pd.to_datetime(df_records["date"])
df_records = df_records.melt(id_vars = ["date"], var_name = "table", value_name = "records")

sns.lineplot(x = "date", y = "records", hue = "table", data = df_records)
