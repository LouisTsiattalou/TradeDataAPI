"""
TITLE: Monthly Update
AUTHOR: Louis Tsiattalou
DATE STARTED: 2020-02-17
REPOSITORY: https://github.com/LouisTsiattalou/TradeDataAPI
DESCRIPTION:
Update the database with each month's new data.

Interesting tidbit; module imports work fine with REPL and `python3 -m
tradedata.update.monthly_update --args`, but not `python3
tradedata/update/monthly_update.py --args` because the latter has the current
working directory set to the update folder when it runs...
"""

import wget
from pathlib import Path
import os
import argparse
import json

from tradedata.initialise.download_data import unzip_trade_data
from tradedata.initialise.create_database import connect_to_postgres
from tradedata.initialise.create_database import etl_control_table
from tradedata.initialise.create_database import etl_trade_table



def download_individual_zipfiles(dest_path, prefixes, month = "01", year = "20"):
    """Downloads single month trade data zip files from UKTradeInfo"""
    url = "https://www.uktradeinfo.com/Statistics/Documents/Data Downloads/"
    dest_path = str(dest_path.absolute())
    os.makedirs(dest_path, exist_ok=True)

    def trade_data_download(remote_file_path, destination = dest_path):
        """Download data if it can be found. Return remote file name if exception occurs"""
        try:
            wget.download(remote_file_path, out = destination)
            return None
        except:
            return remote_file_path

    # Loop over Trade Data files from prefixes
    fails = []
    for prefix in prefixes:
        fails.append(trade_data_download(f"{url}{prefix}{year}{month}.zip", dest_path))

    # Print failed downloads
    fails = [x for x in fails if x is not None]
    if len(fails) == 0:
        print("All Zip Files Downloaded.")
    else:
        print("The following Zip Files could not be downloaded:")
        print("\n".join(fails))



def check_month_in_database(engine, datestring, threshold=50000):
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

    tables_to_load = ["control"] + [table for (table,records) in table_records.items() if records > threshold]

    return tables_to_load


if __name__ == '__main__':

    # Parse Arguments
    parser = argparse.ArgumentParser(description="Load a recent month's trade data to the database.")
    parser.add_argument("-t", "--target",
                        help="Target directory for the files. Created if it does not exist.",
                        required = True, default = 'data/monthlyupdate/')
    parser.add_argument("-y", "--year",
                        help="Year to be downloaded; 2 digit string.",
                        required = True, default = '2020')
    parser.add_argument("-m", "--month",
                        help="Month to be downloaded; 2 digit string.",
                        required = True, default = '01')

    # Params
    args = parser.parse_args()
    data_dir = args.target
    data_year= args.year
    data_month = args.month

    # Handle %Y format (YYYY)
    if len(data_year) == 4:
        data_year = data_year[2:]

    # DATA REFERENCES ====================================================================
    # Table - Table Specification JSON filepath mapping
    table_specs = {
        "control":"data/lookups/controlfilecols.json",
        "exports":"data/lookups/noneuexportcols.json",
        "imports":"data/lookups/noneuimportcols.json",
        "dispatches":"data/lookups/eutradecols.json",
        "arrivals":"data/lookups/eutradecols.json"
    }

    # Table Specification Loads
    controlfilecols = json.loads(open(table_specs["control"], "r").read())
    eutradecols = json.loads(open(table_specs["arrivals"], "r").read())
    noneuimportcols = json.loads(open(table_specs["imports"], "r").read())
    noneuexportcols = json.loads(open(table_specs["exports"], "r").read())

    # Trade File Abbreviation - Table Name mapping
    trade_files = {
        "SMKA12":"control",                               # Commodity Lookups
        "SMKE19":"exports", "SMKI19":"imports",           # Non EU Trade
        "SMKX46":"dispatches", "SMKM46": "arrivals"       # EU Trade
    }

    # Recoding Dicts
    recode_inland_mot = json.loads(open("data/lookups/recode_mode_of_transport.json", "r").read())
    recode_border_mot = {"0"+x:y for (x,y) in recode_inland_mot.items()}


    # DOWNLOAD DATA ======================================================================
    update_path = Path(data_dir)
    download_individual_zipfiles(update_path, list(trade_files.keys()),
                                 month = data_month, year = data_year)

    unzip_trade_data(update_path)

    # CHECK DATABASE FOR UPDATE ==========================================================
    # Connect to Database
    engine = connect_to_postgres(username = "postgres", password = "postgres",
                                 host = "localhost", database = "trade")

    # Return table names if they don't contain data arg year/month combination
    datestring = f"20{data_year}{data_month}01"
    tables_to_load = check_month_in_database(engine, datestring, 50000)

    # LOAD DATA TO DATABASE ==============================================================
    # TODO Data Load
    # Load Files

    # ETL Scripts

    # Load to Database


    # CLEAR DOWNLOAD FOLDER
    # TODO
