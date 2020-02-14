"""
TITLE: Create Database
AUTHOR: Louis Tsiattalou
DATE STARTED: 2020-01-25
REPOSITORY: https://github.com/LouisTsiattalou/TradeDataAPI
DESCRIPTION:
Programatically create and populate the Trade Data Database.
"""

import json
import pandas as pd
import re
import os
from pathlib import Path
from datetime import datetime
from datetime import timedelta

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, Integer, Float, Boolean, BigInteger, Text, CHAR, Date
from sqlalchemy import ForeignKey, Index, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import insert



def connect_to_postgres(username = "", password = "", host = "localhost", database = ""):
    """Returns a SQLAlchemy PostgreSQL engine using psycopg2"""
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}/{database}')
    return engine


def parse_specification(dict_list):
    """Returns a Dictionary of name : SQLAlchemy dtypes from specification jsons"""

    names = [x["name"] for x in dict_list]

    dtypes = []
    for column in dict_list:
        if column["type"] == "boolean":
            dtypes.append(Boolean())
        elif column["type"] == "date":
            dtypes.append(Date())
        elif column["type"] == "integer":
            dtypes.append(Integer())
        elif column["type"] == "bigint":
            dtypes.append(BigInteger())
        elif column["type"] == "float":
            dtypes.append(Float())
        elif column["type"] == "text":
            dtypes.append(Text())
        elif re.findall("varchar", column["type"]):
            stringlength = int(re.findall("[0-9]+", column["type"])[0])
            dtypes.append(String(stringlength))
        elif re.findall("char", column["type"]):
            stringlength = int(re.findall("[0-9]+", column["type"])[0])
            dtypes.append(CHAR(stringlength))
        else:
            dtypes.append("REMOVE")

    dtypes_dict = dict(zip(names,dtypes))
    dtypes_dict = {name:dtype for (name,dtype) in dtypes_dict.items() if not dtype == "REMOVE"}

    return dtypes_dict



def create_trade_table(engine, dict_list, table_name):
    """Create table according to specification in `dict_list`.

    :param engine: SQLAlchemy PostgreSQL Engine class.
    :type engine: SQLAlchemy Engine class `sqlalchemy.engine.base.Engine`.
    :param dict_list: list (of dictionaries) to build tables from. Each entry must contain a `name` and `type` key.
    :type dict_list: list of dictionaries, all of which contain a `name` and `type` key.
    :param table_name: Name for the table to be created.
    :type table_name: String
    :raises AssertionError: Throws an error if `dict_list` is not a list.
    :raises AssertionError: Throws an error if all the dicts in `dict_list` do not contain a `name` and `type` key.
    :return: Does not return anything; builds the table in Postgres Database supplied by `engine`.
    """
    assert type(dict_list) == type([]), "dict_list is not a list"
    assert all(['name' in x.keys() and 'type' in x.keys() for x in dict_list]), "dict_list dicts do not all contain 'name' and 'type' keys"

    # Generate Columns for Table. Ignore REMOVEs
    table_spec = parse_specification(dict_list)

    columns = []
    for column in table_spec.keys():
        columns.append(Column(column, table_spec[column]))

    metadata = MetaData()
    data = Table(table_name, metadata, *columns)
    metadata.create_all(engine)
    print(f"Table {table_name} Created Successfully!")



def create_and_load_lookup_tables(engine, filepath, table_name, dtype_dict):
    """Loads a CSV file with String Columns and loads to the database.

    :param engine: SQLAlchemy PostgreSQL Engine class.
    :type engine: SQLAlchemy Engine class `sqlalchemy.engine.base.Engine`.
    :param filepath: Path to the CSV file
    :type filepath: String
    :param table_name: Name for the table to be created.
    :type table_name: String
    :param dtype_dict: Dictionary with table column names as keys and SQLAlchemy Column Types as values.
    :type dtype_dict: Dict
    :return: Does not return anything; builds the table in Postgres Database supplied by `engine`.
    """

    lookup = pd.read_csv(filepath, dtype="object", keep_default_na=False)
    lookup.columns = dtype_dict.keys()
    lookup.to_sql(table_name,
                  engine,
                  if_exists='replace',
                  index=False,
                  dtype=dtype_dict)



def etl_control_table(path, spec_list):
    """Loads and manipulates the Control files (Comcode Lookups)

    :param path: Path to the Trade Data File
    :type path: pathlib.Path() object, or str.
    :param spec_list: Specification for the data file to be loaded.
    :type spec_list: List of Dictionaries with keys `name` and `type`.
    :raises AssertionError: If the `name` column is not found in every dict contained within the spec_list argument, the function will fail.
    :return: Returns a processed DataFrame.
    """
    assert type(spec_list) == type([]), "`spec_list` is not a list."
    assert all(["name" in x.keys() for x in spec_list]), "`name` column not found in all column specifications in `spec_list`"

    # Read Control File as string, kill NULs and split by newline & delim into list of lists
    path = Path(path)
    x_file = open(path, "r", encoding = "windows-1252").read().replace("\0","").split("\n")
    x_file = [x.split("|") for x in x_file]

    # Kill first, last and second last row
    x_file = x_file[1:-2]

    # If len(x_file[i] == 28), there is a double description column that needs to be merged.
    if len(x_file[0]) == 28:
        for i in range(len(x_file)):
            x_file[i][26] = x_file[i][26].strip() + " " + x_file[i].pop().strip()

    # Convert To DataFrame, make pretty
    data = pd.DataFrame(x_file)
    data = data.iloc[:,[0,7,24,25,26]]
    data.columns = [x["name"] for x in spec_list]
    data["comcode"] = data["comcode"].str[0:-1]
    data = data.apply(lambda x: x.str.strip())

    return data



def etl_trade_table(path, spec_list, recode_dict, date_format):
    """Loads and manipulates the EU/NonEU Import/Export files

    :param path: Path to the Trade Data File
    :type path: pathlib.Path() object, or str.
    :param spec_list: Specification for the data file to be loaded.
    :type spec_list: List of Dictionaries with keys `name` and `type`.
    :param recode_dict: Dict of Dicts that specifies recoding for data columns.
    :type recode_dict: Dictionary with keys corresponding to column names from `spec_list`.
    :param date_format: `strptime` Date String to transform date columns.
    :type date_format: String.
    :raises AssertionError: If the `name` or `type` column is not found in every dict contained within the spec_list argument, the function will fail.
    :return: Returns a processed DataFrame.
    """
    assert type(path) == type("") or type(path) == type(Path(".")), "`path` is not a pathlib Path or string."
    assert type(spec_list) == type([]), "`spec_list` is not a list."
    assert all(["name" in x.keys() for x in spec_list]), "`name` column not found in all column specifications in `spec_list`"
    assert all(["type" in x.keys() for x in spec_list]), "`type` column not found in all column specifications in `spec_list`"
    assert type(recode_dict) == type({}), "`recode_dict` is not a dictionary."

    # Load Table
    path = Path(path)
    column_names = [x["name"] for x in spec_list]
    data = pd.read_csv(path, sep = "|", header = None,
                       names = column_names, dtype = 'str',
                       skiprows = 1, skipfooter = 1, keep_default_na = False)

    # Process spec_list
    specification = pd.DataFrame(spec_list)

    # Convert Column DataTypes
    for i in range(0,len(specification)):
        column = specification["name"][i]
        col_dtype = specification["type"][i]

        # Special handling for 13 month dates in EU Trade
        if re.findall("date", col_dtype):
            dates = data[column]
            dates_transformed = [""] * len(data)
            for i,date in enumerate(dates):
                if re.search("020..13", date):
                    date_transformed = datetime.strptime(date[0:5] + "12", date_format).date()
                    date_transformed = date_transformed + timedelta(days = 30) # Set to YYYY/12/31
                    dates_transformed[i] = date_transformed
                elif date == "0000000":
                    dates_transformed[i] = datetime.strptime(str(trade_file)[-4:] + "01", "%y%m%d").date() # Set to current year/month
                else:
                    dates_transformed[i] = datetime.strptime(date, date_format).date()
            data[column] = dates_transformed
        # All Other Types
        elif re.findall("char", col_dtype) or re.findall("str", col_dtype):
            data[column] = data[column].astype("str")
        elif re.findall("bigint", col_dtype):
            data[column] = data[column].astype("int64")
        elif re.findall("int", col_dtype):
            data[column] = data[column].astype("int32")
        elif re.findall("float", col_dtype):
            data[column] = data[column].astype("float")
        elif re.findall("boo", col_dtype):
            data[column] = data[column].astype("bool")
        else:
            data.drop(column, axis = 1, inplace = True)

    # Recode Columns
    for column in recode_dict.keys():
        data[column].replace(recode_dict[column], inplace=True)
    data["comcode"] = data["comcode"].str[0:-1]

    return data



# Main Program Loop
if __name__ == '__main__':

    # CONNECT TO DATABASE ----------------------------------------------------------------
    engine = connect_to_postgres(username = "postgres", password = "postgres",
                                 host = "localhost", database = "trade")

    # CREATE TABLES ----------------------------------------------------------------------

    # Trade Tables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Table - Table Specification JSON filepath mapping
    table_specs = {
        "control":"data/lookups/controlfilecols.json",
        "exports":"data/lookups/noneuexportcols.json",
        "imports":"data/lookups/noneuimportcols.json",
        "dispatches":"data/lookups/eutradecols.json",
        "arrivals":"data/lookups/eutradecols.json"
    }

    # Loop Over Dict and Create Table
    for table in table_specs.keys():
        spec_filepath = table_specs[table]
        specification = json.loads(open(spec_filepath, "r").read())
        create_trade_table(engine, specification["columns"], table)

    # Lookup Tables ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Load Data, define dtypes, load to Postgres
    clearance_dtypes = {"name":Text, "seq":String(3), "code":String(3)}
    country_dtypes = {"name":Text, "code":String(2), "seq":String(3)}
    port_dtypes = {"name":Text, "code":String(3), "seq":String(3), "type":Text}
    quantity_dtypes = {"code":Text, "name":Text}

    create_and_load_lookup_tables(engine, "data/lookups/clearance_lookup.csv",
                                  "clearance", clearance_dtypes)
    create_and_load_lookup_tables(engine, "data/lookups/country_lookup.csv",
                                  "country", country_dtypes)
    create_and_load_lookup_tables(engine, "data/lookups/port_lookup.csv",
                                  "port", port_dtypes)
    create_and_load_lookup_tables(engine, "data/lookups/quantity_lookup.csv",
                                  "quantity", quantity_dtypes)


    # CREATE CONSTRAINTS ON ALL TABLES ---------------------------------------------------

    # Welp, looks like SQLAlchemy doesn't provide methods for creating indices &
    # constraints after creation. Let's just do normal SQL.
    with engine.connect() as conn:
        # PK on Lookups & Control
        conn.execute('ALTER TABLE clearance ADD PRIMARY KEY (code);')
        conn.execute('ALTER TABLE country ADD PRIMARY KEY (code);')
        conn.execute('ALTER TABLE quantity ADD PRIMARY KEY (code);')
        conn.execute('ALTER TABLE port ADD PRIMARY KEY (code);')
        conn.execute('ALTER TABLE control ADD PRIMARY KEY (comcode);')

        # Not doing FKs at this stage; there are like 24 in total.


    # LOAD DATA TO TRADE TABLES ----------------------------------------------------------
    # File Prefix - Table Mapping
    trade_files = {
        "SMKA12":"control",                               # Commodity Lookups
        "SMKE19":"exports", "SMKI19":"imports",           # Non EU Trade
        "SMKX46":"dispatches", "SMKM46": "arrivals"       # EU Trade
    }

    # Recoding Dicts
    recode_inland_mot = json.loads(open("data/lookups/recode_mode_of_transport.json", "r").read())
    recode_border_mot = {"0"+x:y for (x,y) in recode_inland_mot.items()}

    # Table Specifications
    controlfilecols = json.loads(open(table_specs["control"], "r").read())
    eutradecols = json.loads(open(table_specs["arrivals"], "r").read())
    noneuimportcols = json.loads(open(table_specs["imports"], "r").read())
    noneuexportcols = json.loads(open(table_specs["exports"], "r").read())

    # File List
    data_dir = Path("data/")
    files = [x for x in data_dir.glob("*") if x.is_file()]
    files.sort()

    # Load data to tables
    for trade_file in files:
        file_type = trade_file.stem[0:6].upper()
        table_name = trade_files[file_type]
        print(f"Processing {trade_file}...")

        if table_name == "control":
            # Data load
            data = etl_control_table(trade_file, controlfilecols["columns"])
            # Connect to Table
            metadata = MetaData()
            control = Table('control', metadata, autoload=True, autoload_with=engine)

            # Execute Update
            conn = engine.connect()
            for i in range(len(data)):
                if i % 1000 == 0: print(f"Insert: {i}/{len(data)}")

                insert_data = dict(data.iloc[i,:])
                insert_stmt = insert(control).values(insert_data)
                do_update_stmt = insert_stmt.on_conflict_do_update(
                    constraint='control_pkey', set_=insert_data)
                conn.execute(do_update_stmt)


        elif table_name == "dispatches" or table_name == "arrivals":
            recode_dict = {}
            data = etl_trade_table(trade_file, eutradecols["columns"],
                                   recode_dict, "0%Y%m")
            dtype_dict = parse_specification(eutradecols["columns"])
            data.to_sql(table_name, engine, if_exists='append',
                        index=False, dtype=dtype_dict)

        elif table_name == "imports":
            recode_dict = {"border_mot":recode_border_mot, "inland_mot":recode_inland_mot}
            data = etl_trade_table(trade_file, noneuimportcols["columns"],
                                   recode_dict, "%m/%Y")
            dtype_dict = parse_specification(noneuimportcols["columns"])
            data.to_sql(table_name, engine, if_exists='append',
                        index=False, dtype=dtype_dict)

        elif table_name == "exports":
            recode_dict = {"border_mot":recode_border_mot, "inland_mot":recode_inland_mot}
            data = etl_trade_table(trade_file, noneuexportcols["columns"],
                                   recode_dict, "%m/%Y")
            dtype_dict = parse_specification(noneuexportcols["columns"])
            data.to_sql(table_name, engine, if_exists='append',
                        index=False, dtype=dtype_dict)


# TODO Indices
