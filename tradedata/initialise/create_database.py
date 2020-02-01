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
from pathlib import Path
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, Integer, Float, Boolean, BigInteger, Text, CHAR
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
        elif column["type"] == "integer":
            dtypes.append(Integer())
        elif column["type"] == "bigint":
            dtypes.append(BigInteger())
        elif column["type"] == "float":
            dtypes.append(Float())
        elif column["type"] == "text":
            dtypes.append(Text())
        elif column["type"] == "varchar":
            stringlength = int(re.findall("[0-9]+", column["type"])[0])
            dtypes.append(String(stringlength))
        elif re.findall("char", column["type"]):
            stringlength = int(re.findall("[0-9]+", column["type"])[0])
            dtypes.append(CHAR(stringlength))

    return dict(zip(names,dtypes))



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
    columns = []
    for i,column in enumerate(dict_list):
        if column["type"] == "boolean":
            columns.append(Column(column["name"], Boolean()))
        elif column["type"] == "integer":
            columns.append(Column(column["name"], Integer()))
        elif column["type"] == "bigint":
            columns.append(Column(column["name"], BigInteger()))
        elif column["type"] == "float":
            columns.append(Column(column["name"], Float()))
        elif column["type"] == "varchar":
            columns.append(Column(column["name"], String()))
        elif column["type"] == "text":
            columns.append(Column(column["name"], Text()))
        elif re.findall("char", column["type"]):
            stringlength = int(re.findall("[0-9]", column["type"])[0])
            columns.append(Column(column["name"], String(stringlength)))

    print(columns)
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

    lookup = pd.read_csv(filepath, dtype="object")
    lookup.columns = dtype_dict.keys()
    lookup.to_sql(table_name,
                  engine,
                  if_exists='replace',
                  index=False,
                  dtype=dtype_dict)



# TODO Load Control File Function
def etl_control_table(path, spec_dict):
    # UPSERT: https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
    # Use Skiprows or skipfooter for read_csv to ignore first and last row. skiprows = lambda x: x[1:-1])
    pass


# TODO etl_trade_table Assertions & Docstring
def etl_trade_table(path, spec_dict, recode_dict, date_format):
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
    assert type(path) == type("") | type(path) == type(Path(".")), "`path` is not a pathlib Path or string."
    assert type(spec_list) == type({}), "`spec_list` is not a dictionary."
    assert all(["name" in x.keys() for x in spec_list]), "`name` column not found in all column specifications in `spec_list`"
    assert all(["type" in x.keys() for x in spec_list]), "`type` column not found in all column specifications in `spec_list`"
    assert type(recode_dict) == type({}), "`recode_dict` is not a dictionary."

    # Load Table
    path = Path(path)
    data = pd.read_csv(path, sep = "|", header = None,
                       names = column_names, dtype = 'str', skiprows = 1)

    # Process spec_list
    specification = pd.DataFrame(spec_list)

    # Convert Column DataTypes
    for i in range(0,len(specification)):
        column = specification["name"][i]
        col_dtype = specification["type"][i]

        if re.findall("char", col_dtype) or re.findall("str", col_dtype):
            data[column] = data[column].astype("str")
        elif re.findall("date", col_dtype):
            data[column] = [datetime.strptime(x, date_format).date() for x in data[column]]
        elif re.findall("bigint", col_dtype):
            data[column] = data[column].astype("int64")
        elif re.findall("int", col_dtype):
            data[column] = data[column].astype("int32")
        elif re.findall("float", col_dtype):
            data[column] = data[column].astype("float")
        elif re.findall("boo", col_dtype):
            data[column] = data[column].astype("bool")
        else:
            data.drop(column, axis = 1)

    # Recode Columns
    for column in recode_dict.keys():
        data[column].replace(recode_dict[column], inplace=True)

    return data



# Main Program Loop
if __name__ == '__main__':

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


    # LOAD DATA TO TRADE TABLES ----------------------------------------------------------
    # Table - File Prefix Mapping
    trade_files = {
        "control":"SMKA12",                             # Commodity Lookups
        "exports":"SMKE19", "imports":"SMKI19",         # Non EU Trade
        "dispatches":"SMKX46", "arrivals":"SMKM46"      # EU Trade
    }

    # Recoding Dicts
    recode_mot = json.loads(open("data/lookups/recode_mode_of_transport.json", "r").read())
    recode_mot = {int(i):x for i,x in recode_mot.items()}
