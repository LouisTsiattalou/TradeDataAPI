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

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, Integer, Float, Boolean, BigInteger, Text
from sqlalchemy.dialects.postgresql import insert



def connect_to_postgres(username = "", password = "", host = "localhost", database = ""):
    """Returns a SQLAlchemy PostgreSQL engine using psycopg2"""
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}/{database}')
    return engine



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
def load_control_file(path):
    # UPSERT: https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
    pass



# TODO Load Control File Function
def load_noneu_exports(path, prefix, table, recode_dicts):
    # Recoding Border MOT
    # Recoding Inland MOT
    # Recoding Period -> Date
    # datetime.strptime("2019/01", "%Y/%m").date()
    pass



def load_noneu_imports(path, prefix, table):
    # Recoding Border MOT
    # Recoding Inland MOT
    # Recoding Period -> Date
    pass



def load_eu_trade(path, prefix, table):
    # Recoding Border MOT
    # Recoding Period -> Date
    pass



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
