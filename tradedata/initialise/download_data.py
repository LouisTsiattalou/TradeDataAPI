"""
TITLE: Download Data
AUTHOR: Louis Tsiattalou
DATE STARTED: 2020-01-17
REPOSITORY: https://github.com/LouisTsiattalou/TradeDataAPI
DESCRIPTION:
Functions for downloading the data from the uktradeinfo website.
"""

import wget
import os
import zipfile
import re
import argparse

from fnmatch import filter
from pathlib import Path
from tqdm import tqdm

def download_zipfiles(dest_path, min_year=2010, max_year=2019):
    """Downloads zipfiles from UKTradeInfo for later extraction."""

    def trade_data_download(remote_file_path, destination = dest_path):
        """Download data if it can be found. Return remote file name if exception occurs"""
        try:
            wget.download(remote_file_path, out = destination)
            return None
        except:
            return remote_file_path

    # Run trade_data_download func for side effect, build list of failed downloads
    # for future inspection if necessary.

    url = "https://www.uktradeinfo.com/Statistics/Documents/Data Downloads/"
    prefixes = ["SMKE19", "SMKI19", "SMKX46", "SMKM46", "SMKA12"]
    os.makedirs(dest_path, exist_ok=True)

    fails = []
    # Loop over year & trade type, download zip
    for i in tqdm(range(min_year, max_year+1)):
        for prefix in prefixes:
            fails.append(trade_data_download(f"{url}{prefix}_{i}archive.zip"))
            if i >= 2016:
                fails.append(trade_data_download(f"{url}{prefix}_{i}archive_JulDec.zip"))

    fails = [x for x in fails if x is not None]
    if len(fails) == 0:
        print("All Zip Files Downloaded.")
    else:
        print("The following Zip Files could not be downloaded:")
        print("\n".join(fails))

def unzip_trade_data(data_dir):
    """Recursively unzip the files and rename for loading into the database."""

    # Get list of zip files in path.
    data_dir = Path(data_dir)
    zipfiles = data_dir.rglob("*.[Zz][Ii][Pp]")
    zipfiles = list(zipfiles)

    # Terminate condition, or unzip + remove zipfiles one by one
    if len(list(zipfiles)) == 0:
        return
    else:
        for zip_file in tqdm(zipfiles):
            zipfile.ZipFile(zip_file).extractall(data_dir)
            os.remove(zip_file)

    # Recursion Activate! AAAAaAaAaaaa.....
    unzip_trade_data(data_dir)

def check_for_missing(data_dir, min_year = 2010, max_year = 2019):
    """Check for expected files in data_dir that are missing"""

    # Create looping parameters, formatted to lower / 2 digit month/year strings
    data_dir = Path(data_dir)
    files = data_dir.rglob("*")
    files = [str(x).lower() for x in files]
    prefixes = ["SMKE19", "SMKI19", "SMKX46", "SMKM46", "SMKA12"]
    prefixes = [x.lower() for x in prefixes]
    years = [f"{x:02d}" for x in range(min_year - 2000, max_year + 1 - 2000)]
    months = [f"{x:02d}" for x in range(1,13)]

    # Loop over and search for files, add those that aren't found.
    not_found = []
    for i in tqdm(years):
        for prefix in prefixes:
            for j in months:
                expected_file = f".*{prefix}{i}{j}.*"
                search_results = [re.search(expected_file, x) for x in files]
                if not any(search_results):
                    not_found.append(f"{prefix}{i}{j}")

    print("Files not found:")
    print("\n".join(not_found))

if __name__ == "__main__":

    # Parse Arguments
    parser = argparse.ArgumentParser(description="Download and Unzip Trade Data Files.")
    parser.add_argument("-t", "--target",
                        help="Target directory for the files. Created if it does not exist.",
                        required = True, default = 'data/')
    parser.add_argument("--min_year", type=int,
                        help="Starting Year for the downloads.",
                        required = True, default = 2010)
    parser.add_argument("--max_year",type=int,
                        help="Ending Year for the downloads.",
                        required = True, default = 2019)

    # Params
    args = parser.parse_args()
    data_dir = args.target
    min_year = args.min_year
    max_year = args.max_year

    # Download + Unzip Data
    print("Downloading Trade Data Files...")
    download_zipfiles(data_dir, min_year, max_year)
    print("Unzipping Trade Data Files...")
    unzip_trade_data(data_dir)

    # Print Missing Files
    print("Testing for Missing Files...")
    check_for_missing(data_dir, min_year, max_year)
