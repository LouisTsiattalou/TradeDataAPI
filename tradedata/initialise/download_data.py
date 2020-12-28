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

import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

from pathlib import Path
from tqdm import tqdm

def get_hyperlinks(prefixes = ["SMKE19", "SMKI19", "SMKX46", "SMKM46", "SMKA12"]):
    """Programatically extract links from the UK Trade Info Bulk Datasets page."""
    prefix_regex = "(" + "|".join(prefixes) + ")"

    # Archive
    url_base = "https://www.uktradeinfo.com/trade-data/latest-bulk-datasets/bulk-datasets-archive/"
    soup = BeautifulSoup(requests.get(url_base).content, "html.parser")
    links = [x["href"] for x in soup.findAll("a")]
    links = [str(url) for url in links if re.search(prefix_regex, str(url), re.IGNORECASE)]
    links = [x for x in links if not re.search("#", x)]
    links = ["https://www.uktradeinfo.com" + x for x in links]

    # This Year
    url_base = "https://www.uktradeinfo.com/trade-data/latest-bulk-datasets/"
    soup = BeautifulSoup(requests.get(url_base).content, "html.parser")
    ty_links = [x["href"] for x in soup.findAll("a")]
    ty_links = [str(url) for url in ty_links if re.search(prefix_regex, str(url), re.IGNORECASE)]
    ty_links = [x for x in ty_links if not re.search("#", x)]
    ty_links = ["https://www.uktradeinfo.com" + x for x in ty_links]

    links.extend(ty_links)

    return(links)


def download_zipfiles(dest_path, min_year=2010, max_year=2019):
    """Downloads zipfiles from UKTradeInfo for later extraction."""

    def trade_data_download(remote_file_path, destination = dest_path):
        """Download data if it can be found. Return remote file name if exception occurs"""
        try:
            wget.download(remote_file_path, out = destination)
            return None
        except:
            return remote_file_path

    os.makedirs(dest_path, exist_ok=True)

    links = get_hyperlinks() # Get all files by default
    years = list(range(min_year, max_year+1))
    years = [str(year) + "archive" for year in years]
    years_regex = "(" + "|".join(years) + ")"
    links = [x for x in links if re.search(years_regex, x, re.IGNORECASE)]

    fails = []

    # Run trade_data_download func for side effect, build list of failed downloads
    # for future inspection if necessary.
    for link in tqdm(links):
        fails.append(trade_data_download(link))

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
    parser.add_argument("--max_year", type=int,
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
