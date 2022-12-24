from pyspark.sql.types import *
from os import times
from typing import Dict, List, Tuple
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import regex as re
from tomlkit import string
import pandas as pd
import datetime as dt
import tarfile
from utils import charge_all_parquets_from_folder


#region classes
# 
spark = SparkSession.builder.appName('pfql_class').getOrCreate() 

class Time:
    def __init__(self, beginning_time : dt.datetime, ending_time : dt.datetime = None) -> None:
        """
        Initializes a time interval given datetimes limits.
        """
        self.interval = self.__fill_interval(beginning_time, ending_time)
        self.time = self.interval.length
    
    def __fill_interval(self, beginning_time : dt.datetime, ending_time : dt.datetime):
        """
        Fills time interval limits.
        """
        if ending_time != None:
            interval = pd.Interval(pd.Timestamp(beginning_time), pd.Timestamp(ending_time), closed = 'both')
        else :
            ending_time = dt.datetime(beginning_time.year, beginning_time.month, beginning_time.day) + dt.timedelta(days = 1)
            interval = pd.Interval(pd.Timestamp(beginning_time), pd.Timestamp(ending_time), closed = 'left')
        return interval

class Packet:
    def __init__(self, parq_path: string) -> None:
        path = parq_path
        date_time = self._get_time(parq_path)

    def _get_time(path: string):
        date_regex = re.compile(r'\d*-\d*-\d*')
        date = date_regex.search(path)
        hour_regex = re.compile(r'\d\d\d\d\d')
        h = hour_regex.search(path)
        hour = h[3] + h[4] + ":00:00"
        #Devolver dia y hora


def __towers_location_dataframes() -> Dict:
    """Returns the relation between cells id and regions"""

    #Charge regions
    municipality_df = pd.read_csv("api/municipios_tower.csv", engine="pyarrow")
    health_areas_df = pd.read_csv("api/as_havana_tower.csv", engine="pyarrow")
    municipality_df.sort_values(by=['percent'], ascending=False)

    towid_region = {}
    for i in range(len(municipality_df)):
        id = municipality_df["id"]
        reg = municipality_df["category"]
        towid_region[id[i]] = reg[i]

    cellid_towid = {}
    local_path = "api/cell_area/"
    files = charge_all_parquets_from_folder(local_path)
    print(files)
    for file in files:
        print(local_path + file)
        id_regions_from_one_parquet(local_path + file, cellid_towid)
    
    
    
    cellid_region = {}
    for i in cellid_towid.keys():
        try:
            cellid_region[i] = towid_region[cellid_towid[i]]
        except:
            continue

    print(cellid_region)

    return cellid_region


def id_regions_from_one_parquet(path, cellid_towid):
    print(path)
    regDF=spark.read.parquet(f"{path}")
    idn = regDF.select("id")
    ida = regDF.select("area_correlator")
    regDF=spark.read.parquet(path)
    for row in regDF.collect():    
        cellid_towid[row["id"]] = row["area_correlator"]
    print("OVER")

__towers_location_dataframes()
