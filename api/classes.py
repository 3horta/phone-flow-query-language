from pyspark.sql.types import *
from os import times
from typing import List, Tuple
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import regex as re
from tomlkit import string
import pandas as pd
import datetime as dt


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
        date = hour_regex.search(path)
        hour_regex = re.compile(r'\d\d\d\d\d')
        h = hour_regex.search(path)
        hour = h[3] + h[4] + ":00:00"
        #Devolver dia y hora


def __towers_location_dataframes() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Returns id = region dictionary"""
    municipality_df = pd.read_csv("api/municipios_tower.csv", engine="pyarrow")
    health_areas_df = pd.read_csv("api/as_havana_tower.csv", engine="pyarrow")
    municipality_df.sort_values(by=['percent'], ascending=False)
    id_region = {}
    for i in range(len(municipality_df)):
        id = municipality_df["id"]
        reg = municipality_df["category"]
        id_region[id[i]] = reg[i]
    print(id_region)


    return id_region

__towers_location_dataframes()
