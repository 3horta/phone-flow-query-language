from pyspark.sql.types import *
from os import times
from typing import Dict, List, Tuple
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
        date = date_regex.search(path)
        hour_regex = re.compile(r'\d\d\d\d\d')
        h = hour_regex.search(path)
        hour = h[3] + h[4] + ":00:00"
        #Devolver dia y hora