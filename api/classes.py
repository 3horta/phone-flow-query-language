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
class TimeInterval:
    def __init__(self, beginning_time : dt.datetime, ending_time : dt.datetime = None) -> None:
        """
        Initializes a time interval given datetimes limits.
        """
        self.interval = self.__fill_interval(beginning_time, ending_time)
        self.time = self.interval.length
    
    def time_difference(self, start_time : dt.datetime, end_time : dt.datetime):
        """
        Fills time interval limits.
        """
        time_list = []
        if start_time == "": 
            date_list.append(end_time)

        elif end_time == "":
            date_list.append(start_time)

        else:
            start = dt.strptime(start_time, "%Y-%m-%d")
            end = dt.strptime(end_time, "%Y-%m-%d")
            difference = end - start


            date_list = [(start + dt.timedelta(days=d)).strftime("%Y-%m-%d")
                        for d in range(difference.days + 1)] 
        return date_list