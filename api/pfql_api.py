from os import times
from typing import List, Tuple
import pandas as pd
import datetime as dt
from pyspark import *
import pyspark as spark
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import *
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import regex as re
from tomlkit import string
from utils import charge_all_parquets_from_folder, print_data_parquet
from auxiliar_filter_methods import __towers_location_dataframes
import os

spark = SparkSession.builder.appName('pfql').getOrCreate() 

ID_REGION = __towers_location_dataframes()

def get_tower_by_region(location : str) -> List[str]:
    cells = []
    for id in ID_REGION.keys():
        if(ID_REGION[id] == location):
            cells.append(id)

    return cells

# time -> year - month - day
def filter_by_date(date):
    main_path = 'Data/1/'
    filtered_path = []
    files_by_month = list(os.listdir(main_path))
    for file in files_by_month:
        if file == date:
            parquets = charge_all_parquets_from_folder(main_path + file)

    count = 0
    for parquet in parquets:
        filtered_path.append(main_path + file + "/" + parquet)
        #For visualization
        count += 1
        print_data_parquet(main_path + file + "/" + parquet, f"REGISTER{count}")
        
    print(filtered_path)
    return filtered_path


def get_collection(collection_name : str) -> List[str]:
    """
    Returns collection.
    """
    pass

def filter(set, *filters):
    """
    Returns a new set filtered by filters.
    """
    pass


#region set operations

def union(A, B):
    """
    Returns two sets union.
    """
    pass

def intersection(A, B):
    """
    Returns two sets intersections.
    """
    pass

def difference(A, B):
    """
    Returns two sets difference. (parameters order)
    """
    pass


def charge_data(path):

    regDF=spark.read.parquet(path)
    print(regDF)

#charge_data("api/part-00000-78181276-20b4-47ea-8cad-0ee84ef18436-c000.snappy.parquet")
get_tower_by_region("Boyeros-La H")
filter_by_date("2021-03-01")
#endregion