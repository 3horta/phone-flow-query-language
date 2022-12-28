from os import times
from typing import List, Tuple
import pandas as pd
from pandas import DataFrame as df
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
from utils import charge_all_parquets_from_folder, preprocess_parquets, print_data_parquet
from auxiliar_filter_methods import __towers_location_dataframes, date_difference
import os

spark = SparkSession.builder.appName('pfql').getOrCreate() 

ID_REGION = __towers_location_dataframes()

######################################## Region filter ##################################################

def get_tower_by_province(data: df, location : str) -> List[str]:

    new_dataDF = preprocess_parquets(data)

    new_dataDF = ID_REGION.set_index('Cells_id').join(new_dataDF.set_index('Cells_id'))

    new_dataDF = new_dataDF.dropna()
    filtered_data = new_dataDF[new_dataDF.Province == location]
    filtered_data.head()

    return filtered_data

def get_tower_by_municipality(data: df, location : str) -> List[str]:

    new_dataDF = preprocess_parquets(data)

    new_dataDF = ID_REGION.set_index('Cells_id').join(new_dataDF.set_index('Cells_id'))

    new_dataDF = new_dataDF.dropna()
    filtered_data = new_dataDF[new_dataDF.Municipality == location]
    filtered_data.head()
    print(filtered_data)
    return filtered_data


######################################## Date-Time filter ################################################

# time -> year - month - day
def filter_by_date(star_date = "", end_date = ""):
    date_list = date_difference(star_date, end_date)
    date_filteredDF = pd.DataFrame()
    # It will filter all at Data folder
    main_path = 'Data/1/'
    parquets = []
    folders = list(os.listdir(main_path))
    print(folders)
    print(date_list)
    for folder in folders:
        for d in date_list:
            if folder == d :
                parquets = charge_all_parquets_from_folder(main_path + folder)
                for parquet in parquets:
                    regDF = spark.read.parquet(f"{main_path}{folder}/{parquet}").toPandas()
                    date_filteredDF = pd.concat((date_filteredDF, regDF))
        
    return date_filteredDF


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
    regDF=spark.read.parquet(path).toPandas()
    return regDF

d = charge_data("Data/1/2021-03-01/part-00000-78181276-20b4-47ea-8cad-0ee84ef18436-c000.snappy.parquet")
#get_tower_by_municipality(d, "Playa")
a = preprocess_parquets(d)
#filter_by_date("2021-03-01")
#endregion