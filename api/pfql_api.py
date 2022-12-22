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
from classes import __towers_location_dataframes

spark = SparkSession.builder.appName('pfql').getOrCreate() 

ID_REGION = __towers_location_dataframes()

def get_tower_by_region(location : str) -> List[str]:
    towers_id = []
    for i in ID_REGION.keys():
        if(ID_REGION[i] == location):
            towers_id.append(i)
    return towers_id

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
def print_data_parquet(path):
    spark.sql(f"CREATE TEMPORARY VIEW REGISTER USING parquet OPTIONS (path \"{path}\")")
    spark.sql("SELECT * FROM REGISTER").show()

def charge_data(path):
    regDF=spark.read.parquet(path)

    regDF.select("cell_ids").show()
    print(regDF)
    print_data_parquet(path)

charge_data("api/part-00000-78181276-20b4-47ea-8cad-0ee84ef18436-c000.snappy.parquet")
get_tower_by_region("Arroyo Naranjo-La H")
#endregion