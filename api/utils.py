import os
from re import S 
import regex as re
import pyspark as spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pfql_utils').getOrCreate() 

def charge_all_parquets_from_folder(path):
    #path -> path to the folder where the parquets are
    files = list(os.listdir(path))
    parquets = []

    for file in files:
        parquet_reg = re.compile(r'(\S*.parquet)')
        parquet = parquet_reg.search(file)
        if parquet == None:
            continue
        else:
            if '.crc' in parquet.string:
                continue
            else:  
                parquets.append(parquet.string)

    return parquets


def print_data_parquet(path):
    spark.sql(f"CREATE TEMPORARY VIEW REGISTER USING parquet OPTIONS (path \"{path}\")")
    spark.sql("SELECT * FROM REGISTER").show()