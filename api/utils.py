import os
from re import S 
import regex as re
import pyspark as spark
from pyspark.sql import SparkSession
import pandas as pd
from math import modf

spark = SparkSession.builder.appName('pfql_utils').getOrCreate() 

def charge_all_parquets_from_folder(path):
    #path -> path to the folder where the parquets are
    files = list(os.listdir(path))
    parquetsDF = pd.DataFrame()

    for file in files:
        parquet_reg = re.compile(r'(\S*.parquet)')
        parquet = parquet_reg.search(file)
        if parquet == None:
            continue
        else:
            if '.crc' in parquet.string:
                continue
            else:  
                new_parquet = spark.read.parquet(path + '/' + parquet.string).toPandas()
                parquetsDF = pd.concat([parquetsDF, new_parquet])


    return parquetsDF

def separate_time(time):
    try:
        index = time.index('.')
        print(index)

        first = time.replace(time[index::], "")
        second = time.replace(time[0:index+1], "")
        print(first)
        print(second)
    except:
        first = round(float(time),2)
        second = "00"

    return first, second

def preprocess_parquets(data):
    codes = []
    cells_id = []
    times = []  
    # Iterate over each row
    for _, rows in data.iterrows():
        # Create list for the current row
        for _ in range(len(rows.cell_ids)):
            codes.append(rows.code)
            
        cells_id.extend(rows.cell_ids)

        times.extend(rows.times)
     
    subdiv_rows = {'Codes': codes, 'Cells_id': cells_id, 'Times': times}

    subdiv_rowsDF = pd.DataFrame(subdiv_rows)
    return subdiv_rowsDF

def print_data_parquet(path, name):
    spark.sql(f"CREATE TEMPORARY VIEW {name} USING parquet OPTIONS (path \"{path}\")")
    spark.sql(f"SELECT * FROM {name}").show()