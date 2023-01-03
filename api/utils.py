import os
from re import S 
import regex as re
import pyspark as spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
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
    df = spark.createDataFrame(data)
    df2 = df.select(df.code, explode(df.cell_ids))
    df3 = df.select(df.code, explode(df.times))
   
    subdiv_rowsDF = df2.join(df3, df2.code == df3.code, 'inner')
    subdiv_rowsDF.show()
     
    #subdiv_rows = {'Codes': codes, 'Cells_id': cells_id, 'Times': times}

    #subdiv_rowsDF = pd.DataFrame(subdiv_rows)
    return subdiv_rowsDF

def print_data_parquet(path, name):
    spark.sql(f"CREATE TEMPORARY VIEW {name} USING parquet OPTIONS (path \"{path}\")")
    spark.sql(f"SELECT * FROM {name}").show()

