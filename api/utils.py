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

        for t in rows.times:
            time = t // 3600
            index = time.index('.')
            
            hours = t.replace(t[index::], "")
            mins = t.replace(t[0:index+1], "")

            sec = 00
            if mins > 60:
                sec = mins // 60
            time = f"{hours}:{mins}:{sec}"
            times.append(time)
    
    print(len(codes))
    print(len(cells_id))
    print(len(times))
    subdiv_rows = {'Codes': codes, 'Cells_id': cells_id, 'Times': times}

    subdiv_rowsDF = pd.DataFrame(subdiv_rows)
    print(subdiv_rowsDF)
    return subdiv_rowsDF

def print_data_parquet(path, name):
    spark.sql(f"CREATE TEMPORARY VIEW {name} USING parquet OPTIONS (path \"{path}\")")
    spark.sql(f"SELECT * FROM {name}").show()