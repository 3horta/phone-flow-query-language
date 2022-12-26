from pyspark.sql import SparkSession
from tomlkit import string
from utils import charge_all_parquets_from_folder
import pandas as pd
from pandas import DataFrame as df
from typing import Dict, List, Tuple
import pyspark as spark
from datetime import datetime, timedelta


#################################### REGION FILTER AUXILIARS #######################################

spark = SparkSession.builder.appName('pfql_auxiliar').getOrCreate() 

def __towers_location_dataframes():
    """Returns the relation between cells id and regions"""

    #Charge regions of towers-id
    municipality_df = pd.read_csv("Data/municipios_tower.csv", engine="pyarrow")
    health_areas_df = pd.read_csv("Data/as_havana_tower.csv", engine="pyarrow")
    municipality_df.sort_values(by=['percent'], ascending=False)

    id_list = []
    mun_list = []
    for i in range(len(municipality_df)):
        id = municipality_df["id"]
        id_list.append(id[i])
        municipalities = municipality_df["category"]
        mun = municipalities[i]
        index = mun.index('-')
        mun = mun.replace(mun[index::], "")
        mun_list.append(mun)
    
    towid_region = {'Towers_id': id_list, 'Municipality': mun_list}

    towid_regionDF = pd.DataFrame(towid_region)


    #Charge cells-id and towers-id relation
    cellid_towid = {}
    local_path = "Data/cell_area/"
    files = charge_all_parquets_from_folder(local_path)

    cellid_towidDF = pd.DataFrame()
    for file in files:
        regDF=spark.read.parquet(f"{local_path + file}").toPandas()
        cellid_towidDF = pd.concat((cellid_towidDF, regDF))
        #idn = fileDF.select("id")
        #ida = fileDF.select("area_correlator")
        #cellid_towidDF = pd.concat(idn, ida)
    #cellid_towidDF = fileDF[['id','area_correlactor']]
    cellid_towidDF = cellid_towidDF.rename(columns={"id":"Cells_id", "area_correlator":"Towers_id", "province":"Province"})
    
    #Creating relation between cell-id and region
    cellid_regionDF = towid_regionDF.merge(cellid_towidDF)

    
    return cellid_regionDF


def id_regions_from_one_parquet(path, cellid_towid):

    regDF=spark.read.parquet(f"{path}")
    idn = regDF.select("id")
    ida = regDF.select("area_correlator")
    regDF=spark.read.parquet(path)
    for row in regDF.collect():    
        cellid_towid[row["id"]] = row["area_correlator"]
    cellid_towid = pd.DataFrame(cellid_towid, columns=[])


############################ DATE FILTER AUXILIAR ##########################################

def date_difference(start_date: string, end_date: string):
    date_list = []
    if start_date == "": 
        date_list.append(end_date)

    elif end_date == "":
        date_list.append(start_date)

    else:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        difference = end - start

        date_list = [(start + timedelta(days=d)).strftime("%Y-%m-%d")
                    for d in range(difference.days + 1)] 
    return date_list
