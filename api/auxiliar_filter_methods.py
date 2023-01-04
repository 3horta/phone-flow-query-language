from pyspark.sql import SparkSession
from tomlkit import string
from api.utils import charge_all_parquets_from_folder
import pandas as pd
from pandas import DataFrame as df
from typing import Dict, List, Tuple
import pyspark as spark
from datetime import datetime, timedelta


#################################### REGION FILTER AUXILIARS #######################################

spark = SparkSession.builder.appName('pfql_auxiliar').getOrCreate() 

def towers_location_dataframes():
    cellid_regionDF= pd.read_csv("Data/cellID_region.csv", engine="pyarrow")
    return cellid_regionDF

def create_towers_location_csv():
    """Returns the relation between cells id and regions"""

    #Charge regions of towers-id
    print("in")
    municipality_df = pd.read_csv("Data/municipios_tower.csv", engine="pyarrow")
    #health_areas_df = pd.read_csv("Data/as_havana_tower.csv", engine="pyarrow")
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
    cellid_towidDF = charge_all_parquets_from_folder(local_path)

    #cellid_towidDF = pd.DataFrame()
    #for file in files:
    #    regDF=spark.read.parquet(f"{local_path + file}").toPandas()
    #    cellid_towidDF = pd.concat((cellid_towidDF, regDF))
        #idn = fileDF.select("id")
        #ida = fileDF.select("area_correlator")
        #cellid_towidDF = pd.concat(idn, ida)
    #cellid_towidDF = fileDF[['id','area_correlactor']]
    cellid_towidDF = cellid_towidDF.rename(columns={"id":"Cells_id", "area_correlator":"Towers_id", "province":"Province"})
    
    #Creating relation between cell-id and region
    cellid_regionDF = towid_regionDF.merge(cellid_towidDF)

    cellid_regionDF.to_csv("./Data/cellID_region.csv")
    #return cellid_regionDF


def id_regions_from_one_parquet(path, cellid_towid):

    regDF=spark.read.parquet(f"{path}")
    idn = regDF.select("id")
    ida = regDF.select("area_correlator")
    regDF=spark.read.parquet(path)
    for row in regDF.collect():    
        cellid_towid[row["id"]] = row["area_correlator"]
    cellid_towid = pd.DataFrame(cellid_towid, columns=[])


############################ DATE FILTER AUXILIAR ############################################

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

############################ TIME FILTER AUXILIAR ############################################

def time_difference(start_time: string, end_time: string):
    
    if start_time == "": 
        date_list.append(end_time)

    elif end_time == "":
        date_list.append(start_time)

    else:
        start = datetime.strptime(start_time, "%Y-%m-%d")
        end = datetime.strptime(end_time, "%Y-%m-%d")
        difference = end - start


        date_list = [(start + timedelta(days=d)).strftime("%Y-%m-%d")
                    for d in range(difference.days + 1)] 
    return date_list

def convert_to_seconds(time: str) -> int:

    if len(time) > 0:
        time = int(time[0:2]) * 3600 + int(time[3:5]) * 60

    return time

a = towers_location_dataframes()