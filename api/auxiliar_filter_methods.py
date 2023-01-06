import pandas as pd
import pyspark as spark
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.types import *


#################################### REGION FILTER AUXILIARS #######################################

def towers_location_dataframes():
    """
    Returns the relation between cells id and regions
    """

    cellid_regionDF= pd.read_csv("Data/cellID_region.csv", engine="pyarrow")
    return cellid_regionDF

def create_towers_location_csv():

    #Charge regions of towers-id
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
    local_path = "Data/cell_area/"
  
    cellid_towidDF = cellid_towidDF.rename(columns={"id":"Cells_id", "area_correlator":"Towers_id", "province":"Province"})
    
    #Creating relation between cell-id and region
    cellid_regionDF = towid_regionDF.merge(cellid_towidDF)

    cellid_regionDF.to_csv("./Data/cellID_region.csv")


def id_regions_from_one_parquet(path, cellid_towid):

    regDF=spark.read.parquet(f"{path}")
    idn = regDF.select("id")
    ida = regDF.select("area_correlator")
    regDF=spark.read.parquet(path)
    for row in regDF.collect():    
        cellid_towid[row["id"]] = row["area_correlator"]
    cellid_towid = pd.DataFrame(cellid_towid, columns=[])


############################ DATE FILTER AUXILIAR ############################################

def date_difference(start_date: str, end_date: str):

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

def time_difference(start_time: str, end_time: str):
    
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

########################## PARQUET PROCESSING AUXILIARS ###############################

def preprocess_parquets(data):

    df = data
    combine = F.udf(lambda x, y: list(zip(x, y)),
              ArrayType(StructType([StructField("cell_ids", StringType()),
                                    StructField("times", StringType())])))

    explode_rowsDF = df.withColumn("new", combine("cell_ids", "times"))\
       .withColumn("new", F.explode("new"))\
       .select("code", F.col("new.cell_ids").alias("cell_ids"), F.col("new.times").alias("times"))
    

    return explode_rowsDF