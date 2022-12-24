from pyspark.sql import SparkSession
from utils import charge_all_parquets_from_folder
import pandas as pd
from typing import Dict, List, Tuple
import pyspark as spark


#################################### REGION FILTER AUXILIARS #######################################

spark = SparkSession.builder.appName('pfql_auxiliar').getOrCreate() 

def __towers_location_dataframes() -> Dict:
    """Returns the relation between cells id and regions"""

    #Charge regions
    municipality_df = pd.read_csv("Data/municipios_tower.csv", engine="pyarrow")
    health_areas_df = pd.read_csv("Data/as_havana_tower.csv", engine="pyarrow")
    municipality_df.sort_values(by=['percent'], ascending=False)

    towid_region = {}
    for i in range(len(municipality_df)):
        id = municipality_df["id"]
        reg = municipality_df["category"]
        towid_region[id[i]] = reg[i]

    cellid_towid = {}
    local_path = "Data/cell_area/"
    files = charge_all_parquets_from_folder(local_path)

    for file in files:
        id_regions_from_one_parquet(local_path + file, cellid_towid)
    
    
    cellid_region = {}
    for i in cellid_towid.keys():
        try:
            cellid_region[i] = towid_region[cellid_towid[i]]
        except:
            continue

    return cellid_region


def id_regions_from_one_parquet(path, cellid_towid):

    regDF=spark.read.parquet(f"{path}")
    idn = regDF.select("id")
    ida = regDF.select("area_correlator")
    regDF=spark.read.parquet(path)
    for row in regDF.collect():    
        cellid_towid[row["id"]] = row["area_correlator"]

__towers_location_dataframes()
