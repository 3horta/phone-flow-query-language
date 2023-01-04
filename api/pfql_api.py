from os import times
from telnetlib import TSPEED
from typing import List, Tuple
import pandas as pd
from pandas import DataFrame
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
from api.classes import TimeInterval
#from abstract_syntax_tree import MunicipalitiesCollection, Towers
from api.utils import charge_all_parquets_from_folder, preprocess_parquets, print_data_parquet
from api.auxiliar_filter_methods import towers_location_dataframes, convert_to_seconds, date_difference
import os


spark = SparkSession.builder.appName('pfql').getOrCreate() 
ID_REGION = towers_location_dataframes()


######################################## Region filter ######################################

def filter_by_province(data: DataFrame, location : str) -> DataFrame :

    #if ID_REGION == None:
    #ID_REGION = __towers_location_dataframes()

    new_dataDF = preprocess_parquets(data).toPandas()

    new_dataDF = ID_REGION.set_index('Cells_id').join(new_dataDF.set_index('Cells_id'))

    new_dataDF = new_dataDF.dropna()
    filtered_data = new_dataDF[new_dataDF.Province == location]
    filtered_data.head()

    return filtered_data

def filter_by_municipality(data: DataFrame, location : str) -> DataFrame:

    #if ID_REGION == None:
    #    ID_REGION = __towers_location_dataframes()
    print(data)
    new_dataDF = preprocess_parquets(data).toPandas()
    print(new_dataDF)
    #new_dataDF = data

    new_dataDF = ID_REGION.set_index('Cells_id').join(new_dataDF.set_index('Cells_id'))

    new_dataDF = new_dataDF.dropna()
    filtered_data = new_dataDF[new_dataDF.Municipality == location]
    print(ID_REGION)
    filtered_data.head()

    print(filtered_data)
    return filtered_data


######################################## Date-Time filter #################################

# time -> year - month - day
def filter_by_date(star_date : str = "" , end_date : str = ""):
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

def filter_by_time(data, start_time: str = "", end_time: str = ""):

    start_time = convert_to_seconds(start_time)
    end_time = convert_to_seconds(end_time)
    data = preprocess_parquets(data) #if de si hay un alista
    filtered_data = data.loc[(data.Times >= start_time) & (data.Times <= end_time)]

    print(filtered_data)
    return filtered_data


def get_collection(collection_name : str) -> List[str]: #need to define how to load collections from language
    """
    Returns collection.
    """
    pass
    #clusterset b = group a by {PROVINCES};


def filter(data, filters):
    """
    Returns a new set filtered by filters.
    """
    filters = filters.split(",")
    filteredDF = data
    print(filteredDF)
    for fil in filters:
        #filter = str(fil)
        if isinstance(fil, TimeInterval):
    
            filteredDF = filter_by_time(str(fil.start_date), str(fil.end_date))
        
       # if isinstance(fil, LocationPredicate):
        
                
       #     filteredDF = filter_by_municipality(filteredDF, fil.location)


        return filteredDF



############################## Set Operations ###########################################

def union(A, B):
    """
    Returns two df union.
    """
    unionDF = pd.concat((A, B))
    return unionDF

def intersection(A, B):
    """
    Returns two sets intersections.
    """
    intersectionDF = pd.merge(A, B, how='inner')
    return intersectionDF

def difference(A, B):
    """
    Returns two sets difference. (parameters order)
    """
    differenceDF = pd.concat([A, B]).drop_duplicates(keep=False)
    return differenceDF


########################## Other Operations #############################################

def get_towers_columns(data):
    data = preprocess_parquets(data)
    towerd_colsDF = data[['Cells_id']].drop_duplicates(keep=False)

    return towerd_colsDF

def get_users_columns(data):
    data = preprocess_parquets(data)
    users_colsDF = data[['Codes']].drop_duplicates(keep=False)

    return users_colsDF

def count(data):
    data = preprocess_parquets(data)
    data_countDF = data.count()

    return data_countDF

def group_by(data,  ):
    pass 


def charge_data(path = 'Data/1/'):

    data = spark.read.format("parquet")\
    .option("recursiveFileLookup", "true")\
    .load(path)
    data.show()
    """
    folders = list(os.listdir(path))
    all_dataDF = pd.DataFrame()
    print(folders)
    for folder in folders:
        folder_reg = re.compile(r'\d\d\d\d-\d\d-\d\d')
        folder = folder_reg.search(folder)
        print(folder)
        if folder == None:
            continue
        print(path + folder.string)
        folder_data = charge_all_parquets_from_folder(path + folder.string)
        all_dataDF = pd.concat([all_dataDF, folder_data])
    """
    return data.toPandas()

#d = charge_data()
#b = preprocess_parquets(d)
#a = b.toPandas()

#get_tower_by_municipality(d, "Playa")
#filter_by_time(d, "02:00", "02:45")
#print(d)
#b = filter(d, "location(La Habana.Playa)")
#filter_by_date("2021-03-01")
#c = union(a, b)
#print(a)
#endregion