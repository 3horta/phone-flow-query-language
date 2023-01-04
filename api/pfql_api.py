import pandas as pd
import pyspark as spark
import os
from tokenize import String
from typing import List, Tuple
from pandas import DataFrame
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from api.classes import TimeInterval
from api.auxiliar_filter_methods import towers_location_dataframes, convert_to_seconds, preprocess_parquets

spark = SparkSession.builder.appName('pfql').getOrCreate() 

ID_REGION = towers_location_dataframes()
PROVINCIES = ["Pinar del Río", "Artemisa", "La Habana", "Mayabeque", "Matanzas", "Villa Clara", "Cienfuegos", "Sancti Spíritus", "Ciego de Ávila", "Camagüey", "Las Tunas", "Holguín", "Granma", "Santiago de Cuba", "Guantánamo"]
MUNICIPALITIES = []
######################################## Region filter ######################################

def filter_by_province(data: DataFrame, location : str) -> DataFrame :
    """
    Filter a DataFrame by province's name
    """
    new_dataDF = data
    new_dataDF = ID_REGION.set_index('Cells_id').join(new_dataDF.set_index('cell_ids'))

    new_dataDF = new_dataDF.dropna()
    filtered_data = new_dataDF[new_dataDF.Province == location]

    return filtered_data

def filter_by_municipality(data: DataFrame, location : str) -> DataFrame:
    """
    Filter a DataFrame by municipality's name
    """
    new_dataDF = data
    new_dataDF = ID_REGION.set_index('Cells_id').join(new_dataDF.set_index('Cells_id'))

    new_dataDF = new_dataDF.dropna()
    filtered_data = new_dataDF[new_dataDF.Municipality == location]
    
    return filtered_data

######################################## Date-Time filter #################################


def filter_by_date(time_interval: TimeInterval):
    """
    Take just the data from the specified dates (Format ex: 2000-03-01)
    """
    date_list = time_interval.interval
    date_filteredDF = pd.DataFrame()
    # It will filter all at Data folder
    main_path = 'Data/1/'
    parquets = []
    folders = list(os.listdir(main_path))

    for folder in folders:
        if folder in date_list:
            parquets = charge_data(main_path + folder)
            date_filteredDF = pd.concat((date_filteredDF, parquets))
        
    return date_filteredDF

def filter_by_time(data: DataFrame, start_time: str = "", end_time: str = ""):
    """
    Filter a DataFrame by time (Format ex: 00:00:00)
    """
    start_time = convert_to_seconds(start_time)
    end_time = convert_to_seconds(end_time)
    
    filtered_data = data.loc[(data.Times >= start_time) & (data.Times <= end_time)]

    return filtered_data


def get_collection(collection_name : str) -> List[str]: #need to define how to load collections from language
    """
    Returns collection.
    """
    pass
    #clusterset b = group a by {PROVINCES};


def filter(data: DataFrame, filters: list):
    """
    Returns a DataFrame filtered by filters.
    """
    
    filteredDF = data

    for fil in filters:
        
        if isinstance(fil, TimeInterval):
    
            filteredDF = filter_by_date(fil)
        
        if isinstance(fil, str):
            location = fil
        
            if "." in location:
                index = location.index(".")
                province = location[0:index]
                municipality = location[index+1::]
                #filteredDF = filter_by_province(filteredDF, province)
                filteredDF = filter_by_municipality(filteredDF, municipality)


            else:
                province = location  
                filteredDF = filter_by_province(filteredDF, province)

        return filteredDF



############################## Set Operations ###########################################

def union(df1, df2):
    """
    Returns two DataFrame union.
    """
    unionDF = pd.concat((df1, df2))
    return unionDF

def intersection(df1, df2):
    """
    Returns two DataFrame intersections.
    """
    intersectionDF = pd.merge(df1, df2, how='inner')
    return intersectionDF

def difference(df1, df2):
    """
    Returns two DataFrame difference. (parameters order)
    """
    differenceDF = pd.concat([df1, df2]).drop_duplicates(keep=False)
    return differenceDF


########################## Other Operations #############################################

def get_towers_columns(data: DataFrame):
    """
    Get the Towers DataFrame column
    """
    towerd_colsDF = data[['Cells_id']].drop_duplicates(keep=False).tolist()

    return towerd_colsDF

def get_users_columns(data: DataFrame):
    """
    Get the users DataFrame column
    """
    users_colsDF = data[['Codes']].drop_duplicates(keep=False).tolist()

    return users_colsDF

def count(data: DataFrame) -> int:
    """
    Return de count of no-null rows in DataFrame
    """

    data_count = data.count()[0]

    return data_count

def group_by(data: DataFrame, collections: list):
    """
    Return a DataFrame gruoped by specified collections
    """
    groupedDF = data.groupby( by = collections )
    return groupedDF

def charge_data(path = 'Data/1/'):
    """
    Charge all parquets on main folder and turn it on DataFrame
    """
    data = spark.read.format("parquet")\
    .option("recursiveFileLookup", "true")\
    .load(path)
    data.show()

    data = preprocess_parquets(data)
    
    return data.toPandas()
"""
d = charge_data()
f = filter(d, ["La Habana"])
print(count(f))
print(count(d))"""