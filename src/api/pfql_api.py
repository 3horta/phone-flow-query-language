import os
import re
from typing import List

import pandas as pd
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from api.auxiliar_filter_methods import (convert_to_seconds,
                                         preprocess_parquets,
                                         towers_location_dataframes)
from api.classes import TimeInterval

spark = SparkSession.builder.appName('pfql').getOrCreate() 

ID_REGION = towers_location_dataframes()



######################################## Region filter ######################################

def filter_by_province(data: DataFrame, location : str) -> DataFrame :
    """
    Filter a DataFrame by province's name
    """
    new_dataDF = data
    
    new_dataDF= new_dataDF.merge(ID_REGION, left_on='cell_ids', right_on= 'Cells_id')
    
    new_dataDF = new_dataDF.dropna()
    
    filtered_data = new_dataDF[new_dataDF.Province == location]
    
    return filtered_data.drop(columns = ['Cells_id'])

def filter_by_municipality(data: DataFrame, location : str) -> DataFrame:
    """
    Filter a DataFrame by municipality's name
    """
    new_dataDF = data
    
    #new_dataDF= new_dataDF.merge(ID_REGION, left_on='cell_ids', right_on= 'Cells_id')
    
    new_dataDF = new_dataDF.dropna()
    
    filtered_data = new_dataDF[new_dataDF.Municipality == location]
    
    return filtered_data

######################################## Date-Time filter #################################


def filter_by_date(data: DataFrame, time_interval: TimeInterval):
    """
    Filter specific dates data  (Format ex: 2000-03-01)
    """
    
    dates_df = pd.DataFrame({'Date': time_interval.interval})
    
    filtered_data = data.merge(dates_df, left_on='Date', right_on='Date')
    
    return filtered_data

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


def filter(data: DataFrame, filters: list):
    """
    Returns a DataFrame filtered by filters.
    """
    
    filteredDF = data

    for fil in filters:
        
        if isinstance(fil, TimeInterval):
    
            filteredDF = filter_by_date(filteredDF, fil)
        
        if isinstance(fil, str):
            location = fil
        
            if "." in location:
                index = location.index(".")
                province = location[0:index]
                municipality = location[index+1::]
                filteredDF = filter_by_province(filteredDF, province)
                filteredDF = filter_by_municipality(filteredDF, municipality)


            else:
                province = location  
                filteredDF = filter_by_province(filteredDF, province)

    return filteredDF



############################## Set Operations ###########################################

def set_operations(df1, df2, operator):
    return OPERATORS[operator](df1, df2)

def union(df1, df2):
    """
    Returns DataFrames union.
    """
    unionDF = pd.concat([df1, df2], axis=0).drop_duplicates(keep="first")
    return unionDF

def intersection(df1, df2):
    """
    Returns two DataFrame intersections.
    """
    intersectionDF = pd.merge(df1, df2, how='inner')
    return intersectionDF

def difference(df1, df2):
    """
    Returns two DataFrame difference. (parameters ordered)
    """
    differenceDF = pd.concat([df1, df2]).drop_duplicates(keep=False)
    return differenceDF


########################## Other Operations #############################################

def get_towers_columns(data: DataFrame):
    """
    Get the Towers DataFrame column
    """
    towerd_colsDF = data['cell_ids'].drop_duplicates(keep="first").values.tolist()

    return towerd_colsDF

def get_users(data: DataFrame):
    """
    Get users.
    """
    users_colsDF = data['code'].drop_duplicates(keep="first").values.tolist()

    return users_colsDF

def count(data: DataFrame) -> int:
    """
    Return register rows count.
    """
    return data.shape[0]

def group_by(data: DataFrame, collections: list):
    """
    Return a DataFrame grouped by specific collections.
    """
    locations_in_df = merge_locations(data)
    
    str_collections = []
    for collection in collections:
        if collection == PROVINCES:
            str_collections.append('Province')
        elif collection == MUNICIPALITIES:
            str_collections.append('Municipality')
    
    
    str_collections = [*set(str_collections)]
    
    gb = locations_in_df
    for item in str_collections:
        gb = gb.groupby(item, group_keys=True).apply(lambda x: x)
    
    return gb

def merge_locations(data: DataFrame):
    return data.merge(ID_REGION, left_on='cell_ids', right_on= 'Cells_id').dropna().drop(columns = ['Cells_id'])

def load_parquets(path):
    """
    Load all parquets on main folder and turn it on DataFrame
    """
    
    # data = spark.read\
    # .option("recursiveFileLookup", "true")\
    # .parquet('/Users/hanselblanco/Documents/School/3ro/2do_semestre/C/py_spark/PFQL_data')
    
    data = spark.read.format("parquet")\
    .option("recursiveFileLookup", "true")\
    .load(path)
    
    # data.show()

    data = preprocess_parquets(data)
    
    return data.toPandas()

def load_data(path = 'data/registers/'):
    folders = list(os.listdir(path))
    df = pd.DataFrame()
    
    for folder in folders:
        pattern = re.compile("\d{4}-\d\d?-\d\d?")
        cond = pattern.fullmatch(folder)
        if not cond:
            continue
        parquets_df = load_parquets(path + folder)
        parquets_df['Date'] = [folder] * parquets_df.shape[0]
        df = pd.concat((df, parquets_df))
    
    return df
















PROVINCES = ["Pinar del Río", "Artemisa", "La Habana", "Mayabeque", "Matanzas", "Villa Clara", "Cienfuegos", "Sancti Spíritus","Ciego de Ávila", "Camagüey",
 "Las Tunas", "Holguín", "Granma", "Santiago de Cuba", "Guantánamo"]

MUNICIPALITIES = ["Pinar del Río.Consolación del Sur", "Pinar del Río.Guane", "Pinar del Río.La Palma", "Pinar del Río.Los Palacios", "Pinar del Río.Mantua", 
"Pinar del Río.Minas de Matahambre", "Pinar del Río.Pinar del Río.", "Pinar del Río.San Juan y Martínez", "Pinar del Río.San Luis", 
"Pinar del Río.Sandino", "Pinar del Río.Viñales", 
"Artemisa.Alquízar", "Artemisa.Artemisa", "Artemisa.Bahía Honda", "Artemisa.Bauta", "Artemisa.Caimito", "Artemisa.Candelaria",
"Artemisa.Guanajay", "Artemisa.Güira de Melena", "Artemisa.Mariel", "Artemisa.San Antonio de los Baños", "Artemisa.San Cristóbal",
"La Habana.Arroyo Naranjo", "La Habana.Boyeros", "La Habana.Centro Habana", "La Habana.Cerro","La Habana.Cotorro", "La Habana.Diez de Octubre",
"La Habana.Guanabacoa", "La Habana.La Habana del Este", "La Habana.La Habana Vieja", "La Habana.La Lisa", "La Habana.Marianao",
"La Habana.Playa", "La Habana.Plaza de la Revolución", "La Habana.Regla", "La Habana.San Miguel del Padrón",
"Mayabeque.Batabanó", "Mayabeque.Bejucal", "Mayabeque.Güines", "Mayabeque.Jaruco", "Mayabeque.Madruga", "Mayabeque.Melena del Sur", 
"Mayabeque.Nueva Paz", "Mayabeque.Quivicán", "Mayabeque.San José de las Lajas", "Mayabeque.San Nicolás", "Mayabeque.Santa Cruz del Norte"
"Matanzas.", "Matanzas.Calimete", "Matanzas.Cárdenas", "Matanzas.Ciénaga de Zapata", "Matanzas.Colón", "Matanzas.Jagüey Grande", "Matanzas.Jovellanos",
"Matanzas.Los Arabos", "Matanzas.Martí", "Matanzas.Matanzas", "Matanzas.Pedro Betancourt", "Matanzas.Perico", "Matanzas.Unión de Reyes"
"Cienfuegos.Abreus", "Cienfuegos.Aguada de Pasajeros", "Cienfuegos.Cienfuegos", "Cienfuegos.Cruces", "Cienfuegos.Cumanayagua", "Cienfuegos.Lajas", 
"Cienfuegos.Palmira", "Cienfuegos.Rodas"
"Villa Clara.Caibarién", "Villa Clara.Camajuaní", "Villa Clara.Cifuentes", "Villa Clara.Corralillo", "Villa Clara.Encrucijada",
"Villa Clara.Manicaragua", "Villa Clara.Placetas", "Villa Clara.Quemado de Güines",
"Villa Clara.Ranchuelo", "Villa Clara.San Juan de los Remedios", "Villa Clara.Sagua la Grande", "Villa Clara.Santa Clara", "Villa Clara.Santo Domingo",
"Sancti Spíritus.Cabaiguán", "Sancti Spíritus.Fomento", "Sancti Spíritus.Jatibonico", "Sancti Spíritus.La Sierpe", "Sancti Spíritus.Sancti Spíritus",
"Sancti Spíritus.Taguasco", "Sancti Spíritus.Trinidad", "Sancti Spíritus.Yaguajay",
"Ciego de Ávila.Baraguá", "Ciego de Ávila.Bolivia", "Ciego de Ávila.Chambas", "Ciego de Ávila.Ciego de Ávila", "Ciego de Ávila.Ciro Redondo", 
"Ciego de Ávila.Florencia", "Ciego de Ávila.Majagua", "Ciego de Ávila.Morón", "Ciego de Ávila.Primero de Enero", "Ciego de Ávila.Venezuela",
"Camagüey.Camagüey", "Camagüey.Carlos M. de Céspedes", "Camagüey.Esmeralda", "Camagüey.Florida", "Camagüey.Guáimaro", "Camagüey.Jimaguayú",
"Camagüey.Minas", "Camagüey.Najasa", "Camagüey.Nuevitas", "Camagüey.Santa Cruz del Sur", "Camagüey.Sibanicú", "Camagüey.Sierra de Cubitas", 
"Camagüey.Vertientes", "Las Tunas.Amancio", "Las Tunas.Colombia", "Las Tunas.Jesús Menéndez", "Las Tunas.Jobabo", "Las Tunas.Las Tunas", 
"Las Tunas.Majibacoa", "Las Tunas.Manatí", "Las Tunas.Puerto Padre"
"Holguín.Antilla", "Holguín.Báguanos", "Holguín.Banes", "Holguín.Cacocum", "Holguín.Calixto García", "Holguín.Cueto", "Holguín.Frank País",
"Holguín.Gibara", "Holguín.Gibara", "Holguín.Mayarí", "Holguín.Moa", "Holguín.Rafael Freyre", "Holguín.Sagua de Tánamo", "Holguín.Urbano Noris",
"Granma.Bartolomé Masó", "Granma.Bayamo", "Granma.Buey Arriba", "Granma.Campechuela", "Granma.Cauto Cristo", "Granma.Guisa", "Granma.Jiguaní",
"Granma.Manzanillo", "Granma.Media Luna", "Granma.Niquero", "Granma.Pilón", "Granma.Río Cauto", "Granma.Yara",
"Santiago de Cuba.Contramaestre", "Santiago de Cuba.Guamá", "Santiago de Cuba.Mella", "Santiago de Cuba.Palma Soriano", "Santiago de Cuba.San Luis",
"Santiago de Cuba.Santiago de Cuba", "Santiago de Cuba.Segundo Frente", "Santiago de Cuba.Songo-La Maya", "Santiago de Cuba.Tercer Frente",
"Guantánamo.Baracoa", "Guantánamo.Caimanera", "Guantánamo.El Salvador", "Guantánamo.Guantánamo", "Guantánamo.Imías", "Guantánamo.Maisí", 
"Guantánamo.Manuel Tames","Guantánamo.Niceto Pérez", "Guantánamo.San Antonio del Sur", "Guantánamo.Yateras", "Guantánamo.Isla de la Juventud"
]

OPERATORS = {'+': union, '-': difference}