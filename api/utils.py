import os
from re import S 
import regex as re


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




charge_all_parquets_from_folder("api/cell_area/")