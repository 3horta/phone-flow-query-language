from typing import List, Tuple
import pandas as pd
import datetime as dt

LOCATIONS = {}


#region classes

class TimeInterval:
    def __init__(self, beginning_time : dt.datetime, ending_time : dt.datetime = None) -> None:
        """
        Initializes a time interval given datetimes limits.
        """
        self.interval = self.__fill_interval(beginning_time, ending_time)
        self.time = self.interval.length
    
    def __fill_interval(self, beginning_time : dt.datetime, ending_time : dt.datetime):
        """
        Fills time interval limits.
        """
        if ending_time != None:
            interval = pd.Interval(pd.Timestamp(beginning_time), pd.Timestamp(ending_time), closed = 'both')
        else :
            ending_time = dt.datetime(beginning_time.year, beginning_time.month, beginning_time.day) + dt.timedelta(days = 1)
            interval = pd.Interval(pd.Timestamp(beginning_time), pd.Timestamp(ending_time), closed = 'left')
        return interval

#endregion


def __towers_location_dataframes() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Returns towers location(s) dataframes."""
    municipality_df = pd.read_csv("data/municipios_tower.csv", engine="pyarrow")
    health_areas_df = pd.read_csv("data/as_havana_tower.csv", engine="pyarrow")
    return municipality_df, health_areas_df

def get_towers(location : str) -> List[str]:
    """
    Returns towers list.
    """
    pass

def get_collection(collection_name : str) -> List[str]: #need to define how to load collections from language
    """
    Returns collection.
    """
    pass

def filter(set, *filters):
    """
    Returns a new set filtered by filters.
    """
    pass


#region set operations

def union(A, B):
    """
    Returns two sets union.
    """
    pass

def intersection(A, B):
    """
    Returns two sets intersections.
    """
    pass

def difference(A, B):
    """
    Returns two sets difference. (parameters order)
    """
    pass

#endregion


