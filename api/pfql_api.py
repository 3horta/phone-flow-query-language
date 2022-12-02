from typing import List
import pandas as pd
import datetime as dt

class Time:
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

def __towers_location_dataframes():
    """Returns towers location(s) dataframes."""
    municipality_df = pd.read_csv("data/municipios_tower.csv", engine="pyarrow")
    health_areas_df = pd.read_csv("data/as_havana_tower.csv", engine="pyarrow")
    return municipality_df, health_areas_df

def get_towers(location : str) -> List[str]: # returns towers list
    pass
    