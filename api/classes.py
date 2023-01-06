import datetime as dt

class TimeInterval:
    def __init__(self, beginning_time : dt.datetime, ending_time : dt.datetime = None) -> None:
        """
        Initializes a time interval given datetimes limits.
        """
        self.interval = self.__time_difference(beginning_time, ending_time)
        self.time = len(self.interval)
    
    def __time_difference(self, start_time : dt.datetime, end_time : dt.datetime):
        """
        Fills time interval limits.
        """ 

        difference = end_time - start_time

        date_list = [(start_time + dt.timedelta(days=d)).strftime("%Y-%m-%d")
                        for d in range(difference.days + 1)] 

        return date_list