"""
global_tables

This is a class to hold the standard tables that are needed across
the synthetic ecosystem generation process.

Tables currently include:

 - Total Population by geographic area
 - Total number of households per geographic area
 """
import pandas as pd
from converters.converter_base import ConverterBase

class GlobalTables:
    def __init__(self, geo_unit_ = None, converter_ = None,
                 data_ = pd.DataFrame()):
        """
        Creation operator
        """
        self.geo_unit = geo_unit_
        self.converter = converter_
        self.data = data_
        self.total_population_by_geo_df, self.total_household_by_geo_df = \
                                                        self.converter.convert()

    def validate(self):
        """
        This is a function that will validate the data for the Global Tables
        """

        return True
