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
        self.data = self.converter.convert()

    def validate(self):
        """
        This is a function that will validate the data for the Global Tables
        """

        return True

    def __str__(self):
        """
        This method returns a nice print out of the GlobalTables
        """

        return '\n'.join(["Global Tables",
                          "------------------------------------------------------"] +\
                          ["{}\n{}".format(x.name,x) for x in self.data.values()])
