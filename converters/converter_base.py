"""
converter_base

This is a base class to define convertors for
taking raw census data and converting it to syntheco tables,

This is mostly here to stub out the class and ensure that the members that
need to be implemented are, as well as define functions that are global to
all converters.

"""
import pandas as pd


class ConverterBase:
    """
    Converter Base Class
    This class defines the members required for a Syntheco Converter
    """


    def __init__(self, input_params):
        """
        Constructor

        Arguments:
           input_params - the validated input params dictionary

        Returns:
            an instantiated ConverterBase

        NOTE: This is not meant to be called explicitly
        """
        #self.input_file_dict = {"census_summary_data": None}
        self.raw_data_df = pd.DataFrame()
        self.clean_data_df = pd.DataFrame()
        print("This is the base class and should not be directly called")
        return

    def _read_raw_data_into_pandas(self):
        """
        TODO: Make this real error handling
        """
        print("This is being called from the base class")
        return pd.DataFrame()

    def clean_data(self):
        '''
        TODO: Make this real error handling
        '''
        print("This is being called from the base class")
        return

    def transform(self):
        '''
        TODO: Make this real error handling
        '''
        print("This is being called from the base class")
        return

    def extract_variable_data(self, variable_):
        print("This is being called from the base class")
        return

    def convert(self):
        clean_df = self.clean_data()
        return self.transform()
