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


    def __init__(self, input_):
        """
        Constructor

        Arguments:
           input_params - the validated input params dictionary

        Returns:
            an instantiated ConverterBase

        NOTE: This is not meant to be called explicitly
        """
        #self.input_file_dict = {"census_summary_data": None}
        self.input_params = input_
        self.raw_data_df = pd.DataFrame()
        self.processed_data_df = pd.DataFrame()


    def _read_raw_data_into_pandas(self):
        """
        TODO: Make this real error handling
        """


        #print("This is being called from the base class")
        raise NotImplementedError(f"Calling _read_raw_data_into_pandas from the {__name__} base class")

        #return pd.DataFrame()

    def pre_transform_clean_data(self):
        '''
        TODO: Make this real error handling
        '''


        raise NotImplementedError(f"Calling pre_transform_clean_data from the {__name__} base class")
        #return False

    def post_transform_clean_data(self):
        '''
        TODO: Make this real error handling
        '''


        raise NotImplementedError(f"Calling post_transform_clean_data from the {__name__} base class")

    def transform(self):
        '''
        TODO: Make this real error handling
        '''

        raise NotImplementedError(f"Calling transform from the {__name__} base class")
        #return False

    def extract_variable_data(self, variable_):
        '''
        TODO: Make this real error handling
        '''


        raise NotImplementedError(f"Calling extract_variable_data from the {__name__} base class")

    def convert(self):
        '''
        This method performs the conversion from raw data to
        the final table for core Syntheco use


        Returns:
            A data with processed data, the format and tables
            that are in this data are dependent on the converter and
            type of table that is to be the result. Please refer to
            the concrete converter class for details in the "converters"
            directory
        '''


        self.pre_transform_clean_data()
        self.transform()
        self.post_transform_clean_data()
        return self.processed_data_df
