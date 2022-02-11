"""
hookspec

This module holds the specification for the plugins
and defines which members need to be implemented
"""


import pluggy
from census_converters import hookspec


class CensusConverterSpec:
    @hookspec
    def read_raw_data_into_pandas(cens_conv_inst):
        """
        read in the raw data into a pandas DataFrame

        This is templated with a keyword that is requered to be
        self when passed in through the class
        """
        return

    @hookspec
    def pre_transform_clean_data(cens_conv_inst):
        """
        pre_transform_clean_data:

        Required function that does any cleaning of the data that needs to be done
        prior to the transformation step.

        This is templated with a keyword that is requered to be
        self when passed in through the class
        """
        return

    @hookspec
    def post_transform_clean_data(cens_conv_inst):
        """
        This function performs post transformation cleaning on the data.

        This is templated with a keyword that is requered to be
        self when passed in through the class
        """
        return

    @hookspec
    def transform(cens_conv_inst):
        """
        transform

        This function actually transforms the raw data.

        This is templated with a keyword that is requered to be
        self when passed in through the class
        """
        return
