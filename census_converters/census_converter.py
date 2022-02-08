"""
census_converter

This module houses the skeleton class and initialization functions for
the main plugin framework for census conversion

TODO: Add Debug and Verbosity
TODO: Add error handling and variable checking
"""

import importlib
import pluggy
import pandas as pd

from census_converters.hookspec import hookspec, CensusConverterSpec

# Map dictionary used to translate input commands to the modules needed
# needed to import
plugin_map = {'canada': {'module': "census_converters.plugins.canada_census_converters",
                         'global': "CanadaCensusGlobalPlugin",
                         'summary': "canada_census_summary_converter"},
              'us': {'module': 'census_converters.plugins.us_census_converters',
                     'global': 'us_census_global_converter',
                     'summary': 'us_census_summary_converter'}}


def initialize(census_converter="canada", table_type="global"):
    """
    initialize

    This funciton initializes and registers the plugin for the skeleton
    class

    Arguments:
        - census_converter (str): defines the top level of the plugin map
        - table_type (str): defines the type of table for the plugin

    Returns:
        A pluggy plugin manager that has been registered with the proper
        implementation plugin.
    """

    plug_manager = pluggy.PluginManager("census_converters")
    plug_manager.add_hookspecs(CensusConverterSpec)
    plug_map_entry = plugin_map[census_converter]
    mod = importlib.import_module(plug_map_entry['module'])
    plug = getattr(mod, plug_map_entry[table_type])
    plug_manager.register(plug)
    return plug_manager


class CensusConverter:
    """
    CensusConveter

    Skeleton class for the plugin framework for conveting census
    data to Syntheco data standards
    """
    def __init__(self, input_params, census_conveter, table_type):
        """
        Constructor

        Arguments:
            - input_params (InputParams): the input paramters for Syntheco
            - census_conveter (str): defines which set of converters needed
            - table_type (str): defines the type of table needed (e.g. global)

        Returns:
            An instance of CensusConverter
        """
        self.input_params = input_params
        plug_manager = initialize(census_conveter, table_type)
        self.hook = plug_manager.hook
        self.raw_data_df = self.read_raw_data_into_pandas()
        self.processed_data_df = pd.DataFrame()

    def read_raw_data_into_pandas(self):
        """
        read_raw_data_into_pandas

        Stub function that wraps to the plugin specific implementation
        """
        return self.hook.read_raw_data_into_pandas(cens_conv_inst=self)[0]

    def pre_transform_clean_data(self):
        """
        pre_transform_clean_data

        Stub function that wraps to the plugin specific implementation
        """
        return self.hook.pre_transform_clean_data(cens_conv_inst=self)[0]

    def transform(self):
        """
        transform

        Stub function that wraps to the plugin specific implementation
        """
        return self.hook.transform(cens_conv_inst=self)[0]

    def post_transform_clean_data(self):
        """
        post_transform_clean_data

        Stub function that wraps to the plugin specific implementation
        """
        return self.hook.post_transform_clean_data(cens_conv_inst=self)[0]

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
        self.processed_data_df = self.pre_transform_clean_data()
        self.processed_data_df = self.transform()
        self.processed_data_df = self.post_transform_clean_data()
        return self.processed_data_df
