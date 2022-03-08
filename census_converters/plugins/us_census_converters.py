"""
us_census_converter

This is the code for the US Census Plugins
"""


import pandas as pd
import requests
from census_converters import hookimpl
from census_converters.census_converter import CensusConverter


class USCensusGlobalPlugin:
    """
    us_census_global_converter

    This is class that houses the implemented hooks for the us census plugins
    for global tables
    """
    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst):
        """
        _read_raw_data_into_pandas
        Private member that defines how the raw data is read into pandas
        data frame for the conversion

        Returns:
            returns the raw data table from the us census data
        """
        #TODO: implement input file compatability

        url = 'https://api.census.gov/data/2020/dec/pl'
        params = {'get': 'P1_001N,H1_001N', 'for': 'tract:*', 'in': 'state:10 county:*'} # initial testing data

        response = requests.get(url, params)
        raw_df = pd.read_json(response.url)

        raw_df, raw_df.columns = raw_df[1:], raw_df.iloc[0] # adjust first row to header row

        return raw_df

    @hookimpl
    def pre_transform_clean_data(cens_conv_inst: CensusConverter):
        """
        pre_transform_clean_data:

        Required function that does any cleaning of the data that needs to be done
        prior to the transformation step.

        This sets the stage to return a dictionary with two dataframes, one for the
        total population and one for the number of households per the high resolution
        geographic unit of interest.

        Returns:
            The function updates the instance member processed_data_df
        """
        proc_df = {"total_population_by_geo": cens_conv_inst.raw_data_df.copy(),
                   "number_households_by_geo": cens_conv_inst.raw_data_df.copy()}
        return proc_df
    
    @hookimpl
    def transform(cens_conv_inst: CensusConverter):
        """
        transform

        This function actually transforms the raw data from the us census data
        and derives the dataframes for the total population and total number_households

        Returns:
            An updated dataframe to be set to processed_data_df
        """
        # total population table
        pop_df = cens_conv_inst.processed_data_df['total_population_by_geo']
        pop_df = pop_df.drop(columns=[column for column in pop_df.columns if column != "P1_001N"])
        pop_df = pop_df.rename(columns={'P1_001N': 'total'})
        cens_conv_inst.processed_data_df['total_population_by_geo'] = pop_df
        
        # total number of households table
        nh_df = cens_conv_inst.processed_data_df['number_households_by_geo']
        nh_df = nh_df.drop(columns=[column for column in pop_df.columns if column != "H1_001N"])
        nh_df = nh_df.rename(columns={'H1_001N': 'total'})
        cens_conv_inst.processed_data_df['number_households_by_geo'] = nh_df
        
        proc_df = {"total_population_by_geo": pop_df, 
                   "number_households_by_geo": nh_df}
        return proc_df
    
    @hookimpl
    def post_transform_clean_data(cens_conv_inst):
        """
        This function performs post transformation cleaning on the data.

        Returns:
            The function updates the instance member processed_data_df
        """

        print(f"type: {cens_conv_inst.processed_data_df}")
        pop_df = cens_conv_inst.processed_data_df['total_population_by_geo']
        nh_df = cens_conv_inst.processed_data_df['number_households_by_geo']

        pop_df.name = "Total Population by High Resolution Geo Unit"
        nh_df.name = "Number of Households by High Resolution Geo Unit"

        proc_df = {"total_population_by_geo": pop_df, 
                   "number_households_by_geo": nh_df}
        return proc_df