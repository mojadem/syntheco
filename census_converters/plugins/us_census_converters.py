"""
us_census_converter

This is the code for the US Census Plugins
"""


import pandas as pd
import numpy as np
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
        # TODO: implement input file compatability

        url = 'https://api.census.gov/data/2020/dec/pl'
        params = {'get': 'P1_001N,H1_001N', 'for': 'tract:*', 'in': 'state:10 county:*'} # initial testing data

        response = requests.get(url, params)
        raw_df = pd.read_json(response.url) # TODO: add chunks

        raw_df, raw_df.columns = raw_df[1:], raw_df.iloc[0] # adjust first row to header row

        return raw_df
    
    @hookimpl
    def transform(cens_conv_inst: CensusConverter):
        """
        transform

        This function actually transforms the raw data from the US census profile
        and derives the dataframes for the total population and total number_households

        Returns:
            An updated dataframe to be set to processed_data_df
        """
        # total population table
        pop_df = cens_conv_inst.raw_data_df.copy() \
            .drop(columns=[column for column in pop_df.columns if column != "P1_001N"]) \
                .rename(columns={'P1_001N': 'total'})
                
        # total number of households table
        nh_df = cens_conv_inst.raw_data_df.copy() \
            .drop(columns=[column for column in pop_df.columns if column != "H1_001N"]) \
                .rename(columns={'H1_001N': 'total'})
        
        pop_df.name = "Total Population by High Resolution Geo Unit"
        nh_df.name = "Number of Households by High Resolution Geo Unit"

        return {"total_population_by_geo": pop_df, "number_households_by_geo": nh_df}


class USCensusPUMSPlugin:
    """
    USCensusPUMSPlugin

    This class houses the implemented hooks for the US census plugins
    for processed PUMS Tables
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
        url = 'https://api.census.gov/data/2020/acs/acs5/pums'
        pums_vars = ['AGEP', 'SCHL', 'MAR', 'NP', 'HINCP']
        params = {'get': ','.join(pums_vars), 'for': 'state:10'} # initial testing data

        response = requests.get(url, params)
        raw_df = pd.read_json(response.url) # TODO: add chunks

        raw_df, raw_df.columns = raw_df[1:], raw_df.iloc[0] # adjust first row to header row

        return raw_df
    
    @hookimpl
    def transform(cens_conv_inst):
        """
        transform
        Data transformations
        Input: Cleaned and pre-processed pandas df called processed_data_df
        Output: processed pums_freq_df as pandas df which is now in the correct format as a frequency table
        """
        pums_vars = cens_conv_inst.input_params.input_params['census_fitting_vars']
        pums_ds = cens_conv_inst.metadata_json
        proc_df = cens_conv_inst.raw_data_df.astype(int)

        for v in pums_vars:
            new_col_name = f"{v}_m"
            proc_df[new_col_name] = [np.NaN for _ in range(proc_df.shape[0])]
            
            lookup = [(int(i), c['pums_inds']) for i, c in pums_ds[v]['common_var_map'].items()]
            for i, c in lookup:
                if len(c) == 1: # for single indices
                    proc_df.loc[proc_df[v] == c[0], new_col_name] = i
                else: # for index ranges
                    lower = int(c[0])
                    upper = int(c[1])
                    proc_df.loc[(proc_df[v] >= lower) & (proc_df[v] <= upper), new_col_name] = i

        proc_df = proc_df \
            .rename(columns={x: f"{x}_V" for x in pums_vars}) \
                .rename(columns={f"{x}_m": x for x in pums_vars})

        freq_df = proc_df\
            .groupby(pums_vars).size() \
                .reset_index() \
                    .rename(columns={0: 'total'})

        freq_df['total'] = freq_df['total'].astype(np.float64)
        freq_df = freq_df.astype({v: np.int64 for v in pums_vars})

        proc_df.name = "PUMS Data Categorical Representation"
        freq_df.name = "PUMS Data Frequency Representation"
        
        return {"categorical_table": proc_df, "frequency_table": freq_df}
