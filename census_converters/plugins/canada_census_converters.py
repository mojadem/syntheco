"""
canada_census_conveters

This is the code for the Canada Census Plugins
"""


import pandas as pd
from census_converters import hookimpl


class CanadaCensusGlobalPlugin:
    """
    canada_census_global_conveter

    This is class that houses the implemented hooks for the canada census plugins
    for global Tables
    """
    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst):
        """
        _read_raw_data_into_pandas
        Private member that reads defines how the raw data is read into pandas
        data frame for the conversion

        Returns:
            returns the raw data table from the canadian census profile data
        """
        census_year = cens_conv_inst.input_params.input_params['census_year']
        low_res_geo = cens_conv_inst.input_params.input_params['census_low_res_geo_unit']
        high_res_geo = cens_conv_inst.input_params.input_params['census_high_res_geo_unit']
        profile_data_csv = cens_conv_inst.input_params.input_params['census_input_files']['profile_data_csv']

        prof_iter = pd.read_csv(profile_data_csv,
                                iterator=True,
                                dtype={'GEO_CODE (POR)': str,
                                       'DIM: Profile of Census Tracts (2247)': str},
                                chunksize=1000)
        raw_df = pd.concat([chunk[(chunk['GEO_CODE (POR)'].str.find(str(low_res_geo), 0) == 0)
                                  & (chunk['GEO_LEVEL'] == high_res_geo)
                                  & (chunk['CENSUS_YEAR'] == census_year)]
                            for chunk in prof_iter])
        return raw_df

    @hookimpl
    def pre_transform_clean_data(cens_conv_inst):
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
    def transform(cens_conv_inst):
        """
        transform

        This function actually transforms the raw data from the canadian census profile
        and derives the dataframes for the total population and total number_households

        Returns:
            An updated dataframe to be set to processed_data_df
        """
        # total population table
        pop_df = cens_conv_inst.processed_data_df['total_population_by_geo']
        pop_df = pop_df[(pop_df['DIM: Profile of Census Tracts (2247)'] == "Population, 2016")]
        pop_df = pop_df.drop(columns=["DIM: Profile of Census Tracts (2247)",
                                      "Dim: Sex (3): Member ID: [2]: Male",
                                      "Dim: Sex (3): Member ID: [3]: Female",
                                      "GNR", "GNR_LF", "ALT_GEO_CODE",
                                      "Member ID: Profile of Census Tracts (2247)",
                                      "DATA_QUALITY_FLAG"])
        pop_df = pop_df.rename(columns={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'total',
                                        'GEO_CODE (POR)': 'GEO_CODE'})
        cens_conv_inst.processed_data_df['total_population_by_geo'] = pop_df
        # total number of houshold data
        nh_df = cens_conv_inst.processed_data_df['number_households_by_geo']
        nh_df = nh_df[(nh_df['DIM: Profile of Census Tracts (2247)'] == "Total private dwellings")]
        nh_df = nh_df.drop(columns=["DIM: Profile of Census Tracts (2247)",
                                    "Dim: Sex (3): Member ID: [2]: Male",
                                    "Dim: Sex (3): Member ID: [3]: Female",
                                    "GNR", "GNR_LF", "ALT_GEO_CODE",
                                    "Member ID: Profile of Census Tracts (2247)",
                                    "DATA_QUALITY_FLAG"])
        nh_df = nh_df.rename(columns={'Dim: Sex (3): Member ID: [1]: Total - Sex': 'total',
                                      'GEO_CODE (POR)': 'GEO_CODE'})

        return {"total_population_by_geo": pop_df,
                "number_households_by_geo": nh_df}

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

        # Fix .. values, for now convert them to zero
        pop_df['total'] = pd.to_numeric(pop_df['total'], downcast='float', errors='coerce').fillna(0.0)
        pop_df = pop_df.set_index('GEO_CODE')
        pop_df.name = "Total Population by High Resolution Geo Unit"

        # Fix .. values, for now convert them to zero
        nh_df['total'] = pd.to_numeric(nh_df['total'], downcast='float', errors='coerce').fillna(0.0)
        nh_df = nh_df.set_index('GEO_CODE')
        nh_df.name = "Number of Households by High Resolution Geo Unit"

        return {"total_population_by_geo": pop_df,
                "number_housholds_by_geo": nh_df}


'''
class canada_census_summary_converter:

    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst):
        print("Hell Yeah")
        return pd.DataFrame([1,1,1])
'''
