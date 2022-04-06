"""
canada_census_conveters

This is the code for the Canada Census Plugins
"""


import pandas as pd
import numpy as np
import json
from census_converters import hookimpl


class CanadaCensusGlobalPlugin:
    """
    CanadaCensusGlobalPlugin

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


class CanadaCensusPUMSPlugin:
    """
    CanadaCensusPUMSConverter

    This class houses the implemented hooks for the canada census plugins
    for processed PUMS Tables
    """
    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst):
        """
        read_raw_data_into_pandas
        Actually reads in raw data in csv format and saves as pandas df
        Input: A csv file holding all PUMS data called pums_h_csv
        Output: a pandas data frame with raw PUMS data matched to user-specified low res geo units (CMA)
                called raw_data_df
        """
        pums_h_csv = cens_conv_inst.input_params.input_params['census_input_files']['pums_h_csv']
        low_res_geo = cens_conv_inst.input_params.input_params['census_low_res_geo_unit']
        pums_iter = pd.read_csv(pums_h_csv,
                                iterator=True,
                                dtype={'CMA': str},
                                chunksize=1000)

        raw_df = pd.concat([chunk[(chunk['CMA'].str.find(str(low_res_geo), 0) == 0)] for chunk in pums_iter])
        return raw_df

    @hookimpl
    def pre_transform_clean_data(cens_conv_inst):
        """
        pre_transform_clean_data
        Does data cleaning before the data is being transformed to the right format
        Input: pandas df with raw PUMS data called raw_data_df
        Output: pandas df with raw PUMS data which has been cleaned from missing values called processed_data_df
        """

        # Not relly anything to do before transformation of this data.
        proc_df = {"categorical_table": None,
                   "frequency_table": None}
        return proc_df

    @hookimpl
    def transform(cens_conv_inst):
        """
        transform
        Data transformations
        Input: Cleaned and pre-processed pandas df called processed_data_df
        Output: processed pums_freq_df as pandas df which is now in the correct format as a frequency table
        """
        try:
            fitting_variables = cens_conv_inst.input_params.input_params['census_fitting_vars']

            # draws from pums structure file for chosen variables and excludes pums type 'special'
            # (will be handled separately)
            processed_data_df = cens_conv_inst.raw_data_df[['HH_ID', 'EF_ID', 'CF_ID', 'PP_ID', 'CF_RP', 'WEIGHT'] +
                                                           [x for x in fitting_variables
                                                           if cens_conv_inst.metadata_json[x]['pums_type']
                                                           != "special"]]

            # selecting Census Family Rep 1 or 3 to represent household
            processed_data_df = processed_data_df[(processed_data_df['CF_RP'] != 2)]

            # removing NA
            processed_data_df = processed_data_df.dropna()

            # This is for pums continous and profile categorical
            for v in [x for x in fitting_variables if cens_conv_inst.metadata_json[x]['pums_type'] == "continuous"]:
                new_col_name = "{}_m".format(v)
                processed_data_df[new_col_name] = [np.NaN for x in range(0, processed_data_df.shape[0])]
                lookup = [(int(i), x['pums_inds'])
                          for i, x in cens_conv_inst.metadata_json[v]["common_var_map"].items()]
                for i, c in lookup:
                    upper = c[1]
                    lower = c[0]
                    processed_data_df.loc[(processed_data_df[v] >= lower) &
                                          (processed_data_df[v] <= upper), new_col_name] = i

            # This for pums categorical to profile categorical
            for v in [x for x in fitting_variables if cens_conv_inst.metadata_json[x]['pums_type'] == "categorical"]:
                new_col_name = "{}_m".format(v)
                processed_data_df[new_col_name] = [np.NaN for x in range(0, processed_data_df.shape[0])]
                lookup = cens_conv_inst.metadata_json[v]["common_var_map"]
                for i, c in lookup.items():
                    processed_data_df.loc[(processed_data_df[v]).isin(c["pums_inds"]), new_col_name] = i

            # Handle Special Variables that have functions associated with them
            special_vars = [x for x in fitting_variables if cens_conv_inst.metadata_json[x]['pums_type'] == "special"]
            for v in special_vars:
                processed_data_df = CanadaCensusPUMSPlugin._special(processed_data_df, v)

            # Renaming columns
            processed_data_df = processed_data_df.rename(columns={x: "{}_V".format(x) for x in fitting_variables})
            processed_data_df = processed_data_df.rename(columns={"{}_m".format(x): x for x in fitting_variables})

            # another round of removing NA
            processed_data_df = processed_data_df.dropna()

            # create freq table by grouping by fitting var and calculating totals
            processed_data_df = processed_data_df.reset_index()
            pums_freq_df = processed_data_df.groupby(fitting_variables).size()
            pums_freq_df = pums_freq_df.reset_index() \
                                       .rename(columns={0: "total"})

            pums_freq_df['total'] = pums_freq_df['total'].astype(np.float64)

            pums_freq_df = pums_freq_df.astype({x: "int64" for x in fitting_variables})

            return {"categorical_table": processed_data_df,
                    "frequency_table": pums_freq_df}
        except Exception as e:
            print("There was an error: {}".format(str(e)))

    @hookimpl
    def post_transform_clean_data(cens_conv_inst):
        """
        This function performs post transformation cleaning on the data.

        Returns:
            The function updates the instance member processed_data_df
        """
        cat_df = cens_conv_inst.processed_data_df['categorical_table']
        cat_df.name = "PUMS Data Categorical Representation"

        freq_df = cens_conv_inst.processed_data_df['frequency_table']
        freq_df.name = "PUMS Data Frequency Representation"

        return {"categorical_table": cat_df,
                "frequency_table": freq_df}

    # Special function for dealing with HHSIZE variables
    @staticmethod
    def _special(pums_df, var):
        """
        static private function that will hold the special function that need
        to be used for certain census variables that require additional processing
        """
        if var == "HHSIZE":
            new_col_name = "{}_m".format(var)
            hhsize = pums_df.groupby('HH_ID').size()
            pums_df['HHSIZE_m'] = pums_df.apply(lambda row: hhsize[row['HH_ID']]
                                                if hhsize[row['HH_ID']] <= 5
                                                else 5, axis=1)
        else:
            print("Variable {} is not a special variable")
            raise

        return pums_df


'''
class canada_census_summary_converter:

    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst):
        print("Hell Yeah")
        return pd.DataFrame([1,1,1])
'''
