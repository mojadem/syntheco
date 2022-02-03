from converters.converter_base import ConverterBase
import pandas as pd

_required_files = ['profile_data_csv']

class canada_global_table_converter(ConverterBase):
    def __init__(self, input_):
        self.census_year = input_.input_params['census_year']
        self.cma = input_.input_params['census_low_res_geo_unit']
        self.geo_level = input_.input_params['census_high_res_geo_unit']
        self.profile_data_csv = input_.input_params['census_input_files']['profile_data_csv']
        self.raw_data_df = self._read_raw_data_into_pandas()
        super(ConverterBase,self).__init__()

    def _read_raw_data_into_pandas(self):
        """
        _read_raw_data_into_pandas
        Private member that reads defines how the raw data is read into pandas
        data frame for the conversion
        """
        prof_iter = pd.read_csv(self.profile_data_csv,
                                 iterator = True,
                                 dtype = {'GEO_CODE (POR)':str,
                                          'DIM: Profile of Census Tracts (2247)':str},
                                 chunksize = 1000)
        return pd.concat([chunk[(chunk['GEO_CODE (POR)'].str.find(str(self.cma),0) == 0)\
                              & (chunk['GEO_LEVEL'] == self.geo_level) \
                              & (chunk['CENSUS_YEAR'] == self.census_year)]
                          for chunk in prof_iter])

    def pre_transform_clean_data(self):
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

        self.processed_data_df =  {"total_population_by_geo":self.raw_data_df.copy(),
                                   "number_households_by_geo":self.raw_data_df.copy()}
        return True

    def transform(self):
        """
        transform

        This function actually transforms the raw data from the canadian census profile
        and derives the dataframes for the total population and total number_households

        Returns:
            The function updates the instance member processed_data_df
        """


        ### total population table
        pop_df = self.processed_data_df['total_population_by_geo']
        pop_df = pop_df[(pop_df['DIM: Profile of Census Tracts (2247)'] == "Population, 2016")]
        pop_df = pop_df.drop(columns= ["DIM: Profile of Census Tracts (2247)",
                                       "Dim: Sex (3): Member ID: [2]: Male",
                                       "Dim: Sex (3): Member ID: [3]: Female",
                                       "GNR","GNR_LF","ALT_GEO_CODE",
                                       "Member ID: Profile of Census Tracts (2247)",
                                       "DATA_QUALITY_FLAG"])
        pop_df = pop_df\
                 .rename(columns = {'Dim: Sex (3): Member ID: [1]: Total - Sex':'total',
                                    'GEO_CODE (POR)':'GEO_CODE'})
        pop_df.name = "Total Population by High Resolution Geo Unit"

        self.processed_data_df['total_population_by_geo'] = pop_df
        ### total number of houshold data
        nh_df = self.processed_data_df['number_households_by_geo']
        nh_df = nh_df[(nh_df['DIM: Profile of Census Tracts (2247)'] == "Total private dwellings")]
        nh_df = nh_df.drop(columns= ["DIM: Profile of Census Tracts (2247)",
                                     "Dim: Sex (3): Member ID: [2]: Male",
                                     "Dim: Sex (3): Member ID: [3]: Female",
                                     "GNR","GNR_LF","ALT_GEO_CODE",
                                     "Member ID: Profile of Census Tracts (2247)",
                                     "DATA_QUALITY_FLAG"])
        nh_df = nh_df\
                 .rename(columns = {'Dim: Sex (3): Member ID: [1]: Total - Sex':'total',
                                    'GEO_CODE (POR)':'GEO_CODE'})
        nh_df.name = "Number of Households by High Resolution Geo Unit"
        
        self.processed_data_df['number_households_by_geo'] = nh_df
        return True

    def post_transform_clean_data(self):
        """
        This function performs post transformation cleaning on the data.

        Returns:
            The function updates the instance member processed_data_df
        """


        pop_df = self.processed_data_df['total_population_by_geo']
        nh_df = self.processed_data_df['number_households_by_geo']

        ### Fix .. values, for now convert them to zero
        pop_df['total'] = pd.to_numeric(pop_df['total'],downcast='float',errors='coerce').fillna(0.0)
        pop_df = pop_df.set_index('GEO_CODE')

        ### Fix .. values, for now convert them to zero
        nh_df['total'] = pd.to_numeric(nh_df['total'],downcast='float',errors='coerce').fillna(0.0)
        nh_df = nh_df.set_index('GEO_CODE')

        return True

#     def __init__(self, input_params):
#         super(ConverterBase,self).__init__()
#         self.input_file_dict = {"profile_data_csv": input_params['census_profile_data_csv']}
#         self.census_year = input_params['census_year']
#         self.cma = input_params['cma']
#         self.geo_level = input_params['geo_level']
#         self.fitting_variables = input_params['fitting_vars']
#         self.raw_data_df = _read_raw_data_into_pandas()
#
#
#     def _read_raw_data_into_pandas(self):
#         """
#         _read_raw_data_into_pandas
#         Private member that reads defines how the raw data is read into pandas
#         data frame for the conversion
#         """
#
#         prof_inter = pd.read_csv(self.input_file_dict['profile_data_csv'],
#                                  iterator = True,
#                                  dtype = {'GEO_CODE (POR)':str,
#                                           'DIM: Profile of Census Tracts (2247)':str},
#                                  chunksize = 1000)
#         return pd.concat([chunk[(chunk['GEO_CODE (POR)'].str.find(cma,0) == 0)\
#                               & (chunk['GEO_LEVEL'] == 2) \
#                               & (chunk['CENSUS_YEAR'] == 2016)]
#                           for chunk in prof_iter])

if __name__ == "__main__":
    import sys
    sys.path.append("..")

    from global_tables import GlobalTables
    from input import InputParams



    ip = InputParams('canada.yaml')
    print(ip)
    can_converter = canada_global_table_converter(ip)

    gt = GlobalTables(geo_unit_ = 2,
                      converter_= can_converter)
