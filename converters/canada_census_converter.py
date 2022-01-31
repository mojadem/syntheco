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

    def clean_data(self):
        return pd.DataFrame()


# class canada_summary_table_converter(ConverterBase):
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
