"""
input_schema

A module to house the input schema for the yaml input files
"""

import os
from schema import Schema, And, SchemaError

_schema = Schema({
    'census_year':And(int),
    'census_high_res_geo_unit': And(int), ## this needs work different
    'census_low_res_geo_unit': And(int), ## this needs to work different
    'census_converter':And(str,lambda x: os.path.exists(f'converters/{x}_converter.py'),
                                                        error = "Invalid census_converter"),
    'census_input_files': And(dict, {str: lambda x: os.path.exists(x)},
                              error = "Invald Census Input Files")
    })

"""
For Testing
"""
if __name__ == "__main__":
    test_dict = {'census_year':1994,
                 'census_converter':'canada_census',
                 'census_low_res_geo_unit':2,
                 'census_high_res_geo_unit':462,
                 'census_input_files':{'profile_data_csv':'file.csv',
                                       'pums_h': 'file.csv'}
                }
    try:
        assert _schema.validate(test_dict) == test_dict
    except SchemaError as e:
        print("Your Input does not conform to the schema")
        print(f"Error: {e}")
