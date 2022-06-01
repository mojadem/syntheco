"""
input_schema

A module to house the input schema for the yaml input files
"""

import os
from schema import Schema, And, Or, Optional, SchemaError

_schema = Schema(
    {
        "census_year": int,
        "census_high_res_geo_unit": Or(str, int),
        "census_low_res_geo_unit": Or(str, int),
        "census_converter": And(
            str,
            lambda x: os.path.exists(
                f"census_converters/plugins/{x}_census_converters.py"
            ),
            error="Invalid Census Converter",
        ),
        Optional("census_input_files"): And(
            dict, {str: lambda x: os.path.exists(x)}, error="Invald Census Input Files"
        ),
        "census_fitting_vars": list[str],
        Optional("api_key"): str,
        Optional("output_log_file", default="sytheco_out.txt"): str,
        Optional("output_data_log_file", default="syntheco_data_out.txt"): str,
        "census_fitting_procedure": str,
        Optional("ipf_max_iterations", default=10000): int,
        Optional("ipf_fail_on_nonconvergence", default=False): bool,
        Optional("ipf_convergence_rate", default=1.0e-5): float,
        Optional("ipf_rate_tolerance", default=1.0e-8): float,
        Optional("debug_limit_geo_codes"): int,
        Optional("parallel_num_cores", default=1): int,
    }
)

"""
For Testing
"""
if __name__ == "__main__":
    test_dict = {
        "census_year": 1994,
        "census_converter": "canada",
        "census_low_res_geo_unit": 2,
        "census_high_res_geo_unit": 462,
        "census_fitting_vars": ["AGEGRP", "HDGREE", "HHSIZE", "TOTINC"],
        "census_fitting_procedure": "ipf",
    }
    try:
        assert _schema.is_valid(test_dict)
    except SchemaError as e:
        print("Your Input does not conform to the schema")
        print(f"Error: {e}")
