"""
input_schema

A module to house the input schema for the yaml input files
"""

import os
from schema import Schema, And, Or, Optional, SchemaError

_schema_specs = {
    "common": {
        "census_year": int,
        "census_converter": And(str, Or("us", "canada")),
        "census_fitting_vars": list[str],
        "census_fitting_procedure": str,
        Optional("output_log_file", default="sytheco_out.txt"): str,
        Optional("output_data_log_file", default="syntheco_data_out.txt"): str,
        Optional("ipf_max_iterations", default=10000): int,
        Optional("ipf_fail_on_nonconvergence", default=False): bool,
        Optional("ipf_convergence_rate", default=1.0e-5): float,
        Optional("ipf_rate_tolerance", default=1.0e-8): float,
        Optional("debug_limit_geo_codes"): int,
        Optional("parallel_num_cores", default=1): int,
    },
    "us": {
        "api_key": str,
        "census_high_res_geo_unit": str,
        "census_low_res_geo_unit": str,
    },
    "canada": {
        "census_input_files": And(dict, {str: os.path.exists}),
        "census_high_res_geo_unit": int,
        "census_low_res_geo_unit": int,
    },
}


class SynthEcoSchema(Schema):
    def validate(self, data: dict):
        # validate common data
        validated_data: dict = self._validate_common_schema(data)

        # validate converter specific data
        if data["census_converter"] == "us":
            validated_data |= self._validate_us_schema(data)
        elif data["census_converter"] == "canada":
            validated_data |= self._validate_canada_schema(data)

        if any(key not in validated_data.keys() for key in data.keys()):
            unused_keys = [
                key for key in data.keys() if key not in validated_data.keys()
            ]
            print(
                f"The following input keys are not used by the schema: {unused_keys}",
            )

        return validated_data

    def _validate_common_schema(self, data: dict):
        return Schema(_schema_specs["common"], ignore_extra_keys=True).validate(data)

    def _validate_us_schema(self, data: dict):
        return Schema(_schema_specs["us"], ignore_extra_keys=True).validate(data)

    def _validate_canada_schema(self, data: dict):
        return Schema(_schema_specs["canada"], ignore_extra_keys=True).validate(data)


_schema = SynthEcoSchema({})  # initialized with empty schema


"""
For Testing
"""
if __name__ == "__main__":
    test_dict = {
        "census_year": 2016,
        "census_converter": "canada",
        "census_high_res_geo_unit": 1,
        "census_low_res_geo_unit": 1,
        "census_fitting_vars": ["AGEGRP", "HDGREE", "HHSIZE", "TOTINC"],
        "census_input_files": {
            "profile_data_csv": "/home/mojadem/PHDL/SynthEco/data/98-401-X2016043_English_CSV_data.csv",
            "pums_h_csv": "/home/mojadem/PHDL/SynthEco/data/pumf-98M0002-E-2016-hierarchical_F1.csv",
        },
        "output_log_file": "canada.out.txt",
        "output_data_log_file": "canada_data_out.txt",
        "census_fitting_procedure": "ipf",
        "debug_limit_geo_codes": 10,
        "parallel_num_cores": 4,
        "useless_key": 1,
    }
    try:
        data = _schema.validate(test_dict)
        print(data)
    except SchemaError as e:
        print("Your Input does not conform to the schema")
        print(f"Error: {e}")
