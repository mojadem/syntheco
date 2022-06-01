"""
input_schema

A module to house the input schema for the yaml input files
"""

import os
from schema import Schema, And, Or, Optional, Use, SchemaError


_common_schema = {
    "census_year": int,
    "census_high_res_geo_unit": Or(str, int),
    "census_low_res_geo_unit": Or(str, int),
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
}

_us_schema = {"api_key": str}

_canada_schema = {
    "census_input_files": And(
        dict, {str: Use(os.path.exists)}, error="Invald Census Input Files"
    )
}


class SynthEcoSchema(Schema):
    def validate(self, data: dict, _is_syntheco_schema=True):
        # validate common data
        validated_data = super().validate(data, _is_syntheco_schema=False)

        # validate census converter specific data
        if _is_syntheco_schema:  # dont execute this at recursive super calls
            if data["census_converter"] == "us":
                validated_data |= self._validate_us_inputs(data)
            elif data["census_converter"] == "canada":
                validated_data |= self._validate_canada_inputs(data)

        return validated_data

    def _validate_us_inputs(self, data: dict):
        return Schema(_us_schema, ignore_extra_keys=True).validate(data)

    def _validate_canada_inputs(self, data: dict):
        return Schema(_canada_schema, ignore_extra_keys=True).validate(data)


_schema = SynthEcoSchema(_common_schema, ignore_extra_keys=True)


"""
For Testing
"""
if __name__ == "__main__":
    test_dict = {
        "census_year": 2016,
        "census_converter": "canada",
        "census_high_res_geo_unit": 2,
        "census_low_res_geo_unit": 462,
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
    }
    try:
        d = _schema.validate(test_dict)
    except SchemaError as e:
        print("Your Input does not conform to the schema")
        print(f"Error: {e}")
