import pytest
import input_schema


@pytest.fixture
def schema_obj():
    return input_schema._schema


@pytest.fixture
def canada_input(tmp_path):
    return {
        "census_year": 2016,
        "census_converter": "canada",
        "census_high_res_geo_unit": 2,
        "census_low_res_geo_unit": 462,
        "census_fitting_vars": ["AGEGRP", "HDGREE", "HHSIZE", "TOTINC"],
        "census_input_files": {
            "profile_data_csv": tmp_path,
            "pums_h_csv": tmp_path,
        },
        "output_log_file": "canada.out.txt",
        "output_data_log_file": "canada_data_out.txt",
        "census_fitting_procedure": "ipf",
        "debug_limit_geo_codes": 10,
        "parallel_num_cores": 4,
    }


@pytest.fixture
def us_input():
    return {
        "census_year": 2020,
        "census_converter": "us",
        "census_high_res_geo_unit": "tract",
        "census_low_res_geo_unit": "state:10",
        "census_fitting_vars": ["AGEP", "SCHL", "MAR", "HINCP"],
        "output_log_file": "us.out.txt",
        "output_data_log_file": "us_data_out.txt",
        "census_fitting_procedure": "ipf",
        "debug_limit_geo_codes": 10,
        "parallel_num_cores": 4,
        "api_key": "e3061d8962ee2b9822717e18093c29337bca18df",
    }


def test_valid_canada_input(schema_obj, canada_input):
    assert schema_obj.is_valid(canada_input)


def test_invalid_canada_inputs(schema_obj, canada_input):
    for key in canada_input:
        # tests each key individually for invalid input
        invalid_input = canada_input.copy()
        invalid_input[key] = None
        assert not schema_obj.is_valid(invalid_input)


def test_invalid_census_input_files_canada_input(schema_obj, canada_input):
    canada_input["census_input_files"]["profile_data_csv"] = "not a file"
    assert not schema_obj.is_valid(canada_input)


def test_valid_us_input(schema_obj, us_input):
    assert schema_obj.is_valid(us_input)


def test_invalid_us_inputs(schema_obj, us_input):
    for key in us_input:
        invalid_input = us_input.copy()
        invalid_input[key] = None
        assert not schema_obj.is_valid(invalid_input)


def test_empty_input_is_invalid(schema_obj):
    assert not schema_obj.is_valid({})
