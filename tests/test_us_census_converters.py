import pandas as pd
import pytest
from unittest.mock import MagicMock
import responses
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException

import census_converters.plugins.us_census_converters as us


class TestAPIManager:
    @pytest.fixture
    def api_manager(self):
        with us.api_manager.session.cache_disabled():
            yield us.api_manager

    @responses.activate
    def test_successful_call(self, api_manager):
        expected_data = {"success": "valid"}
        responses.get(api_manager.base_url, json=expected_data, status=200)

        data = api_manager.api_call()
        assert data == expected_data

    @responses.activate
    def test_httperror(self, api_manager):
        responses.get(api_manager.base_url, body=HTTPError("mock HTTPError"))

        with pytest.raises(HTTPError):
            api_manager.api_call()

    @responses.activate
    def test_timeout(self, api_manager):
        responses.get(api_manager.base_url, body=Timeout("mock Timeout"))

        with pytest.raises(Timeout):
            api_manager.api_call()

    @responses.activate
    def test_connectionerror(self, api_manager):
        responses.get(
            api_manager.base_url, body=ConnectionError("mock ConnectionError")
        )

        with pytest.raises(ConnectionError):
            api_manager.api_call()

    @responses.activate
    def test_requestexception(self, api_manager):
        responses.get(
            api_manager.base_url, body=RequestException("mock RequestException")
        )

        with pytest.raises(RequestException):
            api_manager.api_call()


class TestFormatDF:
    def test_successful_county_format_df(self):
        example_data = [
            ["P1_001N", "H1_001N", "state", "county"],
            ["570719", "233747", "10", "003"],
            ["237378", "142280", "10", "005"],
            ["181851", "72708", "10", "001"],
        ]
        ip = {"census_high_res_geo_unit": "county"}
        api_vars = ["P1_001N", "H1_001N"]
        df = us._format_df(example_data, ip, api_vars)

        expected_df = pd.DataFrame(
            {
                "FIPS": ["10003", "10005", "10001"],
                "P1_001N": ["570719", "237378", "181851"],
                "H1_001N": ["233747", "142280", "72708"],
            }
        ).set_index("FIPS")

        assert df.equals(expected_df)

    def test_successful_tract_format_df(self):
        example_data = [
            ["P1_001N", "H1_001N", "state", "county", "tract"],
            ["570719", "233747", "10", "003", "000200"],
            ["237378", "142280", "10", "005", "000300"],
            ["181851", "72708", "10", "001", "000400"],
        ]
        ip = {"census_high_res_geo_unit": "tract"}
        api_vars = ["P1_001N", "H1_001N"]
        df = us._format_df(example_data, ip, api_vars)

        expected_df = pd.DataFrame(
            {
                "FIPS": ["10003000200", "10005000300", "10001000400"],
                "P1_001N": ["570719", "237378", "181851"],
                "H1_001N": ["233747", "142280", "72708"],
            }
        ).set_index("FIPS")

        assert df.equals(expected_df)


class TestUSCensusPlugins:
    @pytest.fixture
    def ip(self):
        input_params = MagicMock()
        input_params.data = {
            "census_year": 2020,
            "census_fitting_vars": ["AGEP", "SCHL", "MAR", "HINCP"],
            "census_high_res_geo_unit": "county",
            "census_low_res_geo_unit": "state:10",
            "api_key": "key",
        }

        def mock_get_item(key):
            return input_params.data[key]

        input_params.__getitem__.side_effect = mock_get_item

        def has_keyword(kw):
            return kw in input_params.data

        input_params.has_keyword.side_effect = has_keyword

        return input_params

    @pytest.fixture
    def cens_conv_inst(self, ip):
        mock_census_converter = MagicMock()
        mock_census_converter.input_params = ip
        return mock_census_converter

    @pytest.fixture
    def mock_api_manager(self):
        us.api_manager = MagicMock()
        return us.api_manager

    @pytest.fixture
    def mock_format_df(self):
        us._format_df = MagicMock()
        return us._format_df

    def test_global_read_raw_data_into_pandas(
        self, cens_conv_inst, mock_api_manager, mock_format_df
    ):
        mock_api_call_ret_val = [
            ["P1_001N", "H1_001N", "state", "county"],
            ["570719", "233747", "10", "003"],
            ["237378", "142280", "10", "005"],
            ["181851", "72708", "10", "001"],
        ]
        mock_api_manager.api_call.return_value = mock_api_call_ret_val

        mock_format_df_ret_val = pd.DataFrame(
            {
                "FIPS": ["10003", "10005", "10001"],
                "P1_001N": ["570719", "237378", "181851"],
                "H1_001N": ["233747", "142280", "72708"],
            }
        ).set_index("FIPS")
        mock_format_df.return_val = mock_format_df_ret_val

        expected_output = mock_format_df_ret_val
        output = us.USCensusGlobalPlugin.read_raw_data_into_pandas(cens_conv_inst)

        expected_api_call_url = "/2020/dec/pl"
        expected_api_call_params = {
            "get": "P1_001N,H1_001N",
            "for": "county:*",
            "in": "state:10",
            "key": "key",
        }
        mock_api_manager.api_call.assert_called_with(
            expected_api_call_url, expected_api_call_params
        )

        expected_format_df_data = mock_api_call_ret_val
        expected_format_df_ip = cens_conv_inst.input_params
        expected_format_df_api_vars = [
            "P1_001N",
            "H1_001N",
        ]
        mock_format_df.assert_called_with(
            expected_format_df_data, expected_format_df_ip, expected_format_df_api_vars
        )
        assert output.equals(expected_output)

    def test_global_transform(self, cens_conv_inst):
        cens_conv_inst.raw_data_df = pd.DataFrame(
            {
                "FIPS": ["10003", "10005", "10001"],
                "P1_001N": ["570719", "237378", "181851"],
                "H1_001N": ["233747", "142280", "72708"],
            }
        ).set_index("FIPS")

        expected_pop_df = pd.DataFrame(
            {
                "FIPS": ["10003", "10005", "10001"],
                "total": [570719.0, 237378.0, 181851.0],
            }
        ).set_index("FIPS")
        expected_nh_df = pd.DataFrame(
            {
                "FIPS": ["10003", "10005", "10001"],
                "total": [233747.0, 142280.0, 72708.0],
            }
        ).set_index("FIPS")
        expected_geos_of_interest = ["10003", "10005", "10001"]

        expected_output = {
            "total_population_by_geo": expected_pop_df,
            "number_households_by_geo": expected_nh_df,
            "geos_of_interest": expected_geos_of_interest,
        }
        output = us.USCensusGlobalPlugin.transform(cens_conv_inst)

        output_matched = (
            output["total_population_by_geo"].equals(
                expected_output["total_population_by_geo"]
            )
            and output["number_households_by_geo"].equals(
                expected_output["number_households_by_geo"]
            )
            and output["geos_of_interest"] == expected_output["geos_of_interest"]
        )
        assert output_matched
