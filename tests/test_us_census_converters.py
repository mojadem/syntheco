import pandas as pd
import pytest
from unittest.mock import MagicMock, create_autospec
import responses
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import json
from collections import namedtuple
from typing import Callable

import census_converters.plugins.us_census_converters as us

FunctionCallParameters = namedtuple("FunctionCallParameters", ["args", "kwargs"])
FunctionCallMetadata = namedtuple("FunctionCallMetadata", ["params", "return_value"])


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
            "census_fitting_vars": ["BDSP"],
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

        with open("census_converters/plugins/us_pums_info.json") as meta:
            mock_census_converter.metadata_json = json.load(meta)

        return mock_census_converter

    def read_scaffold(
        self,
        cens_conv_inst,
        api_call_meta: FunctionCallMetadata,
        format_df_meta: FunctionCallMetadata,
        read_func: Callable,
    ):
        us.api_manager = MagicMock()
        us.api_manager.api_call.return_value = api_call_meta.return_value

        us._format_df = MagicMock()
        us._format_df.return_value = format_df_meta.return_value

        expected_output: pd.DataFrame = format_df_meta.return_value
        output: pd.DataFrame = read_func(cens_conv_inst)

        us.api_manager.api_call.assert_called_once_with(
            *api_call_meta.params.args, **api_call_meta.params.kwargs
        )
        us._format_df.assert_called_once_with(
            *format_df_meta.params.args, **format_df_meta.params.kwargs
        )

        assert output.equals(expected_output)

    def test_global_read(self, cens_conv_inst):
        api_call_params = FunctionCallParameters(
            [
                "/2020/dec/pl",
                {
                    "get": "P1_001N,H1_001N",
                    "for": "county:*",
                    "in": "state:10",
                    "key": "key",
                },
            ],
            {},
        )
        api_call_return_value = [
            ["P1_001N", "H1_001N", "state", "county"],
            ["570719", "233747", "10", "003"],
            ["237378", "142280", "10", "005"],
            ["181851", "72708", "10", "001"],
        ]
        api_call_meta = FunctionCallMetadata(api_call_params, api_call_return_value)

        format_df_params = FunctionCallParameters(
            [
                api_call_meta.return_value,
                cens_conv_inst.input_params,
                [
                    "P1_001N",
                    "H1_001N",
                ],
            ],
            {},
        )
        format_df_return_value = pd.DataFrame(
            {
                "FIPS": ["10003", "10005", "10001"],
                "P1_001N": ["570719", "237378", "181851"],
                "H1_001N": ["233747", "142280", "72708"],
            }
        ).set_index("FIPS")
        format_df_meta = FunctionCallMetadata(format_df_params, format_df_return_value)

        self.read_scaffold(
            cens_conv_inst,
            api_call_meta,
            format_df_meta,
            us.USCensusGlobalPlugin.read_raw_data_into_pandas,
        )

    def test_summary_read(self, cens_conv_inst):
        api_call_params = FunctionCallParameters(
            [
                "/2020/acs/acs5/profile",
                {
                    "get": "DP04_0039E,DP04_0040E,DP04_0041E,DP04_0042E,DP04_0043E,DP04_0044E",
                    "for": "county:*",
                    "in": "state:10",
                    "key": "key",
                },
            ],
            {},
        )
        api_call_return_value = [
            [
                "DP04_0039E",
                "DP04_0040E",
                "DP04_0041E",
                "DP04_0042E",
                "DP04_0043E",
                "DP04_0044E",
                "state",
                "county",
            ],
            ["4021", "21129", "46047", "88742", "54050", "10253", "10", "003"],
            ["1109", "3736", "26295", "74145", "28034", "7804", "10", "005"],
            ["699", "4101", "13745", "36720", "14367", "3441", "10", "001"],
        ]
        api_call_meta = FunctionCallMetadata(api_call_params, api_call_return_value)

        format_df_params = FunctionCallParameters(
            [
                api_call_meta.return_value,
                cens_conv_inst.input_params,
                [
                    "DP04_0039E",
                    "DP04_0040E",
                    "DP04_0041E",
                    "DP04_0042E",
                    "DP04_0043E",
                    "DP04_0044E",
                ],
            ],
            {},
        )
        format_df_return_value = pd.DataFrame(
            {
                "FIPS": ["10003", "10005", "10001"],
                "DP04_0039E": ["4021", "1109", "699"],
                "DP04_0040E": ["21129", "3736", "4101"],
                "DP04_0041E": ["46047", "26295", "13745"],
                "DP04_0042E": ["88742", "74145", "36720"],
                "DP04_0043E": ["54050", "28034", "14367"],
                "DP04_0044E": ["10253", "7804", "3441"],
            }
        ).set_index("FIPS")
        format_df_meta = FunctionCallMetadata(format_df_params, format_df_return_value)

        self.read_scaffold(
            cens_conv_inst,
            api_call_meta,
            format_df_meta,
            us.USCensusSummaryPlugin.read_raw_data_into_pandas,
        )

    def test_pums_read(self, cens_conv_inst):
        api_call_params = FunctionCallParameters(
            [
                "/2020/acs/acs5/pums",
                {"get": "BDSP", "for": "state:10", "key": "key"},
            ],
            {},
        )
        api_call_return_value = [
            ["BDSP", "state"],
            ["-1", "10"],
            ["4", "10"],
            ["3", "10"],
            ["3", "10"],
            ["2", "10"],
            ["1", "10"],
            ["-1", "10"],
            ["3", "10"],
            ["4", "10"],
        ]
        api_call_meta = FunctionCallMetadata(api_call_params, api_call_return_value)

        format_df_params = FunctionCallParameters(
            [api_call_meta.return_value],
            {"formulate_fips": False},
        )
        format_df_return_value = pd.DataFrame(
            {
                "BDSP": ["-1", "4", "3", "3", "2", "1", "-1", "3", "4"],
                "state": ["10" for _ in range(9)],
            }
        )
        format_df_meta = FunctionCallMetadata(format_df_params, format_df_return_value)

        self.read_scaffold(
            cens_conv_inst,
            api_call_meta,
            format_df_meta,
            us.USCensusPUMSPlugin.read_raw_data_into_pandas,
        )

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

    def test_summary_transform(self, cens_conv_inst):
        cens_conv_inst.raw_data_df = pd.DataFrame(
            {
                "FIPS": ["10003"],
                "DP04_0039E": ["4021"],
                "DP04_0040E": ["21129"],
                "DP04_0041E": ["46047"],
                "DP04_0042E": ["88742"],
                "DP04_0043E": ["54050"],
                "DP04_0044E": ["10253"],
            }
        ).set_index("FIPS")

        expected_output = {
            "BDSP": pd.DataFrame(
                {
                    "FIPS": [
                        "10003",
                        "10003",
                        "10003",
                        "10003",
                        "10003",
                        "10003",
                    ],
                    "BDSP": [
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                    ],
                    "total": [
                        4021.0,
                        21129.0,
                        46047.0,
                        88742.0,
                        54050.0,
                        10253.0,
                    ],
                }
            ).set_index("FIPS")
        }
        output = us.USCensusSummaryPlugin.transform(cens_conv_inst)

        keys_match = output.keys() == expected_output.keys()
        data_match = output["BDSP"].equals(expected_output["BDSP"])
        assert keys_match and data_match
