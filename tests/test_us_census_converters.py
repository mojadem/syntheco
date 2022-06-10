import pandas as pd
import pytest
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
