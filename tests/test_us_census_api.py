import pytest
import json
import requests


# TODO: dynamically skip based on command line options
@pytest.mark.apitest
class TestUSCensusAPI:
    def test_pl_api_endpoint(self):
        url = "https://api.census.gov/data/2020/dec/pl?get=P1_001N%2CH1_001N&for=county%3A%2A&in=state%3A10&key=e3061d8962ee2b9822717e18093c29337bca18df"
        response = requests.get(url)

        assert response.status_code == 200

        with open("tests/example_api_responses/pl.json") as pl:
            assert response.json() == json.load(pl)

    def test_pums_api_endpoint(self):
        url = "https://api.census.gov/data/2020/acs/acs5/pums?get=AGEP%2CSCHL%2CMAR%2CHINCP&for=state%3A10&key=e3061d8962ee2b9822717e18093c29337bca18df"
        response = requests.get(url)

        assert response.status_code == 200

        # data body is in a randomized order, so just assert the columns are identical
        with open("tests/example_api_responses/pums.json") as pums:
            assert response.json()[0] == json.load(pums)[0]

    def test_profile_api_endpoint(self):
        url = "https://api.census.gov/data/2020/acs/acs5/profile?get=DP05_0005E%2CDP05_0006E%2CDP05_0007E%2CDP05_0008E%2CDP05_0009E%2CDP05_0010E%2CDP05_0011E%2CDP05_0012E%2CDP05_0013E%2CDP05_0014E%2CDP05_0015E%2CDP05_0016E%2CDP05_0017E%2CDP02_0060E%2CDP02_0061E%2CDP02_0062E%2CDP02_0063E%2CDP02_0064E%2CDP02_0065E%2CDP02_0066E%2CDP02_0026E%2CDP02_0027E%2CDP02_0028E%2CDP02_0029E%2CDP02_0030E%2CDP02_0032E%2CDP02_0033E%2CDP02_0034E%2CDP02_0035E%2CDP02_0036E%2CDP04_0039E%2CDP04_0040E%2CDP04_0041E%2CDP04_0042E%2CDP04_0043E%2CDP04_0044E%2CDP03_0052E%2CDP03_0053E%2CDP03_0054E%2CDP03_0055E%2CDP03_0056E%2CDP03_0057E%2CDP03_0058E%2CDP03_0059E%2CDP03_0060E%2CDP03_0061E&for=county%3A%2A&in=state%3A10&key=e3061d8962ee2b9822717e18093c29337bca18df"
        response = requests.get(url)

        assert response.status_code == 200

        with open("tests/example_api_responses/profile.json") as profile:
            assert response.json() == json.load(profile)
