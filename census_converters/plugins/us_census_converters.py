"""
us_census_converters

This is the code for the US Census Plugins
"""


import pandas as pd
import numpy as np

import requests
from requests.exceptions import Timeout
from requests.exceptions import ConnectionError

from census_converters import hookimpl
from census_converters.census_converter import CensusConverter

# TODO: implement these variables into input file
high_res_geo_unit = "tract"  # for now will grab all in state
low_res_geo_unit = "state"
state_num = "10"  # could specify multiple states or all with *
census_year = 2020
acs_year = 2020  # potentially differs from census_year
acs_source = "acs5"  # could use acs1 or default to 5
api_key = "e3061d8962ee2b9822717e18093c29337bca18df"


def api_call(*args, **kwargs):
    try:
        response = requests.get(*args, **kwargs)
    except (Timeout, ConnectionError) as e:
        print(e)
        response = api_call(*args, **kwargs)
    else:
        print("success")
    finally:
        return response


class USCensusGlobalPlugin:
    """
    USCensusGlobalPlugin

    This is class that houses the implemented hooks for the US Census plugin
    for global tables
    """

    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst: CensusConverter):
        """
        read_raw_data_into_pandas
        Retrieves relevant data from the Census API

        Returns:
            the raw data table from the US Census data
        """
        # data variables we will be retrieving from api
        api_vars = [
            "P1_001N",  # total population data
            "H1_001N",  # total housing data
        ]

        url = f"https://api.census.gov/data/{census_year}/dec/pl"
        params = {
            "get": ",".join(api_vars),
            "for": f"{high_res_geo_unit}:*",
            "in": f"{low_res_geo_unit}:{state_num}",
            "key": api_key,
        }

        response = api_call(url, params, timeout=1)
        data = response.json()
        raw_df = pd.DataFrame(data[1:], columns=data[0])

        # formulate fips from area codes and set to first column
        raw_df["FIPS"] = raw_df["state"] + raw_df["county"]
        if high_res_geo_unit == "tract":
            raw_df["FIPS"] += raw_df["tract"]
        raw_df = raw_df[["FIPS"] + api_vars]
        raw_df = raw_df.set_index("FIPS")
        # NOTE: if using tracts, we will also append tracts on to FIPS code

        return raw_df

    @hookimpl
    def transform(cens_conv_inst: CensusConverter):
        """
        transform
        Formats the raw data into global tables

        Returns:
            an updated dataframe to be set to processed_data_df
        """
        # total population table
        pop_df = (
            cens_conv_inst.raw_data_df.copy()
            .drop(columns=["H1_001N"])
            .rename(columns={"P1_001N": "total"})
        )

        # total number of households table
        nh_df = (
            cens_conv_inst.raw_data_df.copy()
            .drop(columns=["P1_001N"])
            .rename(columns={"H1_001N": "total"})
        )

        pop_df.name = "Total Population by High Resolution Geo Unit"
        nh_df.name = "Number of Households by High Resolution Geo Unit"

        return {"total_population_by_geo": pop_df, "number_households_by_geo": nh_df}


class USCensusSummaryPlugin:
    """
    USCensusSummaryPlugin

    This class houses the implemented hooks for the US Census plugins
    for processed summary count tables
    """

    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst: CensusConverter):
        """
        _read_raw_data_into_pandas
        Retrieves relevant data from the Census API

        Returns:
            the raw data table from the US Census data
        """
        # retrieve api vars from metadata_json
        metadata_json = cens_conv_inst.metadata_json
        api_vars = [
            cens_conv_inst.metadata_json[v]["profile_vars"] for v in metadata_json
        ]
        api_vars = [
            pv for v in api_vars for pv in v
        ]  # flatten nested list of profile_vars

        assert len(api_vars) <= 50  # TODO: split this case into multiple api calls

        url = f"https://api.census.gov/data/{acs_year}/acs/{acs_source}/profile"
        params = {
            "get": ",".join(api_vars),
            "for": f"{high_res_geo_unit}:*",
            "in": f"{low_res_geo_unit}:{state_num}",
            "key": api_key,
        }

        response = api_call(url, params, timeout=1)
        data = response.json()
        raw_df = pd.DataFrame(data[1:], columns=data[0])

        # formulate fips from area codes and set to first column
        raw_df["FIPS"] = raw_df["state"] + raw_df["county"]
        if high_res_geo_unit == "tract":
            raw_df["FIPS"] += raw_df["tract"]
        raw_df = raw_df[["FIPS"] + api_vars]
        raw_df = raw_df.set_index("FIPS")

        return raw_df

    @hookimpl
    def transform(cens_conv_inst: CensusConverter):
        """
        transform
        Formats the raw data into processed summary count tables

        Returns:
            an updated dataframe to be set to processed_data_df
        """
        pums_vars = cens_conv_inst.input_params.input_params["census_fitting_vars"]
        proc_df = cens_conv_inst.raw_data_df.copy()

        sum_tables = {}
        for var in pums_vars:
            var_ds = cens_conv_inst.metadata_json[var]
            var_df = proc_df[var_ds["profile_vars"]].astype(int)

            lookup = [
                (int(i), c["profile_vars"]) for i, c in var_ds["common_var_map"].items()
            ]
            for i, c in lookup:
                var_df[i] = var_df[c].sum(axis=1)

            var_df = var_df.drop(columns=var_ds["profile_vars"])
            var_df.name = f"{var} Summary Table"

            sum_tables[var] = var_df

        return sum_tables


class USCensusPUMSPlugin:
    """
    USCensusPUMSPlugin

    This class houses the implemented hooks for the US Census plugins
    for processed PUMS tables
    """

    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst: CensusConverter):
        """
        _read_raw_data_into_pandas
        Retrieves relevant data from the Census API

        Returns:
            the raw data table from the US Census data
        """
        api_vars = cens_conv_inst.input_params.input_params["census_fitting_vars"]

        url = f"https://api.census.gov/data/{acs_year}/acs/{acs_source}/pums"
        params = {
            "get": ",".join(api_vars),
            "for": f"{low_res_geo_unit}:{state_num}",
            "key": api_key,
        }

        response = api_call(url, params, timeout=1)
        data = response.json()
        raw_df = pd.DataFrame(data[1:], columns=data[0])

        return raw_df

    @hookimpl
    def transform(cens_conv_inst: CensusConverter):
        """
        transform
        Formats the raw data into processed PUMS tables

        Returns:
            an updated dataframe to be set to processed_data_df
        """
        pums_vars = cens_conv_inst.input_params.input_params["census_fitting_vars"]
        proc_df = cens_conv_inst.raw_data_df.astype(int)

        for var in pums_vars:
            new_col_name = f"{var}_m"
            proc_df[new_col_name] = [np.NaN for _ in range(proc_df.shape[0])]

            lookup = [
                (int(i), c["pums_inds"])
                for i, c in cens_conv_inst.metadata_json[var]["common_var_map"].items()
            ]
            for i, c in lookup:
                if len(c) == 1:  # for single indices
                    proc_df.loc[proc_df[var] == int(c[0]), new_col_name] = i
                else:  # for index ranges
                    lower = int(c[0])
                    upper = int(c[1])
                    proc_df.loc[
                        (proc_df[var] >= lower) & (proc_df[var] <= upper), new_col_name
                    ] = i

        proc_df = proc_df.rename(columns={x: f"{x}_V" for x in pums_vars}).rename(
            columns={f"{x}_m": x for x in pums_vars}
        )

        freq_df = (
            proc_df.groupby(pums_vars).size().reset_index().rename(columns={0: "total"})
        )

        freq_df["total"] = freq_df["total"].astype(np.float64)
        freq_df = freq_df.astype({v: np.int64 for v in pums_vars})

        proc_df.name = "PUMS Data Categorical Representation"
        freq_df.name = "PUMS Data Frequency Representation"

        return {"categorical_table": proc_df, "frequency_table": freq_df}
