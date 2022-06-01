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
from logger import log

# TODO: implement these variables into input file
state_num = "10"


def api_call(*args, **kwargs):
    try:
        response = requests.get(*args, **kwargs)
    except (Timeout, ConnectionError) as e:
        log("DEBUG", f"Failed API call: {e}")
        response = api_call(*args, **kwargs)
    else:
        log("DEBUG", f"Successful API call: {response.url}")
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

        url = "https://api.census.gov/data/{}/dec/pl".format(
            cens_conv_inst.input_params["census_year"]
        )
        params = {
            "get": ",".join(api_vars),
            "for": "{}:*".format(
                cens_conv_inst.input_params["census_high_res_geo_unit"]
            ),
            "in": "{}:{}".format(
                cens_conv_inst.input_params["census_low_res_geo_unit"], state_num
            ),
            "key": cens_conv_inst.input_params["api_key"],
        }

        response = api_call(url, params, timeout=1)
        data = response.json()
        raw_df = pd.DataFrame(data[1:], columns=data[0])

        # formulate fips from area codes and set to first column
        raw_df["FIPS"] = raw_df["state"] + raw_df["county"]
        if cens_conv_inst.input_params["census_high_res_geo_unit"] == "tract":
            raw_df["FIPS"] += raw_df["tract"]

        raw_df = raw_df[["FIPS"] + api_vars]
        raw_df = raw_df.set_index("FIPS")

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

        pop_df["total"] = pop_df["total"].astype(np.float64)

        # total number of households table
        nh_df = (
            cens_conv_inst.raw_data_df.copy()
            .drop(columns=["P1_001N"])
            .rename(columns={"H1_001N": "total"})
        )

        nh_df["total"] = nh_df["total"].astype(np.float64)

        pop_df.name = "Total Population by High Resolution Geo Unit"
        nh_df.name = "Number of Households by High Resolution Geo Unit"

        geos_hh_interest = list(nh_df[(nh_df["total"] != 0)].index)
        geos_pop_interest = list(pop_df[pop_df["total"] != 0].index)

        geos_of_interest = [x for x in geos_hh_interest if x in geos_pop_interest]

        if cens_conv_inst.input_params.has_keyword("debug_limit_geo_codes"):
            debug_limit = cens_conv_inst.input_params["debug_limit_geo_codes"]

            if debug_limit < len(geos_of_interest):
                geos_of_interest = geos_of_interest[:debug_limit]

        return {
            "total_population_by_geo": pop_df,
            "number_households_by_geo": nh_df,
            "geos_of_interest": geos_of_interest,
        }


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
        api_vars = [metadata_json[v]["profile_vars"] for v in metadata_json]
        api_vars = [
            pv for v in api_vars for pv in v
        ]  # flatten nested list of profile_vars

        assert len(api_vars) <= 50  # TODO: split this case into multiple api calls

        url = "https://api.census.gov/data/{}/acs/acs5/profile".format(
            cens_conv_inst.input_params["census_year"]
        )
        params = {
            "get": ",".join(api_vars),
            "for": "{}:*".format(
                cens_conv_inst.input_params["census_high_res_geo_unit"]
            ),
            "in": "{}:{}".format(
                cens_conv_inst.input_params["census_low_res_geo_unit"], state_num
            ),
            "key": cens_conv_inst.input_params["api_key"],
        }

        response = api_call(url, params, timeout=1)
        data = response.json()
        raw_df = pd.DataFrame(data[1:], columns=data[0])

        # formulate fips from area codes and set to first column
        raw_df["FIPS"] = raw_df["state"] + raw_df["county"]
        if cens_conv_inst.input_params["census_high_res_geo_unit"] == "tract":
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
            var_df = proc_df[var_ds["profile_vars"]].astype(np.int64)

            # sum profile vars for each index i in common_var_map to get summary totals
            lookup = [
                (int(i), c["profile_vars"]) for i, c in var_ds["common_var_map"].items()
            ]
            for i, prof_vars in lookup:
                var_df[i] = var_df[prof_vars].sum(axis=1)

            var_df = var_df.drop(columns=var_ds["profile_vars"]).stack().reset_index()
            var_df.columns = ["FIPS", var, "total"]

            # handle cases where total is 0 for all indeces in common_var_map
            sum_by_geo = var_df.groupby("FIPS").sum()
            var_df["total"] = var_df.apply(
                lambda x: 1.0  # right now just converts them to 1.0
                if sum_by_geo.loc[x["FIPS"]]["total"] == 0
                else x["total"],
                axis=1,
            )

            var_df = var_df.set_index("FIPS")

            var_df["total"] = var_df["total"].astype(np.float64)

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

        url = "https://api.census.gov/data/{}/acs/acs5/pums".format(
            cens_conv_inst.input_params["census_year"]
        )
        params = {
            "get": ",".join(api_vars),
            "for": "{}:{}".format(
                cens_conv_inst.input_params["census_low_res_geo_unit"], state_num
            ),
            "key": cens_conv_inst.input_params["api_key"],
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
        proc_df = cens_conv_inst.raw_data_df.astype(np.int64)

        for var in pums_vars:
            new_col_name = f"{var}_m"
            proc_df[new_col_name] = [np.NaN for _ in range(proc_df.shape[0])]

            lookup = [
                (int(i), c["pums_inds"])
                for i, c in cens_conv_inst.metadata_json[var]["common_var_map"].items()
            ]
            for i, constraints in lookup:
                if len(constraints) == 1:  # for single index constraints
                    proc_df.loc[proc_df[var] == int(constraints[0]), new_col_name] = i
                elif len(constraints) == 2:  # for index range constraints
                    lower = int(constraints[0])
                    upper = int(constraints[1])
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
