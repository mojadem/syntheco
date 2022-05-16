"""
ipf_census_fitting

This is the implementation of IPF for fitting census data
"""

import pandas as pd
import numpy as np
from ipfn import ipfn
from census_fitting_procedures import hookimpl
from logger import log, data_log
from error import SynthEcoError
from util import random_round_to_integer
import random as rn


class IPFCensusHouseholdFittingProcedure:
    """
    IPFCensusFittingProcedure

    This class houses the implemented hooks for the iterative proportional
    fitting procedure for census fitting
    """

    @hookimpl
    def perform_fitting(fit_proc_inst):
        """
        perform_fitting

        Function that performs the IPF fitting, the data must be prepared before
        calling this function

        Arguments:
            fit_proc_inst: instance of the plugin class

        geo_code: the code fo the geographic unit that you want to fit
        summary_tables: marginal tables for the geographic area corresponding to geo_code
                        There will be one marginal series per the fitting variables
        pums_freq: This is the pums table modified to represent the frequency of each
                   combination of the values for the fitting variables

        fitting_vars: the variable that are being used for marginal tables
                      (TODO: Make this nomenclature consistent)

        n_houses: the number of houses in the geographic area corresponding to geo_code

        returns a pandas dataFrame that provides the results of the IPF procedure
        """
        log("INFO", "Beginning Iterative Proportional Fitting Procedure")
        try:
            fitting_vars = fit_proc_inst.input_params["census_fitting_vars"]
            geo_codes_of_interest = fit_proc_inst.global_tables.data['geos_of_interest']
            num_houses = fit_proc_inst.global_tables.data['number_households_by_geo']
            summary_tables = fit_proc_inst.summary_tables.data
            pums_freq_org = fit_proc_inst.pums_tables.data['frequency_table']

            fit_res = {}
            for geo_code in geo_codes_of_interest:
                log("DEBUG", "Beginning processing for geo_code {}".format(geo_code))
                n_houses = num_houses.loc[geo_code, 'total']
                summary_geo_tables = []
                pums_freq = pums_freq_org.copy(deep=True)
                # Extract the geo_code information from summary Tables
                for var in fitting_vars:
                    log("DEBUG", "IPF: Transforming {} {}".format(geo_code, var))
                    sum_t_df = summary_tables[var]
                    sum_t_df = sum_t_df.set_index('GEO_CODE')

                    sum_g_df = sum_t_df.loc[geo_code, ].reset_index().set_index([var])

                    sum_g_ser = sum_g_df['total']
                    sum_g_total = sum_g_ser.sum()
                    sum_g_ser = sum_g_ser.astype("float64")
                    sum_g_ser = sum_g_ser.apply(lambda x: 0 if sum_g_total == 0 else round((x/sum_g_total)*n_houses, 0))
                    summary_geo_tables.append(sum_g_ser)
                log("INFO", "Starting IPF for {}".format(geo_code))
                IPF = ipfn.ipfn(pums_freq, summary_geo_tables, [[x] for x in fitting_vars],
                                max_iteration=10000)
                results = IPF.iteration()
                log("INFO", "Successfully converged IPF for {}".format(geo_code))

                # Round the floating point answers to integers (will still be floats)
                results_rounded = results.copy()
                results_rounded['total'] = results_rounded['total'].apply(lambda x: random_round_to_integer(x))

                # elminate the zero entries as they are not important anymore
                results_rounded = results_rounded[results_rounded['total'] != 0]

                log("DEBUG", "GEO_CODE: {} SUM: {} NHOUSES: {}".format(geo_code,
                                                                       results_rounded['total'].sum(),
                                                                       n_houses))
                '''
                This is necessary because if the random rounding eliminates all of the houses in
                a geographic area, we need to do something different.
                it means that non of the frequencies are above one, so I will assign 1 to the
                n_house highest answers rather than round
                '''
                previous_sum = -100000.00
                if results_rounded['total'].sum() == 0:
                    # If the IPF results in zero (which can happen when there is very
                    # few houses in the area) set the highest fractional answer to 1.0
                    results_rounded = results.nlargest(int(n_houses), 'total')
                    results_rounded['total'] = results_rounded['total'].apply(lambda x: 1.0)
                else:
                    # This part ensures that the number of houses in the fitting is consitent
                    # basically if the sum of the results is higher or lower than the number
                    # of households in an area, increment or decrement randomly till we they
                    # are the same
                    while results_rounded['total'].sum() < n_houses:
                        current_sum = results_rounded['total'].sum()
                        if current_sum == previous_sum:
                            raise SynthEcoError("There was a problem in IPF rounding procedure for {}".format(geo_code))
                        random_hh = rn.randint(0, results_rounded.shape[0]-1)
                        results_rounded.loc[results_rounded.index[random_hh], 'total'] = \
                            results_rounded.loc[results_rounded.index[random_hh], 'total'] + 1
                        previous_sum = current_sum
                    while results_rounded['total'].sum() > n_houses:
                        current_sum = results_rounded['total'].sum()
                        if current_sum == previous_sum:
                            raise SynthEcoError("There was a problem in IPF rounding procedure for {}".format(geo_code))
                        random_hh = rn.randint(0, results_rounded.shape[0]-1)
                        results_rounded.loc[results_rounded.index[random_hh], 'total'] = \
                            results_rounded.loc[results_rounded.index[random_hh], 'total'] - 1
                        results_rounded = results_rounded[results_rounded['total'] != 0]
                        previous_sum = current_sum

                # We don't need no stinking zeros
                results_rounded = results_rounded[results_rounded['total'] != 0]
                log("DEBUG", "FINAL RESULTS: GEO_CODE: {} SUM: {} NHOUSES: {}".format(geo_code,
                                                                                      results_rounded['total'].sum(),
                                                                                      n_houses))
                fit_res[geo_code] = results_rounded
            return fit_res

        except Exception as e:
            raise SynthEcoError("There was a problem during Iterative Proportional Fitting: {}".format(e))
