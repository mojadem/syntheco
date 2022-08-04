"""
census_sampling_result

This class is for the results of the households sampleing procedures
"""

import pandas as pd
from census_household_sampling.census_household_sampling import CensusHouseholdSampling
from error import SynthEcoError
from logger import log, data_log

class CensusHouseholdSamplingResult:
    def __init__(self, sampling_proc_=None,
                 data_=None):
        if not isinstance(sampling_proc_, CensusHouseholdSampling):
            raise SynthEcoError("Trying to initilize CensusHousholdSamplingResult with " +
                                "plugin of the wrong type {}".format(type(sampling_proc_)))

        self.sampling_proc = sampling_proc_

        if data_ is None:
            self.data = self.sampling_proc.perform_household_sampling()
        else:
            self.data = data_

    def __str__(self):
        """
        This method returns a nice print out of the CensusHouseholdSamplingResult
        """

        return '\n'.join(["Census Houshold Sampling Result",
                          "------------------------------------------------------",
                          "{}".format(self.data['Household Geographic Assignments'])])
