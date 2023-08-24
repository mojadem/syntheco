"""
pums_data_table

This is a class to hold the standard pums data tables that are needed across
the synthetic ecosystem generation process.
"""

from error import SynthEcoError
import pandas as pd
import multiprocessing as mp
from itertools import chain
import sys


class PUMSDataTables:
    """
    PUMSDataTables class

    This is class to hold the standard pums data tables that are needed
    for synthetic population generation.
    """
    def __init__(self, geo_unit_=None, converter_=None):
        """
        Creation operator
        """
        self.geo_unit = geo_unit_
        self.converter = converter_
        self.data = self.converter.convert()

    def __str__(self):
        """
        This method returns a nice print out of the GlobalTables
        # fix this
        """
        return '\n'.join(["PUMS Tables",
                          "------------------------------------------------------"] +
                         [f"{x.name}\n{x}" for x in self.data.values() if isinstance(x,pd.DataFrame)])
    def is_separate(self):
        if isinstance(self.data["raw_data"],dict):
            return True
        return False

    def create_new_pums_table_from_household_ids(self, hh_inds_by_geo):
        """
        create_new_pums_table_from_household_ids

        This function creates a new pums table from a set of household ids. Essentially
        it is a gather function.

        Arguments:
            hh_inds_by_geo: a dictionary of household Id by geographic areas

        Returns:
            A new dataframe that is a pums like df of the selected households
        """
        if "categorical_table" not in self.data:
            SynthEcoError("PUMSDataTables: no categorical table in self.data " +
                          "You need to have converted the PUMS tables" +
                          "before running create_new_pums_table_from_household_ids")

        if "raw_data" not in self.data:
            SynthEcoError("PUMSDataTables: no raw data in self.data " +
                          "You need to have converted the PUMS tables" +
                          "before running create_new_pums_table_from_household_ids")

        pums_hier_org_df = self.data['raw_data']
        pums_hier_proc_df = self.data['categorical_table']

        index_dict = {}
        geo_list = []
        overall_hh_ind = []
        hh_counter = 1
        for g, hh_inds in hh_inds_by_geo.items():
            ind_list = []
            for i in hh_inds:
                hh_id = pums_hier_proc_df.loc[i]['HH_ID']
                hh_list = pums_hier_org_df.index[pums_hier_org_df['HH_ID'] == hh_id].tolist()
                ind_list += hh_list
                overall_hh_ind += [hh_counter for x in range(0, len(hh_list))]
                hh_counter += 1
            index_dict[g] = ind_list
            geo_list += [g for x in range(0, len(ind_list))]

        total_list = list(chain(*index_dict.values()))
        new_df = pums_hier_org_df.loc[total_list]
        new_df['GEO_CODE'] = geo_list
        new_df['HH_ID_2'] = new_df['HH_ID']
        new_df['HH_ID'] = overall_hh_ind
        # renumber the Households
        new_df = new_df.reset_index().drop(columns=["index"])

        return new_df

    def create_new_pums_table_from_household_ids_with_separate_files(self, hh_inds_by_geo):
        """
        create_new_pums_table_from_household_ids_with_separate_files

        This function creates a new pums table from a set of household ids. Essentially
        it is a gather function.

        This flavor utilizes a pums data set where the people and the households are mapped in
        separate files with a 1 to 1 relationship

        This is not the monte carlo procedure outlined in Pritchards' papers for Canadian census

        Arguments:
            hh_inds_by_geo: a dictionary of household Id by geographic areas

        Returns:
            A new dataframe that is a pums like df of the selected households
        """
        if "categorical_table" not in self.data:
            SynthEcoError("PUMSDataTables: no categorical table in self.data " +
                          "You need to have converted the PUMS tables" +
                          "before running create_new_pums_table_from_household_ids")

        if "raw_data" not in self.data:
            SynthEcoError("PUMSDataTables: no raw data in self.data " +
                          "You need to have converted the PUMS tables" +
                          "before running create_new_pums_table_from_household_ids")

        pums_hier_org_df = self.data['raw_data']['Household']
        pums_people_org_df = self.data['raw_data']['Person']
        pums_hier_proc_df = self.data['categorical_table']

        
        #print(f"pums_p\n{pums_hier_org_df}")
        new_h_df = pd.DataFrame()
        new_p_df = pd.DataFrame()
        new_h = []
        new_p = []
        print("here")
        hh_counter = 1
        for g, hh_inds in hh_inds_by_geo.items():
            #print(hh_inds)
            for hh_i in hh_inds:
                pums_h = pums_hier_org_df.loc[hh_i].copy()
                pums_p = pums_people_org_df[pums_people_org_df['HH_ID'] == pums_h["HH_ID"]].copy()
                #print(type(pums_p))
                #print(pums_p)
                pums_h['HH_ID'] = hh_counter
                pums_p['HH_ID'] = hh_counter
                new_p_df = pd.concat([new_p_df, pums_p])
                new_h.append(pums_h)
                hh_counter += 1
        new_h_df = pd.DataFrame(new_h)
        print("there")
        print(new_h_df)
        #print(new_p_df)
        sys.exit()
        #reindex things, we don't want these big serial numbers

        #print(f"HHs\n{hh_ids_by_geo}")
        #print(f"pums\n{pums_hier_org_df['HH_ID']}")
        return self.data

    def update_pums_table_with_hh_coordinates(self, fitting_result=None, sample_result=None):
        """
        update_pums_table_with_hh_coordinates

        This function takes the result of the household sampling and
        updates the pums_table with the coordnates

        Arguments:
            hh_dict: the result of the sampling should be a dataframe that has households
                     ids and lat long coordinates

        Returns:
            An updated pums table with all of the entries having the appropriate household coordinates

        """
        from census_household_sampling_result import CensusHouseholdSamplingResult
        from census_fitting_result import CensusFittingResult

        if sample_result is None or not isinstance(sample_result, CensusHouseholdSamplingResult):
            raise SynthEcoError("pums_data_tables:  update_pums_table_with_hh_coordinates " +
                                "called with wrong sample_result of {}".format(type(sample_result)))

        if fitting_result is None or not isinstance(fitting_result, CensusFittingResult):
            raise SynthEcoError("pums_data_tables:  update_pums_table_with_hh_coordinates " +
                                "called with wrong fitting_result type of {}".format(type(fitting_result)))

        hh_df = sample_result.data["Household Geographic Assignments"]
        pums_deriv_df = fitting_result.data['Derived PUMS']
        pums_deriv_df['latitude'] = pums_deriv_df.apply(lambda x:
                                                        hh_df[hh_df['HH_ID'] == x['HH_ID']]['latlon'].values[0][0],
                                                        axis=1)
        pums_deriv_df['longitude'] = pums_deriv_df.apply(lambda x:
                                                         hh_df[hh_df['HH_ID'] == x['HH_ID']]['latlon'].values[0][1],
                                                         axis=1)

        return pums_deriv_df
