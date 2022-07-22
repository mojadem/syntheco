"""
pums_data_table

This is a class to hold the standard pums data tables that are needed across
the synthetic ecosystem generation process.
"""

import pandas as pd
from error import SynthEcoError
import multiprocessing as mp
from itertools import chain

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
        """
        return '\n'.join(["PUMS Tables",
                          "------------------------------------------------------"] +
                         [f"{x.name}\n{x}" for x in self.data.values()])

### going to have to move this to something plugin specific since it has diversity.
    def create_from_index(self,hh_inds_by_geo):
        if not "categorical_table" in self.data:
            SynthEcoError("PUMSDataTables: no categorical table in self.data " +
                        "You need to have converted the PUMS tables" +
                        "before running create_new_pums_table_from_household_ids")

        if not "raw_data" in self.data:
            SynthEcoError("PUMSDataTables: no raw data in self.data " +
                        "You need to have converted the PUMS tables" +
                        "before running create_new_pums_table_from_household_ids")

        pums_hier_org_df = self.data['raw_data']
        pums_hier_proc_df = self.data['categorical_table']

        index_dict = {}
        geo_list = []
        overall_hh_ind = []
        hh_counter = 1
        for g,hh_inds in hh_inds_by_geo.items():
            ind_list = []
            for i in hh_inds:
                hh_id = pums_hier_proc_df.loc[i]['HH_ID']
                hh_list =  pums_hier_org_df.index[pums_hier_org_df['HH_ID'] == hh_id].tolist()
                ind_list += hh_list
                overall_hh_ind += [hh_counter for x in range(0,len(hh_list))]
                hh_counter += 1
            index_dict[g] = ind_list
            geo_list += [g for x in range(0,len(ind_list))]

        total_list = list(chain(*index_dict.values()))
        new_df = pums_hier_org_df.loc[total_list]
        new_df['GEO_CODE'] = geo_list
        new_df['HH_ID_2'] = overall_hh_ind
        # renumber the Households
        new_df = new_df.reset_index().drop(columns=["index"])

        return new_df
