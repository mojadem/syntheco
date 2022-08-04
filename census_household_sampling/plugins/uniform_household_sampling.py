"""
uniform_household_sampling

This is the implementation of uniform household sampling to place
households within a geographic area
"""

from census_household_sampling import hookimpl
from logger import log, data_log
from error import SynthEcoError

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import multiprocessing as mp
import random as rn

class UniformHouseholdSampling:
    """
    UniformHouseholdSampling

    This class houses the implemented hooks for uniform geographic
    sampling of households from the result of the fitting selection
    procedures (e.g. IPF)
    """

    @hookimpl
    def sample_households(house_samp_inst):
        """
        sample_households

        Function that takes the households in a list and places them on
        a map

        Arguments:
            house_samp_inst - instance variable for all of the necessary data
                              through the plugin (see census_fitting_procedure)

        Returns:
            A new PUMS file with the geographic coordinates for all of the
            households
        """
        try:
            pums_deriv_df = house_samp_inst.fitting_result.data['Derived PUMS']
            pums_deriv_df = pums_deriv_df.sort_values(by=['GEO_CODE','HH_ID'])
            border_gml = house_samp_inst.input_params['census_input_files']['border_gml']
            border_gdf = gpd.read_file(border_gml).to_crs("WGS 84")



            hh_df = pums_deriv_df[['HH_ID','GEO_CODE']].drop_duplicates()
            hh_dict = {}

            hh_freq = hh_df.groupby('GEO_CODE').size()
            for gc,n in hh_freq.items():
                hh_dict[gc] = UniformHouseholdSampling.select_house_coordinates(gc,border_gdf,n)
                #print(type(hh_dict[gc]))

            hh_df['latlon'] = hh_df.apply(lambda x: hh_dict[x['GEO_CODE']].pop(), axis=1)

            #pums_deriv_df['latlon'] = pums_deriv_df.apply(lambda x:
            #                                              hh_df[hh_df['HH_ID'] == x['HH_ID']]['latlon'].values[0],
            #                                              axis=1)
            return {"Household Geographic Assignments":hh_df}                       

        except Exception as e:
            raise SynthEcoError("uniform_household_sampling:sample_households\n{}".format(e))


    @staticmethod
    def select_house_coordinates(geo_code, border_gdf, n):
        geocode_gdf = border_gdf.loc[border_gdf['CTUID'] == float(geo_code)]
        coords = []
        while len(coords) < n:
            random_point = Point(rn.uniform(geocode_gdf.bounds['minx'], geocode_gdf.bounds['maxx']),
                                 rn.uniform(geocode_gdf.bounds['miny'], geocode_gdf.bounds['maxy']))
            g_test = geocode_gdf['geometry'].contains(random_point)
            g_test.index = ['0']
            if g_test[0]:
            #if random_point.within(geocode_gdf['geometry']):
                coords.append((random_point.x,random_point.y))

        return coords
