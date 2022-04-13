"""
main

This is the main control module for syntheco
"""
import argparse
from census_converters.census_converter import CensusConverter
from input import InputParams
from global_tables import GlobalTables
from pums_data_tables import PUMSDataTables
from summary_data_tables import SummaryDataTables


def main():
    """
    main

    The main control module that defines the workflow of
    syntheco
    """
    parser = argparse.ArgumentParser(description="Synthco Command Line Parse")
    parser.add_argument("-i", "--input_file", action="store",
                        help="The input yaml file")
    parser.add_argument("-o", "--output_prefix", action="store",
                        help="The prefix for the output files",
                        default="syntheco_output")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Activates verbose output")
    parser.add_argument("-d", "--debug", action="store_true",
                        help="Activates debugging output")

    args = parser.parse_args()

    ip = InputParams(args.input_file)
    print("input = {}".format(ip.input_params))
    census_conv = ip.input_params['census_converter']

    glob_table_conv = CensusConverter(ip, census_conv, "global")
    global_tables = GlobalTables(geo_unit_=ip.input_params['census_high_res_geo_unit'],
                                 converter_=glob_table_conv)
    print("Global Tables Created")
    pums_table_conv = CensusConverter(ip, census_conv, "pums")
    summary_table_conv = CensusConverter(ip, census_conv, "summary",
                                         _global_tables=global_tables)
    summary_tables = SummaryDataTables(summary_variables_=ip.input_params['census_fitting_vars'],
                                       geo_unit_=ip.input_params['census_high_res_geo_unit'],
                                       converter_=summary_table_conv)

    pums_heir_tables = PUMSDataTables(geo_unit_=ip.input_params['census_low_res_geo_unit'],
                                      converter_=pums_table_conv)

    print(f"{pums_heir_tables}")
    print(f"{global_tables}")
    print(f"{summary_tables}")


if __name__ == "__main__":
    main()
