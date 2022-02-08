"""
main

This is the main control module for syntheco
"""
import argparse
from census_converters.census_converter import CensusConverter
from input import InputParams
from global_tables import GlobalTables


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

    census_conv = ip.input_params['census_converter']
    glob_table_conv = CensusConverter(ip, census_conv, "global")

    global_tables = GlobalTables(geo_unit_=ip.input_params['census_high_res_geo_unit'],
                                 converter_=glob_table_conv)

    print(f"{global_tables}")


if __name__ == "__main__":
    main()
