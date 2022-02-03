from global_tables import GlobalTables
from converters.canada_census_converter import canada_global_table_converter
from input import InputParams

ip = InputParams('canada.yaml')

print(ip)

can_converter = canada_global_table_converter(ip)
gt = GlobalTables(can_converter.geo_level, can_converter)

print("{}".format(str(gt)))
