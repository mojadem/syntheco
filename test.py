from global_tables import GlobalTables
from converters.canada_census_converter import CanadaCensusGlobalTableConverter
from input import InputParams

ip = InputParams('canada.yaml')

print(ip)

can_converter = CanadaCensusGlobalTableConverter(ip)
gt = GlobalTables(can_converter.geo_level, can_converter)

print(f"{str(gt)}")
