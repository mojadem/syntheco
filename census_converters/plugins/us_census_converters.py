from census_converters import hookimpl
import pandas as pd

class us_census_global_converter:

    @hookimpl
    def read_raw_data_into_pandas(ip):
        print("I hate my country")
        return pd.DataFrame([3,3,3])

class us_census_summary_converter:

    @hookimpl
    def read_raw_data_into_pandas(ip):
        print("I love my country")
        return pd.DataFrame([4,4,4])
