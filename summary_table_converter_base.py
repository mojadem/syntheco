"""
summary_table_converter_base

This is a base class to define summary table convertors for
taking raw census data and converting it to standard summary tables
"""

class SummaryTableConverterBase:

    def __init__(input_params, raw_data = None):
        print("This is the base class and should not be directly called")
        return

    def clean_data(self):
        '''
        TODO: Make this real error handling
        '''
        print("This is being called from the base class")
        return

    def extract_variable_data(self, variable_):
        print("This is being called from the base class")
        return
