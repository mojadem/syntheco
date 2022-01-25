"""
summary tables

This class is a base class for summary tables needed to
create sythetic ecosystems. This class should provide a ready representation
to be able to perform the Iterative Proportional Fitting procedure and should
be completely independent from where the data comes
"""


import pandas as pd

class SummaryTable:
    def __init__(summary_variables_ = None, geo_unit = None, data = pd.DataFrame()):
        """
        Creation Operator
        """

        self.summary_variables = summary_variables_
        self.geo_unit = geo_unit_
        self.data = data

        if self.validate():



    def validate(self):
        """
        This is a function that will validate that the data for the Summary
        Table is correct
        """

        return True