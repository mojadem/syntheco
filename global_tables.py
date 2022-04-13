"""
global_tables

This class that holds the standard tables that are needed across
the synthetic ecosystem generation process.

"""


class GlobalTables:
    """
    GlobalTables class

    This is a class to hold the standard tables needed for various aspects
    of the Syntheco ecosystem generation system.
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
        return '\n'.join(["Global Tables",
                          "------------------------------------------------------"] +
                         [f"{x.name}\n{x}" for x in self.data.values()])
