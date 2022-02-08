"""
hookimpl

This module defines the plugin speck default implementation
"""


from census_converters import hookimpl


class CensusConverterSpec:
    """
    CensusConveterSpec

    wrapper class that defines the implmentation hooks for Plugins
    """
    @hookimpl
    def read_raw_data_into_pandas(cens_conv_inst):
        """
        read_raw_data_into_pandas implementation spec
        """
        return

    @hookimpl
    def pre_transform_clean_data(cens_conv_inst):
        """
        pre_transform_clean_data implmentation spec
        """
        return

    @hookimpl
    def post_transform_clean_data(cens_conv_inst):
        """
        post_transform_clean_data implementation spec
        """
        return

    @hookimpl
    def transform(cens_conv_inst):
        """
        transform implementation spec
        """
        return
