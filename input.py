"""
input.def
This is a class to read input files, validate against the Schema
and provide a dictinary of hte input parameters
"""

import pprint
import yaml
import input_schema


class InputParams:
    """
    InputParams class

    This class reads an input file for Syntheco and does all of the validation
    checking.
    """


    def __init__(self, input_file = "input.yml", input_schema_= input_schema._schema):
        """
        Instance Constructor

        Arguments:
            input_file: a string defining an existing yaml file with inputs
            input_schema_: the validation schema

        Returns:
            instance of InputParams
        """

        self.schema = input_schema_
        self.input_file = input_file
        self.input_params = self._read_yaml_file()

    def _read_yaml_file(self):
        """
        _read_yaml_file
        private member that reads and validates the input input_yaml

        Arguments:
            None

        Returns:
            dictionary of validated input parameters
        """

        _yml = None
        try:
            with open(self.input_file, "rb") as yaml_file:
                _yml = yaml.load(yaml_file,yaml.Loader)
            try:
                self.schema.validate(_yml)
                return _yml
            except Exception as err:
                print("error: {}".format(err))
        except Exception as err:
            print("InputParams Error, Unable to read input file {}\n{}".format(self.input_file,err))

    def __str__(self):
        pp_print = pprint.PrettyPrinter(indent=4)
        return pp_print.pformat(self.input_params)

"""
For Testing
"""
if __name__ == "__main__":
    i = InputParams('canada.yaml')
    print(i)
