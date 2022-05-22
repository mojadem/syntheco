"""
util

Module that captures utility functions needed for Synthetic population
generation
"""

import math
import random as rn


def random_round_to_integer(float_value, seed=None):
    """
    Function that implements a more fair rounding based on a threshold
    rather than straing rounding.

    float_value: The value you want to round to an integer
    seed: Specify a seed for reproducibility

    returns float value that is rounded to an integer

    """
    threshold = float_value - int(float_value)
    r_num = rn.random()
    if r_num < threshold:
        return math.ceil(float_value)
    else:
        return math.floor(float_value)
