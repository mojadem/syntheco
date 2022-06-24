import pytest
import random as rn

import util


class TestRandomRoundToInteger:
    def test_round_float_up(self):
        float_value = 1.2
        seed = 1

        rn.seed(1)
        assert rn.random() == 0.13436424411240122

        assert util.random_round_to_integer(float_value, seed) == 2

    def test_round_float_down(self):
        float_value = 1.1
        seed = 1

        rn.seed(1)
        assert rn.random() == 0.13436424411240122

        assert util.random_round_to_integer(float_value, seed) == 1
