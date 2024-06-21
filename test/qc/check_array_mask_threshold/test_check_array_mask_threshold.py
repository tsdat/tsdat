import numpy as np
from pytest import raises

from tsdat.qc.checkers import CheckArrayMaskThreshold
from .array_mask_data import array_mask_data


class TestCheckArrayMaskThreshold:
    def test_defaults(self):
        checker = CheckArrayMaskThreshold()
        checker_result = checker.run(array_mask_data["defaults"]["data"], "corr")
        assert np.all(checker_result == array_mask_data["defaults"]["expected"])

    def test_55_gte(self):
        checker = CheckArrayMaskThreshold()
        checker.parameters.correlation_threshold = 55
        checker.parameters.comparitor = ">="
        checker_result = checker.run(array_mask_data["55_gte"]["data"], "corr")
        assert np.all(checker_result == array_mask_data["55_gte"]["expected"])

    def test_wrong_var_name(self):
        checker = CheckArrayMaskThreshold()
        with raises(KeyError):
            checker.run(array_mask_data["other"]["data"], "notCorr")

    def test_invalid_comparitor(self):
        checker = CheckArrayMaskThreshold()
        checker.parameters.comparitor = "invalid"
        with raises(ValueError):
            checker.run(array_mask_data["other"]["data"], "corr")
