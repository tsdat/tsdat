import numpy as np
import xarray as xr

from tsdat.qc.checkers import CheckOutliers


class TestCheckOutliers:
    test_data = xr.Dataset(
        {"variable": ("time", np.array([1, 2, 3, 4, 5, 6, 7, 8, 100, 10]))},
        coords={"time": np.arange(10)},
    )
    test_var = "variable"

    def test_check_outliers_no_outliers(self):
        checker = CheckOutliers(parameters=CheckOutliers.Parameters(n_std=5))
        result = checker.run(self.test_data, self.test_var)
        expected = np.zeros(10, dtype=bool)
        assert np.array_equal(
            result, expected
        ), f"Expected {expected}, but got {result}"

    def test_check_outliers_with_outliers(self):
        checker = CheckOutliers(parameters=CheckOutliers.Parameters(n_std=2))
        result = checker.run(self.test_data, self.test_var)
        expected = np.array(
            [False, False, False, False, False, False, False, False, True, False],
            dtype=bool,
        )
        assert np.array_equal(
            result, expected
        ), f"Expected {expected}, but got {result}"
