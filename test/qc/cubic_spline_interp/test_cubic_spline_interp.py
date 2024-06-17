import pandas as pd
import numpy as np

from tsdat.qc.handlers import CubicSplineInterp


class TestCubicSplineInterp:
    test_data = pd.read_csv(
        "./test/qc/cubic_spline_interp/cubic_spline_interp_test_data.csv"
    ).to_xarray()
    test_col = "col1"

    def test_cubic_spline_interp(self):
        active_data = self.test_data.copy(deep=True)
        handler = CubicSplineInterp()

        mask, missing_idx = self._build_missing_mask(
            n=len(active_data[self.test_col]),
            n_points=handler.parameters.n_points,
        )

        results = handler.run(active_data, variable_name=self.test_col, failures=mask)
        assert (
            results[self.test_col].values[missing_idx]
            != self.test_data[self.test_col].values[missing_idx]
        )

    @staticmethod
    def _build_missing_mask(n: int, n_points: int) -> tuple[np.ndarray[np.bool_], int]:
        valid_missing_idx = np.arange(n_points, n - n_points)
        missing_idx = np.random.choice(valid_missing_idx)
        mask = np.array([False] * n)
        mask[missing_idx] = True
        return mask, missing_idx
