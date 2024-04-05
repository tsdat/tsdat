import xarray as xr
import numpy as np
from pytest import raises

from tsdat.qc.checkers.oceanography import CheckAcousticSignalCorrelation


class TestCheckAcousticSignalCorrelation:
    raw_data = np.array(([
        [0, 10, 20, 30, 40],
        [1, 11, 21, 31, 41],
        [2, 12, 22, 32, 42],
    ], [
        [35, 45, 65, 100, 120],
        [20, 40, 60, 80, 95],
        [22, 43, 73, 101, 131],
    ]))

    expected_results = {
        'default': np.array(([
            [True, True, True, False, False],
            [True, True, True, False, False],
            [True, True, True, False, False],
        ], [
            [False, False, False, False, False],
            [True, False, False, False, False],
            [True, False, False, False, False],
        ])),
        '55_above_eq': np.array(([
            [False, False, False, False, False],
            [False, False, False, False, False],
            [False, False, False, False, False],
        ], [
            [False, False, True, True, True],
            [False, False, True, True, True],
            [False, False, True, True, True],
        ]))
    }

    dataset = xr.Dataset(
        {'corr': (['beam', 'range', 'time'], raw_data)},
        coords={
            'beam': np.array([1, 2]),
            'range': np.array([0.5, 1.0, 1.5]),
            'time': np.array([0, 1, 2, 3, 4]),
        },
    )

    def test_defaults(self):
        checker = CheckAcousticSignalCorrelation()
        checker_result = checker.run(self.dataset, 'corr')
        assert np.all(checker_result == self.expected_results['default'])

    def test_55_above_eq(self):
        checker = CheckAcousticSignalCorrelation()
        checker.parameters.correlation_threshold = 55
        checker.parameters.below_above = 'above'
        checker.parameters.eq = True
        checker_result = checker.run(self.dataset, 'corr')
        assert np.all(checker_result == self.expected_results['55_above_eq'])

    def test_wrong_var_name(self):
        checker = CheckAcousticSignalCorrelation()
        with raises(KeyError):
            checker.run(self.dataset, 'notCorr')

    def test_invalid_below_above(self):
        checker = CheckAcousticSignalCorrelation()
        checker.parameters.below_above = 'invalid'
        with raises(ValueError):
            checker.run(self.dataset, 'corr')
