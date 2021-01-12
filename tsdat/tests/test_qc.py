from unittest import TestCase
import xarray as xr
import pandas as pd
import numpy as np
from tsdat.config import Config
from tsdat.qc import QC


class TestQC(TestCase):

    def setUp(self):
        """-------------------------------------------------------------------
        Create a sample dataset to run the tests against
        -------------------------------------------------------------------"""
        ds_dict = {
            'time': xr.DataArray(
                data=np.array([
                    pd.to_datetime(1498867200, unit='s'),
                    pd.to_datetime(1498867260, unit='s'),
                    pd.to_datetime(1498867320, unit='s')],
                    np.datetime64),
                dims=['time'],
                attrs={
                    'units': 'seconds since 1970-01-01T00:00:00'
                }
            ),
            'height': xr.DataArray(
                data=np.array([1000, 2000, 8000], np.float32),
                dims=['height'],
                attrs={
                    'units': 'millimeters'
                }
            ),
            'SWdown': xr.DataArray(
                data=np.array([1.23, 8.47, -9999], np.float32),
                dims=['time'],
                attrs={
                    '_FillValue': -9999,
                    'data_type': 'float',
                    'long_name': 'Shortwave Downwelling Radiation',
                    'units': 'W/m2',
                    'comment': 'Short-Wave Downwelling Radiation measured at ground level. Short-wave radiation (visible light) comes from the sun and contains much more energy than Long-wave radiation.'
                }
            ),
            'LWdown': xr.DataArray(
                data=np.array([1, 3, 800], np.float32),
                dims=['time'],
                attrs={
                    '_FillValue': -9999,
                    'fail_range': [1, 10],
                    'data_type': 'float',
                    'long_name': 'Longwave Downwelling Radiation',
                    'units': 'W/m2'
                }
            ),
            'foo': xr.DataArray(
                data=np.array([
                    [1, 2, 5],
                    [3, 4, 7],
                    [5, -9999, -9999]
                ], np.float32),
                dims=['time', 'height'],
                attrs={
                    '_FillValue': -9999,
                    'valid_delta': 2,
                    'data_type': 'float',
                    'long_name': 'junky test variable',
                    'units': 'W/m2'
                }
            ),
        }
        self.ds: xr.Dataset = xr.Dataset(ds_dict, attrs={'example_attr': 'this is a global attribute'})

    def test_qc_tests(self):
        # First load the config with the qc test definitions
        config = Config.load('data/qc/qc.yml')

        # Now apply the qc tests
        qc_dataset = QC.apply_tests(self.ds, config, None)

        # Validate the results


