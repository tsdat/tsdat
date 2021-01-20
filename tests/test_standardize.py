import os
import unittest
import xarray as xr
import pandas as pd
import numpy as np
from tsdat.config import Config
from tsdat.pipeline import IngestPipeline


class TestStandardize(unittest.TestCase):
    """-------------------------------------------------------------------
    Test standardizing an XArray Dataset without running the full pipeline.
    -------------------------------------------------------------------"""

    def setUp(self):
        """-------------------------------------------------------------------
        Create a sample raw dataset to run the standardization against
        -------------------------------------------------------------------"""
        self.basedir = os.path.abspath(os.path.dirname(__file__))
        dataframe: pd.DataFrame = pd.read_csv(os.path.join(self.basedir, 'data/standardize/buoy.z05.00.20200925.000000.gill.csv'))
        self.raw_ds: xr.Dataset = dataframe.to_xarray()

    def test_standardize(self):
        # First load the config with the qc test definitions
        config = Config.load(os.path.join(self.basedir, 'data/standardize/standardize.yml'))

        # Now create a Pipeline
        pipeline = IngestPipeline(config=config, storage=None)

        # Now standardize our test data
        self.std_ds = pipeline.standardize(self.raw_ds)

        # Validate time conversion from raw
        time_dt64 = np.array(self.std_ds["time"].data, dtype='datetime64[s]')
        time_long = np.array(time_dt64, dtype=np.int64)
        assert time_long[0] == 1601017205, "Error: timestamp is not correct"  # 2020-09-25T07:00:05

        # Validate conversion of speed from km/s to m/s
        raw_speed = self.raw_ds["HorizontalSpeedAverage(Double)"].data
        std_speed = self.std_ds["HorizontalSpeedAverage"].data
        assert np.all(raw_speed * 1e3 == std_speed), "Error: km/s to m/s conversion is not correct"

        # Validate initialization of derived variable
        assert np.all(self.std_ds["WindDistance"].data == -9999), "Error: derived variable was not initialized to _FillValue"
        assert self.std_ds["WindDistance"].data.shape == self.std_ds["time"].shape, "Error: derived variable was not initialize with the correct shape"


if __name__ == '__main__':
    unittest.main()
