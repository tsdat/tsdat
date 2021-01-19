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
        pipeline.standardize(self.raw_ds)



        # TODO: validate the results


if __name__ == '__main__':
    unittest.main()
