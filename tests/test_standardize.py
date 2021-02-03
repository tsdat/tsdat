import os
import unittest
import shutil

import numpy as np
import pandas as pd
import xarray as xr
from tsdat.config import Config
from tsdat.io import FileHandler
from tsdat.pipeline import IngestPipeline
from tsdat.utils import DSUtil


class TestStandardize(unittest.TestCase):
    """-------------------------------------------------------------------
    Test standardizing an XArray Dataset without running the full pipeline.
    -------------------------------------------------------------------"""

    def setUp(self):
        """-------------------------------------------------------------------
        Create a sample raw dataset to run the standardization against
        -------------------------------------------------------------------"""
        basedir = os.path.abspath(os.path.dirname(__file__))
        self.basedir = os.path.join(basedir, 'data/standardize')

        # temp folder to write files to
        self.temp = os.path.join(self.basedir, 'temp')
        os.makedirs(self.temp, exist_ok=True)

    def tearDown(self) -> None:
        super().tearDown()

        # Clean up temporary folders
        shutil.rmtree(self.temp)

    def test_buoy_data(self):
        dataframe: pd.DataFrame = pd.read_csv(
            os.path.join(self.basedir, 'buoy.z05.00.20200925.000000.gill.csv'))
        raw_ds: xr.Dataset = dataframe.to_xarray()

        # First load the config with the qc test definitions
        config = Config.load(os.path.join(self.basedir, 'standardize.yml'))

        # Now create a Pipeline
        pipeline = IngestPipeline(config=config, storage=None)

        # Now standardize our test data
        std_ds = pipeline.standardize_dataset(raw_ds, {"buoy.z05.00.20200925.000000.gill.csv": None})

        self._check_time_conversion(raw_ds, std_ds)
        self._check_derived_variable_initialization(raw_ds, std_ds)
        self._check_predefined_variable_initialization(raw_ds, std_ds)
        self._check_speed_conversion(raw_ds, std_ds)

    def test_modaq_gps_data(self):
        config = Config.load(os.path.join(self.basedir, 'ingest_pipeline_template_WEC_gps_example.yml'))
        dictionary = config.dataset_definition.to_dict()
        dataset = xr.Dataset.from_dict(dictionary)
        filename = 'ingest_pipeline_template_WEC_gps_example.nc'
        FileHandler.write(dataset, os.path.join(self.temp, filename))

    def test_modaq_powraw_data(self):
        config = Config.load(os.path.join(self.basedir, 'ingest_pipeline_template_WEC_powraw_example.yml'))
        dictionary = config.dataset_definition.to_dict()
        dataset = xr.Dataset.from_dict(dictionary)
        filename = 'ingest_pipeline_template_WEC_powraw_example.nc'
        FileHandler.write(dataset, os.path.join(self.temp, filename))

    def _check_time_conversion(self, raw_ds, std_ds):
        # Validate time conversion from raw
        time_dt64 = np.array(std_ds["time"].data, dtype='datetime64[s]')
        time_long = np.array(time_dt64, dtype=np.int64)
        assert time_long[0] == 1601017205, "Error: timestamp is not correct"  # 2020-09-25T07:00:05

    def _check_speed_conversion(self, raw_ds, std_ds):
        # Validate conversion of speed from km/s to m/s
        raw_speed = raw_ds["HorizontalSpeedAverage(Double)"].data
        std_speed = std_ds["HorizontalSpeedAverage"].data
        assert np.all(raw_speed * 1e3 == std_speed), "Error: km/s to m/s conversion is not correct"

    def _check_derived_variable_initialization(self, raw_ds, std_ds):
        # Validate initialization of derived variable
        assert np.all(std_ds["WindDirection"].data == -9999), "Error: derived variable was not initialized to _FillValue"
        assert std_ds["WindDirection"].data.shape == std_ds["time"].shape, "Error: derived variable was not initialize with the correct shape"

    def _check_predefined_variable_initialization(self, raw_ds, std_ds):
        # Validate existance of predefined variable -- Planck's constant
        assert std_ds["PlanckConstant"].data == 6.62607015e-34, "Error: predefined variable initialized improperly"


if __name__ == '__main__':
    unittest.main()
