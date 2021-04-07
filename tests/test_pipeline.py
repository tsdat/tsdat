import os
import shutil
import sys
import unittest

# Add the examples directory to the pythonpath
test_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(test_dir)
examples_dir = os.path.join(project_dir, 'examples')
sys.path.append(examples_dir)

from a2e_buoy_ingest.runner import run_pipeline as run_buoy_ingest
from a2e_imu_ingest.runner import run_pipeline as run_imu_ingest
from a2e_lidar_ingest.runner import run_pipeline as run_lidar_ingest
from a2e_waves_ingest.runner import run_pipeline as run_waves_ingest


def _delete_dir(folder_path):
    if os.path.isdir(folder_path):
        shutil.rmtree(folder_path)


class TestIngestPipeline(unittest.TestCase):
    """-------------------------------------------------------------------
    Test the full pipeline using the custom examples from the pipeline
    folder
    -------------------------------------------------------------------"""

    def test_a2e_buoy_ingest(self):
        # Clean up the storage folder if it already exists
        _delete_dir(os.path.join(examples_dir, 'a2e_buoy_ingest/storage'))
        run_buoy_ingest()

    def test_a2e_imu_ingest(self):
        # Clean up the storage folder if it already exists
        _delete_dir(os.path.join(examples_dir, 'a2e_imu_ingest/storage'))
        run_imu_ingest()

    def test_a2e_lidar_ingest(self):
        # Clean up the storage folder if it already exists
        _delete_dir(os.path.join(examples_dir, 'a2e_lidar_ingest/storage'))
        run_lidar_ingest()

    def test_a2e_waes_ingest(self):
        # Clean up the storage folder if it already exists
        _delete_dir(os.path.join(examples_dir, 'a2e_waves_ingest/storage'))
        run_waves_ingest()


if __name__ == '__main__':
    unittest.main()
