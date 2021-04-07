import os
import shutil
import sys
import unittest

# Add the examples directory to the pythonpath
test_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(test_dir)
examples_dir = os.path.join(project_dir, 'examples')


def _delete_dir(folder_path):
    if os.path.isdir(folder_path):
        shutil.rmtree(folder_path)


def run_example(folder_name):
    # Clean up the storage folder if it already exists
    _delete_dir(os.path.join(examples_dir, folder_name, 'storage'))

    package_dir = os.path.join(examples_dir, folder_name)
    sys.path.insert(0, package_dir)
    from pipeline.runner import run_pipeline
    run_pipeline()
    sys.path.pop(0)


class TestIngestPipeline(unittest.TestCase):
    """-------------------------------------------------------------------
    Test the full pipeline using the custom examples from the pipeline
    folder
    -------------------------------------------------------------------"""

    def test_a2e_buoy_ingest(self):
        run_example('a2e_buoy_ingest')

    def test_a2e_imu_ingest(self):
        run_example('a2e_imu_ingest')

    def test_a2e_lidar_ingest(self):
        run_example('a2e_lidar_ingest')

    def test_a2e_waves_ingest(self):
        run_example('a2e_waves_ingest')


if __name__ == '__main__':
    unittest.main()
