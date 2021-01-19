import os
import shutil
import unittest
from tsdat.config import Config
from tsdat.io.storage import FilesystemStorage
from tsdat.pipeline import IngestPipeline

def copy_raw(raw_path):
    """-----------------------------------------------------------------------
    Copies the raw file into its parent's 'data' directory and returns the new
    path. This is done so that the IngestPipeline can safely move/delete the 
    file provided to its constructor without harming future test cases.
    -----------------------------------------------------------------------"""
    parent_dir, basename = os.path.split(raw_path)
    new_path = os.path.join(parent_dir, "data", basename)
    os.makedirs(os.path.join(parent_dir, "data"), exist_ok=True)
    shutil.copy(raw_path, new_path)
    return new_path


class TestIngestPipeline(unittest.TestCase):

    def test_ingest_pipeline(self):
        raw_file = "tsdat/tests/data/test1/buoy.z05.00.20200930.000000.temperature.csv"
        test_data = copy_raw(raw_file)

        config: Config = Config.load("tsdat/tests/data/test1/ingest_pipeline.yml")
        storage: FilesystemStorage = FilesystemStorage(root="tsdat/tests/data/test1/outputs")
        ingest: IngestPipeline = IngestPipeline(config, storage)
        ingest.run(test_data)

if __name__ == '__main__':
    unittest.main()
