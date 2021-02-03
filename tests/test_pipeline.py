import os
import shutil
import unittest

from tsdat.config import Config
from tsdat.io import FilesystemStorage
from tsdat.pipeline import IngestPipeline


class TestIngestPipeline(unittest.TestCase):

    def setUp(self) -> None:
        testdir = os.path.abspath(os.path.dirname(__file__))
        self.basedir = os.path.join(testdir, 'data/pipeline')

        # Root folder of datastream storage
        self.root = os.path.join(testdir, 'data/storage/root')
        os.makedirs(self.root, exist_ok=True)

        # Input directory where incoming raw files will be dropped
        self.raw = os.path.join(testdir, 'data/storage/raw')
        os.makedirs(self.raw, exist_ok=True)


    def tearDown(self) -> None:
        super().tearDown()

        # Clean up temporary folders
        shutil.rmtree(self.root)
        shutil.rmtree(self.raw)

    def get_raw_file(self, raw_filename):
        """-----------------------------------------------------------------------
        Copies the raw file into the temporary raw folder representing the pipeline
        input folder.  We need to do this because the pipeline will remove the
        processed file from the input folder if it completes with no error.
        -----------------------------------------------------------------------"""
        original_raw_file = os.path.join(self.basedir, raw_filename)
        temp_raw_file = os.path.join(self.raw, raw_filename)
        shutil.copy(original_raw_file, temp_raw_file)
        return temp_raw_file

    def test_temperature_one_day(self):
        raw_file = self.get_raw_file('buoy.z05.00.20200930.000000.temperature.csv')
        config_file = os.path.join(self.basedir, 'temperature_one_day.yml')

        storage: FilesystemStorage = FilesystemStorage(self.root)
        config: Config = Config.load(config_file)

        pipeline: IngestPipeline = IngestPipeline(config, storage)
        pipeline.run(raw_file)

    def test_ingest_zip(self):
        # raw_file = self.get_raw_file('buoy.z05.00.20201004.000000.zip')
        raw_file = self.get_raw_file('buoy.z05.00.20201004.000000_no_gill_waves.zip')
        config_file = os.path.join(self.basedir, 'ingest_zip.yml')

        storage: FilesystemStorage = FilesystemStorage(self.root)
        config: Config = Config.load(config_file)

        pipeline: IngestPipeline = IngestPipeline(config, storage)
        pipeline.run(raw_file)


if __name__ == '__main__':
    unittest.main()
