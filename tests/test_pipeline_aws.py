import os
import shutil
import unittest

from tsdat.config import Config
from tsdat.io import AwsStorage, S3Path
from tsdat.pipeline import IngestPipeline


class TestIngestPipeline(unittest.TestCase):

    def setUp(self) -> None:
        # path to local files
        testdir = os.path.abspath(os.path.dirname(__file__))
        self.basedir = os.path.join(testdir, 'data/pipeline')

        self.storage = AwsStorage(bucket_name='mhk-datalake-test',
                                  storage_root_path='tsdat/storage/root',
                                  storage_temp_path='tsdat/storage/temp')

        # Input directory where incoming raw files will be dropped
        self.raw = S3Path('mhk-datalake-test', 'tsdat/storage/raw')

    def tearDown(self) -> None:
        super().tearDown()

        # Clean up temporary folders
        self.storage.tmp.delete(self.storage.root)
        self.storage.tmp.delete(self.storage.temp_path)
        self.storage.tmp.delete(self.raw)

    def get_raw_file(self, raw_filename):
        """-----------------------------------------------------------------------
        Copies the raw file into the temporary raw folder representing the pipeline
        input folder.  We need to do this because the pipeline will remove the
        processed file from the input folder if it completes with no error.
        -----------------------------------------------------------------------"""
        original_raw_file = os.path.join(self.basedir, raw_filename)
        temp_raw_file = self.raw.join(raw_filename)
        self.storage.tmp.upload(original_raw_file, temp_raw_file)
        return temp_raw_file

    def test_temperature_one_day(self):
        raw_file = self.get_raw_file('buoy.z05.00.20200930.000000.temperature.csv')
        config_file = os.path.join(self.basedir, 'temperature_one_day.yml')

        config: Config = Config.load(config_file)

        pipeline: IngestPipeline = IngestPipeline(config, self.storage)
        pipeline.run(raw_file)

    def test_ingest_zip(self):
        raw_file = self.get_raw_file('buoy.z05.00.20201004.000000_no_gill_waves.zip')
        config_file = os.path.join(self.basedir, 'ingest_zip.yml')

        config: Config = Config.load(config_file)

        pipeline: IngestPipeline = IngestPipeline(config, self.storage)
        pipeline.run(raw_file)

    def test_ingest_tar(self):

        raw_file = self.get_raw_file('buoy.z05.00.20201004.000000_no_gill_waves.tar.gz')
        config_file = os.path.join(self.basedir, 'ingest_zip.yml')

        config: Config = Config.load(config_file)

        pipeline: IngestPipeline = IngestPipeline(config, self.storage)
        pipeline.run(raw_file)

if __name__ == '__main__':
    unittest.main()
