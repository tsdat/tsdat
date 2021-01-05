import unittest
from tsdat.config import Config
from tsdat.io.storage import FilesystemStorage
from tsdat.pipeline import IngestPipeline


class TestIngestPipeline(unittest.TestCase):

    def test_ingest_pipeline(self):
        config: Config = Config.load(["data/test1/pipeline.yml"])
        storage: FilesystemStorage = FilesystemStorage("data/test1/outputs")
        ingest: IngestPipeline = IngestPipeline(config, storage)
        ingest.run("data/test1/buoy.z05.00.20200930.000000.temperature.csv")

if __name__ == '__main__':
    unittest.main()
