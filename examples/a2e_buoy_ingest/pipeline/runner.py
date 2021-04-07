import os

from tsdat import DatastreamStorage
from .pipeline import BuoyIngestPipeline


def run_pipeline():

    pipeline_dir = os.path.dirname(os.path.realpath(__file__))
    example_dir = os.path.dirname(pipeline_dir)
    config_dir = os.path.join(example_dir, "config")
    data_dir = os.path.join(example_dir, "data")

    # Load the storage
    storage_config = os.path.join(config_dir, 'storage_config.yml')
    storage = DatastreamStorage.from_config(storage_config)

    # Run the ingest for Humboldt
    humboldt_config = os.path.join(config_dir, 'humboldt_config.yml')
    humboldt_pipeline = BuoyIngestPipeline(humboldt_config, storage)
    humboldt_raw_file = os.path.join(data_dir, 'humboldt/buoy.z05.00.20201201.000000.zip')
    humboldt_pipeline.run(humboldt_raw_file)

    # Run the ingest for Morro Bay
    morro_config = os.path.join(config_dir, 'morro_config.yml')
    morro_pipeline = BuoyIngestPipeline(morro_config, storage)
    morro_raw_file = os.path.join(data_dir, 'morro/buoy.z06.00.20201201.000000.zip')
    morro_pipeline.run(morro_raw_file)
