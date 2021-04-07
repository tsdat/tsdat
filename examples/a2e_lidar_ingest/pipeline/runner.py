import os

from tsdat import DatastreamStorage
from .sta_pipeline import StaPipeline


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
    humboldt_pipeline = StaPipeline(humboldt_config, storage)
    humboldt_raw_file = os.path.join(data_dir, 'humboldt/lidar.z05.00.20201201.000000.sta')
    humboldt_pipeline.run(humboldt_raw_file)

    # Run the ingest for Morro Bay
    morro_config = os.path.join(config_dir, 'morro_config.yml')
    morro_pipeline = StaPipeline(morro_config, storage)
    morro_raw_file = os.path.join(data_dir, 'morro/lidar.z06.00.20201201.000000.sta')
    morro_pipeline.run(morro_raw_file)


if __name__ == "__main__":
    run_pipeline()
