import os
from .pipeline import WaveIngestPipeline


_example_dir = os.path.dirname(os.path.realpath(__file__))
_config_dir = os.path.join(_example_dir, "config")
_data_dir = os.path.join(_example_dir, "data")

STORAGE_CONFIG = os.path.join(_config_dir, "storage_config.yml")

HUMBOLDT_CONFIG = os.path.join(_config_dir, "humboldt_config.yml")
HUMBOLDT_FILE = os.path.join(
    _data_dir, "humboldt/buoy.z05.00.20201201.000000.waves.csv"
)

MORRO_CONFIG = os.path.join(_config_dir, "morro_config.yml")
MORRO_FILE = os.path.join(_data_dir, "morro/buoy.z06.00.20201201.000000.waves.csv")
