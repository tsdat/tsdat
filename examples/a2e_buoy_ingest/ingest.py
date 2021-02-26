import os
import shutil
from tsdat import Config, FilesystemStorage
from pipeline import CustomIngestPipeline

# Folders where input data and configurations are stored
example_dir = os.path.abspath(os.path.dirname(__file__))
raw_dir = os.path.join(example_dir, 'storage/input')
root_dir = os.path.join(example_dir, 'storage/root')
os.makedirs(root_dir, exist_ok=True)

# Paths to the raw and config files used for this ingest
raw_file = os.path.join(raw_dir, 'buoy.z05.00.20201117.000000.zip')
config_file = os.path.join(example_dir, 'config.yml')

# Create the necessary structures and run the ingest
storage = FilesystemStorage(root_dir)
config = Config.load(config_file)
pipeline = CustomIngestPipeline(config, storage)
pipeline.run(raw_file)

# Remove processed data
shutil.rmtree(root_dir)