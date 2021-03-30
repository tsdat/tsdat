import os
import shutil
from tsdat.pipeline import IngestPipeline
from tsdat import Config, FilesystemStorage
from pipeline import BuoyIngestPipeline

# Folders where input data and configurations are stored
example_dir = os.path.abspath(os.path.dirname(__file__))
raw_dir = os.path.join(example_dir, 'storage/input')
root_dir = os.path.join(example_dir, 'storage/root')

def get_raw_file_copy(raw_filename, data_input="data/"):
    """Copies the raw file into the temporary raw folder representing the 
    pipeline input folder.  We need to do this because the pipeline will 
    remove the processed file from the input folder if it completes with no
    error."""
    file_to_copy = os.path.join(example_dir, data_input, raw_filename)
    pipeline_input_file = os.path.join(raw_dir, raw_filename)
    shutil.copy(file_to_copy, pipeline_input_file)
    return pipeline_input_file

# Make and/or clean folders for this ingest to allow for successive runs
os.makedirs(root_dir, exist_ok=True)
os.makedirs(raw_dir, exist_ok=True)
shutil.rmtree(root_dir)
shutil.rmtree(raw_dir)
os.makedirs(root_dir)
os.makedirs(raw_dir)

# Create storage structure for running on local filesystem
storage = FilesystemStorage(root_dir)

# Create structures for humboldt ingest
humboldt_config_file = os.path.join(example_dir, 'humboldt_config.yml')
humboldt_config = Config.load(humboldt_config_file)
humboldt_pipeline = BuoyIngestPipeline(humboldt_config, storage)

# Create structures for morro bay ingest
morro_config_file = os.path.join(example_dir, 'morro_config.yml')
morro_config = Config.load(morro_config_file)
morro_pipeline = BuoyIngestPipeline(morro_config, storage)

# Run the ingest for Humboldt
humboldt_raw_file = get_raw_file_copy('buoy.z05.00.20201201.000000.zip', 'data/humboldt')
humboldt_pipeline.run(humboldt_raw_file)

# Run the ingest for Morro Bay
morro_raw_file = get_raw_file_copy('buoy.z06.00.20201201.000000.zip','data/morro')
morro_pipeline.run(morro_raw_file)
