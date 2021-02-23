import os
import shutil
from tsdat import Config, FilesystemStorage
from gyro_pipeline import GyroPipeline
from gyro_filehandler import GyroFileHandler

# Folders where input data and configurations are stored
example_dir = os.path.abspath(os.path.dirname(__file__))
raw_dir = os.path.join(example_dir, 'storage/input')
root_dir = os.path.join(example_dir, 'storage/root')
os.makedirs(root_dir, exist_ok=True)

def get_raw_file_copy(raw_filename):
    """-----------------------------------------------------------------------
    Copies the raw file into the temporary raw folder representing the pipeline
    input folder.  We need to do this because the pipeline will remove the
    processed file from the input folder if it completes with no error.
    -----------------------------------------------------------------------"""
    file_to_copy = os.path.join(example_dir, "data/input", raw_filename)
    pipeline_input_file = os.path.join(raw_dir, raw_filename)
    shutil.copy(file_to_copy, pipeline_input_file)
    return pipeline_input_file

# Paths to the raw and config files used for this ingest
raw_file = get_raw_file_copy('lidar.z05.00.20201014.000000.gyro')
config_file = os.path.join(example_dir, 'config.yml')

# Create the necessary structures and run the ingest
storage = FilesystemStorage(root_dir)
config = Config.load(config_file)
pipeline = GyroPipeline(config, storage)
pipeline.run(raw_file)

# Remove processed data
shutil.rmtree(root_dir)