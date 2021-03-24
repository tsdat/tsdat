import os
import shutil
from tsdat.pipeline import IngestPipeline
from tsdat import Config, FilesystemStorage
from sta_pipeline import StaPipeline
from sta_filehandler import StaFileHandler

# Folders where input data and configurations are stored
example_dir = os.path.abspath(os.path.dirname(__file__))
raw_dir = os.path.join(example_dir, 'storage/input')
root_dir = os.path.join(example_dir, 'storage/root')
os.makedirs(root_dir, exist_ok=True)

def get_raw_file_copy(raw_filename, data_input="data/input"):
    """-----------------------------------------------------------------------
    Copies the raw file into the temporary raw folder representing the pipeline
    input folder.  We need to do this because the pipeline will remove the
    processed file from the input folder if it completes with no error.
    -----------------------------------------------------------------------"""
    file_to_copy = os.path.join(example_dir, data_input, raw_filename)
    pipeline_input_file = os.path.join(raw_dir, raw_filename)
    shutil.copy(file_to_copy, pipeline_input_file)
    return pipeline_input_file

# Paths to the raw and config files used for this ingest
raw_file = get_raw_file_copy('lidar.z06.00.20201201.000000.sta')
config_file = os.path.join(example_dir, 'config.yml')

# Create necessary structures
storage = FilesystemStorage(root_dir)
config = Config.load(config_file)
pipeline = StaPipeline(config, storage)

# Run the ingest and remove the output if successful
# Create the necessary structures and run the ingest
# pipeline.run(raw_file)
# shutil.rmtree(root_dir)

# Process all available data
for file in sorted(os.listdir(os.path.join(example_dir, "data/input/"))):
    # Ignore non-ingest files (Like .DS_Store or other system files)
    if file.endswith(".sta"):
        raw_file = get_raw_file_copy(file)
        pipeline.run(raw_file)