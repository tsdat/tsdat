import os
import shutil
import sys
import unittest

# Add the examples directory to the pythonpath
test_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(test_dir)
examples_dir = os.path.join(project_dir, 'examples')
sys.path.append(examples_dir)

from a2e_buoy_ingest.runner import run_pipeline as run_buoy_ingest

class TestIngestPipeline(unittest.TestCase):
    """-------------------------------------------------------------------
    Test the full pipeline using the custom examples from the pipeline
    folder
    -------------------------------------------------------------------"""

    def tearDown(self) -> None:
        super().tearDown()

        # Clean up storage folders
        shutil.rmtree(os.path.join(examples_dir, 'a2e_buoy_ingest/storage'))

    def test_a2e_buoy_ingest(self):
        run_buoy_ingest()


if __name__ == '__main__':
    unittest.main()
