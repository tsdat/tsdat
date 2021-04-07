#!/usr/bin/env python

import os
import sys

# Add the parent directory to the pythonpath
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

from a2e_buoy_ingest.runner import run_pipeline


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
