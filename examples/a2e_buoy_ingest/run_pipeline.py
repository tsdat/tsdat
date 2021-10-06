#!/usr/bin/env python

import os
import sys

# Add the pipeline package to the pythonpath
example_path = os.path.dirname(os.path.realpath(__file__))
package_path = os.path.join(example_path, "pipeline")
sys.path.append(package_path)
from pipeline.runner import run_pipeline


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
