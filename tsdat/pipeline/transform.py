import xarray as xr

from typing import Dict, List

from tsdat.pipeline.pipeline import BasePipeline


class TransformationPipeline(BasePipeline):
    # Support input from multiple sources
    # Support use of multiple datasets throughout pipeline
    # Support output of multiple datasets
    # Support transformation of data -- i.e. downsampling, upsampling,

    def run(self, inputs: List[str]) -> Dict[str, xr.Dataset]:
        ...
