from .ingest import IngestPipeline


class TransformationPipeline(IngestPipeline):
    # Support input from multiple sources
    # Support use of multiple datasets throughout pipeline
    # Support output of multiple datasets
    # Support transformation of data -- i.e. downsampling, upsampling
    ...
