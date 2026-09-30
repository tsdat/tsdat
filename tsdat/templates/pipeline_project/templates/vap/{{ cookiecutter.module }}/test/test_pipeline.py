from pathlib import Path
import pytest
import xarray as xr
from tsdat import assert_close, PipelineConfig, TransformationPipeline


# DEVELOPER: Remove the skip after configuring an upstream ingest pipeline and
# updating paths to your configuration(s), test input(s), and expected results.
# The transformation pipeline usually depends on the output of an ingest pipeline.
# Set up that input as a fixture before enabling this test.
def test_{{ cookiecutter.module }}_pipeline():
    pytest.skip("Configure an upstream ingest pipeline and expected VAP output before running this test.")

    config_path = Path("pipelines/{{ cookiecutter.module }}/config/pipeline.yaml")
    config = PipelineConfig.from_yaml(config_path)
    pipeline: TransformationPipeline = config.instantiate_pipeline()  # type: ignore

    # Transformation pipelines require an input of [date.time, date.time] formatted as
    # YYYYMMDD.hhmmss. The start date is inclusive, the end date is exclusive. E.g., the
    # default below would run the pipeline on data from midnight on 2022-04-24 to just
    # before midnight on 2022-04-25:
    run_dates = ["20220424.000000", "20220425.000000"]
    dataset = pipeline.run(run_dates)

    # You will need to create this file after running the data through the pipeline
    # OR: Delete this and perform sanity checks on the input data instead of comparing
    # with an expected output file
    expected_file = "pipelines/{{ cookiecutter.module }}/test/data/expected/abc.example.c1.20220424.000000.nc"
    expected: xr.Dataset = xr.open_dataset(expected_file)  # type: ignore
    assert_close(dataset, expected, check_attrs=False)
