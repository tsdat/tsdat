import xarray as xr
from pathlib import Path
from tsdat import PipelineConfig, assert_close


def test_example_pipeline_pipeline():
    config_path = Path("pipelines/example_pipeline/config/pipeline.yaml")
    config = PipelineConfig.from_yaml(config_path)
    pipeline = config.instantiate_pipeline()

    test_file = "pipelines/example_pipeline/test/data/input/buoy.z06.00.20201201.000000.waves.csv"
    expected_file = "pipelines/example_pipeline/test/data/expected/morro.buoy_z06-waves.a1.20201201.000000.nc"

    dataset = pipeline.run([test_file])
    expected: xr.Dataset = xr.open_dataset(expected_file)  # type: ignore
    assert_close(dataset, expected, check_attrs=False)
