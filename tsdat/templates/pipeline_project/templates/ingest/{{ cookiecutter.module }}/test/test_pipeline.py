import xarray as xr
from pathlib import Path
from tsdat import PipelineConfig, assert_close

# DEVELOPER: Update paths to your configuration(s), test input(s), and expected test
# results files.
def test_{{ cookiecutter.module }}_pipeline():
    config_path = Path("pipelines/{{ cookiecutter.module }}/config/pipeline.yaml")
    config = PipelineConfig.from_yaml(config_path)
    pipeline = config.instantiate_pipeline()

    test_file = "pipelines/{{ cookiecutter.module }}/test/data/input/{{ cookiecutter.location_id }}_data.csv"
    expected_file = "pipelines/{{ cookiecutter.module }}/test/data/expected/abc.example.a1.20220424.000000.nc"

    dataset = pipeline.run([test_file])
    expected: xr.Dataset = xr.open_dataset(expected_file)  # type: ignore
    assert_close(dataset, expected, check_attrs=False)
