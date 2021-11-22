import os
import sys
import shutil
import xarray as xr

from tsdat import DSUtil, FilesystemStorage, IngestPipeline
from tests import (
    EXPECTED_HUMBOLDT_BUOY_FILE,
    EXPECTED_HUMBOLDT_IMU_FILE,
    EXPECTED_HUMBOLDT_LIDAR_FILE,
    EXPECTED_HUMBOLDT_WAVES_FILE,
    EXPECTED_MORRO_BUOY_FILE,
    EXPECTED_MORRO_IMU_FILE,
    EXPECTED_MORRO_LIDAR_FILE,
    EXPECTED_MORRO_WAVES_FILE,
)


def delete_existing_outputs(storage_config_file: str):
    example_path = os.path.dirname(os.path.dirname(storage_config_file))
    storage_path = os.path.join(example_path, "storage")
    if os.path.isdir(storage_path):
        shutil.rmtree(storage_path)


def add_pipeline_module_to_path(storage_config_file: str):
    example_path = os.path.dirname(os.path.dirname(storage_config_file))
    sys.path = [example_path] + sys.path


def pipeline_produced_expected_directory_tree(pipeline: IngestPipeline) -> bool:
    # Need directory tree to look like:
    # {root}
    #   {loc}
    #       {datastream name}
    #           {datastream_name}.(date).(time).nc
    #           humboldt.buoy_z05-10min.a1.(date).(time).*.png (some number)
    #       {raw datastream name}
    #           {raw datastream name}.(date).(time).raw.* (any number > 0 allowed)

    # Naming parameters
    loc_id = pipeline.config.pipeline_definition.location_id
    level_in = pipeline.config.pipeline_definition.input_data_level
    datastream = DSUtil.get_datastream_name(config=pipeline.config)
    raw_datastream = datastream[:-2] + level_in

    storage: FilesystemStorage = pipeline.storage
    root = str(storage._root)

    path_to_loc_dir = os.path.join(root, loc_id)
    path_to_processed_dir = os.path.join(root, loc_id, datastream)
    path_to_raw_dir = os.path.join(root, loc_id, raw_datastream)

    # Top level
    assert os.path.isdir(root)
    assert loc_id in os.listdir(root)

    # Output location level
    assert os.path.isdir(path_to_loc_dir)
    assert datastream in os.listdir(path_to_loc_dir)
    assert raw_datastream in os.listdir(path_to_loc_dir)

    # Lowest-level – files
    assert os.listdir(path_to_processed_dir)
    assert os.listdir(path_to_raw_dir)

    return True


def execute_test(
    storage_config: str,
    pipeline_config: str,
    pipeline: IngestPipeline,
    input_filepath: str,
    expected_filepath: str,
):
    delete_existing_outputs(storage_config)
    add_pipeline_module_to_path(storage_config)

    _pipeline = pipeline(pipeline_config, storage_config)
    ds = _pipeline.run(input_filepath)
    expected_ds = xr.open_dataset(expected_filepath)
    xr.testing.assert_allclose(ds, expected_ds)
    assert pipeline_produced_expected_directory_tree(_pipeline)


def test_a2e_buoy_ingest_example():
    from examples.a2e_buoy_ingest import (
        BuoyIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=HUMBOLDT_CONFIG,
        pipeline=BuoyIngestPipeline,
        input_filepath=HUMBOLDT_FILE,
        expected_filepath=EXPECTED_HUMBOLDT_BUOY_FILE,
    )
    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=MORRO_CONFIG,
        pipeline=BuoyIngestPipeline,
        input_filepath=MORRO_FILE,
        expected_filepath=EXPECTED_MORRO_BUOY_FILE,
    )


def test_a2e_imu_ingest_example():
    from examples.a2e_imu_ingest import (
        ImuIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=HUMBOLDT_CONFIG,
        pipeline=ImuIngestPipeline,
        input_filepath=HUMBOLDT_FILE,
        expected_filepath=EXPECTED_HUMBOLDT_IMU_FILE,
    )
    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=MORRO_CONFIG,
        pipeline=ImuIngestPipeline,
        input_filepath=MORRO_FILE,
        expected_filepath=EXPECTED_MORRO_IMU_FILE,
    )


def test_a2e_lidar_ingest_example():
    from examples.a2e_lidar_ingest import (
        LidarIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=HUMBOLDT_CONFIG,
        pipeline=LidarIngestPipeline,
        input_filepath=HUMBOLDT_FILE,
        expected_filepath=EXPECTED_HUMBOLDT_LIDAR_FILE,
    )
    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=MORRO_CONFIG,
        pipeline=LidarIngestPipeline,
        input_filepath=MORRO_FILE,
        expected_filepath=EXPECTED_MORRO_LIDAR_FILE,
    )


def test_a2e_waves_ingest_example():
    from examples.a2e_waves_ingest import (
        WaveIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=HUMBOLDT_CONFIG,
        pipeline=WaveIngestPipeline,
        input_filepath=HUMBOLDT_FILE,
        expected_filepath=EXPECTED_HUMBOLDT_WAVES_FILE,
    )
    execute_test(
        storage_config=STORAGE_CONFIG,
        pipeline_config=MORRO_CONFIG,
        pipeline=WaveIngestPipeline,
        input_filepath=MORRO_FILE,
        expected_filepath=EXPECTED_MORRO_WAVES_FILE,
    )
