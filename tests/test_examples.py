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


def pipeline_produced_expected_data(
    pipeline: IngestPipeline, expected_data_file: str
) -> bool:
    filename = os.path.basename(expected_data_file)

    # Retrieve the output data file
    loc_id = pipeline.config.pipeline_definition.location_id
    datastream = DSUtil.get_datastream_name(config=pipeline.config)
    root: str = pipeline.storage._root
    output_file = os.path.join(root, loc_id, datastream, filename)

    # Assert that the basename of the processed file and expected file match
    assert os.path.isfile(output_file)

    # Compare data and optionally attributes to ensure everything matches.
    ds_out: xr.Dataset = xr.open_dataset(output_file)
    ds_exp: xr.Dataset = xr.open_dataset(expected_data_file)

    return ds_out.equals(ds_exp)


def test_a2e_buoy_ingest_example():
    from examples.a2e_buoy_ingest import (
        BuoyIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    delete_existing_outputs(STORAGE_CONFIG)

    add_pipeline_module_to_path(STORAGE_CONFIG)

    humboldt_pipeline = BuoyIngestPipeline(HUMBOLDT_CONFIG, STORAGE_CONFIG)
    morro_pipeline = BuoyIngestPipeline(MORRO_CONFIG, STORAGE_CONFIG)

    humboldt_pipeline.run(HUMBOLDT_FILE)
    morro_pipeline.run(MORRO_FILE)

    assert pipeline_produced_expected_directory_tree(humboldt_pipeline)
    assert pipeline_produced_expected_directory_tree(morro_pipeline)

    assert pipeline_produced_expected_data(
        humboldt_pipeline, EXPECTED_HUMBOLDT_BUOY_FILE
    )
    assert pipeline_produced_expected_data(morro_pipeline, EXPECTED_MORRO_BUOY_FILE)


def test_a2e_imu_ingest_example():
    from examples.a2e_imu_ingest import (
        ImuIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    delete_existing_outputs(STORAGE_CONFIG)

    add_pipeline_module_to_path(STORAGE_CONFIG)

    humboldt_pipeline = ImuIngestPipeline(HUMBOLDT_CONFIG, STORAGE_CONFIG)
    morro_pipeline = ImuIngestPipeline(MORRO_CONFIG, STORAGE_CONFIG)

    humboldt_pipeline.run(HUMBOLDT_FILE)
    morro_pipeline.run(MORRO_FILE)

    assert pipeline_produced_expected_directory_tree(humboldt_pipeline)
    assert pipeline_produced_expected_directory_tree(morro_pipeline)

    assert pipeline_produced_expected_data(
        humboldt_pipeline, EXPECTED_HUMBOLDT_IMU_FILE
    )
    assert pipeline_produced_expected_data(morro_pipeline, EXPECTED_MORRO_IMU_FILE)


def test_a2e_lidar_ingest_example():
    from examples.a2e_lidar_ingest import (
        LidarIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    delete_existing_outputs(STORAGE_CONFIG)

    add_pipeline_module_to_path(STORAGE_CONFIG)

    humboldt_pipeline = LidarIngestPipeline(HUMBOLDT_CONFIG, STORAGE_CONFIG)
    morro_pipeline = LidarIngestPipeline(MORRO_CONFIG, STORAGE_CONFIG)

    humboldt_pipeline.run(HUMBOLDT_FILE)
    morro_pipeline.run(MORRO_FILE)

    assert pipeline_produced_expected_directory_tree(humboldt_pipeline)
    assert pipeline_produced_expected_directory_tree(morro_pipeline)

    assert pipeline_produced_expected_data(
        humboldt_pipeline, EXPECTED_HUMBOLDT_LIDAR_FILE
    )
    assert pipeline_produced_expected_data(morro_pipeline, EXPECTED_MORRO_LIDAR_FILE)


def test_a2e_waves_ingest_example():
    from examples.a2e_waves_ingest import (
        WaveIngestPipeline,
        HUMBOLDT_CONFIG,
        HUMBOLDT_FILE,
        MORRO_CONFIG,
        MORRO_FILE,
        STORAGE_CONFIG,
    )

    delete_existing_outputs(STORAGE_CONFIG)

    add_pipeline_module_to_path(STORAGE_CONFIG)

    humboldt_pipeline = WaveIngestPipeline(HUMBOLDT_CONFIG, STORAGE_CONFIG)
    morro_pipeline = WaveIngestPipeline(MORRO_CONFIG, STORAGE_CONFIG)

    humboldt_pipeline.run(HUMBOLDT_FILE)
    morro_pipeline.run(MORRO_FILE)

    assert pipeline_produced_expected_directory_tree(humboldt_pipeline)
    assert pipeline_produced_expected_directory_tree(morro_pipeline)

    assert pipeline_produced_expected_data(
        humboldt_pipeline, EXPECTED_HUMBOLDT_WAVES_FILE
    )
    assert pipeline_produced_expected_data(morro_pipeline, EXPECTED_MORRO_WAVES_FILE)
