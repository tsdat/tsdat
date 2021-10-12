import os

DATA_PATH = os.path.join(os.path.dirname(__file__), "data")
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config")
STORAGE_PATH = os.path.join(os.path.dirname(__file__), "storage", "root", "test")

NON_MONOTONIC_CSV = os.path.join(DATA_PATH, "bad_time.csv")
PROCESSED_NC = os.path.join(DATA_PATH, "processed_data.nc")

PIPELINE_INVALID_CONFIG = os.path.join(CONFIG_PATH, "pipeline_invalid.yml")
PIPELINE_FAIL_CONFIG = os.path.join(CONFIG_PATH, "pipeline_fail_monotonic.yml")
PIPELINE_ROBUST_CONFIG = os.path.join(CONFIG_PATH, "pipeline_robust.yml")
STORAGE_CONFIG = os.path.join(CONFIG_PATH, "storage.yml")


_expected_path = os.path.join(os.path.dirname(__file__), "expected")

_expected_files = {
    "HUMBOLDT_BUOY_FILE": "humboldt.buoy_z05-10min.a1.20201201.000000.nc",
    "HUMBOLDT_IMU_FILE": "humboldt.buoy_z05-imu.a1.20201201.000008.nc",
    "HUMBOLDT_LIDAR_FILE": "humboldt.buoy_z05-lidar-10min.a1.20201201.001000.nc",
    "HUMBOLDT_WAVES_FILE": "humboldt.buoy_z05-waves-20min.a1.20201201.000000.nc",
    "MORRO_BUOY_FILE": "morro.buoy_z06-10min.a1.20201201.000000.nc",
    "MORRO_IMU_FILE": "morro.buoy_z06-imu.a1.20201201.000011.nc",
    "MORRO_LIDAR_FILE": "morro.buoy_z06-lidar-10min.a1.20201201.001000.nc",
    "MORRO_WAVES_FILE": "morro.buoy_z06-waves-20min.a1.20201201.000000.nc",
}
_expected_files = {
    key: os.path.join(_expected_path, value) for key, value in _expected_files.items()
}


EXPECTED_HUMBOLDT_BUOY_FILE = _expected_files["HUMBOLDT_BUOY_FILE"]
EXPECTED_HUMBOLDT_IMU_FILE = _expected_files["HUMBOLDT_IMU_FILE"]
EXPECTED_HUMBOLDT_LIDAR_FILE = _expected_files["HUMBOLDT_LIDAR_FILE"]
EXPECTED_HUMBOLDT_WAVES_FILE = _expected_files["HUMBOLDT_WAVES_FILE"]

EXPECTED_MORRO_BUOY_FILE = _expected_files["MORRO_BUOY_FILE"]
EXPECTED_MORRO_IMU_FILE = _expected_files["MORRO_IMU_FILE"]
EXPECTED_MORRO_LIDAR_FILE = _expected_files["MORRO_LIDAR_FILE"]
EXPECTED_MORRO_WAVES_FILE = _expected_files["MORRO_WAVES_FILE"]
