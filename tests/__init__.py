import os

DATA_PATH = os.path.join(os.path.dirname(__file__), "data")
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config")
STORAGE_PATH = os.path.join(os.path.dirname(__file__), "storage", "root", "test")

NON_MONOTONIC_CSV = os.path.join(DATA_PATH, "bad_time.csv")

PIPELINE_INVALID_CONFIG = os.path.join(CONFIG_PATH, "pipeline_invalid.yml")
PIPELINE_FAIL_CONFIG = os.path.join(CONFIG_PATH, "pipeline_fail_monotonic.yml")
PIPELINE_ROBUST_CONFIG = os.path.join(CONFIG_PATH, "pipeline_robust.yml")
STORAGE_CONFIG = os.path.join(CONFIG_PATH, "storage.yml")
