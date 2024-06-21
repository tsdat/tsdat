import numpy as np
import xarray as xr

array_mask_data = {
    "defaults": {
        "data": np.array(
            (
                [
                    [0, 10, 20, 30, 40],
                    [1, 11, 21, 31, 41],
                    [2, 12, 22, 32, 42],
                ],
                [
                    [35, 45, 65, 100, 120],
                    [20, 40, 55, 80, 95],
                    [22, 43, 73, 101, 131],
                ],
            )
        ),
        "expected": np.array(
            (
                [
                    [True, True, True, False, False],
                    [True, True, True, False, False],
                    [True, True, True, False, False],
                ],
                [
                    [False, False, False, False, False],
                    [True, False, False, False, False],
                    [True, False, False, False, False],
                ],
            )
        ),
    },
    "55_gte": {
        "data": np.array(
            (
                [
                    [0, 10, 20, 30, 40],
                    [1, 11, 21, 31, 41],
                    [2, 12, 22, 32, 42],
                ],
                [
                    [35, 45, 65, 100, 120],
                    [20, 40, 55, 80, 95],
                    [22, 43, 73, 101, 131],
                ],
            )
        ),
        "expected": np.array(
            (
                [
                    [False, False, False, False, False],
                    [False, False, False, False, False],
                    [False, False, False, False, False],
                ],
                [
                    [False, False, True, True, True],
                    [False, False, True, True, True],
                    [False, False, True, True, True],
                ],
            )
        ),
    },
    "other": {
        "data": np.array(
            (
                [
                    [0, 10, 20, 30, 40],
                    [1, 11, 21, 31, 41],
                    [2, 12, 22, 32, 42],
                ],
                [
                    [35, 45, 65, 100, 120],
                    [20, 40, 55, 80, 95],
                    [22, 43, 73, 101, 131],
                ],
            )
        ),
    },
}

for key in array_mask_data:
    active_data = array_mask_data[key]["data"]
    array_mask_data[key]["data"] = xr.Dataset(
        {"corr": (["beam", "range", "time"], active_data)},
        coords={
            "beam": np.array([1, 2]),
            "range": np.array([0.5, 1.0, 1.5]),
            "time": np.array([0, 1, 2, 3, 4]),
        },
    )
