from collections import defaultdict

import xarray as xr


def get_dataset_dim_groups(dataset: xr.Dataset) -> dict[tuple[str, ...], list[str]]:
    dim_groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for var_name, data_var in dataset.data_vars.items():
        dims = tuple(str(d) for d in data_var.dims)
        dim_groups[dims].append(var_name)

    return dim_groups
