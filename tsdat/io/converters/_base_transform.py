from typing import List, Optional
import xarray as xr

from ...config.dataset import DatasetConfig  # pragma: no cover


def _base_transform(
    data: xr.DataArray,
    variable_name: str,
    dataset_config: "DatasetConfig",
    input_dataset: Optional[xr.Dataset] = None,
) -> xr.Dataset:

    assert input_dataset is not None

    output_coord_names = dataset_config[variable_name].dims
    input_coord_names = list(data.dims)  # type: ignore
    coord_rename_map = {
        _input_name: _output_name
        for _input_name, _output_name in zip(input_coord_names, output_coord_names)
        if _input_name != _output_name
    }

    # Build the input dataset to be transformed. At a minimum this should contain
    # the input coordinates, input data variable, and a placeholder qc variable. If
    # input bounds are available those will also be included. Also note that the
    # da.name property must be updated to match the output dataset structure.
    input_bounds_vars: List[xr.DataArray] = []
    for i, input_coord_name in enumerate(data.coords):  # type: ignore
        coord_bound = input_dataset.get(f"{input_coord_name}_bounds")
        if coord_bound is not None:
            coord_bound.name = f"{output_coord_names[i]}_bounds"
            input_bounds_vars.append(coord_bound)
    input_qc = input_dataset.get(f"qc_{data.name}", None)
    # Rename QC variable name if it changed in the retriever
    if input_qc is not None:
        input_qc.name = f"qc_{variable_name}"
        if hasattr(data, "ancillary_variables"):
            anc_vars = data.attrs["ancillary_variables"]
            if f"qc_{data.name}" in anc_vars:
                if isinstance(anc_vars, list):
                    anc_vars.remove(f"qc_{data.name}")
                    # Needs to be first for act-atmos qc code to work properly
                    anc_vars.insert(0, f"qc_{variable_name}")
                else:
                    anc_vars = f"qc_{variable_name}"
            data.attrs["ancillary_variables"] = anc_vars
    # Set dataarray
    data.name = variable_name
    trans_input_ds = xr.Dataset(
        coords=data.coords,  # type: ignore
        data_vars={
            v.name: v
            for v in [*input_bounds_vars, data, input_qc]
            if v is not None  # type: ignore
        },
    ).rename(coord_rename_map)
    # NAs must be filled in order for the transformation to work successfully
    trans_input_ds[variable_name].fillna(
        trans_input_ds[variable_name].attrs.get(
            "_FillValue",
            trans_input_ds[variable_name].encoding.get(
                "_FillValue",
                -9999,
            ),
        )
    )

    # Does adi_py drop the quality controlled values?? Yes it does...
    # Apply QC variable mask
    if input_qc is not None:
        trans_input_ds[variable_name] = trans_input_ds[variable_name].where(
            ~trans_input_ds[input_qc.name].astype(bool)
        )

    return trans_input_ds
