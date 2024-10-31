from typing import TYPE_CHECKING, Any, Dict, Hashable, List, Optional, Tuple

import numpy as np
import xarray as xr

from ...io.base import DataConverter, RetrievedDataset

# Prevent any chance of runtime circular imports for typing-only imports
if TYPE_CHECKING:  # pragma: no cover
    from ...config.dataset import DatasetConfig  # pragma: no cover
    from ...io.retrievers import StorageRetriever  # pragma: no cover

from ._create_bounds import _create_bounds
from .error_traceback import error_traceback


class _baseTransformer(DataConverter):
    transformation_type: str
    coord: str = "ALL"

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: "DatasetConfig",
        retrieved_dataset: RetrievedDataset,
        retriever: Optional["StorageRetriever"] = None,
        input_dataset: Optional[xr.Dataset] = None,
        input_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        if variable_name in dataset_config.coords:
            raise ValueError(
                f"{self.__repr_name__} cannot be used for coordinate variables."
                f" Offending coord: '{variable_name}'."
            )
        data = data.copy()  # prevent object metadata changes accidentally propagating

        assert retriever is not None
        assert retriever.parameters is not None
        assert retriever.parameters.trans_params is not None
        assert input_dataset is not None
        assert input_key is not None

        output_coord_names = dataset_config[variable_name].dims
        input_coord_names = list(data.dims)  # type: ignore
        coord_rename_map = {
            _input_name: _output_name
            for _input_name, _output_name in zip(input_coord_names, output_coord_names)
            if _input_name != _output_name
        }
        output_coord_data = {n: retrieved_dataset.coords[n] for n in output_coord_names}

        # Get transformation parameters based on global configurations, which can be
        # input-key specific and local configurations, if provided.
        # IDEA: Also pull in "local" configurations, e.g., parameters on 'self'
        # IDEA: Support coord-dependent local config, e.g., {time: 300s, height: 5m}
        trans_params = retriever.parameters.trans_params.select_parameters(input_key)

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
        input_qc = input_dataset.get(
            f"qc_{data.name}",
            xr.full_like(data, 0).astype(int),  # type: ignore
        )
        # Rename QC variable name if it changed in the retriever
        input_qc.name = f"qc_{variable_name}"
        if hasattr(data, "ancillary_variables"):
            anc_vars = data.attrs["ancillary_variables"]
            if f"qc_{data.name}" in anc_vars:
                if isinstance(anc_vars, list):
                    anc_vars.remove(f"qc_{data.name}")
                    # Needs to be first for act-atmos qc code to work properly
                    anc_vars.insert(f"qc_{variable_name}")
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
        # Fetch coordinate(s) to transform across
        transform_coords = (
            [self.coord] if hasattr(self, "coord") else output_coord_names
        )
        for coord_name in transform_coords:
            # Resample only works for time variables
            # if not np.issubdtype(output_coord_data[coord_name].dtype, np.datetime64):
            #     raise KeyError(
            #         f"Non-time variable {coord_name} cannot be transformed. "
            #         f"Please specify time coordinate in the data converter entry for variable {variable_name}."
            #     )
            width = trans_params["width"].get(coord_name)
            align = trans_params["alignment"].get(coord_name, "CENTER")
            # align = None if (align == "CENTER") else align  # for resample
            # origin = output_coord_data[coord_name][0].values
            # Width should be the timegrid interval or the original timeseries
            # args = {"indexer": {coord_name: width}, "label": align, "origin": origin}

            if self.method == "bin_average":
                # Won't necessarily conform to timestamp in `origin`
                # Handled via self.__trimdataset after this function
                # Build this into act-atmos

                # Groupby_bins allows us to define the width and aligment based on timegrid
                # creates bounds based on alignment and width
                bounds = _create_bounds(
                    output_coord_data[coord_name],
                    alignment=align,  # type: ignore
                    width=width,
                )
                temp = trans_input_ds.groupby_bins(
                    coord_name,
                    bins=np.append(bounds[:, 0].values, bounds[-1, 1].values),
                    labels=output_coord_data[coord_name].values,
                )
                # Mean should be a custom function to handle QC? Or just mask out completely
                trans_output_ds = temp.mean()
                # We don't have control over the timegrid with resample
                # trans_output_ds = trans_input_ds.resample(**args).mean()
            else:
                # Apply QC first for interpolation method
                # Won't need __trim_dataset after this functiony
                # I don't know how to copy libtrans's use of the range parameter here,
                # as the range parameter is equivalent to the interpolated timegrid resolution
                # Maybe that's fine...
                # if self.method != "nearest":

                # Does adi_py drop the quality controlled values?? Yes it does...

                # Apply QC variable mask if using interpolation
                # Nearest neighbor QC will still line up
                trans_input_ds[variable_name] = trans_input_ds[variable_name].where(
                    ~trans_input_ds[input_qc.name].astype(bool)
                )

                trans_output_ds = trans_input_ds.interp(
                    {coord_name: output_coord_data[coord_name]},
                    method=self.method,
                    assume_sorted=True,
                )

        # Update the retrieved dataset object with the transformed data variable and
        # associated qc variable outputs.
        retrieved_dataset.data_vars[variable_name] = trans_output_ds[variable_name]
        retrieved_dataset.data_vars[f"qc_{variable_name}"] = trans_output_ds[
            f"qc_{variable_name}"
        ]

        return None
