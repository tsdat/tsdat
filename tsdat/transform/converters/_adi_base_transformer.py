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


class _ADIBaseTransformer(DataConverter):
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
        trans_type: Dict[str, str] = {}
        if self.coord == "ALL":
            for coord_name in output_coord_names:
                trans_type[coord_name] = self.transformation_type
        else:
            for coord_name in output_coord_names:
                if coord_name == self.coord:
                    trans_type[coord_name] = self.transformation_type
                else:
                    trans_type[coord_name] = "TRANS_PASSTHROUGH"
        trans_params["transformation_type"] = trans_type

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
        input_qc.name = f"qc_{variable_name}"
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

        # Build the structure of the output dataset. This must contain correct
        # coordinates and bound(s) variables and should have placeholder variables for
        # the data variable being transformed and an output qc variable. The bounds are
        # constructed using the width and alignment transform parameters, if provided.
        # IDEA: Set default width to median of distance between coord points
        trans_output_ds = xr.Dataset(output_coord_data)
        for coord_name in output_coord_names:
            width = trans_params["width"].get(coord_name)
            align = trans_params["alignment"].get(coord_name, "CENTER")
            if (width is not None) and (align is not None):
                bounds = _create_bounds(
                    output_coord_data[coord_name],
                    alignment=align,  # type: ignore
                    width=width,
                )
                trans_output_ds[f"{coord_name}_bounds"] = bounds

        # Add empty data variable to the dataset
        trans_output_ds[variable_name] = xr.DataArray(
            coords=output_coord_data,
            dims=output_coord_names,
            attrs={"missing_value": -9999, "_FillValue": -9999, **data.attrs},
        ).fillna(-9999)

        # Add empty qc variable to the dataset
        trans_output_ds[f"qc_{variable_name}"] = (
            xr.DataArray(  # type: ignore
                coords=output_coord_data,
                dims=output_coord_names,
            )
            .fillna(0)
            .astype(int)
        )

        # ADI transformer cannot handle datetime coordinates that are not named 'time',
        # so we must rename datetime coordinates to 'time' before transforming. These
        # must then be renamed to their original values after the transformation.
        name_map: List[Tuple[Hashable, Hashable]] = []
        for coord_name, coord_da in trans_input_ds.coords.items():  # type: ignore
            if coord_name == "time":
                continue
            if np.issubdtype(coord_da.dtype, np.datetime64):  # type: ignore
                if name_map != []:
                    raise ValueError(
                        f"Currently {self.__repr_name__()} only supports transforming"
                        " one datetime-like coordinate"
                    )
                name_map.append((coord_name, "time"))
                name_map.append((f"{coord_name}_bounds", "time_bounds"))

                # We also have to update the transformation parameters
                for param_type in ["alignment", "range", "width"]:
                    if coord_name in trans_params[param_type]:
                        trans_params[param_type]["time"] = trans_params[param_type][
                            coord_name
                        ]
                        del trans_params[param_type][coord_name]
        trans_input_ds = trans_input_ds.rename(
            {old: new for (old, new) in name_map if old in trans_input_ds}
        )
        trans_output_ds = trans_output_ds.rename(
            {old: new for (old, new) in name_map if old in trans_output_ds}
        )

        # Do the transformation using python wrapper around arm.gov C-libraries. Note
        # that although the transformation is wrapped in a generic try/except block, the
        # program can still exit suddenly due to the C-libraries core-dumping or
        # otherwise crashing.
        # TODO: Improve error logging
        try:
            from tsdat.transform.adi import AdiTransformer

            transformer = AdiTransformer()
            transformer.transform(
                variable_name=variable_name,
                input_dataset=trans_input_ds,
                output_dataset=trans_output_ds,
                transform_parameters=trans_params,
            )
        except Exception as e:
            error_traceback(e)

        # Map renamed coordinates/variables to their original names
        trans_input_ds = trans_input_ds.rename(
            {new: old for (old, new) in name_map if new in trans_input_ds}
        )
        trans_output_ds = trans_output_ds.rename(
            {new: old for (old, new) in name_map if new in trans_output_ds}
        )

        # Update the retrieved dataset object with the transformed data variable and
        # associated qc variable outputs.
        retrieved_dataset.data_vars[variable_name] = trans_output_ds[variable_name]
        retrieved_dataset.data_vars[f"qc_{variable_name}"] = trans_output_ds[
            f"qc_{variable_name}"
        ]

        return None
