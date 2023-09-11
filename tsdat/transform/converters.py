from datetime import datetime
import logging
from typing import TYPE_CHECKING, Any, Dict, Hashable, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr

from ..io.base import DataConverter, RetrievedDataset

# Prevent any chance of runtime circular imports for typing-only imports
if TYPE_CHECKING:  # pragma: no cover
    from ..config.dataset import DatasetConfig  # pragma: no cover
    from ..io.retrievers import StorageRetriever  # pragma: no cover

__all__ = [
    "CreateTimeGrid",
    "Automatic",
    "BinAverage",
    "Interpolate",
    "NearestNeighbor",
]

logger = logging.getLogger(__name__)


def _create_bounds(
    coordinate: xr.DataArray,
    alignment: Literal["LEFT", "RIGHT", "CENTER"],
    width: str,
) -> xr.DataArray:
    """Creates coordinate bounds with the specified alignment and bound width."""
    coord_vals = coordinate.data
    # TODO: handle for units

    units = ""
    for i, s in enumerate(width):
        if s.isalpha():
            units = width[i:]
            width = width[:i]
    _width = float(width)

    if np.issubdtype(coordinate.dtype, np.datetime64):  # type: ignore
        coord_vals = np.array([np.datetime64(val) for val in coord_vals])
        _width = np.timedelta64(int(_width), units or "s")

    if alignment == "LEFT":
        begin = coord_vals
        end = coord_vals + _width
    elif alignment == "CENTER":
        begin = coord_vals - _width / 2
        end = coord_vals + _width / 2
    elif alignment == "RIGHT":
        begin = coord_vals - _width
        end = coord_vals

    bounds_array = np.stack((begin, end), axis=-1)  # type: ignore
    return xr.DataArray(
        bounds_array,
        dims=[coordinate.name, "bound"],
        coords={coordinate.name: coordinate},
    )


class CreateTimeGrid(DataConverter):
    interval: str
    """The frequency of time points. This is passed to pd.timedelta_range as the 'freq'
    argument. E.g., '30s', '5min', '10min', '1H', etc."""

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: Optional["DatasetConfig"] = None,
        retrieved_dataset: Optional[RetrievedDataset] = None,
        retriever: Optional["StorageRetriever"] = None,
        time_span: Optional[Tuple[datetime, datetime]] = None,
        input_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        if time_span is None:
            raise ValueError("time_span argument required for CreateTimeGrid variable")

        # TODO: if not time_span, then get the time range from the retrieved data

        start, end = time_span[0], time_span[1]
        time_deltas = pd.timedelta_range(
            start="0 days",
            end=end - start,
            freq=self.interval,
            closed="left",
        )
        date_times = time_deltas + start

        time_grid = xr.DataArray(
            name=variable_name,
            data=date_times,
            dims=variable_name,
            attrs={"units": "Seconds since 1970-01-01 00:00:00"},
        )

        if (
            retrieved_dataset is not None
            and input_key is not None
            and retriever is not None
            and retriever.parameters is not None
            and retriever.parameters.trans_params is not None
            and retriever.parameters.trans_params.select_parameters(input_key)
        ):
            params = retriever.parameters.trans_params.select_parameters(input_key)
            width = params["width"].get(variable_name)
            alignment = params["alignment"].get(variable_name)
            if width is not None and alignment is not None:
                bounds = _create_bounds(time_grid, alignment=alignment, width=width)
                retrieved_dataset.data_vars[f"{variable_name}_bound"] = bounds

        return time_grid


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
            logger.exception(
                "Encountered an error running transformer. Please ensure necessary"
                " dependencies are installed."
            )
            raise e

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


class Automatic(_ADIBaseTransformer):
    transformation_type: str = "TRANS_AUTO"


class BinAverage(_ADIBaseTransformer):
    transformation_type: str = "TRANS_BIN_AVERAGE"


class Interpolate(_ADIBaseTransformer):
    transformation_type: str = "TRANS_INTERPOLATE"


class NearestNeighbor(_ADIBaseTransformer):
    transformation_type: str = "TRANS_SUBSAMPLE"


# tsdat/
#   adi/  (current)
#       __init__.py
#       converters.py
#       transform.py
#   transform/ (proposed)
#       __init__.py
#       adi.py
#       converters.py
#
# retriever.yml
#   converters:
#       - classname: tsdat.io.converters.TransformNearest (Current)
#       - classname: tsdat.transform.NearestNeighbor (Proposed)
