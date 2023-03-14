# IDEA: Implement MultiDimensionalGrouper to group collection of 1D variables into a 2D
# variable. (will need a better name)
# IDEA: Use the flyweight pattern to limit memory usage if identical converters would
# be created.
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple

import act  # type: ignore
import numpy as np
import pandas as pd
import xarray as xr
from numpy.typing import NDArray
from pydantic import validator
from xarray.core.dtypes import is_datetime_like  # type: ignore

from .base import DataConverter, RetrievedDataset

if TYPE_CHECKING:
    # Prevent any chance of runtime circular imports for typing-only imports
    from ..config.dataset import DatasetConfig
    from .retrievers import StorageRetriever


__all__ = [
    "UnitsConverter",
    "StringToDatetime",
    "NearestNeighbor",
    "CreateTimeGrid",
    "TransformAuto",
    "TransformAverage",
    "TransformInterpolate",
    "TransformNearest",
]

logger = logging.getLogger(__name__)


# IDEA: "@data_converter()" decorator so DataConverters can be defined as functions in
# user code. Arguments to data_converter can be parameters to the class.


def _create_bounds(
    coordinate: xr.DataArray,
    alignment: Literal["LEFT", "RIGHT", "CENTER"],
    width: Any,
) -> xr.DataArray:
    """Creates coordinate bounds with the specified alignment and bound width."""
    coord_vals = coordinate.data

    units = ""
    if isinstance(width, str):
        for i, s in enumerate(width):
            if s.isalpha():
                break
        width, units = float(width[:i]), width[i:]

    if np.issubdtype(coordinate.dtype, np.datetime64):  # type: ignore
        coord_vals = np.array([np.datetime64(val) for val in coord_vals])
        width = np.timedelta64(int(width), units or "s")

    if alignment == "LEFT":
        begin = coord_vals
        end = coord_vals + width
    elif alignment == "CENTER":
        begin = coord_vals - width / 2
        end = coord_vals + width / 2
    elif alignment == "RIGHT":
        begin = coord_vals - width
        end = coord_vals

    bounds_array = np.stack((begin, end), axis=-1)  # type: ignore
    return xr.DataArray(
        bounds_array,
        dims=[coordinate.name, "bound"],
        coords={coordinate.name: coordinate},
    )


class UnitsConverter(DataConverter):
    """---------------------------------------------------------------------------------
    Converts the units of a retrieved variable to specified output units.

    If the 'input_units' property is set then that string is used to determine the input
    input units, otherwise the converter will attempt to look up and use the 'units'
    attribute on the specified variable in the dataset provided to the `convert` method.
    If the input units cannot be set then a warning is issued and the original dataset
    is returned. The output units are specified by the output dataset configuration.

    Args:
        input_units (Optional[str]): The units that the retrieved data comes in.

    ---------------------------------------------------------------------------------"""

    # IDEA: Validate the literal value of the input units string according to pint
    input_units: Optional[str] = None
    """The units of the input data."""

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: "DatasetConfig",
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        input_units = self._get_input_units(data)
        if not input_units:
            logger.warning(
                "Input units for variable '%s' could not be found. Please ensure these"
                " are set either in the retrieval configuration file, or are set on the"
                " 'units' attribute of the '%s' variable in the dataset to converted.",
                variable_name,
                variable_name,
            )
            return None

        output_units = dataset_config[variable_name].attrs.units
        if (
            not output_units
            or output_units == "1"
            or output_units == "unitless"
            or input_units == output_units
        ):
            if not output_units:
                logger.warning(
                    "Output units for variable %s could not be found. Please ensure"
                    " these are set in the dataset configuration file for the specified"
                    " variable.",
                    variable_name,
                )
            return None

        converted: NDArray[Any] = act.utils.data_utils.convert_units(  # type: ignore
            data=data.data,
            in_units=input_units,
            out_units=output_units,
        )
        data_array = data.copy(data=converted)
        data_array.attrs["units"] = output_units
        logger.debug(
            "Converted '%s's units from '%s' to '%s'",
            variable_name,
            input_units,
            output_units,
        )
        return data_array

    def _get_input_units(self, data: xr.DataArray) -> str:
        units = ""
        if self.input_units:
            units = self.input_units
        elif "units" in data.attrs:
            units = data.attrs["units"]
        return units


class StringToDatetime(DataConverter):
    """------------------------------------------------------------------------------------
    Converts date strings into datetime64 data.

    Allows parameters to specify the  string format of the input data, as well as the
    timezone the input data are reported in. If the input timezone is not UTC, the data
    are converted to UTC time.

    Args:
        format (Optional[str]): The format of the string data. See strftime.org for more
            information on what components can be used. If None (the default), then
            pandas will try to interpret the format and convert it automatically. This
            can be unsafe but is not explicitly prohibited, so a warning is issued if
            format is not set explicitly.
        timezone (Optional[str]): The timezone of the input data. If not specified it is
            assumed to be UTC.
        to_datetime_kwargs (Dict[str, Any]): A set of keyword arguments passed to the
            pandas.to_datetime() function as keyword arguments. Note that 'format' is
            already included as a keyword argument. Defaults to {}.

    ------------------------------------------------------------------------------------"""

    format: Optional[str] = None
    """The date format the string is using (e.g., '%Y-%m-%d %H:%M:%S' for date strings
    such as '2022-04-13 23:59:00'), or None (the default) to have pandas guess the
    format automatically."""

    timezone: Optional[str] = None
    """The timezone of the data to convert. If provided, this converter will apply the
    appropriate offset to convert data from the specified timezone to UTC. The timezone
    of the output data is assumed to always be UTC."""

    to_datetime_kwargs: Dict[str, Any] = {}
    """Any parameters set here will be passed to `pd.to_datetime` as keyword
    arguments."""

    @validator("format")
    @classmethod
    def warn_if_no_format_set(cls, format: Optional[str]) -> Optional[str]:
        if not format:
            logger.warning(
                "No string format was provided for the StringToDatetime converter. This"
                " may lead to incorrect behavior in some cases. It is recommended to"
                " set the 'format' parameter explicitly to prevent ambiguous dates."
            )
        return format

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: "DatasetConfig",
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        dt: Any = pd.to_datetime(
            data.data,
            format=self.format,
            **self.to_datetime_kwargs,
        )

        if self.timezone and self.timezone != "UTC":
            dt = dt.tz_localize(self.timezone).tz_convert("UTC")  # type: ignore
            # Numpy can't handle localized datetime arrays so we force the datetime to
            # be naive (i.e. timezone 'unaware').
            dt = dt.tz_localize(None)  # type: ignore

        dtype = dataset_config[variable_name].dtype
        converted: NDArray[Any] = np.array(dt, dtype=dtype)  # type: ignore

        if variable_name in dataset_config.coords:
            data_array = xr.DataArray(
                data=converted,
                dims=data.dims,
                coords={variable_name: converted},
                attrs=data.attrs,
                name=variable_name,
            )
        else:
            data_array = xr.DataArray(
                data=converted,
                dims=data.dims,
                coords=data.coords,
                attrs=data.attrs,
                name=variable_name,
            )
        return data_array


class NearestNeighbor(DataConverter):
    """Maps data onto the specified coordinate grid using nearest-neighbor."""

    coord: str = "time"
    """The coordinate axis this converter should be applied on. Defaults to 'time'."""

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: "DatasetConfig",
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        # Assume that the coord index in the output matches coord index in the retrieved
        # structure.
        target_coord = retrieved_dataset.coords[self.coord]
        coord_index = dataset_config[variable_name].dims.index(self.coord)
        current_coord_name = tuple(data.coords.keys())[coord_index]

        # Create an empty DataArray with the shape we want to achieve
        new_coords = {
            k: v.data if k != current_coord_name else target_coord.data
            for k, v in data.coords.items()
        }
        tmp_data = xr.DataArray(coords=new_coords, dims=tuple(new_coords))

        # Resample the data using nearest neighbor
        new_data = data.reindex_like(other=tmp_data, method="nearest")

        return new_data


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
        time_span: Optional[Tuple[str, str]] = None,
        input_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        if time_span is None:
            raise ValueError("time_span argument required for CreateTimeGrid variable")

        # TODO: if not time_span, then get the time range from the retrieved data

        start = pd.to_datetime(time_span[0], format="%Y%m%d.%H%M%S")  # type: ignore
        end = pd.to_datetime(time_span[1], format="%Y%m%d.%H%M%S")  # type: ignore
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
            and (
                params := retriever.parameters.trans_params.select_parameters(input_key)
            )
            is not None
        ):
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
        input_coord_names = list(data.coords)
        coord_rename_map = {
            _input_name: _output_name
            for _input_name, _output_name in zip(input_coord_names, output_coord_names)
        }

        # Input dataset structure
        input_qc = input_dataset.get(
            f"qc_{data.name}", xr.full_like(data, 0).astype(int)
        )
        input_qc.name = f"qc_{variable_name}"  # rename to match output

        input_bounds_vars: List[xr.DataArray] = []
        for i, input_coord_name in enumerate(data.coords):
            coord_bound = input_dataset.get(f"{input_coord_name}_bounds")
            if coord_bound is not None:
                coord_bound.name = f"{output_coord_names[i]}_bounds"  # rename
                input_bounds_vars.append(coord_bound)

        data.name = variable_name
        trans_input_ds = xr.Dataset(
            coords=data.coords,
            data_vars={
                v.name: v for v in [data, input_qc, *input_bounds_vars] if v is not None
            },
        ).rename(coord_rename_map)

        # NAs must be filled in order for the transformation to work successfully
        trans_input_ds[variable_name].fillna(
            trans_input_ds[variable_name].attrs.get(
                "_Fillvalue",
                trans_input_ds[variable_name].encoding.get("_FillValue", -9999),
            )
        )

        # Output dataset structure
        # Just contains the coordinates
        output_coord_data = {n: retrieved_dataset.coords[n] for n in output_coord_names}
        trans_output_ds = xr.Dataset(output_coord_data)
        trans_output_ds[variable_name] = xr.DataArray(
            coords=output_coord_data,
            dims=output_coord_names,
            attrs={"missing_value": -9999, "_FillValue": -9999, **data.attrs},
        ).fillna(-9999)
        trans_output_ds[f"qc_{variable_name}"] = xr.full_like(
            trans_output_ds[variable_name],
            fill_value=0,
        ).astype(int)

        trans_params = retriever.parameters.trans_params.select_parameters(input_key)
        for coord_name in output_coord_names:
            # TODO optionally set default width to median of distance between points
            width = trans_params["width"].get(coord_name)
            align = trans_params["alignment"].get(coord_name)
            if (width is not None) and (align is not None):
                # TODO: Get bounds if they already exist?
                bounds = _create_bounds(
                    output_coord_data[coord_name], alignment=align, width=width
                )
                trans_output_ds[f"{coord_name}_bounds"] = bounds
            # TODO: Try to get bounds created by the CreateTimeGrid converter

        trans_type: Dict[str, str] = {}
        if self.coord == "ALL":
            for coord_name in output_coord_names:
                trans_type[coord_name] = self.transformation_type
        elif isinstance(self.coord, str):  # TODO: possibly support list of str
            for coord_name in output_coord_names:
                if coord_name == self.coord:
                    trans_type[coord_name] = self.transformation_type
                else:
                    trans_type[coord_name] = "TRANS_PASSTHROUGH"
        trans_params["transformation_type"] = trans_type

        try:
            from tsdat.adi.transform import AdiTransformer

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

        retrieved_dataset.data_vars[variable_name] = trans_output_ds[variable_name]
        retrieved_dataset.data_vars[f"qc_{variable_name}"] = trans_output_ds[
            f"qc_{variable_name}"
        ]

        return None


class TransformAuto(_ADIBaseTransformer):
    transformation_type: str = "TRANS_AUTO"


class TransformAverage(_ADIBaseTransformer):
    transformation_type: str = "TRANS_BIN_AVERAGE"


class TransformInterpolate(_ADIBaseTransformer):
    transformation_type: str = "TRANS_INTERPOLATE"


class TransformNearest(_ADIBaseTransformer):
    transformation_type: str = "TRANS_SUBSAMPLE"
