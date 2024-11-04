from typing import Any, Optional, Literal
import numpy as np
import xarray as xr

from ._base_transform import _base_transform
from ..base import DataConverter, RetrievedDataset
from ...config.dataset import DatasetConfig
from ...io.retrievers import StorageRetriever


class BinAverage(DataConverter):
    """Saves data into the specified coordinate grid using bin-averaging."""

    coord: str = "time"
    """The coordinate axis this converter should be applied on. Defaults to 'time'."""

    def _create_bounds(
        self,
        coordinate: xr.DataArray,
        alignment: Literal["LEFT", "RIGHT", "CENTER"],
        width: str,
    ) -> np.ndarray:
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

        out = np.append(begin, end[-1])
        return out

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

        trans_input_ds = _base_transform(
            data,
            variable_name,
            dataset_config,
            input_dataset,
        )

        output_coord_names = dataset_config[variable_name].dims
        output_coord_data = {n: retrieved_dataset.coords[n] for n in output_coord_names}

        # Get transformation parameters based on global configurations, which can be
        # input-key specific and local configurations, if provided.
        # IDEA: Also pull in "local" configurations, e.g., parameters on 'self'
        # IDEA: Support coord-dependent local config, e.g., {time: 300s, height: 5m}
        trans_params = retriever.parameters.trans_params.select_parameters(input_key)

        # Fetch coordinate(s) to transform across
        transform_coords = (
            [self.coord] if hasattr(self, "coord") else output_coord_names
        )
        for coord_name in transform_coords:
            width = trans_params["width"].get(coord_name)
            align = trans_params["alignment"].get(coord_name, "CENTER")

            # Groupby_bins allows us to define the width and aligment based on timegrid
            # creates bounds based on alignment and width
            bounds = self._create_bounds(
                output_coord_data[coord_name],
                alignment=align,  # type: ignore
                width=width,
            )
            temp = trans_input_ds.groupby_bins(
                coord_name,
                bins=bounds,
                labels=output_coord_data[coord_name].values,
            )
            # Mean should be a custom function to handle QC? Or just mask out completely
            trans_output_ds = temp.mean()
            trans_output_ds = trans_output_ds.rename({coord_name + "_bins": coord_name})

        # Update the retrieved dataset object with the transformed data variable and
        # associated qc variable outputs.
        retrieved_dataset.data_vars[variable_name] = trans_output_ds[variable_name]
        if f"qc_{variable_name}" in trans_output_ds:
            retrieved_dataset.data_vars[f"qc_{variable_name}"] = trans_output_ds[
                f"qc_{variable_name}"
            ]

        return None
