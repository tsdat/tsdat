from typing import Any, Optional
import numpy as np
import xarray as xr

from ._base_transform import _base_transform
from ..base import DataConverter, RetrievedDataset
from ...config.dataset import DatasetConfig
from ...io.retrievers import StorageRetriever


class NearestNeighbor(DataConverter):
    """Maps data onto the specified coordinate grid using nearest-neighbor."""

    coord: str = "time"
    """The coordinate axis this converter should be applied on. Defaults to 'time'."""

    def _get_tolerance(self, coordinate, rng):
        if rng is None:
            return None

        coord_vals = coordinate.data
        # TODO: handle for non-time units

        units = ""
        for i, s in enumerate(rng):
            if s.isalpha():
                units = rng[i:]
                rng = rng[:i]
        _rng = float(rng)

        if np.issubdtype(coordinate.dtype, np.datetime64):  # type: ignore
            coord_vals = np.array([np.datetime64(val) for val in coord_vals])
            _rng = np.timedelta64(int(_rng), units or "s")

        return _rng

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
        if retriever.parameters is not None:
            trans_params = retriever.parameters.trans_params.select_parameters(
                input_key
            )
        else:
            trans_params = {"range": {}}

        # Fetch coordinate(s) to transform across
        transform_coords = (
            [self.coord] if hasattr(self, "coord") else output_coord_names
        )
        for coord_name in transform_coords:
            # Create an empty DataArray with the shape we want to achieve
            target_coord = retrieved_dataset.coords[coord_name]
            coord_index = dataset_config[variable_name].dims.index(coord_name)
            current_coord_name = tuple(data.coords.keys())[coord_index]
            new_coords = {
                k: v.data if k != current_coord_name else target_coord.data
                for k, v in output_coord_data.items()
            }
            tmp_data = xr.DataArray(coords=new_coords, dims=tuple(new_coords))

            # Get index tolerance from coordinate
            rng = trans_params["range"].get(coord_name, None)
            tolerance = self._get_tolerance(data[current_coord_name], rng)

            # Do nearest neighbor algorithm
            trans_output_ds = trans_input_ds.reindex_like(
                other=tmp_data,
                method="nearest",
                tolerance=tolerance,  # type: ignore
            )

        # Update the retrieved dataset object with the transformed data variable and
        # associated qc variable outputs.
        retrieved_dataset.data_vars[variable_name] = trans_output_ds[variable_name]
        if f"qc_{variable_name}" in trans_output_ds:
            retrieved_dataset.data_vars[f"qc_{variable_name}"] = trans_output_ds[
                f"qc_{variable_name}"
            ]

        return None
