from typing import Any, Optional
import xarray as xr

from ._base_transform import _base_transform
from ..base import DataConverter, RetrievedDataset
from ...config.dataset import DatasetConfig
from ...io.retrievers import StorageRetriever


class Interpolate(DataConverter):
    coord: str = "time"

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
            retriever,
            input_dataset,
            input_key,
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
            # Won't need __trim_dataset after this function
            # I don't know how to copy libtrans's use of the range parameter here,
            # as the range parameter is equivalent to the interpolated timegrid resolution
            # Maybe that's fine...
            trans_output_ds = trans_input_ds.interp(
                {coord_name: output_coord_data[coord_name]},
                method="linear",
                assume_sorted=True,
            )

        # Update the retrieved dataset object with the transformed data variable and
        # associated qc variable outputs.
        retrieved_dataset.data_vars[variable_name] = trans_output_ds[variable_name]
        retrieved_dataset.data_vars[f"qc_{variable_name}"] = trans_output_ds[
            f"qc_{variable_name}"
        ]

        return None
