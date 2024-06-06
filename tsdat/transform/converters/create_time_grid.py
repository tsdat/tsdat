from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional, Tuple

import pandas as pd
import xarray as xr

from ...io.base import DataConverter, RetrievedDataset

# Prevent any chance of runtime circular imports for typing-only imports
if TYPE_CHECKING:  # pragma: no cover
    from ...config.dataset import DatasetConfig  # pragma: no cover
    from ...io.retrievers import StorageRetriever  # pragma: no cover


from ._create_bounds import _create_bounds


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
