from datetime import timedelta
import pandas as pd
import xarray as xr
from pydantic import BaseModel, Field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)

from .global_arm_transform_params import GlobalARMTransformParams
from .global_fetch_params import GlobalFetchParams
from .storage_retriever_input import StorageRetrieverInput
from .perform_data_retrieval import perform_data_retrieval
from ...config.dataset import DatasetConfig
from ..base import (
    Retriever,
    Storage,
)
from ...const import InputKey


class StorageRetriever(Retriever):
    """Retriever API for pulling input data from the storage area."""

    class TransParameters(BaseModel):
        trans_params: Optional[GlobalARMTransformParams] = Field(
            default=None, alias="transformation_parameters"
        )
        fetch_params: Optional[GlobalFetchParams] = Field(
            default=None, alias="fetch_parameters"
        )

    parameters: Optional[TransParameters] = None

    # TODO: `input_data_hook` is not included in docstring.
    def retrieve(
        self,
        input_keys: List[str],
        dataset_config: DatasetConfig,
        storage: Optional[Storage] = None,
        input_data_hook: Optional[
            Callable[[Dict[str, xr.Dataset]], Dict[str, xr.Dataset]]
        ] = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Retrieves input data from the storage area.

        Note that each input_key is expected to be formatted according to the following
        format:

        ```python
        "--key1 value1 --key2 value2",
        ```

        e.g.,

        ```python
        "--datastream sgp.met.b0 --start 20230801 --end 20230901"
        "--datastream sgp.met.b0 --start 20230801 --end 20230901 --location_id sgp --data_level b0"
        ```

        This format allows the retriever to pull datastream data from the Storage API
        for the desired dates for each desired input source.

        Args:
            input_keys (List[str]): A list of input keys formatted as described above.
            dataset_config (DatasetConfig): The output dataset configuration.
            storage (Storage): Instance of a Storage class used to fetch saved data.

        Returns:
            xr.Dataset: The retrieved dataset

        ------------------------------------------------------------------------------------
        """
        assert storage is not None, "Missing required 'storage' parameter."

        storage_input_keys = [StorageRetrieverInput(key) for key in input_keys]

        input_data = self.__fetch_inputs(storage_input_keys, storage)

        if input_data_hook is not None:
            modded_input_data = input_data_hook(input_data)
            if modded_input_data is not None:
                input_data = modded_input_data

        # Perform coord/variable retrieval
        retrieved_data, retrieval_selections = perform_data_retrieval(
            input_data=input_data,
            coord_rules=self.coords,  # type: ignore
            data_var_rules=self.data_vars,  # type: ignore
        )

        # Ensure selected coords are indexed by themselves
        for name, coord_data in retrieved_data.coords.items():
            if coord_data.equals(xr.DataArray([])):
                continue
            new_coord = xr.DataArray(
                data=coord_data.data,
                coords={name: coord_data.data},
                dims=(name,),
                attrs=coord_data.attrs,
                name=name,
            )
            retrieved_data.coords[name] = new_coord
        # Q: Do data_vars need to be renamed or reindexed before data converters run?

        # Run data converters on coordinates, then on data variables
        for name, coord_def in retrieval_selections.coords.items():
            for converter in coord_def.data_converters:
                coord_data = retrieved_data.coords[name]
                data = converter.convert(
                    data=coord_data,
                    variable_name=name,
                    dataset_config=dataset_config,
                    retrieved_dataset=retrieved_data,
                    time_span=(storage_input_keys[0].start, storage_input_keys[0].end),
                    input_dataset=input_data.get(coord_def.source),
                    retriever=self,
                    input_key=coord_def.source,
                )
                if data is not None:
                    retrieved_data.coords[name] = data

        for name, var_def in retrieval_selections.data_vars.items():
            for converter in var_def.data_converters:
                var_data = retrieved_data.data_vars[name]
                data = converter.convert(
                    data=var_data,
                    variable_name=name,
                    dataset_config=dataset_config,
                    retrieved_dataset=retrieved_data,
                    retriever=self,
                    input_dataset=input_data.get(var_def.source),
                    input_key=var_def.source,
                )
                if data is not None:
                    retrieved_data.data_vars[name] = data

        # Construct the retrieved dataset structure
        # TODO: validate dimension alignment
        retrieved_dataset = xr.Dataset(
            coords=retrieved_data.coords,
            data_vars=retrieved_data.data_vars,
        )

        # Double check that dataset is trimmed to start and end time
        # Need to do this if adi_py is not used and more than one
        # files are pulled in.
        retrieved_dataset = self.__trim_dataset(retrieved_dataset, storage_input_keys)

        # Fix the dtype encoding
        for var_name, var_data in retrieved_dataset.data_vars.items():
            output_var_cfg = dataset_config.data_vars.get(var_name)
            if output_var_cfg is not None:
                dtype = output_var_cfg.dtype
                retrieved_dataset[var_name] = var_data.astype(dtype)
                var_data.encoding["dtype"] = dtype

        return retrieved_dataset

    # TODO: Seems like a static method here, should refactor into as such.
    def _get_timedelta(self, time_string):
        if time_string.replace(".", "").isnumeric():
            return pd.Timedelta(float(time_string), "s")
        else:
            return pd.Timedelta(time_string)

    # TODO: Method definition says that a lone `timedelta` is returned, but return statements return
    #  a `tuple[int, timedelta]`. This should be corrected.
    def _get_retrieval_padding(self, input_key: str) -> timedelta:
        if self.parameters is None:
            return 0, timedelta()
        elif self.parameters.fetch_params is not None:
            param = getattr(self.parameters.fetch_params, "time_padding")
            direction, padding = self.parameters.fetch_params.get_direction(param)
            return direction, self._get_timedelta(padding)
        elif self.parameters.trans_params is not None:
            params = self.parameters.trans_params.select_parameters(input_key)
            range_td = self._get_timedelta(params["range"].get("time", "0s"))
            width_td = self._get_timedelta(params["width"].get("time", "0s"))
            return 0, max(range_td, width_td)
        else:
            return 0, timedelta()

    def __fetch_inputs(
        self, input_keys: List[StorageRetrieverInput], storage: Storage
    ) -> Dict[InputKey, xr.Dataset]:
        input_data: Dict[InputKey, xr.Dataset] = {}
        for key in input_keys:
            padding = self._get_retrieval_padding(key.input_key)
            retrieved_dataset = storage.fetch_data(
                start=key.start - padding[1] if padding[0] < 1 else key.start,
                end=key.end + padding[1] if padding[0] > -1 else key.end,
                datastream=key.datastream,
                metadata_kwargs=key.kwargs,
            )
            input_data[key.input_key] = retrieved_dataset
        return input_data

    # TODO: Seems like a static method here, should refactor into as such.
    def __trim_dataset(
        self, dataset: xr.Dataset, input_keys: List[StorageRetrieverInput]
    ) -> xr.Dataset:
        # Trim dataset to original start and end keys
        # Start and end keys don't change between inputs
        start = input_keys[0].start
        end = input_keys[0].end
        return dataset.sel(time=slice(start, end))
