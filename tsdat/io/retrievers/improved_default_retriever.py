# import xarray as xr
# from typing import (
#     Any,
#     Dict,
#     List,
#     Literal,
#     Pattern,
#     Tuple,
# )
#
# from ...config.dataset import DatasetConfig
# from ..base import (
#     InputKey,
#     Retriever,
# )
#
# OutputVarName = str
#
# class ImprovedDefaultRetriever(Retriever):
#
#     # TODO: Need some way to also retrieve ancillary variables (QC and Bounds)
#
#     def get_input_datasets(
#         self, input_keys: List[str], **kwargs: Any
#     ) -> Dict[InputKey, xr.Dataset]:
#         """Reads in the input data and returns a map of input_key: xr.Dataset."""
#         input_datasets: Dict[InputKey, xr.Dataset] = {}
#         assert self.readers is not None  # type: ignore
#         for input_key in input_keys:
#             for pattern, reader in self.readers.items():  # type: ignore
#                 if pattern.match(input_key):  # type: ignore
#                     input_datasets[input_key] = reader.read(input_key, **kwargs)
#                     break
#         return input_datasets
#
#     def retrieve_variable_arrays(
#         self, input_datasets: Dict[InputKey, xr.Dataset], **kwargs: Any
#     ) -> Dict[OutputVarName, Tuple[Pattern[str], List[xr.DataArray]]]:
#         """Uses retrieval config parameters to extract the variable data arrays that can
#         be retrieved. For each variable, only the first matching pattern is considered.
#         """
#         ...
#
#     def select_retrieved_variables(
#         self,
#         retrieved_variable_arrays: Dict[
#             OutputVarName, Tuple[Pattern[str], List[xr.DataArray]]
#         ],
#         method: Literal["merge", "first"],  # if merge then combine, if first then idx 0
#         **kwargs: Any,
#     ) -> Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]]:
#         ...
#
#     def convert_data(
#         self,
#         retrieved_variables: Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]],
#         input_datasets: Dict[InputKey, xr.Dataset],  # Needed to get bounds/qc
#         **kwargs: Any,
#     ) -> Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]]:
#         ...
#
#     def create_output_dataset(
#         self,
#         converted_data: Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]],
#         # More needed here
#     ) -> xr.Dataset:
#         ...
#
#     def retrieve(
#         self,
#         input_keys: List[str],
#         dataset_config: DatasetConfig,
#         **kwargs: Any,
#     ) -> xr.Dataset:
#         raise NotImplementedError(
#             "ImprovedDefaultRetriever does not implement the 'retrieve' method"
#         )
