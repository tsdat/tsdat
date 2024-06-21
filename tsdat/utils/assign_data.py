from typing import Any
import xarray as xr
from numpy.typing import NDArray


def assign_data(
    dataset: xr.Dataset, data: NDArray[Any], variable_name: str
) -> xr.Dataset:
    """---------------------------------------------------------------------------------
    Assigns the data to the specified variable in the dataset.

    If the variable exists and it is a data variable, then the DataArray for the
    specified variable in the dataset will simply have its data replaced with the new
    numpy array. If the variable exists and it is a coordinate variable, then the data
    will replace the coordinate data. If the variable does not exist in the dataset then
    a KeyError will be raised.


    Args:
        dataset (xr.Dataset): The dataset where the data should be assigned.
        data (NDArray[Any]): The data to assign.
        variable_name (str): The name of the variable in the dataset to assign data to.

    Raises:
        KeyError: Raises a KeyError if the specified variable is not in the dataset's
            coords or data_vars dictionary.

    Returns:
        xr.Dataset: The dataset with data assigned to it.

    ---------------------------------------------------------------------------------"""
    if variable_name in dataset.data_vars:
        dataset[variable_name].data = data
    elif variable_name in dataset.coords:
        tmp_name = f"__{variable_name}__"
        dataset = dataset.rename_vars({variable_name: tmp_name})

        # TODO: ensure attrs are copied over too
        dataset[variable_name] = xr.zeros_like(dataset[tmp_name], dtype=data.dtype)  # type: ignore
        dataset[variable_name].data[:] = data[:]
        # dataset = dataset.swap_dims({tmp_name: variable_name})  # type: ignore
        dataset = dataset.drop_vars(tmp_name)
        # dataset = dataset.rename_dims(
        #     {tmp_name: variable_name}
        # )  # FIXME: This might drop all dimensions other than the one that was just renamed
    else:
        raise KeyError(
            f"'{variable_name}' must be a coord or a data_var in the dataset to assign"
            " data to it."
        )
    return dataset
