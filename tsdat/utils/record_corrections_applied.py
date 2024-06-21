from typing import List
import xarray as xr


def record_corrections_applied(
    dataset: xr.Dataset, variable_name: str, message: str
) -> None:
    """---------------------------------------------------------------------------------
    Records the message on the 'corrections_applied' attribute.

    Args:
        dataset (xr.Dataset): The corrected dataset.
        variable_name (str): The name of the variable in the dataset.
        message (str): The message to record.

    ---------------------------------------------------------------------------------"""
    variable_attrs = dataset[variable_name].attrs
    corrections: List[str] = variable_attrs.get("corrections_applied", [])
    corrections.append(message)
    variable_attrs["corrections_applied"] = corrections
