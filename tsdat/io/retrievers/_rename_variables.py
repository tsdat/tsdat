import logging
import xarray as xr
from typing import (
    Dict,
)

from .input_key_retrieval_rules import InputKeyRetrievalRules
from ..base import (
    RetrievedVariable,
)
from ...const import VarName

logger = logging.getLogger(__name__)


def _rename_variables(
    dataset: xr.Dataset,
    input_config: InputKeyRetrievalRules,
) -> xr.Dataset:
    """-----------------------------------------------------------------------------
    Renames variables in the retrieved dataset according to retrieval configurations.

    Args:
        raw_dataset (xr.Dataset): The raw dataset.

    Returns:
        xr.Dataset: The simplified raw dataset.

    -----------------------------------------------------------------------------"""

    def rename_vars(input_data: Dict[VarName, RetrievedVariable]):
        # Run through list of retreiver variables and organizes them into
        # a simple {input: output} dictionary
        data_to_rename: Dict[str, str] = {}
        for output_name, d in input_data.items():
            if isinstance(d.name, list):
                for n in d.name:
                    data_to_rename[n] = output_name
            else:
                data_to_rename[d.name] = output_name  # type: ignore
        return data_to_rename

    def drop_var_input_config(
        input_data: Dict[VarName, RetrievedVariable], output_name: str
    ):
        # Drop output_name from input_config.coords or input_config.data_vars
        n = input_data[output_name].name  # type: ignore
        if isinstance(n, list):
            n.remove(raw_name)  # type: ignore
            if len(n) == 1:
                input_data[output_name].name = n[0]
        else:
            input_data.pop(output_name)

    to_rename: Dict[str, str] = {}  # {raw_name: output_name}
    coords_to_rename = rename_vars(input_config.coords)
    vars_to_rename = rename_vars(input_config.data_vars)

    to_rename.update(coords_to_rename)
    to_rename.update(vars_to_rename)

    # Check for multiple raw names here
    for raw_name, output_name in coords_to_rename.items():
        if raw_name not in dataset:
            to_rename.pop(raw_name)
            drop_var_input_config(input_config.coords, output_name)
            logger.warning(
                "Coordinate variable '%s' could not be retrieved from input. Please"
                " ensure the retrieval configuration file for the '%s' coord has"
                " the 'name' property set to the exact name of the variable in the"
                " dataset returned by the input DataReader.",
                raw_name,
                output_name,
            )
        # Don't rename coordinate if name hasn't changed
        elif raw_name == output_name:
            to_rename.pop(raw_name)

    for raw_name, output_name in vars_to_rename.items():
        if raw_name not in dataset:
            to_rename.pop(raw_name)
            drop_var_input_config(input_config.data_vars, output_name)
            logger.warning(
                "Data variable '%s' could not be retrieved from input. Please"
                " ensure the retrieval configuration file for the '%s' data"
                " variable has the 'name' property set to the exact name of the"
                " variable in the dataset returned by the input DataReader.",
                raw_name,
                output_name,
            )
    return dataset.rename(to_rename)
