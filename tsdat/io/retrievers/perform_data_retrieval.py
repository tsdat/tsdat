import logging
import xarray as xr
from typing import (
    Any,
    Dict,
    Pattern,
    Tuple,
)

from ..base import (
    RetrievalRuleSelections,
    RetrievedDataset,
    RetrievedVariable,
)
from ...const import InputKey, VarName

logger = logging.getLogger(__name__)


def perform_data_retrieval(
    input_data: Dict[InputKey, xr.Dataset],
    coord_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
    data_var_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
) -> Tuple[RetrievedDataset, RetrievalRuleSelections]:
    # TODO: Also retrieve QC and Bounds variables -- possibly in ancillary structure?

    # Rule selections
    selected_coord_rules: Dict[VarName, RetrievedVariable] = {}
    selected_data_var_rules: Dict[VarName, RetrievedVariable] = {}

    # Retrieved dataset
    coord_data: Dict[VarName, xr.DataArray] = {}
    data_var_data: Dict[VarName, xr.DataArray] = {}

    # Retrieve coordinates
    for name, retriever_dict in coord_rules.items():
        for pattern, variable_retriever in retriever_dict.items():
            if name in selected_coord_rules:  # already matched
                break
            for input_key, dataset in input_data.items():
                if pattern.match(input_key):
                    logger.info(
                        "Coordinate '%s' retrieved from '%s': '%s'",
                        name,
                        input_key,
                        variable_retriever.name,
                    )
                    coord_data[name] = dataset.get(
                        variable_retriever.name, xr.DataArray([])
                    )
                    if not coord_data[name].equals(xr.DataArray([])):
                        variable_retriever.source = input_key
                    selected_coord_rules[name] = variable_retriever
                    break
        if name not in selected_coord_rules:
            logger.warning("Could not retrieve coordinate '%s'.", name)

    # Retrieve data variables
    for name, retriever_dict in data_var_rules.items():
        for pattern, variable_retriever in retriever_dict.items():
            if name in selected_data_var_rules:  # already matched
                break
            for input_key, dataset in input_data.items():
                if pattern.match(input_key):
                    logger.info(
                        "Variable '%s' retrieved from '%s': '%s'",
                        name,
                        input_key,
                        variable_retriever.name,
                    )
                    data_var_data[name] = dataset.get(
                        variable_retriever.name, xr.DataArray([])
                    )
                    if data_var_data[name].equals(xr.DataArray([])):
                        logger.warning(
                            "Input key matched regex pattern but no matching variable"
                            " could be found in the input dataset. A value of"
                            " xr.DataArray([]) will be used instead.\n"
                            "\tVariable: %s\n"
                            "\tInput Variable: %s\n"
                            "\tPattern: %s\n"
                            "\tInput Key: %s\n",
                            name,
                            variable_retriever.name,
                            pattern.pattern,
                            input_key,
                        )
                    variable_retriever.source = input_key
                    selected_data_var_rules[name] = variable_retriever
                    break
        if name not in selected_data_var_rules:
            logger.warning("Could not retrieve variable '%s'.", name)

    return (
        RetrievedDataset(coords=coord_data, data_vars=data_var_data),
        RetrievalRuleSelections(
            coords=selected_coord_rules, data_vars=selected_data_var_rules
        ),
    )

    # TODO: set default dim_range for time dim (ARM uses 1 day)
