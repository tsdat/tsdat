"""-------------------------------------------------------------------------------------
Utility functions used in tsdat tests.

-------------------------------------------------------------------------------------"""

import numpy as np
import xarray as xr
from typing import Any, Hashable, List

__all__ = [
    # "compare",
    "assert_close",
]


def get_pydantic_error_message(error: Any) -> str:
    return error.getrepr().reprcrash.message


def get_pydantic_warning_message(warning: Any) -> str:
    warnings: List[str] = [_warning.message.args[0] for _warning in warning.list]
    return "\n".join(warnings)


# def compare(*model_dicts: Any):
#     """---------------------------------------------------------------------------------
#     Method used to compare dictionaries side-by-side in the terminal. Primarily useful
#     for debugging.

#     ---------------------------------------------------------------------------------"""
#     # IDEA: highlight differences

#     from rich.console import Console
#     from rich.columns import Columns
#     from rich.pretty import Pretty
#     from rich.panel import Panel

#     console = Console()
#     renderables: List[Panel] = [Panel(Pretty(model_dict)) for model_dict in model_dicts]
#     console.print(Columns(renderables, equal=True, expand=True))


def assert_close(
    a: xr.Dataset,
    b: xr.Dataset,
    check_attrs: bool = True,
    check_fill_value: bool = True,
    **kwargs: Any,
) -> None:
    """---------------------------------------------------------------------------------
    Thin wrapper around xarray.assert_allclose.

    Also checks dataset and variable attrs. Removes global attributes that are allowed
    to be different, which are currently just the 'history' attribute and the
    'code_version' attribute. Also handles some obscure edge cases for variable
    attributes.

    Args:
        a (xr.Dataset): The first dataset to compare.
        b (xr.Dataset): The second dataset to compare.
        check_attrs (bool): Check global and variable attributes in addition to the
            data. Defaults to True.
        check_fill_value (bool): Check the _FillValue attribute. This is a special case
            because xarray moves the _FillValue from a variable's attributes to its
            encoding upon saving the dataset. Defaults to True.

    ---------------------------------------------------------------------------------"""
    a, b = a.copy(), b.copy()  # type: ignore
    _convert_time(a, b)
    xr.testing.assert_allclose(a, b, **kwargs)  # type: ignore
    if check_attrs:
        _check_global_attrs(a, b)
        _check_variable_attrs(a, b, check_fill_value)


def _convert_time(a: xr.Dataset, b: xr.Dataset) -> None:
    # Converts datetime64 to seconds since 1970
    for v in a.variables:
        if np.issubdtype(a[v].dtype, np.datetime64):  # type: ignore
            a[v] = a[v].astype("datetime64[ns]").astype("float") / 1e9  # type: ignore
            b[v] = b[v].astype("datetime64[ns]").astype("float") / 1e9  # type: ignore


def _check_global_attrs(a: xr.Dataset, b: xr.Dataset) -> None:
    _drop_incomparable_global_attrs(a)
    _drop_incomparable_global_attrs(b)
    if a.attrs != b.attrs:
        raise AssertionError(f"global attributes do not match:\n{a.attrs}\n{b.attrs}")


def _check_variable_attrs(a: xr.Dataset, b: xr.Dataset, check_fill_value: bool) -> None:
    _drop_incomparable_variable_attrs(a)
    _drop_incomparable_variable_attrs(b)
    for var_name in a.variables:
        _check_var_attrs(a, b, var_name, check_fill_value)


def _check_var_attrs(
    a: xr.Dataset, b: xr.Dataset, var_name: Hashable, check_fill_value: bool
) -> None:
    if check_fill_value:
        _check_fillvalue(a[var_name], b[var_name])
    a_attrs, b_attrs = a[var_name].attrs, b[var_name].attrs
    a_attrs.pop("_FillValue", None),
    b_attrs.pop("_FillValue", None)
    if a.attrs != b.attrs:
        raise AssertionError(f"attributes do not match:\n{a_attrs}\n{b_attrs}")


def _drop_incomparable_global_attrs(ds: xr.Dataset):
    ds.attrs.pop("history", None)
    ds.attrs.pop("code_version", None)


def _drop_incomparable_variable_attrs(ds: xr.Dataset):
    if "time" in ds.variables:
        ds["time"].attrs.pop("units", None)


def _check_fillvalue(a: xr.DataArray, b: xr.DataArray) -> None:
    a_fill = a.attrs.get("_FillValue") or a.encoding.get("_FillValue")
    b_fill = b.attrs.get("_FillValue") or b.encoding.get("_FillValue")

    if not (a_fill == b_fill or (np.isnan(a_fill) and np.isnan(b_fill))):
        raise AssertionError(
            f"'{a.name}' _FillValue attrs/encoding do not match:\n"
            f"{a_fill},\n"
            f"{b_fill}"
        )
