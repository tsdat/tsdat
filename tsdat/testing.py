"""-------------------------------------------------------------------------------------
Utility functions used in tsdat tests.

-------------------------------------------------------------------------------------"""

import xarray as xr
from typing import Any, List

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
    a: xr.Dataset, b: xr.Dataset, check_attrs: bool = True, **kwargs: Any
) -> None:
    """---------------------------------------------------------------------------------
    Thin wrapper around xarray.assert_allclose which also checks dataset and variable
    attrs. Removes global attributes that are allowed to be different, which are
    currently just the 'history' attribute and the 'code_version' attribute.

    Args:
        a (xr.Dataset): The first dataset to compare.
        b (xr.Dataset): The secoond dataset to compare.
        check_attrs (bool, optional): Check global and variable attributes in addition
        to the data. Defaults to True.

    ---------------------------------------------------------------------------------"""
    a, b = a.copy(), b.copy()  # type: ignore
    xr.testing.assert_allclose(a, b, **kwargs)  # type: ignore
    if check_attrs:
        a = _drop_history_attr(a)
        a = _drop_code_version_attr(a)
        b = _drop_history_attr(b)
        b = _drop_code_version_attr(b)
        assert (
            a.attrs == b.attrs
        ), f"global attributes do not match:\n{a.attrs}\n{b.attrs}"
        for var_name in a.variables:
            a_attrs = a[var_name].attrs
            b_attrs = b[var_name].attrs
            assert (
                a_attrs == b_attrs
            ), f"'{var_name}' attributes do not match:\n{a_attrs},\n{b_attrs}"


def _drop_history_attr(ds: xr.Dataset) -> xr.Dataset:
    if "history" in ds.attrs:
        del ds.attrs["history"]
    return ds


def _drop_code_version_attr(ds: xr.Dataset) -> xr.Dataset:
    if "code_version" in ds.attrs:
        del ds.attrs["code_version"]
    return ds
