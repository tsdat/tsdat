import xarray as xr
from typing import Any, List


def get_error_message(error: Any) -> str:
    return error.getrepr().reprcrash.message


def get_warning_message(warning: Any) -> str:
    warnings: List[str] = [_warning.message.args[0] for _warning in warning.list]
    return "\n".join(warnings)


def compare(*model_dicts: Any):
    """------------------------------------------------------------------------------------
    Method used to compare dictionaries side-by-side in the terminal. Primarily useful for
    debugging.

    ------------------------------------------------------------------------------------"""
    # TODO: highlight differences somehow

    from rich.console import Console
    from rich.columns import Columns
    from rich.pretty import Pretty
    from rich.panel import Panel

    console = Console()

    renderables: List[Panel] = [Panel(Pretty(model_dict)) for model_dict in model_dicts]

    console.print(Columns(renderables, equal=True, expand=True))


def assert_close(
    a: xr.Dataset, b: xr.Dataset, check_attrs: bool = True, **kwargs: Any
) -> None:
    if check_attrs:
        a = _drop_history_attr(a)
        b = _drop_history_attr(b)
        assert a.attrs == b.attrs
    xr.testing.assert_allclose(a, b, **kwargs)  # type: ignore


def _drop_history_attr(ds: xr.Dataset) -> xr.Dataset:
    if "history" in ds.attrs:
        del ds.attrs["history"]
    return ds
