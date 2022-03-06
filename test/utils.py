from typing import Any, List


def compare(*model_dicts: Any):
    """Compare json mapping files.

    Args:
        mapping_files (List[Path]): The paths to the files to compare.
    """
    # TODO: highlight differences somehow

    from rich.console import Console
    from rich.columns import Columns
    from rich.pretty import Pretty
    from rich.panel import Panel

    console = Console()

    renderables: List[Panel] = [Panel(Pretty(model_dict)) for model_dict in model_dicts]

    console.print(Columns(renderables, equal=True, expand=True))
