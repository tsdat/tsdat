from typing import Optional

import xarray as xr

from tsdat.const import FILENAME_TEMPLATE

from .get_fields_from_dataset import get_fields_from_dataset


def get_filename(
    dataset: xr.Dataset, extension: str, title: Optional[str] = None
) -> str:
    """---------------------------------------------------------------------------------
    Returns the standardized filename for the provided dataset.

    Returns a key consisting of the dataset's datastream, starting date/time, the
    extension, and an optional title. For file-based storage systems this method may be
    used to generate the basename of the output data file by providing extension as
    '.nc', '.csv', or some other file ending type. For ancillary plot files this can be
    used in the same way by specifying extension as '.png', '.jpeg', etc and by
    specifying the title, resulting in files named like
    '<datastream>.20220424.165314.plot_title.png'.

    Args:
        dataset (xr.Dataset): The dataset (used to extract the datastream and starting /
            ending times).
        extension (str): The file extension that should be used.
        title (Optional[str]): An optional title that will be placed between the start
            time and the extension in the generated filename.

    Returns:
        str: The filename constructed from provided parameters.

    ---------------------------------------------------------------------------------"""
    substitutions: dict[str, str] = dict()
    substitutions.update(get_fields_from_dataset(dataset))
    substitutions.update(extension=extension.lstrip("."), title=title)
    return FILENAME_TEMPLATE.substitute(substitutions)
