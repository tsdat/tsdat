import contextlib
from pathlib import Path
import tempfile
import xarray as xr
from abc import ABC, abstractmethod
from typing import Any, Generator
from pydantic import BaseModel, Extra
from tsdat.io.handlers.handlers import HandlerRegistry


class BaseStorage(BaseModel, ABC, extra=Extra.forbid):

    parameters: Any = {}
    registry: HandlerRegistry

    @abstractmethod
    def save(self, dataset: xr.Dataset):
        ...

    # IDEA: temp dir to upload? E.g., to use for saving plots. Might look like this:
    # ```
    # with BaseStorage.create_tmp_dir("plots", upload=True) as plot_dir:
    #   # create some some plots in the dir
    #   ...
    # ```
    # ... and that's all! The storage class would be responsible for figuring out how to
    # persist the files in the temporary directory if upload=True is used.

    @abstractmethod
    def upload_directory_contents(self, dirpath: Path):
        ...

    @contextlib.contextmanager
    def uploadable_tmp_dir(self, upload: bool = True) -> Generator[Path, None, None]:
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dirpath = Path(tmp_dir.name)
        try:
            yield tmp_dirpath
        except BaseException:  # Must provide an except clause in order to use 'else'
            raise
        else:  # elif not allowed
            if upload:
                self.upload_directory_contents(tmp_dirpath)
        finally:
            tmp_dir.cleanup()
