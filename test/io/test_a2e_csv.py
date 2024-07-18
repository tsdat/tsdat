import tempfile
from pathlib import Path

import pytest
import xarray as xr

from tsdat.io.handlers import A2eCSVHandler
from tsdat.testing import assert_close
from tsdat.utils import get_dataset_dim_groups


@pytest.mark.parametrize(
    "expected, base_filepath, dims",
    [
        (Path("test.time.1d.a2e.csv"), Path("test.csv"), ["time"]),
        (Path("test.time.depth.2d.a2e.csv"), Path("test.csv"), ["time", "depth"]),
        (
            Path("test.time.depth.height.3d.a2e.csv"),
            Path("test.csv"),
            ["time", "depth", "height"],
        ),
        (
            Path(
                "storage/root/data/buoy.z07.a0.20221117.001000.metocean.time.1d.a2e.csv"
            ),
            Path("storage/root/data/buoy.z07.a0.20221117.001000.metocean.csv"),
            ["time"],
        ),
        (
            Path(
                "storage/root/data/buoy.z07.a0.20221117.001000.metocean.time.depth.2d.a2e.csv"
            ),
            Path("storage/root/data/buoy.z07.a0.20221117.001000.metocean.a2e.csv"),
            ["time", "depth"],
        ),
        (
            Path("buoy.z07.a0.20221117.001000.metocean.time.depth.2d.a2e.csv"),
            Path("buoy.z07.a0.20221117.001000.metocean.csv"),
            ["time", "depth"],
        ),
    ],
)
def test_writer_get_filepath(expected: Path, base_filepath: Path, dims: tuple[str]):
    assert A2eCSVHandler().writer.get_filepath(base_filepath, dims) == expected


def test_1D_dataset_roundtrip(multi_var_1D_dataset: xr.Dataset):
    tmp_dir = tempfile.TemporaryDirectory()
    tmp_file = Path(tmp_dir.name) / "test_file.csv"

    handler = A2eCSVHandler()

    handler.writer.write(multi_var_1D_dataset, tmp_file)

    filepath = handler.writer.get_filepath(tmp_file, ("time",))

    dataset = handler.reader.read(filepath.as_posix())

    expected = multi_var_1D_dataset.copy(deep=True)
    expected.attrs["number"] = str(expected.attrs["number"])  # Note: writer does not
    expected.attrs["array"] = str(expected.attrs["array"])  # preserve the attr dtypes.
    expected["first"].attrs["number"] = str(expected["first"].attrs["number"])
    expected["scalar"] = (
        ("time"),
        [expected["scalar"].values] * expected.sizes["time"],
    )
    assert_close(dataset, expected, check_attrs=True)


def test_2D_dataset_roundtrip(sample_2D_dataset: xr.Dataset):
    tmp_dir = tempfile.TemporaryDirectory()
    tmp_file = Path(tmp_dir.name) / "test.csv"

    handler = A2eCSVHandler()

    handler.writer.write(sample_2D_dataset, tmp_file)

    dim_groups = get_dataset_dim_groups(sample_2D_dataset)
    assert len(dim_groups) == 3

    for dims, vars in dim_groups.items():
        filepath = handler.writer.get_filepath(tmp_file, dims)
        dataset = handler.reader.read(filepath.as_posix())

        expected = sample_2D_dataset[vars].copy(deep=True)
        for var in [var for var in vars if "_FillValue" in expected[var].attrs]:
            expected[var].attrs["_FillValue"] = str(expected[var].attrs["_FillValue"])
        if "depth" in dims:
            # pandas to xarray sorts the dims, no way around it afaik (2024-07-08)
            expected = expected.sortby(list(dims))

        xr.testing.assert_identical(dataset, expected)
