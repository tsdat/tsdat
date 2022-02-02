import xarray as xr
from tsdat.io import handlers


def test_read_tar():
    # Mock the parameters passed to TarHandler in a storage config file
    params = {
        "read": {
            "tarfile": {
                "mode": "r:gz",
            },
        },
        "handlers": {
            "netcdf": {
                "file_pattern": r".*\.nc",
                "classname": "tsdat.io.handlers.NetCdfHandler",
                "parameters": {
                    # The TarHandler passes the file to the NetCdfHandler as a binary
                    # string so we need to tell xarray to use a more capable backend.
                    "engine": "h5netcdf"
                },
            }
        },
    }

    # Read the archive using TarHandler and extract just the dataset
    tar = handlers.TarHandler(params)
    out = tar.read("tests/data/humboldt.buoy_z05-10min.a1.20201201.000000.tar.gz")
    result = list(out.values())[0]

    # Compare with the expected output
    expected = xr.open_dataset(
        "tests/expected/humboldt.buoy_z05-10min.a1.20201201.000000.nc"
    )
    xr.testing.assert_allclose(result, expected)


def test_read_zip():
    # Mock the parameters passed to ZipHandler in a storage config file
    params = {
        "handlers": {
            "netcdf": {
                "file_pattern": r".*\.nc",
                "classname": "tsdat.io.handlers.NetCdfHandler",
                "parameters": {
                    # The ZipHandler passes the file to the NetCdfHandler as a binary
                    # string so we need to tell xarray to use a more capable backend.
                    "engine": "h5netcdf"
                },
            }
        },
    }

    # Read the archive using TarHandler and extract just the dataset
    tar = handlers.ZipHandler(params)
    out = tar.read("tests/data/humboldt.buoy_z05-10min.a1.20201201.000000.zip")
    result = list(out.values())[0]

    # Compare with the expected output
    expected = xr.open_dataset(
        "tests/expected/humboldt.buoy_z05-10min.a1.20201201.000000.nc"
    )
    xr.testing.assert_allclose(result, expected)
