import struct
import datetime
import numpy as np
import xarray as xr
from typing import Dict

from tsdat import Config
from tsdat.io import AbstractFileHandler


class DTYPE:
    """Class to wrap matlab data types for python"""

    def __init__(self, format, size):
        self.format = format
        self.size = size


UINT8 = DTYPE("c", 1)  # uint8 not in python; use char (c) instead
UINT16 = DTYPE("h", 2)  # use short (h)
SINGLE = DTYPE("f", 4)  # use python float (f)
DOUBLE = DTYPE("d", 8)  # use python double (d)


# Morro Bay
morro_packet = {
    "header": {
        "sync1": UINT8,
        "sync2": UINT8,
        "max_packet_size": UINT8,
        "payload_size": UINT8,
    },
    "gps_time": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "tow": DOUBLE,
        "week_number": UINT16,
        "flag": UINT16,
    },
    "rpy": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "roll": SINGLE,
        "pitch": SINGLE,
        "yaw": SINGLE,
    },
    "gyro": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "x": SINGLE,
        "y": SINGLE,
        "z": SINGLE,
    },
    "accel": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "x": SINGLE,
        "y": SINGLE,
        "z": SINGLE,
    },
    "mag": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "x": SINGLE,
        "y": SINGLE,
        "z": SINGLE,
    },
    "pressure": {"packet_descriptor": UINT8, "fdescriptor": UINT8, "data": SINGLE},
    "chksum": {"msb": UINT8, "lsb": UINT8},
}

# Humboldt -- gps time packet was duplicated during installation
humbolt_packet = {
    "header": {
        "sync1": UINT8,
        "sync2": UINT8,
        "max_packet_size": UINT8,
        "payload_size": UINT8,
    },
    "gps_time": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "tow": DOUBLE,
        "week_number": UINT16,
        "flag": UINT16,
    },
    "rpy": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "roll": SINGLE,
        "pitch": SINGLE,
        "yaw": SINGLE,
    },
    "gyro": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "x": SINGLE,
        "y": SINGLE,
        "z": SINGLE,
    },
    "accel": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "x": SINGLE,
        "y": SINGLE,
        "z": SINGLE,
    },
    "mag": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "x": SINGLE,
        "y": SINGLE,
        "z": SINGLE,
    },
    "pressure": {"packet_descriptor": UINT8, "fdescriptor": UINT8, "data": SINGLE},
    "_": {
        "packet_descriptor": UINT8,
        "fdescriptor": UINT8,
        "tow": DOUBLE,
        "week_number": UINT16,
        "flag": UINT16,
    },
    "chksum": {"msb": UINT8, "lsb": UINT8},
}


def fread(fileObj, dtype):
    buffer = fileObj.read(dtype.size)
    val = struct.unpack(f">{dtype.format}", buffer)[0]
    if dtype == UINT8:
        val = int.from_bytes(val, "big")
    return val


def extract_data(filename: str, packet: Dict[str, Dict]) -> xr.Dataset:
    # Create dictionary to hold raw binary data
    raw_data = {
        category: {var_name: [] for var_name in subpacket.keys()}
        for category, subpacket in packet.items()
    }

    # Read the binary data from file and append to raw data dictionary
    with open(filename, "rb") as bfile:
        try:
            while True:
                for category, subpacket in packet.items():
                    for name, dtype in subpacket.items():
                        _data = fread(bfile, dtype)
                        if name:
                            raw_data[category][name].append(_data)
        except:
            pass

    # Convert raw data into numpy arrays
    data = {
        category: {
            var_name: np.array(subpacket[var_name]) for var_name in subpacket.keys()
        }
        for category, subpacket in raw_data.items()
    }

    # Create datetime array from gps times
    time = [datetime.datetime(year=1980, month=1, day=6)] * len(data["gps_time"]["tow"])
    for i in range(len(time)):
        days = int(7 * data["gps_time"]["week_number"][i])
        seconds = (
            data["gps_time"]["tow"][i] - 18
        )  # GPS time to UTC is currently 18 seconds off
        time[i] += datetime.timedelta(days=days, seconds=seconds)
    time = np.array(time, dtype=np.datetime64)

    # Create handles for variables
    roll = np.array(data["rpy"]["roll"])
    pitch = np.array(data["rpy"]["pitch"])
    yaw = np.array(data["rpy"]["yaw"])
    gyro = np.array(
        [data["gyro"]["x"], data["gyro"]["y"], data["gyro"]["z"]]
    ).transpose()
    accel = np.array(
        [data["accel"]["x"], data["accel"]["y"], data["accel"]["z"]]
    ).transpose()
    mag = np.array([data["mag"]["x"], data["mag"]["y"], data["mag"]["z"]]).transpose()
    pres = np.array(data["pressure"]["data"])

    # Create data dictionary and return dataset
    dictionary = {
        # Dimensions / coordinates
        "time": {"dims": ["time"], "data": time},
        "space": {"dims": ["space"], "data": ["x", "y", "z"]},
        # Data variables
        "roll": {"dims": ["time"], "data": roll},
        "pitch": {"dims": ["time"], "data": pitch},
        "yaw": {"dims": ["time"], "data": yaw},
        "gyro": {"dims": ["time", "space"], "data": gyro},
        "accel": {"dims": ["time", "space"], "data": accel},
        "mag": {"dims": ["time", "space"], "data": mag},
        "pres": {"dims": ["time"], "data": pres},
    }
    return xr.Dataset.from_dict(dictionary)


class ImuFileHandler(AbstractFileHandler):
    def write(self, ds: xr.Dataset, filename: str, config: Config, **kwargs):
        raise NotImplementedError(
            "Error: this file format should not be used to write to."
        )

    def read(self, filename: str, **kwargs) -> xr.Dataset:

        # Determine which packet to use.
        dataset = None
        try:
            dataset = extract_data(filename, morro_packet)
        except:
            dataset = extract_data(filename, humbolt_packet)

        return dataset
