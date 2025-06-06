import numpy as np


def get_tolerance(coordinate, rng):
    """Sets range tolerance on coordinates for nearest neighbor algorithm."""

    if rng is None:
        return None

    units = ""
    for i, s in enumerate(rng):
        if s.isalpha():
            units = rng[i:]
            rng = rng[:i]
    _rng = float(rng)

    if np.issubdtype(coordinate.dtype, np.datetime64):  # type: ignore
        coord_vals = np.array([np.datetime64(val) for val in coordinate])
        _rng = np.timedelta64(int(_rng), units or "s")

    return _rng
