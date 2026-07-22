import pathlib
import re

files = [
    "tsdat/transform_v2/converters/bin_average.py",
    "tsdat/transform_v2/converters/linear_interpolate.py",
    "tsdat/transform_v2/converters/nearest_neighbor.py",
]
for f in files:
    p = pathlib.Path(f)
    txt = p.read_text()
    orig = txt
    # Replace self.__repr_name__ (without parentheses) with type(self).__name__
    txt = txt.replace("self.__repr_name__", "type(self).__name__")
    if txt != orig:
        p.write_text(txt)
        print("Patched " + f)
    else:
        print("No change " + f)
