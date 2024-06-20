import re
from pint import UnitRegistry

ureg = UnitRegistry(autoconvert_offset_to_baseunit=True)

# Temperature
ureg.define("@alias degree_Fahrenheit = degree_F")
ureg.define("@alias degree_Celsius = degree_C")
ureg.define("@alias degree_Rankine = degree_R")
ureg.define("@alias kelvin = Kelvin")
# Percent
ureg.define("@alias percent = %")
# Other
ureg.define("fraction = []")
ureg.define("unitless = []")


def check_unit(unit_str: str, keep_exp: bool) -> str:
    # Not recognized by pint, but we want it to be valid
    if unit_str.lower().startswith("seconds since"):
        return unit_str

    # Add exponent symbol (m2 s-2 -> m^2 s^-2)
    carrot_flag = 1 if "^" in unit_str else 0
    unit_exponent = re.compile(
        r"(?<=[A-Za-z\)])(?![A-Za-z\)])" r"(?<![0-9\-][eE])(?<![0-9\-])(?=[0-9\-])"
    )
    unit_str = unit_exponent.sub("^", unit_str)

    # Validate with pint unit registry
    ureg(unit_str)

    # Remove exponent if not used
    if not keep_exp and not carrot_flag:
        unit_str = unit_str.replace("^", "")

    return unit_str
