from pint import UnitRegistry

ureg = UnitRegistry()
ureg.define("unitless = count = 1")  # type: ignore
