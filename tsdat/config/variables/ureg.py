from pint import UnitRegistry

ureg = UnitRegistry()
ureg.define("unitless = count = 1")  # type: ignore
ureg.define('@alias degree_Fahrenheit = degree_F')
ureg.define('@alias degree_Celsius = degree_C')
ureg.define('@alias degree_Rankine = degree_R')
ureg.define('@alias kelvin = Kelvin')

