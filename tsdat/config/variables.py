from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Extra, root_validator, Field, StrictStr, validator

from .attributes import AttributeModel
from .utils import ParametrizedClass


__all__ = [
    "VariableAttributes",
    "InputConverter",
    "InputVariable",
    "Variable",
    "Coordinate",
]


class VariableAttributes(AttributeModel):
    """Attributes that will be recorded in the output dataset. These metadata are used to
    record information about the data properties and related fields (e.g., units,
    ancillary_variables, etc), user-facing metadata (e.g., long_name, comment), as well as
    attributes related to quality checks and controls (e.g., valid_*, fail_*, and warn_*
    properties).
    """

    units: Optional[str] = Field(
        description="A string indicating the units the data are measured in. Tsdat uses"
        " pint to handle unit conversions, so this string must be compatible with the"
        " pint list of units, if provided. A complete list of compatible units can be"
        " found here: https://github.com/hgrecco/pint/blob/master/pint/default_en.txt."
        " If the property is unitless, then the string '1' should be used. If the units"
        " of the property are not known, then the units attribute should be omitted and"
        " the comment attribute should include a note indicating that units are not"
        " known. Doing so provides helpful context for data users."
    )
    long_name: Optional[StrictStr] = Field(
        description="A brief label for the name of the measured property. The xarray"
        " python library automatically searches for this attribute to use as an axes"
        " label in plots, so the value should be suitable for display."
    )
    standard_name: Optional[StrictStr] = Field(
        description="A string exactly matching a value in the CF Standard Name table"
        " which is used to provide a standardized way of identifying variables and"
        " measurements across heterogeneous datasets and domains. If a suitable match"
        " does not exist, then this attribute should be omitted. The full list of CF"
        " Standard Names is at: https://cfconventions.org/Data/cf-standard-names."
    )
    comment: Optional[StrictStr] = Field(
        description="A user-friendly description of what the variable represents, how"
        " it was measured or derived, or any other relevant information that increases"
        " the ability of users to understand and use this data. This field plays a"
        " considerable role in creating self-documenting data, so we highly recommend"
        " including this field, especially for any variables which are particularly"
        " important for your dataset. Additionally, if the units for an attribute are"
        " unknown, then this field must include the phrase: 'Unknown units.' so that"
        " users know there is some uncertainty around this property. Variables that are"
        " unitless (e.g., categorical data or ratios), should set the 'units' to '1'."
    )
    valid_range: Optional[List[float]] = Field(
        min_items=2,
        max_items=2,
        description="A two-element list of [min, max] values outside of which the data"
        " should be treated as missing. If applying QC tests, then users should"
        " configure the quality managers to flag values outside of this range as having"
        " a 'Bad' assessment and replace those values with the variable's _FillValue.",
    )
    fail_range: Optional[List[float]] = Field(
        min_items=2,
        max_items=2,
        description="A two-element list of [min, max] values outside of which the data"
        " should be teated with heavy skepticism as missing. If applying QC tests, then"
        " users should configure the quality managers to flag values outside of this"
        " range as having a 'Bad' assessment.",
    )
    warn_range: Optional[List[float]] = Field(
        min_items=2,
        max_items=2,
        description="A two-element list of [min, max] values outside of which the data"
        " should be teated with some skepticism as missing. If applying QC tests, then"
        " users should configure the quality managers to flag values outside of this"
        " range as having an 'Indeterminate' assessment.",
    )
    valid_delta: Optional[float] = Field(
        description="The largest difference between consecutive values in the data"
        " outside of which the data should be treated as missing. If applying QC tests,"
        " then users should configure the quality managers to flag values outside of"
        " this range as having a 'Bad' assessment and replace those values with the"
        " variable's _FillValue."
    )
    fail_delta: Optional[float] = Field(
        description="The largest difference between consecutive values in the data"
        " outside of which the data should be teated with heavy skepticism as missing."
        " If applying QC tests, then users should configure the quality managers to"
        " flag values outside of this range as having a 'Bad' assessment."
    )
    warn_delta: Optional[float] = Field(
        description="The largest difference between consecutive values in the data"
        " outside of which the data should be teated with some skepticism as missing."
        " If applying QC tests, then users should configure the quality managers to"
        " flag values outside of this range as having an 'Indeterminate' assessment."
    )
    fill_value: Optional[float] = Field(
        # TODO: Change the type to Any and use a validator to make it "" for str dtypes
        -9999.0,
        alias="_FillValue",
        description="A value used to initialize the variable's data and indicate that"
        " the data is missing. Defaults to -9999 for numerical data. If choosing a"
        " different value, it is important to use a value that could not reasonably be"
        " mistaken for a physical value or data point.",
    )

    # TODO: Validate units using pint registry
    # ureg = pint.UnitRegistry(autoconvert_offset_to_baseunit=True)
    # ureg.define('percent = 0.01*count = %')
    # ureg.define('unitless = count = 1')
    # try: ureg(units) except: ValueError(units not valid)

    @root_validator
    @classmethod
    def validate_units_are_commented(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if not values["units"]:
            if not values["comment"] or "Unknown units." not in values["comment"]:
                raise ValueError(
                    "The 'units' attr is required if known. If the units are not known,"
                    " then the 'comment' attr should include the phrase 'Unknown"
                    " units.' so that users are aware that the measurement's units are"
                    " not known. Note that 'unitless' quantities (e.g., categorical"
                    " data, ratios, etc) should set the 'units' attr to '1'."
                )
        return values


class InputConverter(ParametrizedClass):
    classname: StrictStr = Field(
        "tsdat.utils.converters.DefaultConverter",
        description="The module path to the Python class that should be used, e.g., if"
        " you would write in your script `from tsdat.config.utils.converters import"
        " DefaultConverter` then you would put"
        " 'tsdat.config.utils.converters.DefaultConverter' as the classname.",
    )


class InputVariable(BaseModel, extra=Extra.allow):
    name: StrictStr = Field(
        title="Input Name",
        description="This is the name of the variable exactly as it appears in the"
        " input dataset. More precisely, this is the key tsdat will use to retrieve the"
        " variable from the dataset returned by the input DataHandler.",
    )
    required: bool = Field(
        True,
        description="If True (the default) then the pipeline will fail loudly if tsdat"
        " is unable to retrieve the variable from an input source.",
    )
    units: Optional[str] = Field(
        description="This gives tsdat context about the units the input dataset is"
        " measured in. If the 'units' property here differs from the 'units' property"
        " under the 'attrs' section, then tsdat will automatically perform a unit"
        " conversion on the input data."
    )
    converter: InputConverter = Field(
        InputConverter(classname="tsdat.utils.converters.DefaultConverter"),  # type: ignore
        description="The converter class that tsdat should use to transform the data"
        " from the input source to the output source. Currently only two converters are"
        " supported: the 'DefaultConverter', which converts input units to output units"
        " using the Python libraries act-atmos and pint, and the 'StringTimeConverter',"
        " which is used exclusively for converting string values into Python datetime"
        " objects that are timezone-aware. If using the 'StringTimeConverter' class,"
        " two parameters are required: 'timezone' - the timezone the data are recorded"
        " in (default UTC), and 'time_format' - a string that is passed to the"
        " strptime() function as the string format used to create a datetime object.",
    )


class Variable(BaseModel, extra=Extra.forbid):
    name: str = Field(
        title="Output Variable Name",
        regex=r"^[a-zA-Z0-9_\(\)\/\[\]\{\}\.]+$",
        description="The name of the variable in the output file. Generally, we"
        " recommend only using lowercase alphanumeric and '_' characters to name"
        " variables, as uniformly-named variables are easier to sort through and read"
        " for users. Spaces and non-ascii characters are explicitly disallowed. The"
        " variable name should be consise, yet clear enough for users to know what the"
        " property measures. A more descriptive name for a variable (i.e. suitable for"
        " a plot title / axis label) should be provided via the 'long_name' attribute"
        " in the attrs section, if desired. The 'comment' attribute is also recommended"
        " to provide additional context about the variable, if needed.",
    )
    input: Optional[InputVariable] = Field(
        description="If the variable should be retrieved from the input dataset, then"
        " this section should be used to specify how the variable retrieval should be"
        " done.",
    )
    data: Optional[Any] = Field(
        description="If the variable is not meant to be retrieved from an input dataset"
        " and the value is known in advance, then the 'data' property should specify"
        " its value exactly as it should appear in the output dataset. This is commonly"
        " used for latitude/longitude/altitude data for datasets measured from a"
        " specific geographical location."
    )
    dtype: StrictStr = Field(
        description="The numpy dtype of the underlying data. This is passed to numpy as"
        " the 'dtype' keyword argument used to initialize an array (e.g.,"
        " `numpy.array([1.0, 2.0], dtype='float')`). Commonly-used values include"
        " 'float', 'int', 'long'."
    )
    dims: List[StrictStr] = Field(
        unique_items=True,
        description="A list of coordinate variable names that dimension this data"
        " variable. Most commonly this will be set to ['time'], but for datasets where"
        " there are multiple dimensions (e.g., ADCP data measuring current velocities"
        " across time and several depths, it may look like ['time', 'depth']).",
    )
    attrs: VariableAttributes = Field(
        description="The attrs section is where variable-specific metadata are stored."
        " This metadata is incredibly important for data users, and we recommend"
        " including several properties for each variable in order to have the greatest"
        " impact. In particular, we recommend adding the 'units', 'long_name', and"
        " 'standard_name' attributes, if possible."
    )

    @property
    def is_static(self) -> bool:
        return self.data is not None

    @property
    def is_retrieved(self) -> bool:
        return self.input is not None

    @property
    def is_dynamic(self) -> bool:
        return self.data is None and self.input is None

    @validator("name")
    @classmethod
    def validate_name_is_ascii(cls, v: str) -> str:
        if not v.isascii():
            raise ValueError(f"'{v}' contains a non-ascii character.")
        return v

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_data_retrival(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values["input"] is not None and values["data"] is not None:
            raise ValueError(
                f"'{values['name']}' cannot be both retrieved from input and set"
                " statically. Please remove either the 'input' section or the 'data'"
                " property."
            )
        return values


class Coordinate(Variable):
    @root_validator(skip_on_failure=True)
    @classmethod
    def coord_dimensioned_by_self(cls, values: Any) -> Any:
        name, dims = values["name"], values["dims"]
        if [name] != dims:
            raise ValueError(f"coord '{name}' must have dims ['{name}']. Found: {dims}")
        return values
