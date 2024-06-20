import re
import logging
from typing import Any, Dict, List, Optional

from pint import PintError
from pydantic import (
    Field,
    StrictStr,
    root_validator,
    validator,
)

from .ureg import check_unit
from ..attributes import AttributeModel

logger = logging.getLogger(__name__)


class VariableAttributes(AttributeModel):
    """Attributes that will be recorded in the output dataset.

    These metadata are to record information about the data properties and related
    fields (e.g., units, ancillary_variables, etc), user-facing metadata (e.g.,
    long_name, comment), as well as attributes related to quality checks and controls
    (e.g., valid_*, fail_*, and warn_* properties)."""

    units: Optional[str] = Field(
        description=(
            "A string indicating the units the data are measured in. Tsdat uses pint to"
            " handle unit conversions, so this string must be compatible with the pint"
            " list of units, if provided. A complete list of compatible units can be"
            " found here:"
            " https://github.com/hgrecco/pint/blob/master/pint/default_en.txt. If the"
            " property is unitless, then the string '1' should be used. If the units of"
            " the property are not known, then the units attribute should be omitted"
            " and the comment attribute should include a note indicating that units are"
            " not known. Doing so provides helpful context for data users."
        )
    )
    long_name: Optional[StrictStr] = Field(
        default=None,
        description=(
            "A brief label for the name of the measured property. The xarray python"
            " library automatically searches for this attribute to use as an axes label"
            " in plots, so the value should be suitable for display."
        ),
    )
    standard_name: Optional[StrictStr] = Field(
        default=None,
        description=(
            "A string exactly matching a value in the CF Standard Name table which is"
            " used to provide a standardized way of identifying variables and"
            " measurements across heterogeneous datasets and domains. If a suitable"
            " match does not exist, then this attribute should be omitted. The full"
            " list of CF Standard Names is at:"
            " https://cfconventions.org/Data/cf-standard-names."
        ),
    )
    coverage_content_type: Optional[str] = Field(
        default=None,
        description=(
            "An ISO 19115-1 code to indicate the source of the data (image, "
            "thematicClassification, physicalMeasurement, auxiliaryInformation, "
            "qualityInformation, referenceInformation, modelResult, or coordinate)."
        ),
    )
    cf_role: Optional[str] = Field(
        title="CF Role",
        default=None,
        description=(
            "Allowed values are defined in Chapter 9.5 CF guidelines and consist of:"
            " timeseries_id, profile_id, and trajectory_id, depending on the"
            " featureType represented in the dataset, as specified by the featureType"
            " global attribute."
        ),
    )
    accuracy: Optional[float] = Field(
        default=None,
        description=(
            "The sensor accuracy is the closeness of the measurements to the variable's"
            " true value. It should be given in the same units as the measured"
            " variable. If the instrument has been calibrated multiple times with"
            " different results, the most recent accuracy should be provided here (see"
            " instrument_variable:calibration_date)."
        ),
    )
    precision: Optional[float] = Field(
        default=None,
        description=(
            "The sensor precision is the closeness of the measurements to each other."
            " It should be given in the same units as the measured variable. If the"
            " instrument has been calibrated multiple times with different results, the"
            " most recent precision should be provided here (see"
            " instrument_variable:calibration_date)."
        ),
    )
    resolution: Optional[float] = Field(
        default=None,
        description=(
            "The sensor resolution is the smallest change it can represent in the"
            " quantity that it is measuring. It should be given in the same units as"
            " the measured variable."
        ),
    )
    instrument: Optional[str] = Field(
        default=None,
        description=(
            "Variable attribute to be specified on each geophysical variable to"
            " identify the instrument that collected the data. The value of the"
            " attribute should be set to another variable which contains the details of"
            " the instrument. There can be multiple instruments involved depending on"
            " if all the instances of the featureType in the collection come from the"
            " same instrument or not. If multiple instruments are involved, a variable"
            " should be defined for each instrument and referenced from the geophysical"
            " variable in a comma separated string."
        ),
    )
    make_model: Optional[str] = Field(
        title="Make and Model",
        default=None,
        description="The make and model of the instrument.",
    )
    calibration_date: Optional[str] = Field(
        default=None,
        description=(
            "The date the instrument was last calibrated. Value should be specified"
            " using ISO-8601 compatible strings."
        ),
    )
    comment: Optional[StrictStr] = Field(
        default=None,
        description=(
            "A user-friendly description of what the variable represents, how it was"
            " measured or derived, or any other relevant information that increases the"
            " ability of users to understand and use this data. This field plays a"
            " considerable role in creating self-documenting data, so we highly"
            " recommend including this field, especially for any variables which are"
            " particularly important for your dataset. Additionally, if the units for"
            " an attribute are unknown, then this field must include the phrase:"
            " 'Unknown units.' so that users know there is some uncertainty around this"
            " property. Variables that are unitless (e.g., categorical data or ratios),"
            " should set the 'units' to '1'."
        ),
    )
    valid_range: Optional[List[float]] = Field(
        default=None,
        min_items=2,
        max_items=2,
        description=(
            "A two-element list of [min, max] values outside of which the data should"
            " be treated as missing. If applying QC tests, then users should configure"
            " the quality managers to flag values outside of this range as having a"
            " 'Bad' assessment and replace those values with the variable's _FillValue."
        ),
    )
    fail_range: Optional[List[float]] = Field(
        default=None,
        min_items=2,
        max_items=2,
        description=(
            "A two-element list of [min, max] values outside of which the data should"
            " be teated with heavy skepticism as missing. If applying QC tests, then"
            " users should configure the quality managers to flag values outside of"
            " this range as having a 'Bad' assessment."
        ),
    )
    warn_range: Optional[List[float]] = Field(
        default=None,
        min_items=2,
        max_items=2,
        description=(
            "A two-element list of [min, max] values outside of which the data should"
            " be teated with some skepticism as missing. If applying QC tests, then"
            " users should configure the quality managers to flag values outside of"
            " this range as having an 'Indeterminate' assessment."
        ),
    )
    valid_delta: Optional[float] = Field(
        default=None,
        description=(
            "The largest difference between consecutive values in the data outside of"
            " which the data should be treated as missing. If applying QC tests, then"
            " users should configure the quality managers to flag values outside of"
            " this range as having a 'Bad' assessment and replace those values with the"
            " variable's _FillValue."
        ),
    )
    fail_delta: Optional[float] = Field(
        default=None,
        description=(
            "The largest difference between consecutive values in the data outside of"
            " which the data should be teated with heavy skepticism as missing. If"
            " applying QC tests, then users should configure the quality managers to"
            " flag values outside of this range as having a 'Bad' assessment."
        ),
    )
    warn_delta: Optional[float] = Field(
        default=None,
        description=(
            "The largest difference between consecutive values in the data outside of"
            " which the data should be teated with some skepticism as missing. If"
            " applying QC tests, then users should configure the quality managers to"
            " flag values outside of this range as having an 'Indeterminate'"
            " assessment."
        ),
    )
    fill_value: Optional[Any] = Field(
        default=None,
        alias="_FillValue",
        description=(
            "A value used to initialize the variable's data and indicate that the data"
            " is missing. Defaults to -9999 for numerical data. If choosing a different"
            " value, it is important to use a value that could not reasonably be"
            " mistaken for a physical value or data point."
        ),
    )

    @validator("units")
    def validate_unit(cls, unit_str: str) -> str:
        try:
            # Validate with pint unit registry
            unit_str = check_unit(unit_str, keep_exp=False)
        except PintError:
            logger.warning(
                f"'{unit_str}' is not a valid unit or combination of units. The string"
                " will be kept as-is."
            )
        return unit_str

    @root_validator
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
