from typing import Any, Dict, List, Optional

import numpy as np
from pydantic import (
    BaseModel,
    Extra,
    Field,
    StrictStr,
    validator,
)

from .variable_attributes import VariableAttributes


class Variable(BaseModel, extra=Extra.forbid):
    name: str = Field("", regex=r"^[a-zA-Z0-9_\(\)\/\[\]\{\}\.]+$")
    """Should be left empty. This property will be set automatically by the data_vars or
    coords pydantic model upon instantiation."""

    data: Optional[Any] = Field(
        description=(
            "If the variable is not meant to be retrieved from an input dataset and the"
            " value is known in advance, then the 'data' property should specify its"
            " value exactly as it should appear in the output dataset. This is commonly"
            " used for latitude/longitude/altitude data for datasets measured from a"
            " specific geographical location."
        )
    )
    dtype: StrictStr = Field(
        description=(
            "The numpy dtype of the underlying data. This is passed to numpy as"
            " the 'dtype' keyword argument used to initialize an array (e.g.,"
            " `numpy.array([1.0, 2.0], dtype='float')`). Commonly-used values include"
            " 'float', 'int', 'long'."
        )
    )
    dims: List[StrictStr] = Field(
        unique_items=True,
        description=(
            "A list of coordinate variable names that dimension this data variable."
            " Most commonly this will be set to ['time'], but for datasets where there"
            " are multiple dimensions (e.g., ADCP data measuring current velocities"
            " across time and several depths, it may look like ['time', 'depth'])."
        ),
    )
    attrs: VariableAttributes = Field(
        description=(
            "The attrs section is where variable-specific metadata are stored. This"
            " metadata is incredibly important for data users, and we recommend"
            " including several properties for each variable in order to have the"
            " greatest impact. In particular, we recommend adding the 'units',"
            " 'long_name', and 'standard_name' attributes, if possible."
        )
    )

    # TODO: Leftover code I assume? Remove?
    # @validator("name")
    # @classmethod
    # def validate_name_is_ascii(cls, v: str) -> str:
    #     if not v.isascii():
    #         raise ValueError(f"'{v}' contains a non-ascii character.")
    #     return v

    @validator("attrs")
    def set_default_fill_value(
        cls, attrs: VariableAttributes, values: Dict[str, Any]
    ) -> VariableAttributes:
        dtype: str = values["dtype"]
        if (
            "fill_value" in attrs.__fields_set__  # Preserve _FillValues set explicitly
            or (dtype == "str")
            or ("datetime" in dtype)
        ):
            return attrs
        attrs.fill_value = np.array([-9999.0], dtype=dtype)[0]  # type: ignore
        return attrs
