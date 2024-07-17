# Note: The "VapRetrieverConfig" file exists only so that we can provide a schema to
# validate vap-retriever.yaml files. It is not used anywhere else in tsdat. This is why
# this class is located here and not in the tsdat.config.retrievers submodule.

from typing import Literal, Optional

from pydantic import BaseModel, Extra, Field

from ...config.retriever.retriever_config import RetrieverConfig


class VapRetrieverConfig(RetrieverConfig):
    class Parameters(BaseModel, extra=Extra.forbid):
        class FetchParameters(BaseModel, extra=Extra.forbid):
            time_padding: str = Field(
                regex=r"^[\+|\-]?[0-9]+[h|m|s|ms]$",
                description=(
                    "The time_padding parameter in the fetch_parameters section"
                    " specifies how far in time to look for data before the 'begin'"
                    " timestamp (e.g., -24h), after the 'end' timestamp (e.g., +24h),"
                    " or both (e.g., 24h).  Units of hours ('h'), minutes ('m'),"
                    " seconds ('s', default), and milliseconds ('ms') are allowed."
                ),
            )

        class TransformationParameters(BaseModel, extra=Extra.forbid):
            alignment: dict[str, Literal["LEFT", "RIGHT", "CENTER"]] = Field(
                description=(
                    "Defines the location of the window in respect to each output"
                    " timestamp (LEFT, RIGHT, or CENTER)"
                )
            )

            dim_range: dict[str, str] = Field(
                ...,
                alias="range",
                regex=r"^[0-9]+[a-zA-Z]+$",
                description=(
                    "Defines how far (in seconds) from the first/last timestamp to "
                    "search for the previous/next measurement."
                ),
            )
            width: dict[str, str] = Field(
                ...,
                regex=r"^[0-9]+[a-zA-Z]+$",
                description=(
                    'Defines the size of the averaging window in seconds ("600s" = 10 '
                    "min)."
                ),
            )

        fetch_parameters: Optional[FetchParameters] = None
        transformation_parameters: Optional[TransformationParameters] = Field(
            default=None,
            description=(
                "Transformation parameters. See "
                "https://tsdat.readthedocs.io/en/stable/tutorials/vap_pipelines/#configuration-files-vap_gps"
                " for more information."
            ),
        )

    parameters: Optional[Parameters] = None  # type: ignore
