import warnings
from typing import Any, Dict, Optional

from pydantic import (
    BaseModel,
    Extra,
    Field,
    HttpUrl,
    StrictStr,
    root_validator,
    validator,
)
from pydantic.fields import ModelField

from ..utils import get_datastream
from .utils import get_code_version


class AttributeModel(BaseModel, extra=Extra.allow):
    # HACK: root is needed for now: https://github.com/samuelcolvin/pydantic/issues/515
    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_all_ascii(cls, values: Dict[Any, Any]) -> Dict[str, str]:
        for key, value in values.items():
            if not isinstance(key, str) or not key.isascii():
                raise ValueError(f"'{key}' contains a non-ascii character.")
            if isinstance(value, str) and not value.isascii():
                raise ValueError(
                    f"attr '{key}' -> '{value}' contains a non-ascii character."
                )
        return values


class GlobalAttributes(AttributeModel):
    """Global attributes that will be recorded in the output dataset. These metadata are
    used to record data provenance information (e.g., location, institution, etc),
    construct datastream and file names (i.e., location_id, dataset_name, qualifier,
    temporal, and data_level attributes), as well as provide metadata that is useful for
    data users (e.g., title, description, ... ).
    """

    title: str = Field(
        min_length=1,
        description=(
            "A succinct description of the dataset. This value may be similar to a"
            " publication title and should be suitable for use as a title in plots or"
            " other references to this dataset."
        ),
    )
    description: str = Field(
        min_length=1,
        description=(
            "A user-friendly description of the dataset. It should provide"
            " enough context about the data for new users to quickly understand how the"
            " data can be used."
        ),
    )
    code_url: Optional[HttpUrl] = Field(
        default=None, description="Where the code is hosted."
    )
    Conventions: Optional[StrictStr] = Field(
        default="CF-1.6",
        description=(
            "A comma-separated list of the conventions that are followed by the"
            " dataset."
        ),
    )
    featureType: Optional[StrictStr] = Field(
        title="Feature Type",
        default="timeSeries",
        description="CF attribute for identifying the featureType.",
    )
    doi: Optional[StrictStr] = Field(
        title="DOI",
        default=None,
        description="The DOI that has been registered for this dataset, if applicable.",
    )
    institution: Optional[StrictStr] = Field(
        default=None,
        description=(
            "The institution or organization that produces or manages this data."
        ),
    )
    references: Optional[StrictStr] = Field(
        default=None,
        description=(
            "Optional attribute used to cite other data, algorithms, etc. as needed."
        ),
    )
    location_id: str = Field(
        min_length=1,
        regex=r"^[a-zA-Z0-9_]+$",  # alphanumeric and '_' characters
        description=(
            "A label or acronym for the location where the data were obtained"
            " from. Only alphanumeric characters and '_' are allowed."
        ),
    )
    dataset_name: str = Field(
        min_length=2,
        regex=r"^[a-z0-9_]+$",  # lowercase alphanumeric and '_' characters
        description=(
            "A string used to identify the data being produced. Ideally"
            " resembles a shortened lowercase version of the title. Only lowercase"
            " alphanumeric characters and '_' are allowed."
        ),
    )
    qualifier: Optional[str] = Field(
        default=None,
        min_length=1,
        regex=r"^[a-zA-Z0-9_]+$",  # lowercase alphanumeric and '_' characters
        description=(
            "An optional string which distinguishes these data from other"
            " datasets produced by the same instrument. Only alphanumeric characters"
            " and '_' are allowed."
        ),
    )
    temporal: Optional[str] = Field(
        default=None,
        min_length=2,
        regex=r"^[0-9]+[a-zA-Z]+$",
        description=(
            "An optional string which describes the temporal resolution of the data (if"
            " spaced in regular intervals). This string should be formatted as a number"
            " followed by a unit of measurement, e.g., '10m' would indicate the data is"
            " sampled every ten minutes. Only lowercase alphanumeric characters are"
            " allowed."
        ),
    )
    data_level: str = Field(
        min_length=2,
        max_length=3,
        regex=r"^[a-z0-9]+$",  # lowercase alphanumeric characters
        description=(
            "A string used to indicate the level of processing of the output data. It"
            " should be formatted as a letter followed by a number. Typical values for"
            " this include: a1 - data is ingested (no qc), b1 - data is ingested and"
            " quality checks applied, c1 (or higher) - one or more a* or b* datastreams"
            " used to create a higher-level data product. Only lowercase alphanumeric"
            " characters are allowed."
        ),
    )

    # Autogenerated attributes:
    datastream: StrictStr = Field(
        "",
        description=(
            "Typically used as a label that uniquely identifies this data product from"
            " any other data product. For file-based storage systems, the datastream"
            " attribute is typically used to generate directory structures as"
            " f'{location_id}/{datastream}/', with files in that directory typically"
            " named as f'{datastream}.{date}.{time}.{ext}'. This attribute is"
            " AUTO-GENERATED at run-time, unless it is explicitly set in the config"
            " file, in which case the value in the config file will override the"
            " default. The default value for 'datastream' is as follows:\n"
            ' f"{location_id}.{dataset_name}{_qualifier}{_temporal}.{data_level}",'
            " \nwhere '_qualifier' and '_temporal' are both prepended with a literal"
            " '-' character if they are provided. This gives some separation between"
            " the 'dataset_name', 'qualifier', and 'temporal' attributes and makes it"
            " possible to parse out these specific attributes given a complete"
            " datastream label."
        ),
    )
    history: StrictStr = Field(
        "",
        description=(
            "Attribute that will be recorded automatically by the pipeline. A"
            " warning will be raised if this is set in the config file."
        ),
    )
    code_version: StrictStr = Field(
        default_factory=get_code_version,
        description=(
            "Attribute that will be recorded automatically by the pipeline. A warning"
            " will be raised if this is set in the config file. The code_version"
            " attribute reads the 'CODE_VERSION' environment variable or parses the git"
            " history to determine the version of the code. Semantic versioning is used"
            " by default (v'major.minor.micro'; e.g., 1.2.3)."
        ),
    )

    @validator("history", "code_version", pre=True)
    @classmethod
    def warn_if_dynamic_properties_are_set(cls, v: str, field: ModelField) -> str:
        if v:
            warnings.warn(
                f"The '{field.name}' attribute should not be set explicitly. The"
                f" current value of '{v}' will be ignored."
            )
        return ""

    @root_validator(skip_on_failure=True)
    @classmethod
    def add_datastream_field(cls, values: Dict[str, StrictStr]) -> Dict[str, StrictStr]:
        if not values["datastream"]:
            values["datastream"] = get_datastream(**values)
        return values


class ACDDGlobalAttrs(GlobalAttributes):
    description: str = Field(
        description=(
            "A user-friendly description of the dataset. It should provide"
            " enough context about the data for new users to quickly understand how the"
            " data can be used."
        ),
        default=None,
    )
    summary: str = Field(
        description=(
            "A paragraph describing the dataset, analogous to an abstract for a paper."
        ),
        minLength=1,
    )
    Conventions: str = Field(
        description=(
            "A comma-separated list of the conventions that are followed by the"
            " dataset. For files that follow this version of ACDD, include the string"
            " 'ACDD-1.3'."
        ),
    )
    id: str = Field(
        title="ID",
        description=(
            "An identifier for the data set, provided by and unique within its naming"
            " authority. The combination of the 'naming_authority' and the 'id' should"
            " be globally unique, but the 'id' can be globally unique by itself also."
            " IDs can be URLs, URNs, DOIs, meaningful text strings, a local key, or any"
            " other unique string of characters. The id should not include white space"
            " characters."
        ),
        default=None,
    )
    naming_authority: str = Field(
        description=(
            "The organization that provides the initial id (see above) for the dataset."
            " The naming authority should be uniquely specified by this attribute. We"
            " recommend using reverse-DNS naming for the naming authority; URIs are"
            " also acceptable. Example: 'edu.ucar.unidata'."
        ),
        default=None,
    )
    source: str = Field(
        description=(
            "The method of production of the original data. If it was model-generated,"
            " source should name the model and its version. If it is observational,"
            " source should characterize it. Examples: 'temperature from CTD #1234';"
            " 'world model v.0.1'."
        ),
        default=None,
    )
    processing_level: str = Field(
        description=(
            "A textual description of the processing (or quality control) level of the"
            " data."
        ),
        default=None,
    )
    comment: str = Field(
        description="Miscellaneous information about the data, not captured elsewhere.",
        default=None,
    )
    acknowledgement: str = Field(
        description=(
            "A place to acknowledge various types of support for the project that"
            " produced this data."
        ),
        default=None,
    )
    license: str = Field(
        description=(
            "Provide the URL to a standard or specific license, enter 'Freely"
            " Distributed' or 'None', or describe any restrictions to data access and"
            " distribution in free text."
        ),
        default=None,
    )
    standard_name_vocabulary: str = Field(
        description=(
            "The name and version of the controlled vocabulary from which variable"
            " standard names are taken. Example: 'CF Standard Name Table v27'."
        ),
        default=None,
    )
    date_created: str = Field(
        description=(
            "The date on which this version of the data was created. (Modification of"
            " values implies a new version, hence this would be assigned the date of"
            " the most recent values modification.) Metadata changes are not considered"
            " when assigning the date_created. The ISO 8601:2004 extended date format"
            " is recommended."
        ),
        default=None,
    )
    creator_name: str = Field(
        description=(
            "The name of the person (or other creator type specified by the"
            " creator_type attribute) principally responsible for creating this data."
        ),
        default=None,
    )
    creator_email: str = Field(
        description=(
            "The email address of the person (or other creator type specified by the"
            " creator_type attribute) principally responsible for creating this data."
        ),
        default=None,
    )
    creator_url: HttpUrl = Field(
        title="Creator URL",
        description=(
            "The URL of the person (or other creator type specified by the creator_type"
            " attribute) principally responsible for creating this data."
        ),
        default=None,
    )
    creator_type: str = Field(
        description=(
            "Specifies type of creator with one of the following: 'person', 'group',"
            " 'institution', or 'position'. If this attribute is not specified, the"
            " creator is assumed to be a person."
        ),
        default=None,
    )
    creator_institution: str = Field(
        description=(
            "The institution of the creator; should uniquely identify the creator's"
            " institution. This attribute's value should be specified even if it"
            " matches the value of publisher_institution, or if creator_type is"
            " institution."
        ),
        default=None,
    )
    contributor_name: str = Field(
        title="Contributor Name(s)",
        description=(
            "The name of any individuals, projects, or institutions that contributed to"
            " the creation of this data. May be presented as free text, or in a"
            " structured format compatible with conversion to ncML (e.g., insensitive"
            " to changes in whitespace, including end-of-line characters)."
        ),
        default=None,
    )
    contributor_role: str = Field(
        title="Contributor Role(s)",
        description=(
            "The role of any individuals, projects, or institutions that contributed to"
            " the creation of this data. May be presented as free text, or in a"
            " structured format compatible with conversion to ncML (e.g., insensitive"
            " to changes in whitespace, including end-of-line characters). Multiple"
            " roles should be presented in the same order and number as the names in"
            " contributor_names."
        ),
        default=None,
    )
    institution: str = Field(
        description=(
            "The name of the institution principally responsible for originating this"
            " data."
        ),
        default=None,
    )
    program: str = Field(
        description=(
            "The overarching program(s) of which the dataset is a part. A program"
            " consists of a set (or portfolio) of related and possibly interdependent"
            " projects that meet an overarching objective. Examples: 'GHRSST', 'NOAA"
            " CDR', 'NASA EOS', 'JPSS', 'GOES-R'."
        ),
        default=None,
    )
    project: str = Field(
        description=(
            "The name of the project(s) principally responsible for originating this"
            " data. Multiple projects can be separated by commas, as described under"
            " Attribute Content Guidelines. Examples: 'PATMOS-X', 'Extended Continental"
            " Shelf Project'."
        ),
        default=None,
    )
    publisher_name: str = Field(
        description=(
            "The name of the person (or other entity specified by the publisher_type"
            " attribute) responsible for publishing the data file or product to users,"
            " with its current metadata and format."
        ),
        default=None,
    )
    publisher_email: str = Field(
        description=(
            "The email address of the person (or other entity specified by the"
            " publisher_type attribute) responsible for publishing the data file or"
            " product to users, with its current metadata and format."
        ),
        default=None,
    )
    publisher_url: HttpUrl = Field(
        title="Publisher URL",
        description=(
            "The URL of the person (or other entity specified by the publisher_type"
            " attribute) responsible for publishing the data file or product to users,"
            " with its current metadata and format."
        ),
        default=None,
    )
    publisher_type: str = Field(
        description=(
            "Specifies type of publisher with one of the following: 'person', 'group',"
            " 'institution', or 'position'. If this attribute is not specified, the"
            " publisher is assumed to be a person."
        ),
        default=None,
    )
    publisher_institution: str = Field(
        description=(
            "Specifies type of publisher with one of the following: 'person', 'group',"
            " 'institution', or 'position'. If this attribute is not specified, the"
            " publisher is assumed to be a person."
        ),
        default=None,
    )
    geospatial_bounds: str = Field(
        description=(
            "Describes the data's 2D or 3D geospatial extent in OGC's Well-Known Text"
            " (WKT) Geometry format (reference the OGC Simple Feature Access (SFA)"
            " specification). The meaning and order of values for each point's"
            " coordinates depends on the coordinate reference system (CRS). The ACDD"
            " default is 2D geometry in the EPSG:4326 coordinate reference system. The"
            " default may be overridden with geospatial_bounds_crs and"
            " geospatial_bounds_vertical_crs (see those attributes). EPSG:4326"
            " coordinate values are latitude (decimal degrees_north) and longitude"
            " (decimal degrees_east), in that order. Longitude values in the default"
            " case are limited to the [-180, 180) range. Example: 'POLYGON ((40.26"
            " -111.29, 41.26 -111.29, 41.26 -110.29, 40.26 -110.29, 40.26 -111.29))'."
        ),
        default=None,
    )
    geospatial_bounds_crs: str = Field(
        title="Geospatial Bounds Coordinate Reference System",
        description=(
            "The coordinate reference system (CRS) of the point coordinates in the"
            " geospatial_bounds attribute. This CRS may be 2-dimensional or"
            " 3-dimensional, but together with geospatial_bounds_vertical_crs, if that"
            " attribute is supplied, must match the dimensionality, order, and meaning"
            " of point coordinate values in the geospatial_bounds attribute. If"
            " geospatial_bounds_vertical_crs is also present then this attribute must"
            " only specify a 2D CRS. EPSG CRSs are strongly recommended. If this"
            " attribute is not specified, the CRS is assumed to be EPSG:4326. Examples:"
            " 'EPSG:4979' (the 3D WGS84 CRS), 'EPSG:4047'."
        ),
        default=None,
    )
    geospatial_bounds_vertical_crs: str = Field(
        title="Geospatial Bounds Vertical Coordinate Reference System",
        description=(
            "The vertical coordinate reference system (CRS) for the Z axis of the point"
            " coordinates in the geospatial_bounds attribute. This attribute cannot be"
            " used if the CRS in geospatial_bounds_crs is 3-dimensional; to use this"
            " attribute, geospatial_bounds_crs must exist and specify a 2D CRS. EPSG"
            " CRSs are strongly recommended. There is no default for this attribute"
            " when not specified. Examples: 'EPSG:5829' (instantaneous height above sea"
            " level), 'EPSG:5831' (instantaneous depth below sea level), or 'EPSG:5703'"
            " (NAVD88 height)."
        ),
        default=None,
    )
    geospatial_lat_min: float = Field(
        title="Geospatial Latitude Minimum",
        description=(
            "Describes a simple lower latitude limit; may be part of a 2- or"
            " 3-dimensional bounding region. Geospatial_lat_min specifies the"
            " southernmost latitude covered by the dataset."
        ),
        default=None,
    )
    geospatial_lat_max: float = Field(
        title="Geospatial Latitude Maximum",
        description=(
            "Describes a simple upper latitude limit; may be part of a 2- or"
            " 3-dimensional bounding region. Geospatial_lat_max specifies the"
            " northernmost latitude covered by the dataset."
        ),
        default=None,
    )
    geospatial_lon_min: float = Field(
        title="Geospatial Longitude Minimum",
        description=(
            "Describes a simple longitude limit; may be part of a 2- or 3-dimensional"
            " bounding region. geospatial_lon_min specifies the westernmost longitude"
            " covered by the dataset. See also geospatial_lon_max."
        ),
        default=None,
    )
    geospatial_lon_max: float = Field(
        title="Geospatial Longitude Maximum",
        description=(
            "Describes a simple longitude limit; may be part of a 2- or 3-dimensional"
            " bounding region. geospatial_lon_max specifies the easternmost longitude"
            " covered by the dataset. Cases where geospatial_lon_min is greater than"
            " geospatial_lon_max indicate the bounding box extends from"
            " geospatial_lon_max, through the longitude range discontinuity meridian"
            " (either the antimeridian for -180:180 values, or Prime Meridian for 0:360"
            " values), to geospatial_lon_min; for example, geospatial_lon_min=170 and"
            " geospatial_lon_max=-175 incorporates 15 degrees of longitude (ranges 170"
            " to 180 and -180 to -175)."
        ),
        default=None,
    )
    geospatial_vertical_min: float = Field(
        title="Geospatial Vertical Minimum",
        description=(
            "Describes the numerically smaller vertical limit; may be part of a 2- or"
            " 3-dimensional bounding region. See geospatial_vertical_positive and"
            " geospatial_vertical_units."
        ),
        default=None,
    )
    geospatial_vertical_max: float = Field(
        title="Geospatial Vertical Maximum",
        description=(
            "Describes the numerically larger vertical limit; may be part of a 2- or"
            " 3-dimensional bounding region. See geospatial_vertical_positive and"
            " geospatial_vertical_units."
        ),
        default=None,
    )
    geospatial_vertical_positive: str = Field(
        title="Geospatial Vertical Maximum",
        description=(
            "One of 'up' or 'down'. If up, vertical values are interpreted as"
            " 'altitude', with negative values corresponding to below the reference"
            " datum (e.g., under water). If down, vertical values are interpreted as"
            " 'depth', positive values correspond to below the reference datum. Note"
            " that if geospatial_vertical_positive is down ('depth' orientation), the"
            " geospatial_vertical_min attribute specifies the data's vertical location"
            " furthest from the earth's center, and the geospatial_vertical_max"
            " attribute specifies the location closest to the earth's center."
        ),
        default=None,
    )
    geospatial_lat_units: str = Field(
        title="Geospatial Latitude Units",
        description=(
            "Units for the latitude axis described in 'geospatial_lat_min' and"
            " 'geospatial_lat_max' attributes. These are presumed to be 'degree_north';"
            " other options from udunits may be specified instead."
        ),
        default=None,
    )
    geospatial_lat_resolution: str = Field(
        title="Geospatial Latitude Resolution",
        description=(
            "Information about the targeted spacing of points in latitude. Recommend"
            " describing resolution as a number value followed by the units. Examples:"
            " '100 meters', '0.1 degree'."
        ),
        default=None,
    )
    geospatial_lon_units: str = Field(
        title="Geospatial Longitude Units",
        description=(
            "Units for the longitude axis described in 'geospatial_lon_min' and"
            " 'geospatial_lon_max' attributes. These are presumed to be 'degree_east';"
            " other options from udunits may be specified instead."
        ),
        default=None,
    )
    geospatial_lon_resolution: str = Field(
        title="Geospatial Longitude Resolution",
        description=(
            "Information about the targeted spacing of points in longitude. Recommend"
            " describing resolution as a number value followed by units. Examples: '100"
            " meters', '0.1 degree'."
        ),
        default=None,
    )
    geospatial_vertical_units: str = Field(
        description=(
            "Units for the vertical axis described in 'geospatial_vertical_min' and"
            " 'geospatial_vertical_max' attributes. The default is EPSG:4979 (height"
            " above the ellipsoid, in meters); other vertical coordinate reference"
            " systems may be specified. Note that the common oceanographic practice of"
            " using pressure for a vertical coordinate, while not strictly a depth, can"
            " be specified using the unit bar. Examples: 'EPSG:5829' (instantaneous"
            " height above sea level), 'EPSG:5831' (instantaneous depth below sea"
            " level)."
        ),
        default=None,
    )
    geospatial_vertical_resolution: str = Field(
        description=(
            "Information about the targeted vertical spacing of points. Example: '25"
            " meters'."
        ),
        default=None,
    )
    time_coverage_start: str = Field(
        description=(
            "Describes the time of the first data point in the data set. Use the ISO"
            " 8601:2004 date format, preferably the extended format as recommended in"
            " the Attribute Content Guidance section."
        ),
        default=None,
    )
    time_coverage_end: str = Field(
        description=(
            "Describes the time of the last data point in the data set. Use ISO"
            " 8601:2004 date format, preferably the extended format as recommended in"
            " the Attribute Content Guidance section."
        ),
        default=None,
    )
    time_coverage_duration: str = Field(
        description=(
            "Describes the duration of the data set. Use ISO 8601:2004 duration format,"
            " preferably the extended format as recommended in the Attribute Content"
            " Guidance section."
        ),
        default=None,
    )
    time_coverage_resolution: str = Field(
        description=(
            "Describes the targeted time period between each value in the data set. Use"
            " ISO 8601:2004 duration format, preferably the extended format as"
            " recommended in the Attribute Content Guidance section."
        ),
        default=None,
    )
    date_modified: str = Field(
        description=(
            "The date on which the data was last modified. Note that this applies just"
            " to the data, not the metadata. The ISO 8601:2004 extended date format is"
            " recommended."
        ),
        default=None,
    )
    date_issued: str = Field(
        description=(
            "The date on which this data (including all modifications) was formally"
            " issued (i.e., made available to a wider audience). Note that these apply"
            " just to the data, not the metadata. The ISO 8601:2004 extended date"
            " format is recommended."
        ),
        default=None,
    )
    date_metadata_modified: str = Field(
        description=(
            "The date on which the metadata was last modified. The ISO 8601:2004"
            " extended date format is recommended."
        ),
        default=None,
    )
    product_version: str = Field(
        description=(
            "Version identifier of the data file or product as assigned by the data"
            " creator. For example, a new algorithm or methodology could result in a"
            " new product_version."
        ),
        default=None,
    )
    keywords: str = Field(
        description=(
            "A comma-separated list of key words and/or phrases. Keywords may be common"
            " words or phrases, terms from a controlled vocabulary (GCMD is often"
            " used), or URIs for terms from a controlled vocabulary (see also"
            " 'keywords_vocabulary' attribute."
        ),
        minLength=1,
    )
    keywords_vocabulary: str = Field(
        description=(
            "If you are using a controlled vocabulary for the words/phrases in your"
            " 'keywords' attribute, this is the unique name or identifier of the"
            " vocabulary from which keywords are taken. If more than one keyword"
            " vocabulary is used, each may be presented with a prefix and a following"
            " comma, so that keywords may optionally be prefixed with the controlled"
            " vocabulary key. Example: 'GCMD:GCMD Keywords, CF:NetCDF COARDS Climate"
            " and Forecast Standard Names'."
        ),
        default=None,
    )
    platform: str = Field(
        description=(
            "Name of the platform(s) that supported the sensor data used to create this"
            " data set or product. Platforms can be of any type, including satellite,"
            " ship, station, aircraft or other. Indicate controlled vocabulary used in"
            " platform_vocabulary."
        ),
        default=None,
    )
    platform_vocabulary: str = Field(
        description=(
            "Controlled vocabulary for the names used in the 'platform' attribute."
        ),
        default=None,
    )
    instrument: str = Field(
        description=(
            "Name of the contributing instrument(s) or sensor(s) used to create this"
            " data set or product. Indicate controlled vocabulary used in"
            " instrument_vocabulary."
        ),
        default=None,
    )
    instrument_vocabulary: str = Field(
        description=(
            "Controlled vocabulary for the names used in the 'instrument' attribute."
        ),
        default=None,
    )
    cdm_data_type: str = Field(
        title="Common Data Model Data Type",
        description=(
            "The data type, as derived from Unidata's Common Data Model Scientific Data"
            " types and understood by THREDDS. (This is a THREDDS 'dataType', and is"
            " different from the CF NetCDF attribute 'featureType', which indicates a"
            " Discrete Sampling Geometry file in CF.)."
        ),
        default=None,
    )
    metadata_link: str = Field(
        description=(
            "A URL that gives the location of more complete metadata. A persistent URL"
            " is recommended for this attribute."
        ),
        default=None,
    )


class IOOSGlobalAttrs(ACDDGlobalAttrs):
    featureType: str = Field(
        title="Feature Type",
        description="CF attribute for identifying the featureType.",
    )
    id: str = Field(
        title="ID",
        description=(
            "An identifier for the data set, provided by and unique within its naming"
            " authority. The combination of the 'naming_authority' and the 'id' should"
            " be globally unique, but the 'id' can be globally unique by itself also."
            " IDs can be URLs, URNs, DOIs, meaningful text strings, a local key, or any"
            " other unique string of characters. The id should not include white space"
            " characters."
        ),
    )
    infoUrl: HttpUrl = Field(
        title="Info URL",
        description="URL for background information about this dataset.",
    )
    naming_authority: str = Field(
        description=(
            "The organization that provides the initial id (see above) for the dataset."
            " The naming authority should be uniquely specified by this attribute. We"
            " recommend using reverse-DNS naming for the naming authority; URIs are"
            " also acceptable. Example: 'edu.ucar.unidata'."
        ),
    )
    license: str = Field(
        description=(
            "Provide the URL to a standard or specific license, enter 'Freely"
            " Distributed' or 'None', or describe any restrictions to data access and"
            " distribution in free text."
        ),
    )
    standard_name_vocabulary: str = Field(
        description=(
            "The name and version of the controlled vocabulary from which variable"
            " standard names are taken. Example: 'CF Standard Name Table v27'."
        ),
    )
    creator_email: str = Field(
        description=(
            "The email address of the person (or other creator type specified by the"
            " creator_type attribute) principally responsible for creating this data."
        ),
    )
    creator_url: HttpUrl = Field(
        title="Creator URL",
        description=(
            "The URL of the person (or other creator type specified by the creator_type"
            " attribute) principally responsible for creating this data."
        ),
    )
    creator_institution: str = Field(
        description=(
            "The institution of the creator; should uniquely identify the creator's"
            " institution. This attribute's value should be specified even if it"
            " matches the value of publisher_institution, or if creator_type is"
            " institution."
        ),
    )
    creator_country: str = Field(
        description=(
            "Country of the person or organization that operates a platform or network,"
            " which collected the observation data."
        ),
    )
    creator_state: str = Field(
        description=(
            "State or province of the person or organization that collected the data."
        ),
        default=None,
    )
    creator_institution_url: HttpUrl = Field(
        title="Creator Institution URL",
        description=(
            "URL for the institution that collected the data. For clarity, it is"
            " recommended that this field is specified even if the creator_type is"
            " institution and a creator_url is provided."
        ),
        default=None,
    )
    creator_sector: str = Field(
        description=(
            "IOOS classifier (https://mmisw.org/ont/ioos/sector) that best describes"
            " the platform (network) operator's societal sector."
        ),
    )
    contributor_email: str = Field(
        title="Contributor Email(s)",
        description=(
            "IOOS classifier (https://mmisw.org/ont/ioos/sector) that best describes"
            " the platform (network) operator's societal sector."
        ),
        default=None,
    )
    contributor_role_vocabulary: str = Field(
        description=(
            "The URL of the controlled vocabulary used for the contributor_role"
            " attribute. The default is"
            " “https://vocab.nerc.ac.uk/collection/G04/current/”."
        ),
        default=None,
    )
    contributor_url: HttpUrl = Field(
        title="Contributor URL(s)",
        description=(
            "The URL of the individuals or institutions that contributed to the"
            " creation of this data. Multiple URLs should be given in CSV format, and"
            " presented in the same order and number as the names in contributor_names."
        ),
        default=None,
    )
    publisher_email: str = Field(
        description=(
            "The email address of the person (or other entity specified by the"
            " publisher_type attribute) responsible for publishing the data file or"
            " product to users, with its current metadata and format."
        ),
    )
    publisher_url: HttpUrl = Field(
        title="Publisher URL",
        description=(
            "The URL of the person (or other entity specified by the publisher_type"
            " attribute) responsible for publishing the data file or product to users,"
            " with its current metadata and format."
        ),
    )
    publisher_institution: str = Field(
        description=(
            "Specifies type of publisher with one of the following: 'person', 'group',"
            " 'institution', or 'position'. If this attribute is not specified, the"
            " publisher is assumed to be a person."
        ),
    )
    publisher_country: str = Field(
        description="Country of the person or organization that distributes the data.",
    )
    publisher_state: str = Field(
        description=(
            "State or province of the person or organization that distributes the data."
        ),
        default=None,
    )
    platform: str = Field(
        description=(
            "Name of the platform(s) that supported the sensor data used to create this"
            " data set or product. Platforms can be of any type, including satellite,"
            " ship, station, aircraft or other. Indicate controlled vocabulary used in"
            " platform_vocabulary."
        ),
    )
    platform_vocabulary: str = Field(
        description=(
            "Controlled vocabulary for the names used in the 'platform' attribute."
        ),
    )
    platform_id: str = Field(
        title="Platform ID",
        description=(
            "An optional, short identifier for the platform, if the data provider"
            " prefers to define an id that differs from the dataset identifier, as"
            " specified by the id attribute. Platform_id should be a single"
            " alphanumeric string with no blank spaces."
        ),
        default=None,
    )
    platform_name: str = Field(
        title="Platform Name",
        description=(
            "A descriptive, long name for the platform used in collecting the data. The"
            " value of platform_name will be used to label the platform in downstream"
            " applications, such as IOOS’ National Products (Environmental Sensor Map,"
            " EDS, etc)."
        ),
    )
    WMO_platform_code: str = Field(
        title="WMO Platform Code",
        description=(
            "The WMO identifier for the platform used to measure the data. This"
            " identifier can be any of the following types: 1. WMO ID for buoys"
            " (numeric, 5 digits), 2. WMO ID for gliders (numeric, 7 digits), 3. NWS ID"
            " (alphanumeric, 5 digits). When a dataset is assigned a wmo_platform_code"
            " it is thereby assigned a secondary Asset Identifier for the 'WMO'"
            " naming_authority. See"
            " https://ioos.github.io/ioos-metadata/ioos-metadata-profile-v1-2.html#rules-for-ioos-asset-identifier-generation"
            " for more details."
        ),
        default=None,
    )
