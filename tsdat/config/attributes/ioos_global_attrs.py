from pydantic import (
    Field,
    HttpUrl,
)

from .acdd_global_attrs import ACDDGlobalAttrs


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
