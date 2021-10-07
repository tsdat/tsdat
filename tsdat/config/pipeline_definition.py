from typing import Dict
from tsdat.exceptions import DefinitionError


class PipelineKeys:
    """Class that provides a handle for keys in the pipeline section of the
    pipeline config file."""

    TYPE = "type"
    INPUT_DATA_LEVEL = "input_data_level"
    OUTPUT_DATA_LEVEL = "data_level"

    LOCATION_ID = "location_id"
    DATASET_NAME = "dataset_name"
    QUALIFIER = "qualifier"
    TEMPORAL = "temporal"


class PipelineDefinition:
    """Wrapper for the pipeline portion of the pipeline config file.

    :param dictionary: The pipeline component of the pipeline config file.
    :type dictionary: Dict[str]
    :raises DefinitionError:
        Raises DefinitionError if one of the file naming components
        contains an illegal character.
    """

    def __init__(self, dictionary: Dict[str, Dict]):
        self.dictionary = dictionary

        # Parse pipeline type and output data level
        valid_types = ["Ingest", "VAP"]
        pipeline_type = dictionary.get(PipelineKeys.TYPE, None)
        if pipeline_type not in valid_types:
            raise DefinitionError(f"Pipeline type must be one of: {valid_types}")
        self.type: str = pipeline_type

        # Parse input data level
        default_input_data_level = {"Ingest": "00", "VAP": "a1"}.get(pipeline_type)
        input_data_level = dictionary.get(
            PipelineKeys.INPUT_DATA_LEVEL, default_input_data_level
        )
        self.input_data_level: str = input_data_level

        # Parse output data level
        default_output_data_level = {"Ingest": "a1", "VAP": "b1"}.get(pipeline_type)
        output_data_level = dictionary.get(
            PipelineKeys.OUTPUT_DATA_LEVEL, default_output_data_level
        )
        self.output_data_level: str = output_data_level

        # Parse file naming components
        self.location_id = dictionary.get(PipelineKeys.LOCATION_ID)
        self.dataset_name = dictionary.get(PipelineKeys.DATASET_NAME)
        self.qualifier = dictionary.get(PipelineKeys.QUALIFIER, "")
        self.temporal = dictionary.get(PipelineKeys.TEMPORAL, "")

        self.check_file_name_components()

        # Parse datastream_name
        base_datastream_name = f"{self.location_id}.{self.dataset_name}"
        if self.qualifier:
            base_datastream_name += f"-{self.qualifier}"
        if self.temporal:
            base_datastream_name += f"-{self.temporal}"
        self.input_datastream_name = f"{base_datastream_name}.{input_data_level}"
        self.output_datastream_name = f"{base_datastream_name}.{output_data_level}"

    def check_file_name_components(self):
        """Performs sanity checks on the config properties used in naming
        files output by tsdat pipelines.

        :raises DefinitionError:
            Raises DefinitionError if a component has been set improperly.
        """
        illegal_characters = [".", "-", "' '"]
        components = [
            self.location_id,
            self.dataset_name,
            self.qualifier,
            self.temporal,
        ]

        def _is_bad(component) -> bool:
            bad_chars = [char in component for char in illegal_characters]
            return sum(bad_chars)

        bad_components = [component for component in components if _is_bad(component)]
        if bad_components:
            message = "Some filename components contained illegal characters: \n"
            message += "\n".join(illegal_characters)
            message += f"\nIllegal characters include: {illegal_characters}"
            raise DefinitionError(message)
