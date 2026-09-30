import logging
import re
from pathlib import Path
from typing import Dict, List, Pattern
from tsdat import PipelineConfig, read_yaml

logger = logging.getLogger(__name__)

__all__ = ["PipelineRegistry"]

class PipelineRegistry:
    """---------------------------------------------------------------------------------
    Registry of Pipelines that can be run on input keys.

    ---------------------------------------------------------------------------------"""

    def __init__(self):
        self._modules: List[str] = list()
        self._cache: Dict[Path, List[Pattern[str]]] = {}
        self._load()

    def dispatch(self, input_keys: List[str], clump: bool = False, multidispatch: bool = False):
        """-----------------------------------------------------------------------------
        Instantiates and runs the appropriate Pipeline for the provided input files.
        according to the ingest's `mapping` specifications.

        Args:
            input_keys (List[str]]): A list of keys that the pipeline will process. Most
                of the time these will be path(s) to files on the local file system.
            clump (bool): A flag indicating if the dispatcher should use a single
                pipeline to process the input keys. If True, the first key will be used to
                determine the pipeline to run. Defaults to False.
            multidispatch (bool): A flag indicating if the dispatcher is allowed to use
                multiple pipelines to process each input key. If True, any pipeline
                whose regex pattern matches an input key will be used to process the
                input key. Defaults to False.

        Returns:
            bool: True if the Pipeline ran without error, False otherwise.

        -----------------------------------------------------------------------------"""
        successes = 0
        failures = 0
        skipped = 0

        for input_key in input_keys:
            config_files = self._match_input_key(input_key)

            if not multidispatch and len(config_files) > 1:
                raise RuntimeError(
                        f"More than one match for input key '{input_key}'. Please"
                        " update the pipeline triggers to remove duplicate matches."
                        f" Found matches: {config_files}"
                    )
            elif not len(config_files):
                logger.warning(
                    "No pipeline configuration found matching input key '%s'", input_key
                )
                skipped += 1
            else:
                for config_file in config_files:
                    config = PipelineConfig.from_yaml(config_file)
                    pipeline = config.instantiate_pipeline()
                    inputs = input_keys if clump else [input_key]
                    logger.debug(
                        "Running pipeline %s on input %s",
                        pipeline.__repr_name__(),
                        inputs,
                    )
                    try:
                        pipeline.run(inputs)
                        successes += 1
                        if clump:
                            break
                    except BaseException:
                        logger.exception(
                            "Pipeline '%s' failed to process input: %s",
                            pipeline.__repr_name__(),
                            inputs,
                        )
                        failures += 1
        
        logger.info(
            "Processing completed with %s successes, %s failures, and %s skipped.",
            successes,
            failures,
            skipped,
        )
        return successes, failures, skipped

    def _load(self, folder: Path = Path("pipelines")):
        """-----------------------------------------------------------------------------
        Discovers all pipelines under the parent module and registers them for later
        use.

        Args:
            folder (str, optional): The module (relative to the "ingest-awaken"
                folder) under which individual ingests live. Defaults to "ingest".

        -----------------------------------------------------------------------------"""
        config_paths = list(folder.glob("**/*pipeline*.yaml"))
        for path in config_paths:
            trigger_strs = read_yaml(path)["triggers"]
            triggers = [re.compile(trigger) for trigger in trigger_strs]
            self._cache[path] = triggers
            logger.debug(
                "Registered pipeline config '%s' with triggers %s", path, trigger_strs
            )

    def _match_input_key(self, input_key: str) -> List[Path]:
        """-----------------------------------------------------------------------------
        Matches the provided key to registered pipeline configuration filepaths.

        Args:
            input_key (str): The input key (most commonly the path to a file to
                process).

        Returns:
            List[Path]: The pathes to the pipeline configuration files whose triggers
                match the input key.

        -----------------------------------------------------------------------------"""

        matches: List[Path] = []
        for path, regex_list in self._cache.items():
            for regex in regex_list:
                if regex.match(input_key):
                    matches.append(path)
                    break
        return matches
        
