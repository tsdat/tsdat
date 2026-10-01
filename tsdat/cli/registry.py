"""Discover and dispatch ingest pipelines in the current pipeline repository."""

import logging
import re
from collections import defaultdict
from pathlib import Path
from re import Pattern

from tsdat import PipelineConfig, read_yaml

logger = logging.getLogger(__name__)


class PipelineRegistry:
    """Match input files against pipeline triggers and run the selected pipelines."""

    def __init__(self, folder: Path = Path("pipelines")) -> None:
        if not folder.is_dir():
            raise ValueError(
                "Run 'tsdat ingest' from a pipeline repository containing pipelines/."
            )
        self._triggers: dict[Path, list[Pattern[str]]] = {}
        for path in sorted(folder.glob("**/*pipeline*.yaml")):
            triggers = read_yaml(path)["triggers"]
            self._triggers[path] = [re.compile(trigger) for trigger in triggers]
            logger.debug(
                "Registered pipeline config '%s' with triggers %s", path, triggers
            )

    def _matches(self, input_key: str) -> list[Path]:
        return [
            path
            for path, triggers in self._triggers.items()
            if any(trigger.match(input_key) for trigger in triggers)
        ]

    def dispatch(
        self, input_keys: list[str], clump: bool = False, multidispatch: bool = False
    ) -> tuple[int, int, int]:
        """Run each selected pipeline once per input, or once per group with clump.

        In clump mode each pipeline receives only the inputs that match its triggers.
        With multidispatch an input can belong to more than one pipeline group.
        """
        groups: dict[Path, list[str]] = defaultdict(list)
        runs: list[tuple[Path, list[str]]] = []
        skipped = 0

        # Resolve every input before running anything, so ambiguous triggers do not
        # leave a partially processed batch behind.
        for input_key in input_keys:
            matches = self._matches(input_key)
            if len(matches) > 1 and not multidispatch:
                raise ValueError(
                    f"More than one pipeline matches input '{input_key}': {matches}. "
                    "Update the pipeline triggers or use --multidispatch."
                )
            if not matches:
                logger.warning(
                    "No pipeline configuration found matching input key '%s'", input_key
                )
                skipped += 1
            for path in matches:
                if clump:
                    groups[path].append(input_key)
                else:
                    runs.append((path, [input_key]))

        if clump:
            runs = list(groups.items())

        successes = failures = 0
        for path, inputs in runs:
            # Configuration errors are not processing failures: surface them to the caller.
            pipeline = PipelineConfig.from_yaml(path).instantiate_pipeline()
            logger.debug(
                "Running pipeline %s on input %s", pipeline.__repr_name__(), inputs
            )
            try:
                pipeline.run(inputs)
                successes += 1
            except Exception:
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
