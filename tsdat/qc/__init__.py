"""The tsdat.qc package provides the classes that the data pipeline
uses to manage quality control/quality assurance for the dataset.  This
includes the infrastructure to run quality tests and handle failures,
as well specific checkers and handlers that can be specified in the
pipeline config file.

We warmly welcome community contribututions to increase this default list.
"""
from .qc import QualityManagement, QualityManager
from .checkers import (
    QualityChecker,
    CheckWarnMax,
    CheckFailMax,
    CheckFailMin,
    CheckMax,
    CheckMin,
    CheckMissing,
    CheckMonotonic,
    CheckValidDelta,
    CheckValidMax,
    CheckValidMin,
    CheckWarnMin,
)
from .handlers import (
    QualityHandler,
    QCParamKeys,
    FailPipeline,
    RecordQualityResults,
    RemoveFailedValues,
    SendEmailAWS,
)
