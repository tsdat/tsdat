class QCError(Exception):
    """Indicates that a given Quality Manager failed with a fatal error."""

    pass


class DefinitionError(Exception):
    """Indicates a fatal error within the YAML Dataset Definition."""

    pass
