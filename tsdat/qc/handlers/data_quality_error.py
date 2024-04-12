class DataQualityError(ValueError):
    """Raised when the quality of a variable indicates a fatal error has occurred.
    Manual review of the data in question is often recommended in this case."""
