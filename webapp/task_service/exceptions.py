class TaskNotFoundError(Exception):
    """Raised when a task is not found or the user lacks permission."""
    pass

class InvalidConfigurationError(Exception):
    """Raised when a task configuration is invalid."""
    pass

class UnsupportedFrequencyError(Exception):
    """Raised when a frequency is not supported."""
    pass