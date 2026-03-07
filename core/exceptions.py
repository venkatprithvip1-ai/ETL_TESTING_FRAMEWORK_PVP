"""
core/exceptions.py
==================
Custom exception hierarchy for the ETL DQ Framework.

Why custom exceptions?
→ Generic Python errors like ValueError are ambiguous.
→ DQColumnError immediately tells you it is a data quality column issue.
→ You can catch ALL dq errors with: except DQException
→ Or catch specific ones: except DQConfigError
"""


class DQException(Exception):
    """Base class — all DQ exceptions inherit from this."""
    pass


class DQColumnError(DQException):
    """Column missing from DataFrame."""
    pass


class DQConfigError(DQException):
    """Configuration value is invalid or missing."""
    pass


class DQSparkError(DQException):
    """Spark session or operation failed."""
    pass


class DQDataReadError(DQException):
    """Failed to read input data file."""
    pass


class DQAlertError(DQException):
    """Alert (Slack/email) delivery failed — non-fatal."""
    pass
