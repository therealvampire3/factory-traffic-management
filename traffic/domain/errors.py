class DomainError(Exception):
    """Base class for domain errors."""

class ValidationError(DomainError):
    """Malformed / invalid input (HTTP 400)."""

class ConflictError(DomainError):
    """Valid request that cannot be honoured in the current state (HTTP 409)."""
