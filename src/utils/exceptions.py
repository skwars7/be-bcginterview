"""
Custom Exceptions
"""

class CustomException( Exception ):
    def __init__(self, message, payload=None, status=500) -> None:
        self.message = message
        self.status = status
        self.payload = payload


class DatabaseConnectionFailureException ( CustomException ):
    def __init__(self, message=None, payload=None) -> None:
        self.message = message or "Database Connection Failed."
        self.status = 500
        self.payload = payload