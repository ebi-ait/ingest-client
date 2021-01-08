import json
import os

from .dcp_auth_client import DCPAuthClient


class S2STokenClient:
    def __init__(self, credentials: dict, audience: str = None):
        self._credentials = credentials
        self.audience = audience

    @classmethod
    def from_env_var(cls, env_var_name):
        service_credentials = json.loads(os.environ.get(env_var_name))
        return cls(service_credentials)

    @classmethod
    def from_file(cls, file_path):
        with open(file_path) as fh:
            service_credentials = json.load(fh)
        return cls(service_credentials)

    def set_audience(self, audience: str):
        self.audience = audience

    def retrieve_token(self) -> str:
        if not self.audience:
            raise Error('The audience must be set.')
        return DCPAuthClient.get_service_jwt(service_credentials=self._credentials, audience=self.audience)


class Error(Exception):
    """Base-class for all exceptions raised by this module."""
