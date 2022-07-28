import json
import os
from typing import Dict

from .dcp_auth_client import DCPAuthClient


class ServiceCredential:
    def __init__(self, value: Dict[str, str]):
        self.value = value

    @classmethod
    def from_env_var(cls, env_var_name):
        service_credentials = json.loads(os.environ.get(env_var_name))
        return cls(service_credentials)

    @classmethod
    def from_file(cls, file_path):
        with open(file_path) as fh:
            service_credentials = json.load(fh)
        return cls(service_credentials)


class S2STokenClient:
    def __init__(self, credential: ServiceCredential, audience: str):
        self._credentials = credential
        self._audience = audience

    def retrieve_token(self) -> str:
        if not self._audience:
            raise Error('The audience must be set.')
        return DCPAuthClient.get_service_jwt(service_credentials=self._credentials.value, audience=self._audience)


class Error(Exception):
    """Base-class for all exceptions raised by this module."""
