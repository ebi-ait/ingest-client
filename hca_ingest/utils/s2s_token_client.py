import json
import time
import os
from typing import Dict

import jwt


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
        return self.get_service_jwt(service_credentials=self._credentials.value, audience=self._audience)

    @staticmethod
    def get_service_jwt(service_credentials, audience):
        iat = time.time()
        exp = iat + 3600
        payload = {'iss': service_credentials["client_email"],
                   'sub': service_credentials["client_email"],
                   'aud': audience,
                   'iat': iat,
                   'exp': exp,
                   'https://auth.data.humancellatlas.org/email': service_credentials["client_email"],
                   'https://auth.data.humancellatlas.org/group': 'hca',
                   'scope': ["openid", "email", "offline_access"]
                   }
        additional_headers = {'kid': service_credentials["private_key_id"]}
        signed_jwt = jwt.encode(payload, service_credentials["private_key"], headers=additional_headers,
                                algorithm='RS256')
        return signed_jwt


class Error(Exception):
    """Base-class for all exceptions raised by this module."""
