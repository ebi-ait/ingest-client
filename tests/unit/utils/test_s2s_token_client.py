from unittest import TestCase
from unittest.mock import patch, Mock

from ingest.utils.s2s_token_client import S2STokenClient, Error as S2STokenClientError, ServiceCredential


class TestS2STokenClient(TestCase):

    @patch('ingest.utils.dcp_auth_client.DCPAuthClient.get_service_jwt')
    def test__retrieve_token__returns_token__with_creds_and_audience(self, mock_retrieve_token):
        mock_retrieve_token.return_value = 'token'
        token_client = S2STokenClient(credential=ServiceCredential({}), audience='audience')

        token = token_client.retrieve_token()

        self.assertEqual(token, 'token')

    def test__retrieve_token__raises_error__with_no_audience(self):
        token_client = S2STokenClient(credential=ServiceCredential({}), audience=None)

        with self.assertRaises(S2STokenClientError):
            token = token_client.retrieve_token()
            self.assertEqual(token, None)
