from unittest import TestCase
from unittest.mock import patch

from ingest.utils.s2s_token_client import S2STokenClient, Error as S2STokenClientError


class TestS2STokenClient(TestCase):

    @patch('ingest.utils.dcp_auth_client.DCPAuthClient.get_service_jwt')
    def test__retrieve_token__returns_token__with_creds_audience_set(self, mock_retrieve_token):
        mock_retrieve_token.return_value = 'token'
        token_client = S2STokenClient(credentials={}, audience='audience')

        token = token_client.retrieve_token()

        self.assertEqual(token, 'token')

    def test__retrieve_token__raises_error__when_no_audience_set(self):
        token_client = S2STokenClient(credentials={}, audience=None)

        with self.assertRaises(S2STokenClientError):
            token = token_client.retrieve_token()
            self.assertEqual(token, None)