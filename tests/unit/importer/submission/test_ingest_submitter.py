from unittest import TestCase
from unittest.mock import MagicMock, patch, call, Mock

import hca_ingest
from hca_ingest.importer.submission.entity import Entity
from hca_ingest.importer.submission.entity_map import EntityMap
from hca_ingest.importer.submission.ingest_submitter import IngestSubmitter


class IngestSubmitterTest(TestCase):

    def setUp(self) -> None:
        self.ingest_api = MagicMock('mock_ingest_api')
        self.ingest_api.get_submission = MagicMock()
        self.ingest_api.create_submission_manifest = MagicMock()
        self.ingest_api.patch = MagicMock()
        self.ingest_api.get_link_from_resource = MagicMock()

    @patch('hca_ingest.importer.submission.ingest_submitter.Submission')
    def test_submit(self, submission_constructor):
        # given:
        submission = self._mock_submission(submission_constructor)

        # and:
        product = Entity('product', 'product_1', {})
        project = Entity('project', 'id', {})
        user = Entity('user', 'user_1', {})
        entity_map = EntityMap(product, user, project)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.add_entity = MagicMock()
        submitter.add_entities(entity_map, submission_url='url')

        # then:
        submission_constructor.assert_called_with(self.ingest_api, 'url')
        submission.define_manifest.assert_called_with(entity_map)
        submission.add_entity.assert_has_calls([call(product), call(user)], any_order=True)

    def test_add_entity(self):
        new_entity_mock_response = {
            'content': {},
            'submissionDate': '2018-05-08T10:17:49.476Z',
            'updateDate': '2018-05-08T10:17:57.254Z',
            'uuid': {
                'uuid': '5a36689b-302b-40e4-bef1-837b47f0cb51'
            },
            'validationState': 'Draft'
        }

        hca_ingest.api.ingestapi.requests.get = MagicMock()
        mock_ingest_api = MagicMock(name='mock_ingest_api')
        mock_ingest_api.load_root = MagicMock()
        mock_ingest_api.create_entity = MagicMock(return_value=new_entity_mock_response)

        submitter = IngestSubmitter(mock_ingest_api)
        entity = Entity(entity_id='id', entity_type='biomaterial', content={})
        entity = submitter.add_entity(entity, 'url')

        self.assertEqual(new_entity_mock_response, entity.ingest_json)

    def test_update_entities(self):
        # given:
        entity_map = self._create_test_entity_map()
        user2 = entity_map.get_entity('user', 'user_2')
        user3 = entity_map.get_entity('user', 'user_3')

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity = MagicMock()
        submitter.update_entities(entity_map)

        # then:
        submitter.update_entity.assert_has_calls([call(user2), call(user3)], any_order=True)

    def _create_test_entity_map(self) -> EntityMap:
        product = Entity('product', 'product_1', {'k': 'v'})
        project = Entity('project', 'id', {'k': 'v'})
        user1 = Entity('user', 'user_1', {'k': 'v'})
        user2 = Entity('user', 'user_2', {'k': 'v'}, {'content': {'k': 'v0'}, '_links': {'self': {'href': 'url'}}},
                       is_reference=True)
        user3 = Entity('user', 'user_3', {'k': 'v'}, {'content': {'k': 'v0'}, '_links': {'self': {'href': 'url'}}},
                       is_reference=True)
        entity_map = EntityMap(product, user1, user2, user3, project)
        return entity_map

    @patch('hca_ingest.importer.submission.ingest_submitter.Submission')
    def test_submit_linked_entity(self, submission_constructor):
        # given:
        submission = self._mock_submission(submission_constructor)

        # and:
        user = Entity('user', 'user_1', {})
        entity_map = EntityMap(user)

        # and:
        link_to_user = {
            'entity': 'user',
            'id': 'user_1',
            'relationship': 'wish_list'
        }
        linked_product = Entity('product', 'product_1', {}, direct_links=[link_to_user], is_reference=False,
                                is_linking_reference=False)
        project = Entity('project', 'id', {}, is_reference=False, is_linking_reference=False)
        entity_map.add_entity(linked_product)
        entity_map.add_entity(project)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.add_entity = MagicMock()
        submitter.link_submission_to_project = MagicMock()
        submitter.PROGRESS_CTR = 1
        submitter.add_entities(entity_map, submission_url='url')

        # then:
        submission_constructor.assert_called_with(self.ingest_api, 'url')
        submission.define_manifest.assert_called_with(entity_map)
        submission.add_entity.assert_has_calls([call(user), call(linked_product)], any_order=True)

    def test_update_entity(self):
        # given:
        user1 = Entity('user', 'user_1', {'k': 'v2'}, {'content': {'k': 'v', 'k2': 'v2'}, '_links': {'self': {'href': 'url'}}})

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity(user1)

        # then:
        self.ingest_api.patch.assert_called_with('url', json={'content': {'k': 'v2', 'k2': 'v2'}})

    def test_update_entity__given_empty_ingest_json__then_fetch_resource(self):
        # given:
        ingest_json = {'content': {'k': 'v', 'k2': 'v2'}, '_links': {'self': {'href': 'url'}}}
        self.ingest_api.get_entity_by_uuid = Mock(return_value=ingest_json)

        # and:
        user1 = Entity('biomaterial', 'biomaterial_uuid', {'k': 'v2'}, None)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity(user1)

        # then:
        self.ingest_api.patch.assert_called_with('url', json={'content': {'k': 'v2', 'k2': 'v2'}})

    def test_update_entity__given_content_has_no_update__then_do_not_patch(self):
        # given:
        ingest_json = {'content': {'k': 'v2', 'k2': 'v2'}, '_links': {'self': {'href': 'url'}}}
        self.ingest_api.get_entity_by_uuid = Mock(return_value=ingest_json)

        # and:
        user1 = Entity('biomaterial', 'biomaterial_uuid', {'k': 'v2'}, None)

        # when:
        submitter = IngestSubmitter(self.ingest_api)
        submitter.update_entity(user1)

        # then:
        self.ingest_api.patch.assert_not_called()

    @staticmethod
    def _mock_submission(submission_constructor):
        submission = MagicMock('submission')
        submission.define_manifest = MagicMock()
        submission.add_entity = MagicMock()
        submission.update_entity = MagicMock()
        submission.link_entity = MagicMock()
        submission.manifest = {}
        submission.is_update = lambda: False
        submission_constructor.return_value = submission
        return submission
