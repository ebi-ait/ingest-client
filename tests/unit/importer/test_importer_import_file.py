from unittest.mock import Mock, patch, MagicMock

from ingest.utils.IngestError import ImporterError
from tests.unit.importer.test_importer import XlsImporterBaseTest


class ImporterImportFileTest(XlsImporterBaseTest):
    def test_when_generate_json_has_errors__then_report_errors(self):
        # given:
        err1 = {'type': 'error1', 'location': 'File', 'detail': 'details'}
        err2 = {'type': 'error2', 'location': 'File', 'detail': 'details'}

        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr, [err1, err2]))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        self.assertEqual(self.mock_ingest_api.create_submission_error.call_count, 2)

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_when_no_project_reference__then_dont_link_to_project(self, mock_link_to_project):
        # given:
        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr, []))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        mock_link_to_project.assert_not_called()
        self.assertTrue(submission)

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_when_has_project_reference__then_link_to_project(self, mock_link_submission_to_project):
        # given:
        self.importer.generate_json = Mock(
            return_value=(self.spreadsheet_json_with_project_reference, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        mock_link_submission_to_project.assert_called_once()
        self.assertTrue(submission)

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_when_spreadsheet_has_project_and_given_project_uuid__then__link_to_project(self, mock_link_to_project):
        # given:
        self.importer.generate_json = Mock(
            return_value=(self.spreadsheet_json_with_project_reference, self.mock_template_mgr, []))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url', project_uuid='project_uuid')

        # then:
        mock_link_to_project.assert_called_once()
        self.assertTrue(submission)

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_when_spreadsheet_has_project_and_no_project_uuid__then__creates_and_links_to_project(self,
                                                                                                  mock_link_to_project):
        # given:
        spreadsheet_json_with_project = {
            'project': {
                'project-uuid': {
                    'is_linking_reference': False,
                    'is_reference': False,
                    'content': {}
                }
            }
        }
        self.importer.generate_json = Mock(return_value=(spreadsheet_json_with_project, self.mock_template_mgr, []))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        self.mock_ingest_api.create_project.assert_called_once()
        mock_link_to_project.assert_called_once()

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_when_update_project_true__and_spreadsheet_has_project__then_updates_project(self, mock_link_to_project):
        # given:
        self.setup_existing_project_in_ingest_and_spreadsheet_with_project()

        # when:
        submission, _ = self.importer.import_file(file_path='path',
                                                  submission_url='url',
                                                  project_uuid='project-uuid',
                                                  update_project=True)

        # then:
        self.mock_ingest_api.patch.assert_called_once_with('project-url', {'content': {'key': 'updated'}})
        mock_link_to_project.assert_called_once()

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_when_update_project_false_and_spreadsheet_has_project__then_dont_update_project(self,
                                                                                             mock_link_to_project):
        # given:
        self.setup_existing_project_in_ingest_and_spreadsheet_with_project()

        # when:
        submission, _ = self.importer.import_file(file_path='path',
                                                  submission_url='url',
                                                  project_uuid='project-uuid')

        # then:
        self.mock_ingest_api.patch.assert_not_called()
        mock_link_to_project.assert_called_once()

    def setup_existing_project_in_ingest_and_spreadsheet_with_project(self):
        spreadsheet_json_with_project = {
            'project': {
                'project-uuid': {
                    'is_linking_reference': False,
                    'is_reference': False,
                    'content': {
                        'key': 'updated'
                    }
                }
            }
        }
        self.importer.generate_json = Mock(return_value=(spreadsheet_json_with_project, self.mock_template_mgr, []))
        entity = {
            'project-uuid': {
                'content': {'key': 'value'},
                '_links': {
                    'self': {
                        'href': 'project-url'
                    }
                }
            }
        }
        self.mock_ingest_api.get_entity_by_uuid = lambda etype, uuid: entity.get(uuid)

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.update_entities')
    def test_when_is_update__then_update_entities(self, mock_update_entities):
        # given:
        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url', is_update=True)

        # then:
        mock_update_entities.assert_called_once()

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_entities')
    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.add_entities')
    def test_when_is_update_false__then_add_and_link_entities(self, mock_add_entities, mock_link_entities):
        # given:
        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        mock_add_entities.assert_called_once()
        mock_link_entities.assert_called_once()
        self.assertTrue(submission)

    def test_throwing_exception(self):
        # given:
        exception = Exception('Error thrown for Unit Test')
        exception_json = ImporterError(str(exception)).getJSON()

        self.importer.generate_json = MagicMock(side_effect=exception)
        self.importer.logger.error = MagicMock()

        # when:
        self.importer.import_file(file_path=None, submission_url=None)

        # then:
        self.mock_ingest_api.create_submission_error.assert_called_once_with(None, exception_json)

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_entity')
    def test_link_supplementary_files_to_project(self, mock_link_entity):
        # given:
        spreadsheet_json = self._create_spreadsheet_json_with_supplementary_file()

        self.importer.generate_json = Mock(return_value=(spreadsheet_json, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        relationships = [args.kwargs.get('relationship') for args in mock_link_entity.call_args_list]

        # then:
        self.assertIn('submissionEnvelopes', relationships,
                      'The submission envelope should be linked to project\'s submission envelopes.')
        self.assertIn('supplementaryFiles', relationships,
                      'The files should be linked to project\'s supplementary files.')
        self.assertIn('project', relationships,
                      'The supplementary file should have a link to the project')

    def _create_spreadsheet_json_with_supplementary_file(self):
        return {
            'project': {
                'project-uuid': {
                    'is_reference': True
                }
            },
            'file': {
                'supplementary-file-id': {
                    'content': {
                        'file_core': {
                            'file_name': 'supplementary_file_name'
                        }
                    },
                    'concrete_type': 'supplementary_file'
                }
            }
        }
