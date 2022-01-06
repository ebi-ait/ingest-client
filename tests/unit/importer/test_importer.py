import os
from unittest.case import TestCase

from mock import MagicMock, patch, Mock

from ingest.api.ingestapi import IngestApi
from ingest.importer.importer import SchemaRetrievalError
from ingest.importer.importer import XlsImporter
from ingest.utils.IngestError import ImporterError

BASE_PATH = os.path.dirname(__file__)

HEADER_ROW = 4


class XlsImporterTest(TestCase):
    def setUp(self):
        self.mock_ingest_api = MagicMock(spec=IngestApi)
        self.importer = XlsImporter(self.mock_ingest_api)
        self.mock_template_mgr = Mock()
        self.mock_template_mgr.get_schema_url = Mock(return_value='')

    @patch('ingest.importer.importer.IngestWorkbook')
    @patch('ingest.importer.importer.WorkbookImporter')
    @patch('ingest.importer.importer.template_manager')
    def test_generate_json_success(self, mock_template_manager, mock_wb_importer, mock_wb):
        # given
        mock_template_manager.build = MagicMock('template_manager', return_value='template_manager')
        mock_wb_importer.return_value.do_import = Mock(return_value=({'test': 'output'}, ['errors']))

        # when
        spreadsheet_json, template_manager, errors = self.importer.generate_json('file_path', is_update=False)

        # then
        self.assertEqual(spreadsheet_json, {'test': 'output'}, )
        self.assertEqual(errors, ['errors'])
        self.assertEqual(template_manager, 'template_manager')

    @patch('ingest.importer.importer.IngestWorkbook')
    @patch('ingest.importer.importer.WorkbookImporter')
    @patch('ingest.importer.importer.template_manager')
    def test_generate_json_error(self, mock_template_manager, mock_wb_importer, mock_wb):
        mock_template_manager.build = MagicMock('template_manager', side_effect=Exception('test error'))

        with self.assertRaises(SchemaRetrievalError):
            self.importer.generate_json('file_path', is_update=False)

    @patch('ingest.importer.submission.entity_linker.EntityLinker.handle_links_from_spreadsheet')
    @patch('ingest.importer.submission.entity_map.EntityMap.load')
    @patch('ingest.importer.importer.XlsImporter.generate_json')
    def test_dry_run_import_file_success(self, mock_generate_json, mock_load, mock_handle_links):
        # given
        mock_entity_map = Mock('entity_map_w_links')
        mock_load.return_value = mock_entity_map
        mock_generate_json.return_value = ({'test': 'output'}, 'template_manager', [])

        # when
        entity_map, errors = self.importer.dry_run_import_file('file_path')

        # then
        self.assertEqual(entity_map, mock_entity_map)
        self.assertFalse(errors)

    @patch('ingest.importer.submission.entity_map.EntityMap.load')
    @patch('ingest.importer.submission.entity_linker.EntityLinker.handle_links_from_spreadsheet')
    @patch('ingest.importer.importer.XlsImporter.generate_json')
    def test_dry_run_import_file_error(self, mock_generate_json, mock_entity_linker, mock_entity_map):
        # given
        mock_entity_map.return_value = MagicMock()
        mock_entity_linker.return_value = 'entity_map_w_links'
        mock_generate_json.return_value = ({'test': 'output'}, 'template_manager', ['error'])

        # when
        entity_map, errors = self.importer.dry_run_import_file('file_path')

        # then
        self.assertEqual(errors, ['error'])
        self.assertFalse(entity_map)

    def test_import_file_throwing_exception(self):
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
    def test_import_file__link_supplementary_files_to_project(self, mock_link_entity):
        # given:
        spreadsheet_json = self._create_spreadsheet_json_with_supplementary_file()

        self.importer.generate_json = Mock(return_value=(spreadsheet_json, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        self.assertEqual([args.kwargs.get('relationship') for args in mock_link_entity.call_args_list],
                         ['submissionEnvelopes', 'supplementaryFiles', 'project'])

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

    def test_import_file__when_generate_json_has_errors__then_report_errors(self):
        # given:
        err1 = {'type': 'error1', 'location': 'File', 'detail': 'details'}
        err2 = {'type': 'error2', 'location': 'File', 'detail': 'details'}

        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr, [err1, err2]))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        self.assertEqual(self.mock_ingest_api.create_submission_error.call_count, 2)

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_import_file__when_generate_json_has_no_project__then__do_not_link_to_project(self, mock_link_submission_to_project):
        # given:
        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        mock_link_submission_to_project.assert_not_called()

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_submission_to_project')
    def test_import_file__when_has_project__then_link_to_project(self, mock_link_submission_to_project):
        # given:
        spreadsheet_json = {
            'project': {
                'project-uuid': {
                    'is_linking_reference': True,
                }
            }
        }
        self.importer.generate_json = Mock(return_value=(spreadsheet_json, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        mock_link_submission_to_project.assert_called_once()

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.update_entities')
    def test_import_file__when_is_update__then_update_entities(self, mock_update_entities):
        # given:
        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url', is_update=True)

        # then:
        mock_update_entities.assert_called_once()

    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.link_entities')
    @patch('ingest.importer.submission.ingest_submitter.IngestSubmitter.add_entities')
    def test_import_file__when_is_update_false__then_add_and_link_entities(self, mock_add_entities, mock_link_entities):
        # given:
        self.importer.generate_json = Mock(return_value=({}, self.mock_template_mgr, None))

        # when:
        submission, _ = self.importer.import_file(file_path='path', submission_url='url')

        # then:
        mock_add_entities.assert_called_once()
        mock_link_entities.assert_called_once()