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

    @patch('ingest.importer.importer.IngestWorkbook')
    @patch('ingest.importer.importer.WorkbookImporter')
    @patch('ingest.importer.importer.template_manager')
    def test_generate_json_success(self, mock_template_manager, mock_wb_importer, mock_wb):
        mock_template_manager.build = MagicMock('template_manager', return_value='template_manager')
        mock_wb_importer.return_value.do_import = Mock(return_value=({'test': 'output'}, ['errors']))
        ingest_api = MagicMock('ingest_api')
        importer = XlsImporter(ingest_api)
        spreadsheet_json, template_manager, errors = importer.generate_json('file_path', is_update=False)

        self.assertEqual(spreadsheet_json, {'test': 'output'}, )
        self.assertEqual(errors, ['errors'])
        self.assertEqual(template_manager, 'template_manager')

    @patch('ingest.importer.importer.IngestWorkbook')
    @patch('ingest.importer.importer.WorkbookImporter')
    @patch('ingest.importer.importer.template_manager')
    def test_generate_json_error(self, mock_template_manager, mock_wb_importer, mock_wb):
        mock_template_manager.build = MagicMock('template_manager', side_effect=Exception('test error'))
        ingest_api = MagicMock('ingest_api')
        importer = XlsImporter(ingest_api)
        with self.assertRaises(SchemaRetrievalError):
            importer.generate_json('file_path', is_update=False)

    @patch('ingest.importer.submission.entity_linker.EntityLinker.handle_links_from_spreadsheet')
    @patch('ingest.importer.submission.entity_map.EntityMap.load')
    @patch('ingest.importer.importer.XlsImporter.generate_json')
    def test_dry_run_import_file_success(self, mock_generate_json, mock_load, mock_handle_links):
        mock_entity_map = Mock('entity_map_w_links')
        mock_load.return_value = mock_entity_map
        mock_generate_json.return_value = ({'test': 'output'}, 'template_manager', [])
        ingest_api = MagicMock('ingest_api')
        importer = XlsImporter(ingest_api)

        entity_map, errors = importer.dry_run_import_file('file_path')
        self.assertEqual(entity_map, mock_entity_map)
        self.assertFalse(errors)

    @patch('ingest.importer.submission.entity_map.EntityMap.load')
    @patch('ingest.importer.submission.entity_linker.EntityLinker.handle_links_from_spreadsheet')
    @patch('ingest.importer.importer.XlsImporter.generate_json')
    def test_dry_run_import_file_error(self, mock_generate_json, mock_entity_linker, mock_entity_map):
        mock_entity_map.return_value = MagicMock()
        mock_entity_linker.return_value = 'entity_map_w_links'
        mock_generate_json.return_value = ({'test': 'output'}, 'template_manager', ['error'])
        ingest_api = MagicMock('ingest_api')
        importer = XlsImporter(ingest_api)
        entity_map, errors = importer.dry_run_import_file('file_path')
        self.assertEqual(errors, ['error'])
        self.assertFalse(entity_map)

    def test_import_file_throwing_exception(self):
        # given:
        exception = Exception('Error thrown for Unit Test')
        exception_json = ImporterError(str(exception)).getJSON()
        importer = XlsImporter(ingest_api=self.mock_ingest_api)
        importer.generate_json = MagicMock(side_effect=exception)
        importer.logger.error = MagicMock()

        # when:
        importer.import_file(file_path=None, submission_url=None)
        # then:
        self.mock_ingest_api.create_submission_error.assert_called_once_with(None, exception_json)

    @patch('ingest.importer.submission.submission.Submission.link_entity')
    def test_import_file__link_supplementary_files_to_project(self, mock_link_entity):
        # given:
        spreadsheet_json = self._create_spreadsheet_json_with_supplementary_file()
        importer = XlsImporter(ingest_api=self.mock_ingest_api)
        mock_template_mgr = Mock()
        mock_template_mgr.get_schema_url = Mock(return_value='')
        importer.generate_json = Mock(return_value=(spreadsheet_json, mock_template_mgr, None))

        # when:
        submission, _ = importer.import_file(file_path='path', submission_url='url')

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
