import os
from unittest.case import TestCase

from mock import MagicMock, patch, Mock

from ingest.api.ingestapi import IngestApi
from ingest.importer.importer import SchemaRetrievalError
from ingest.importer.importer import XlsImporter

BASE_PATH = os.path.dirname(__file__)

HEADER_ROW = 4


class XlsImporterBaseTest(TestCase):
    def setUp(self):
        self.mock_ingest_api = MagicMock(spec=IngestApi)
        self.importer = XlsImporter(self.mock_ingest_api)
        self.mock_template_mgr = Mock()
        self.mock_template_mgr.get_schema_url = Mock(return_value='')
        self.spreadsheet_json_with_project_reference = {
            'project': {
                'project-uuid': {
                    'is_linking_reference': True,
                }
            }
        }


class XlsImporterTest(XlsImporterBaseTest):
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

    @patch('ingest.importer.importer.IngestWorkbook')
    @patch('ingest.importer.importer.WorkbookImporter')
    @patch('ingest.importer.importer.template_manager')
    def test_import_project_from_workbook__no_errors(self, mock_template_manager, mock_wb_importer, mock_workbook):
        # given
        mock_template_manager.build = MagicMock('template_manager', return_value='template_manager')
        mock_project_metadata = {
            'content': {
                'project_title': 'title'
            }
        }
        mock_spreadsheet_json = {
            'project': {
                '_unknown_0': mock_project_metadata
            }
        }

        mock_wb_importer.return_value.do_import = Mock(return_value=(mock_spreadsheet_json, []))

        self.mock_ingest_api.create_project = Mock(return_value={'uuid': {'uuid': 'project-uuid'}})

        # when
        workbook = Mock()
        project_uuid, errors = self.importer.import_project_from_workbook(workbook, 'token')

        # then
        self.assertEqual(len(errors), 0, 'There should be no errors.')
        self.assertEqual(project_uuid, 'project-uuid', 'The project should have been created.')
        mock_workbook.assert_called_with(workbook)
        self.mock_ingest_api.create_project.assert_called_once_with(None,
                                                                    content={'project_title': 'title'},
                                                                    token='token')

    @patch('ingest.importer.importer.IngestWorkbook')
    @patch('ingest.importer.importer.WorkbookImporter')
    @patch('ingest.importer.importer.template_manager')
    def test_import_project_from_workbook__has_errors(self, mock_template_manager, mock_wb_importer, mock_workbook):
        # given
        mock_template_manager.build = MagicMock('template_manager', return_value='template_manager')
        mock_errors = [
            {
                'details': 'error details'
            },
            {
                'details': 'error details 2'
            }
        ]

        mock_wb_importer.return_value.do_import = Mock(return_value=(None, [mock_errors]))

        self.mock_ingest_api.create_project = Mock()

        # when
        workbook = Mock()
        project_uuid, errors = self.importer.import_project_from_workbook(workbook, 'token')

        # then
        mock_workbook.assert_called_with(workbook)
        self.assertEqual(len(errors), 2, 'The errors from importing project from workbook must be returned.')
        self.mock_ingest_api.create_project.assert_not_called()
        self.assertEqual(project_uuid, None, 'The project should not have been created.')
