import json
import os
from unittest import TestCase
from unittest.mock import MagicMock

from ingest.importer.submission.entity_linker import EntityLinker
from ingest.importer.submission.entity_map import EntityMap


class EntityLinkerTest(TestCase):
    def test_handle_links_from_spreadsheet__with_external_links(self):
        # given
        with open(os.path.dirname(__file__) + '/spreadsheet_with_external_links.json') as file:
            spreadsheet_json = json.load(file)
        mocked_template_manager = MagicMock(name='template_manager')
        mocked_template_manager.get_schema_url = MagicMock(return_value='url')
        self.mocked_template_manager = mocked_template_manager

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        # when
        output = entity_linker.handle_links_from_spreadsheet()

        # then
        lib_prep_protocol = output.get_entity('protocol', 'librep-protocol-uuid')
        self.assertTrue(lib_prep_protocol.is_linking_reference)
        self.assertTrue(lib_prep_protocol.is_reference)

        seq_protocol = output.get_entity('protocol', 'seq-protocol-uuid')
        self.assertTrue(seq_protocol.is_linking_reference)
        self.assertTrue(seq_protocol.is_reference)

        cell_suspension = output.get_entity('biomaterial', 'cell-suspension-uuid')
        self.assertTrue(cell_suspension.is_linking_reference)
        self.assertTrue(cell_suspension.is_reference)

        file1 = output.get_entity('file', 'seq-file-uuid-1')
        self.assertFalse(file1.is_linking_reference)
        self.assertTrue(file1.is_reference)

        file2 = output.get_entity('file', 'seq-file-uuid-2')
        self.assertFalse(file2.is_linking_reference)
        self.assertTrue(file2.is_reference)

        assay_process = output.get_entity('process', 'assay_process-uuid')
        self.assertTrue(assay_process.is_linking_reference)
        self.assertTrue(assay_process.is_reference)
        assay_process_content = {
            'process_core': {
                'process_description': 'desc', 'process_id': 'assay_process'
            },
            'schema_type': 'process',
            'describedBy': 'url'
        }
        self.assertEqual(assay_process.content, assay_process_content)
