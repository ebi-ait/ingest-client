import json
import os
from unittest import TestCase
from unittest.mock import MagicMock

from ingest.importer.submission.entity_linker import EntityLinker
from ingest.importer.submission.entity_map import EntityMap
from ingest.importer.submission.errors import MultipleProcessesFound, InvalidLinkInSpreadsheet, LinkedEntityNotFound
from tests.utils import load_json


class EntityLinkerTest(TestCase):
    def setUp(self):
        mocked_template_manager = MagicMock(name='template_manager')
        mocked_template_manager.get_schema_url = MagicMock(return_value='url')
        self.mocked_template_manager = mocked_template_manager
        self.script_dir = os.path.dirname(__file__)
        self.json_dir = os.path.join(self.script_dir, 'spreadsheet_json')

    def test_convert_links__biomaterial_to_biomaterial_has_process(self):
        self._test_convert_links('biomaterial_to_biomaterial_has_process__before_convert.json',
                                 'biomaterial_to_biomaterial_has_process__after_convert.json')

    def test_convert_links__biomaterial_to_biomaterial_no_process(self):
        self._test_convert_links('biomaterial_to_biomaterial_no_process__before_convert.json',
                                 'biomaterial_to_biomaterial_no_process__after_convert.json')

    def test_convert_links__file_to_file_no_process(self):
        self._test_convert_links('file_to_file_no_process__before_convert.json',
                                 'file_to_file_no_process__after_convert.json')

    def test_convert_links__file_to_file_has_process(self):
        self._test_convert_links('file_to_file_has_process__before_convert.json',
                                 'file_to_file_has_process__after_convert.json')

    def test_convert_links__file_to_biomaterial_has_process(self):
        self._test_convert_links('file_to_biomaterial_has_process__before_convert.json',
                                 'file_to_biomaterial_has_process__after_convert.json')

    def test_convert_links__external_links(self):
        self._test_convert_links('external_links__before_convert.json',
                                 'external_links__after_convert.json')

    def test_convert_links__external_links__multiple_inputs(self):
        self._test_convert_links('external_links__multiple_inputs__before_convert.json',
                                 'external_links__multiple_inputs__after_convert.json')

    def test_convert_links__external_and_local_ids__multiple_inputs(self):
        self._test_convert_links('external_and_local_links__before_convert.json',
                                 'external_and_local_links__after_convert.json')

    def test_convert_links__link_not_found_error(self):
        # given
        spreadsheet_json = load_json(f'{self.json_dir}/link_not_found.json')

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        # when
        with self.assertRaises(LinkedEntityNotFound) as context:
            entity_linker.convert_spreadsheet_links_to_ingest_links()

        # then
        self.assertEqual('biomaterial', context.exception.entity)
        self.assertEqual('biomaterial_id_1', context.exception.id)

    def test_convert_links__invalid_spreadsheet_link(self):
        # given
        spreadsheet_json = load_json(f'{self.json_dir}/invalid_spreadsheet_links.json')

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        # when
        with self.assertRaises(InvalidLinkInSpreadsheet) as context:
            entity_linker.convert_spreadsheet_links_to_ingest_links()

        # then
        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual('file', context.exception.link_entity_type)

        self.assertEqual('biomaterial_id_1', context.exception.from_entity.id)
        self.assertEqual('file_id_1', context.exception.link_entity_id)

    def test_convert_links__multiple_process_links(self):
        # given
        spreadsheet_json = load_json(f'{self.json_dir}/multiple_process_links.json')

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        # when
        with self.assertRaises(MultipleProcessesFound) as context:
            entity_linker.convert_spreadsheet_links_to_ingest_links()

        # then
        self.assertEqual('biomaterial', context.exception.from_entity.type)
        self.assertEqual(['process_id_1', 'process_id_2'], context.exception.process_ids)

    def _test_convert_links(self, input_file, expected_output_file):
        # given
        spreadsheet_json = load_json(f'{self.json_dir}/{input_file}')

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(self.mocked_template_manager, entity_map)

        # when
        output = entity_linker.convert_spreadsheet_links_to_ingest_links()

        # then
        expected_json = load_json(f'{self.json_dir}/{expected_output_file}')

        self._assert_equal_direct_links(expected_json, output)

    def _assert_equal_direct_links(self, expected_json, output):
        for entity_type, entities_dict in expected_json.items():
            for entity_id, entity_dict in entities_dict.items():
                expected_links = entities_dict[entity_id].get('direct_links')
                expected_links = expected_links if expected_links else []
                entity = output.get_entity(entity_type, entity_id)
                self.assertTrue(entity, f'The {entity_type} entity with id {entity_id} is missing in the entity map.')

                for link in expected_links:
                    self.assertTrue(link in entity.direct_links, f'{json.dumps(link)} is not in direct links for '
                                                                 f'entity type {entity_type} and id {entity_id}')
