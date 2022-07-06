import os
from unittest import TestCase

from hca_ingest.importer.submission.entity import Entity
from hca_ingest.importer.submission.entity_map import EntityMap
from tests.utils import load_json


class EntityMapTest(TestCase):
    def setUp(self) -> None:
        self.script_dir = os.path.dirname(__file__)
        self.json_dir = os.path.join(self.script_dir, 'spreadsheet_json')

    def test_load(self):
        # given:
        spreadsheet_json = load_json(f'{self.json_dir}/sample_spreadsheet.json')

        # when:
        entity_map = EntityMap.load(spreadsheet_json)

        # then:
        self.assertEqual(['project', 'biomaterial', 'file', 'protocol'],
                         list(entity_map.get_entity_types()))

        # and:
        # TODO shouldn't entity id's be unique and that there's no need to specify entity type?
        biomaterial1 = entity_map.get_entity('biomaterial', 'biomaterial_id_1')
        self._assert_correct_entity(biomaterial1, entity_id='biomaterial_id_1',
                                    entity_type='biomaterial', content={'key': 'biomaterial_1'})

        # and:
        biomaterial2 = entity_map.get_entity('biomaterial', 'biomaterial_id_2')
        links = {'biomaterial': ['biomaterial_id_1'], 'process': ['process_id_1']}
        self._assert_correct_entity(biomaterial2, entity_id='biomaterial_id_2',
                                    entity_type='biomaterial', content={'key': 'biomaterial_2'},
                                    links=links)

        # and:
        protocol1 = entity_map.get_entity('protocol', 'protocol_id_1')
        self.assertEqual({'key': 'protocol_1'}, protocol1.content)

    def test_load__is_linking_reference(self):
        # given:
        spreadsheet_json = {
            'biomaterial': {
                'biomaterial_id': {
                    'content': {
                        'key': 'biomaterial_3'
                    },
                    'links_by_entity': {
                        'biomaterial': ['biomaterial_id_2'],
                        'process': ['process_id_2']
                    },
                    'external_links_by_entity': {
                        'biomaterial': ['biomaterial_uuid']
                    },

                },
            }
        }

        # when:
        entity_map = EntityMap.load(spreadsheet_json)

        # then:
        self.assertEqual(['biomaterial'], list(entity_map.get_entity_types()))

    def test_load__is_reference(self):
        # given:
        spreadsheet_json = {
            'biomaterial': {
                'biomaterial_uuid': {
                    'content': {
                        'key': 'value'
                    },
                    'is_reference': True
                }
            }
        }

        # when:
        entity_map = EntityMap.load(spreadsheet_json)

        # then:
        self.assertEqual(['biomaterial'], list(entity_map.get_entity_types()))

    def _assert_correct_entity(self, entity, entity_id='', content={}, entity_type='', links={}):
        self.assertTrue(entity)
        self.assertEqual(entity_id, entity.id)
        self.assertEqual(content, entity.content)
        self.assertEqual(entity_type, entity.type)
        self.assertEqual(links, entity.links_by_entity)

    def test_count_total(self):
        # given:
        zero_map = EntityMap()

        # and:
        one_map = EntityMap()
        one_map.add_entity(Entity('product', 'product_1', {}))

        # and:
        three_map = EntityMap()
        three_map.add_entity(Entity('profile', 'profile_1', {}))
        for product_id in range(0, 2):
            three_map.add_entity(Entity('product', f'product_{product_id}', {}))

        # expect:
        self.assertEqual(0, zero_map.count_total())
        self.assertEqual(1, one_map.count_total())
        self.assertEqual(3, three_map.count_total())

    def test_count_links(self):
        entity_map = EntityMap()

        # no element
        self.assertEqual(entity_map.count_links(), 0)

        # has 1 element without links
        entity_map.add_entity(Entity('product', 'product_0', {}))
        self.assertEqual(entity_map.count_links(), 0)

        # has 1 element with links
        entity_map.add_entity(Entity('product', 'product_1', {}, direct_links=[{}, {}, {}]))
        self.assertEqual(entity_map.count_links(), 3)

        # has many element with links
        entity_map.add_entity(Entity('product', 'product_2', {}, direct_links=[{}, {}, {}, {}]))
        self.assertEqual(entity_map.count_links(), 7)

    def test_get_project__returns_project(self):
        # given
        entity_map = EntityMap()
        project_entity = Entity('project', 'project_0', {})
        entity_map.add_entity(project_entity)

        # when
        output = entity_map.get_project()

        # then
        self.assertEqual(output, project_entity)

    def test_get_project__returns_none(self):
        # given
        entity_map = EntityMap()

        # when
        output = entity_map.get_project()

        # then
        self.assertEqual(output, None)

    def test_add_entity(self):
        # given
        entity_map = EntityMap()
        content = {'key': 'val'}
        entity = Entity(entity_type='protocol',
                        entity_id='protocol-uuid',
                        content=content,
                        spreadsheet_location={})
        # when
        entity_map.add_entity(entity)

        # then
        saved_entity = entity_map.get_entity('protocol', 'protocol-uuid')
        self.assertFalse(saved_entity.is_reference)
        self.assertFalse(saved_entity.is_linking_reference)
        self.assertEqual(saved_entity.content, content)

    def test_add_entity__when_added_as_reference_entity_first__then_set_entity_as_linking_reference(self):
        # given
        entity_map = EntityMap()
        content = {'key': 'val'}
        entity = Entity(entity_type='protocol',
                        entity_id='protocol-uuid',
                        content=content,
                        is_reference=True,
                        spreadsheet_location={})
        entity_map.add_entity(entity)

        # when
        new_entity = Entity(entity_type='protocol',
                            entity_id='protocol-uuid',
                            content=content,
                            is_linking_reference=True,
                            spreadsheet_location={})
        entity_map.add_entity(new_entity)

        # then
        saved_entity = entity_map.get_entity('protocol', 'protocol-uuid')
        self.assertTrue(saved_entity.is_reference, 'The entity must be set as reference entity')
        self.assertTrue(saved_entity.is_linking_reference, 'The entity must be also set as a linking reference entity')
        self.assertEqual(saved_entity.content, content, 'The content must not be touched')

    def test_add_entity__when_added_as_linking_reference_entity_first__then_set_as_reference_and_copy_new_content(self):
        # given
        entity_map = EntityMap()
        content = {'key': 'val'}
        entity = Entity(entity_type='protocol',
                        entity_id='protocol-uuid',
                        content=content,
                        is_linking_reference=True,
                        spreadsheet_location={})
        entity_map.add_entity(entity)

        # when
        new_content = {'key': 'val2'}
        new_entity = Entity(entity_type='protocol',
                            entity_id='protocol-uuid',
                            content=new_content,
                            is_reference=True,
                            spreadsheet_location={})
        entity_map.add_entity(new_entity)

        # then
        saved_entity = entity_map.get_entity('protocol', 'protocol-uuid')
        self.assertTrue(saved_entity.is_linking_reference, 'The entity must be set as a linking reference entity')
        self.assertTrue(saved_entity.is_reference, 'The entity must be set as reference entity')
        self.assertEqual(saved_entity.content, new_content, 'The content must be updated')
