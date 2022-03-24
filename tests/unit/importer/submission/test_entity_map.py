from unittest import TestCase

from ingest.importer.submission.entity import Entity
from ingest.importer.submission.entity_map import EntityMap
from tests.unit.importer.submission.test_submission import _create_spreadsheet_json


class EntityMapTest(TestCase):

    def test_load(self):
        # given:
        spreadsheet_json = _create_spreadsheet_json()

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
