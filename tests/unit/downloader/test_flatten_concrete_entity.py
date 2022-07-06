import json

from hca_ingest.downloader.entity import Entity
from hca_ingest.downloader.flattener import Flattener
from tests.unit.downloader.test_flattener import BaseFlattenerTest


class FlattenConcreteEntityTest(BaseFlattenerTest):
    def test_flatten__project_metadata(self):
        # given
        with open(self.script_dir + '/project-list.json') as file:
            entity_list = json.load(file)

        # when
        flattener = Flattener()
        actual = flattener.flatten(Entity.from_json_list(entity_list))

        with open(self.script_dir + '/project-list-flattened.json') as file:
            expected = json.load(file)

        # then
        self.assertEqual(actual, expected)

    def test_flatten__has_different_entities(self):
        # given
        with open(self.script_dir + '/entities.json') as file:
            entity_list = json.load(file)

        # when
        flattener = Flattener()
        actual = flattener.flatten(Entity.from_json_list(entity_list))

        with open(self.script_dir + '/entities-flattened.json') as file:
            expected = json.load(file)

        # then
        self.assertEqual(actual, expected)

    def test_flatten__collection_protocol_metadata(self):
        # given
        with open(self.script_dir + '/collection_protocol-list.json') as file:
            entity_list = json.load(file)

        # when
        flattener = Flattener()
        actual = flattener.flatten(Entity.from_json_list(entity_list))

        with open(self.script_dir + '/collection_protocol-list-flattened.json') as file:
            expected = json.load(file)

        # then
        self.assertEqual(actual, expected)
