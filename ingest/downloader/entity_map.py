from typing import List

from ingest.downloader.entity import Entity


class EntityMap:
    def __init__(self, entity_dict: dict):
        self._map = entity_dict

    @classmethod
    def from_json_list(cls, json_list: List[dict]):
        entity_dict = {}
        for entity_json in json_list:
            entity = Entity.from_json(entity_json)
            entity_dict[entity.id] = entity
        return cls(entity_dict)

    @classmethod
    def from_entity_list(cls, entity_list: List[Entity]):
        entity_dict = {}
        for entity in entity_list:
            entity_dict[entity.id] = entity
        return cls(entity_dict)

    def get(self, id: str):
        return self._map[id]

    def get_entities(self):
        return self._map.values()
