from typing import Iterable

import requests

from hca_ingest.downloader.entity import Entity


class SchemaCollector:
    def __init__(self):
        self.schema_cache = {}

    def get_schemas_from_entities(self, entity_list: Iterable[Entity]):
        schemas = {}
        for entity in entity_list:
            if entity.schema_url and entity.schema_url not in schemas:
                schemas[entity.schema_url] = self.__get_schema_from_cache(entity.schema_url)
        return schemas

    def __get_schema_from_cache(self, url: str) -> dict:
        if url in self.schema_cache:
            return self.schema_cache[url]
        schema = self.__get_schema(url)
        self.schema_cache[url] = schema
        return schema

    @staticmethod
    def __get_schema(url: str)-> dict:
        response = requests.get(url)
        if response.ok:
            return response.json()
        response.raise_for_status()
