from typing import Iterable

import requests

from hca_ingest.downloader.entity import Entity
from downloader.schema_url import SchemaUrl


class SchemaCollector:
    def __init__(self):
        self.schema_cache = {}

    def get_schemas_from_entities(self, entity_list: Iterable[Entity]) -> dict:
        schema_urls = self.get_schema_urls(entity_list)
        duplicate_schemas = self.duplicate_schemas_by_concrete_type(schema_urls)
        if duplicate_schemas:
            raise ValueError(f'The concrete entity schema version should be consistent across entities. '
                             f'Multiple versions of same concrete entity schema found: '
                             f'{[schema.url for schema in duplicate_schemas]}')
        return { schema.url : self.__get_schema_from_cache(schema.url) for schema in schema_urls }

    def __get_schema_from_cache(self, url: str) -> dict:
        if url in self.schema_cache:
            return self.schema_cache[url]
        schema = self.__get_schema(url)
        self.schema_cache[url] = schema
        return schema

    @staticmethod
    def get_schema_urls(entity_list: Iterable[Entity]) -> set[SchemaUrl]:
        return set([entity.schema for entity in entity_list])

    @staticmethod
    def duplicate_schemas_by_concrete_type(schema_urls: set[SchemaUrl]) -> set[SchemaUrl]:
        concrete_types = [url.concrete_type for url in schema_urls]
        distinct_types = set(concrete_types)
        duplicate_types = concrete_types.copy()
        for type in distinct_types:
            duplicate_types.remove(type)
        duplicate_types = set(duplicate_types)
        return set(filter(lambda url: url.concrete_type in duplicate_types, schema_urls))

    @staticmethod
    def __get_schema(url: str)-> dict:
        response = requests.get(url)
        if response.ok:
            return response.json()
        response.raise_for_status()
