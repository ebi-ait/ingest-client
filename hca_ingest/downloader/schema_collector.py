from typing import Iterable

import requests

from .entity import Entity
from .schema_url import SchemaUrl


class SchemaCollector:
    def __init__(self):
        self.schema_cache = {}

    def get_schemas_for_entities(self, entity_list: Iterable[Entity]) -> dict:
        schema_urls = self.get_schema_urls_for_entities(entity_list)
        return self.get_schemas(schema_urls)

    def get_schemas(self, schema_urls: set[SchemaUrl]):
        self.check_for_duplicate_schemas(schema_urls)
        return {schema.url: self.__get_schema_from_cache(schema.url) for schema in schema_urls}

    def __get_schema_from_cache(self, url: str) -> dict:
        if url in self.schema_cache:
            return self.schema_cache[url]
        return self.__add_schema_to_cache(url)

    def __add_schema_to_cache(self, url: str) -> dict:
        schema = self.__get_schema(url)
        self.__add_linked_schema(schema)
        self.schema_cache[url] = schema
        return schema

    def __add_linked_schema(self, schema: dict):
        for schema_element in schema.setdefault('properties', {}).values():
            ref_url = ''
            element_type = schema_element.get('type')
            if element_type == 'object':
                ref_url = schema_element.get('$ref')
            elif element_type == 'array':
                ref_url = schema_element.get('items', {}).get('$ref')
            if ref_url:
                schema_element.update(self.__get_schema_from_cache(ref_url))

    @staticmethod
    def get_schema_urls_for_entities(entity_list: Iterable[Entity]) -> set[SchemaUrl]:
        return set([entity.schema for entity in entity_list])

    @staticmethod
    def check_for_duplicate_schemas(schema_urls: set[SchemaUrl]):
        duplicate_schemas = SchemaCollector.get_duplicate_schemas_by_concrete_type(schema_urls)
        if duplicate_schemas:
            raise ValueError(f'The concrete entity schema version should be consistent across entities. '
                             f'Multiple versions of same concrete entity schema found: '
                             f'{[schema.url for schema in duplicate_schemas]}')

    @staticmethod
    def get_duplicate_schemas_by_concrete_type(schema_urls: set[SchemaUrl]) -> set[SchemaUrl]:
        concrete_types = [schema.concrete_type for schema in schema_urls]
        distinct_types = set(concrete_types)
        duplicate_types = concrete_types.copy()
        for concrete_type in distinct_types:
            duplicate_types.remove(concrete_type)
        duplicate_types = set(duplicate_types)
        return set([schema for schema in schema_urls if schema.concrete_type in duplicate_types])

    @staticmethod
    def __get_schema(url: str)-> dict:
        response = requests.get(url)
        if response.ok:
            return response.json()
        response.raise_for_status()
