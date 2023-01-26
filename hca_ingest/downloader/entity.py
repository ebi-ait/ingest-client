from __future__ import annotations
from dataclasses import dataclass, field, InitVar

from .schema_url import SchemaUrl


@dataclass
class Entity:
    entity_json: InitVar[dict]
    content: dict = field(init=False, default_factory=dict)
    schema: SchemaUrl = field(init=False, default_factory=SchemaUrl)
    uuid: str = field(init=False, default='')
    id: str = field(init=False, default='')
    input_biomaterials: list[Entity] = field(init=False, default_factory=list)
    input_files: list[Entity] = field(init=False, default_factory=list)
    process: Entity = field(init=False, default=None)
    protocols: list[Entity] = field(init=False, default_factory=list)

    def __post_init__(self, entity_json: dict = None):
        if not entity_json:
            entity_json = {}
        self.content = self.__get_item(entity_json, 'content', {})
        uuid = self.__get_item(entity_json, 'uuid', {})
        self.uuid = self.__get_item(uuid, 'uuid', '')
        links = self.__get_item(entity_json, '_links', {})
        self_link = self.__get_item(links, 'self', {})
        self_href = self.__get_item(self_link, 'href', '')
        if self_href and '/' in self_href:
            self.id = self_href.split('/')[-1]
        self.schema = SchemaUrl(self.__get_item(self.content, 'describedBy', ''))

    @classmethod
    def from_json_list(cls, entity_json_list: list[dict]) -> list[Entity]:
        return [Entity(e) for e in entity_json_list]

    def set_input(self, input_biomaterials=None, input_files=None, process: Entity = None, protocols: list[Entity] = None):
        if not input_biomaterials:
            input_biomaterials = []
        if not input_files:
            input_files = []
        if not protocols:
            protocols = []
        assert isinstance(process, Entity)
        assert all(isinstance(protocol, Entity) for protocol in protocols)
        assert all(isinstance(input_biomaterial, Entity) for input_biomaterial in input_biomaterials)
        assert all(isinstance(input_file, Entity) for input_file in input_files)
        self.input_files = input_files
        self.input_biomaterials = input_biomaterials
        self.process = process
        self.protocols = protocols

    @staticmethod
    def __get_item(item: dict, key: str, default):
        result = item.get(key)
        return result if result else default
