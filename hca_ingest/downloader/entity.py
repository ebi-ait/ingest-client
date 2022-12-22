from __future__ import annotations
from dataclasses import dataclass, field, InitVar


@dataclass
class Entity:
    entity_json: InitVar[dict]
    content: dict = field(init=False, default_factory=dict)
    uuid: str = field(init=False, default='')
    id: str = field(init=False, default='')
    input_biomaterials: list[Entity] = field(init=False, default_factory=list)
    input_files: list[Entity] = field(init=False, default_factory=list)
    process: Entity = field(init=False, default=None)
    protocols: list[Entity] = field(init=False, default_factory=list)

    def __post_init__(self, entity_json: dict):
        content = entity_json.get('content')
        if content:
            self.content = content
        uuid = entity_json.get('uuid', {}).get('uuid')
        if uuid:
            self.uuid = uuid
        self_href = entity_json.get('_links', {}).get('self', {}).get('href')
        if self_href:
            self.id = self_href.split('/')[-1]

    @classmethod
    def from_json_list(cls, entity_json_list: list[dict]) -> list[Entity]:
        return [Entity(e) for e in entity_json_list]

    @property
    def schema_url(self):
        return self.content.get('describedBy', '')

    @property
    def concrete_type(self):
        if self.schema_url:
            return self.schema_url.rsplit('/', 1)[-1]

    @property
    def domain_type(self):
        if self.schema_url:
            return self.schema_url.split('/')[4]

    def set_input(self, input_biomaterials=None, input_files=None, process: Entity = None, protocols: list[Entity] = None):
        if input_biomaterials is None:
            input_biomaterials = []
        if input_files is None:
            input_files = []
        if protocols is None:
            protocols = []
        assert isinstance(process, Entity)
        assert all(isinstance(protocol, Entity) for protocol in protocols)
        assert all(isinstance(input_biomaterial, Entity) for input_biomaterial in input_biomaterials)
        assert all(isinstance(input_file, Entity) for input_file in input_files)
        self.input_files = input_files
        self.input_biomaterials = input_biomaterials
        self.process = process
        self.protocols = protocols
