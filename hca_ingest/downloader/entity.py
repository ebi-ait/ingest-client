from typing import List


class Entity:
    def __init__(self, content: dict, uuid: str, entity_id: str = None):
        self._content = content
        self._uuid = uuid
        self._id = entity_id
        self._input_biomaterials = None
        self._input_files = None
        self._process = None
        self._protocols = None

    @classmethod
    def from_json(cls, entity_json: dict):
        content = entity_json.get('content')
        uuid = entity_json.get('uuid', {}).get('uuid')
        links = entity_json.get('_links', {})
        self_link = links.get('self', {})
        self_href = self_link.get('href')
        entity_id = self_href.split('/')[-1] if self_href else None
        return cls(content, uuid, entity_id)

    @classmethod
    def from_json_list(cls, entity_json_list: List[dict]):
        return [Entity.from_json(e) for e in entity_json_list]

    @property
    def content(self):
        return self._content

    @property
    def uuid(self):
        return self._uuid

    @property
    def id(self):
        return self._id

    @property
    def input_biomaterials(self):
        return self._input_biomaterials

    @property
    def input_files(self):
        return self._input_files

    @property
    def protocols(self):
        return self._protocols

    @property
    def process(self):
        return self._process

    @property
    def concrete_type(self):
        if self._content and 'describedBy' in self._content:
            return self._content.get('describedBy').rsplit('/', 1)[-1]

    @property
    def domain_type(self):
        if self._content and 'describedBy' in self._content:
            return self._content.get('describedBy').split('/')[4]

    def set_input(self, input_biomaterials, input_files, process, protocols):
        assert isinstance(process, Entity)
        assert all(isinstance(protocol, Entity) for protocol in protocols)
        assert all(isinstance(input_biomaterial, Entity) for input_biomaterial in input_biomaterials)
        assert all(isinstance(input_file, Entity) for input_file in input_files)
        self._input_files = input_files
        self._input_biomaterials = input_biomaterials
        self._process = process
        self._protocols = protocols
