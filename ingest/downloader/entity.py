from typing import List


class Entity:
    def __init__(self, content: dict, uuid: str):
        self._content = content
        self._uuid = uuid
        self._inputs = None
        self._process = None
        self._protocols = None

    @classmethod
    def from_json(cls, entity_json: dict):
        content = entity_json.get('content')
        uuid = entity_json.get('uuid', {})
        return cls(content, uuid.get('uuid'))

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
    def inputs(self):
        return self._inputs

    @property
    def protocols(self):
        return self._protocols

    @property
    def process(self):
        return self._process

    @property
    def concrete_type(self):
        return self._content.get('describedBy').rsplit('/', 1)[-1]

    def set_input(self, inputs, process, protocols):
        assert isinstance(process, Entity)
        assert all(isinstance(protocol, Entity) for protocol in protocols)
        assert all(isinstance(input, Entity) for input in inputs)
        self._inputs = inputs
        self._process = process
        self._protocols = protocols
