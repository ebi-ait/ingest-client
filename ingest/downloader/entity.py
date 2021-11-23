from typing import List


class Entity:
    def __init__(self, content: dict, uuid: str):
        self._content = content
        self._uuid = uuid
        self._input = None
        self._process = None
        self._protocols = None

    @classmethod
    def from_json(cls, entity_json: dict):
        content = entity_json.get('content')
        uuid = entity_json.get('uuid', {})
        return cls(content, uuid.get('uuid'))

    @property
    def content(self):
        return self._content

    @property
    def uuid(self):
        return self._uuid

    @property
    def input(self):
        return self._input

    @property
    def protocols(self):
        return self._protocols

    @property
    def process(self):
        return self._process

    def set_input(self, input, process, protocols):
        assert all(isinstance(arg, Entity) for arg in [input, process])
        assert all(isinstance(protocol, Entity) for protocol in protocols)
        self._input = input
        self._process = process
        self._protocols = protocols
