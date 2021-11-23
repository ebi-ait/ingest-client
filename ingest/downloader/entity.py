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

        self.embed_link_columns()

    def embed_link_columns(self):
        self._embed_process()
        self._embed_protocol_ids()
        self._embed_input_ids()

    def _embed_process(self):
        embed_process = {
            'process': self._process.content
        }
        self._content.update(embed_process)
        bp = 0

    def _embed_protocol_ids(self):
        protocols_by_type = {}
        for p in self._protocols:
            p: Entity
            protocols = protocols_by_type.get(p.concrete_type, [])
            protocols.append(p)
            protocols_by_type[p.concrete_type] = protocols

        for concrete_type, protocols in protocols_by_type.items():
            protocol_ids = [p.content['protocol_core']['protocol_id'] for p in protocols]
            protocol_uuids = [p.uuid for p in protocols]
            embedded_protocol_ids = {
                concrete_type: {
                    'protocol_core': {
                        'protocol_id': protocol_ids
                    },
                    'uuid': protocol_uuids
                }
            }
            self._content.update(embedded_protocol_ids)

    def _embed_input_ids(self):
        # TODO only supports input biomaterials for now
        # raise an error pls
        inputs_by_type = {}
        for i in self._inputs:
            i: Entity
            inputs = inputs_by_type.get(i.concrete_type, [])
            inputs.append(i)
            inputs_by_type[i.concrete_type] = inputs

        for concrete_type, inputs in inputs_by_type.items():
            input_ids_ids = [i.content['biomaterial_core']['biomaterial_id'] for i in inputs]
            input_ids_uuids = [i.uuid for i in inputs]
            embedded_input_ids = {
                concrete_type: {
                    'biomaterial_core': {
                        'biomaterial_id': input_ids_ids
                    },
                    'uuid': input_ids_uuids
                }
            }
            self._content.update(embedded_input_ids)
