# TODO Check how we can refactor to merge MetadataEntity and Entity
class Entity(object):
    def __init__(self, entity_type, entity_id, content, ingest_json=None, links_by_entity=None,
                 direct_links=None, external_links=None, is_reference=False, is_linking_reference=False, linking_details=None, concrete_type=None,
                 spreadsheet_location=None):
        self.type = entity_type
        self.id = entity_id
        self.content = content
        self._prepare_links_by_entity(links_by_entity)
        self._prepare_direct_links(direct_links)
        self._prepare_linking_details(linking_details)
        self._prepare_external_links(external_links)
        self.ingest_json = ingest_json
        self.is_reference = is_reference  # if entity is in a row and has uuid
        self.is_linking_reference = is_linking_reference  # if the entity uuid is only specified in a linking column
        self.concrete_type = concrete_type
        self.spreadsheet_location = spreadsheet_location

    def _prepare_links_by_entity(self, links_by_entity):
        self.links_by_entity = {}
        if links_by_entity is not None:
            self.links_by_entity.update(links_by_entity)

    def _prepare_direct_links(self, direct_links):
        self.direct_links = []
        if direct_links is not None:
            self.direct_links.extend(direct_links)

    def _prepare_external_links(self, external_links):
        self.external_links = {}
        if external_links is not None:
            self.external_links.update(external_links)

    def _prepare_linking_details(self, linking_details):
        self.linking_details = {}
        if linking_details is not None:
            self.linking_details.update(linking_details)

    @property
    def uuid(self):
        return self.ingest_json.get('uuid', {}).get('uuid')

    @property
    def url(self):
        return self.ingest_json['_links']['self']['href']

    @property
    def is_new(self):
        return not self.is_reference and not self.is_linking_reference

    def add_link(self, type:str, entity_id:str):
        links = self.links_by_entity.get(type, [])
        links.append(entity_id)
        self.links_by_entity[type] = links