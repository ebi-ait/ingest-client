from typing import List


from hca_ingest.importer.submission.entity import Entity


class EntityMap(object):

    def __init__(self, *entities):
        self.entities_dict_by_type = {}
        if entities is None:
            return
        for entity in entities:
            self.add_entity(entity)

    @staticmethod
    def load(entity_json):
        entity_map = EntityMap()

        for entity_type, entities_dict in entity_json.items():
            for entity_id, entity_body in entities_dict.items():
                entity = Entity(entity_type=entity_type,
                                entity_id=entity_id,
                                content=entity_body.get('content'),
                                external_links=entity_body.get('external_links_by_entity'),
                                links_by_entity=entity_body.get('links_by_entity', {}),
                                is_reference=entity_body.get('is_reference', False),
                                is_linking_reference=entity_body.get('is_linking_reference', False),
                                linking_details=entity_body.get('linking_details', {}),
                                concrete_type=entity_body.get('concrete_type'),
                                spreadsheet_location=entity_body.get(
                                    'spreadsheet_location'))
                entity_map.add_entity(entity)

        return entity_map

    def get_entity_types(self):
        return list(self.entities_dict_by_type.keys())

    def get_entities_of_type(self, type) -> List[Entity]:
        entities = []

        entities_dict = self.entities_dict_by_type.get(type, {})

        for entity_id, entity in entities_dict.items():
            entities.append(entity)

        return entities

    def get_new_entities_of_type(self, type) -> List[Entity]:
        entities = []
        entities_dict = self.entities_dict_by_type.get(type, {})
        for entity_id, entity in entities_dict.items():
            if not (entity.is_reference and entity.is_linking_reference):
                entities.append(entity)

        return entities

    def get_entity(self, type, id) -> Entity:
        if self.entities_dict_by_type.get(type) and self.entities_dict_by_type[type].get(id):
            return self.entities_dict_by_type[type][id]

    def add_entity(self, entity: Entity):
        entities_of_type = self.entities_dict_by_type.get(entity.type)
        if not entities_of_type:
            self.entities_dict_by_type[entity.type] = {}
            entities_of_type = self.entities_dict_by_type.get(entity.type)

        existing_entity = self.get_entity(entity.type, entity.id)
        if existing_entity and existing_entity.is_reference and entity.is_linking_reference:
            existing_entity.is_linking_reference = True
        elif existing_entity and existing_entity.is_linking_reference and entity.is_reference:
            existing_entity.is_reference = entity.is_reference
            existing_entity.content = entity.content
        elif existing_entity:
            entities_of_type[entity.id] = existing_entity
        else:
            entities_of_type[entity.id] = entity

    def get_entities(self) -> List[Entity]:
        all_entities = []
        for entity_type, entities_dict in self.entities_dict_by_type.items():
            all_entities.extend(entities_dict.values())
        return all_entities

    def get_new_entities(self) -> List[Entity]:
        all_entities = []
        for entity_type, entities_dict in self.entities_dict_by_type.items():
            for entity_id, entity in entities_dict.items():
                if entity.is_new:
                    all_entities.append(entity)
        return all_entities

    def get_project(self) -> Entity:
        project_ids = list(self.entities_dict_by_type.get('project', {}).keys())
        return self.get_entity('project', project_ids[0]) if project_ids else None

    def count_total(self) -> int:
        return len(self.get_entities())

    def count_entities_of_type(self, type) -> int:
        return len(self.get_new_entities_of_type(type))

    def count_links(self) -> int:
        count = 0
        for entity in self.get_entities():
            count = count + len(entity.direct_links)
        return count
