from ingest.importer.submission.entity import Entity
from ingest.importer.submission.errors import LinkedEntityNotFound, InvalidLinkInSpreadsheet, MultipleProcessesFound


class EntityLinker(object):

    def __init__(self, template_manager):
        self.template_manager = template_manager
        self.process_id_ctr = 0

    def process_links_from_spreadsheet(self, entity_map):
        for entity in entity_map.get_entities():
            if entity.is_reference:
                continue

            self._validate_entity_links(entity_map, entity)
            self._generate_direct_links(entity_map, entity)

        return entity_map

    def _generate_direct_links(self, entity_map, entity):
        project = entity_map.get_project()

        if project and entity.type != 'project':
            entity.direct_links.append({
                'entity': 'project',
                'id': project.id,
                'relationship': 'project',
                'is_collection': False
            })
            # TODO: Remove when biomaterial/process.projects is deprecated
            # https://github.com/ebi-ait/dcp-ingest-central/issues/88
            if entity.type == 'biomaterial' or entity.type == 'process':
                entity.direct_links.append({
                    'entity': 'project',
                    'id': project.id,
                    'relationship': 'projects'
                })

        if project and entity.concrete_type == 'supplementary_file':
            project.direct_links.append({
                'entity': 'file',
                'id': entity.id,
                'relationship': 'supplementaryFiles'
            })

        links_by_entity = entity.links_by_entity

        linked_biomaterial_ids = links_by_entity.get('biomaterial', [])
        linked_process_id = links_by_entity['process'][0] if links_by_entity.get('process') else None
        linked_protocol_ids = links_by_entity.get('protocol', [])
        linked_file_ids = links_by_entity.get('file', [])

        linking_details = entity.linking_details

        if linked_biomaterial_ids or linked_file_ids:

            linking_process = self.link_process(entity_map, linked_process_id, linking_details)
            linking_process.direct_links.append({
                'entity': 'project',
                'id': project.id,
                'relationship': 'project',
                'is_collection': False
            })
            # TODO: Remove when process.projects is deprectated
            linking_process.direct_links.append({
                'entity': 'project',
                'id': project.id,
                'relationship': 'projects'
            })
            entity_map.add_entity(linking_process)

            # link output of process
            entity.direct_links.append({
                'entity': linking_process.type,
                'id': linking_process.id,
                'relationship': 'derivedByProcesses'
            })

            # apply all protocols to the linking process
            for linked_protocol_id in linked_protocol_ids:
                linking_process.direct_links.append({
                    'entity': 'protocol',
                    'id': linked_protocol_id,
                    'relationship': 'protocols'
                })

            # biomaterial-biomaterial
            # file-biomaterial
            for linked_biomaterial_id in linked_biomaterial_ids:
                linked_biomaterial_entity = entity_map.get_entity('biomaterial', linked_biomaterial_id)
                linked_biomaterial_entity.direct_links.append({
                    'entity': linking_process.type,
                    'id': linking_process.id,
                    'relationship': 'inputToProcesses'
                })

            # file-file
            for linked_file_id in linked_file_ids:
                linked_file_entity = entity_map.get_entity('file', linked_file_id)
                linked_file_entity.direct_links.append({
                    'entity': linking_process.type,
                    'id': linking_process.id,
                    'relationship': 'inputToProcesses'
                })

    def link_process(self, entity_map, linked_process_id, linking_details):
        if not linked_process_id:
            linked_process_id = self._generate_empty_process_id()

        linking_process = self.create_or_get_process(entity_map, linked_process_id, linking_details)

        return linking_process

    def _validate_entity_links(self, entity_map, entity):
        links_by_entity = entity.links_by_entity

        for link_entity_type, link_entity_ids in links_by_entity.items():
            for link_entity_id in link_entity_ids:
                if not link_entity_type == 'process':  # it is expected that no processes are defined in any tab,
                    # these will be created later
                    if not self._is_valid_spreadsheet_link(entity.type, link_entity_type):
                        raise InvalidLinkInSpreadsheet(entity, link_entity_type, link_entity_id)
                    if not entity_map.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)
                    if not entity_map.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)

                if link_entity_type == 'process' and not len(link_entity_ids) == 1:
                    raise MultipleProcessesFound(entity, link_entity_ids)

    def create_or_get_process(self, entity_map, process_id, linking_details):
        process = entity_map.get_entity('process', process_id)

        if not process:
            process = self.create_process(process_id, linking_details)

        return process

    @staticmethod
    def _is_valid_spreadsheet_link(from_entity_type, to_entity_type):
        VALID_ENTITY_LINKS_MAP = [
            'biomaterial-biomaterial',
            'file-biomaterial',
            'file-file',
            'biomaterial-process',
            'biomaterial-protocol',
            'file-process',
            'file-protocol',
        ]
        link_key = from_entity_type + '-' + to_entity_type

        return link_key in VALID_ENTITY_LINKS_MAP

    def create_process(self, process_id, linking_details):
        schema_type = 'process'
        described_by = self.template_manager.get_schema_url(schema_type)

        if linking_details:
            if not linking_details.get('process_core'):
                linking_details['process_core'] = {}

            linking_details['process_core']['process_id'] = process_id
            linking_details['schema_type'] = schema_type
            linking_details['describedBy'] = described_by
        else:
            process_core = {'process_id': process_id}
            linking_details = {
                "process_core": process_core,
                "schema_type": schema_type,
                "describedBy": described_by
            }

        process = Entity(
            entity_type='process',
            entity_id=process_id,
            content=linking_details
        )

        return process

    def _generate_empty_process_id(self):
        self.process_id_ctr += 1

        return 'process_id_' + str(self.process_id_ctr)
