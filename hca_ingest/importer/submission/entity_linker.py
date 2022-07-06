from typing import List

from hca_ingest.importer.conversion.template_manager import TemplateManager
from hca_ingest.importer.submission.entity import Entity
from hca_ingest.importer.submission.entity_map import EntityMap
from hca_ingest.importer.submission.errors import LinkedEntityNotFound, InvalidLinkInSpreadsheet, MultipleProcessesFound

"""
EntityLinker is responsible for converting linking info from spreadsheet to linking in Ingest Core
"""


class EntityLinker(object):

    def __init__(self, template_manager: TemplateManager, entity_map: EntityMap):
        self.template_manager = template_manager
        self.process_id_ctr = 0
        self.entity_map = entity_map

    def convert_spreadsheet_links_to_ingest_links(self):
        for entity in self.entity_map.get_entities():
            self._load_external_links_to_entity_map(entity)
            self._validate_entity_links(entity)
            self._generate_direct_links(entity)
        return self.entity_map

    def _load_external_links_to_entity_map(self, entity: Entity):
        external_links = entity.external_links
        for external_link_type, external_link_uuids in external_links.items():
            for entity_uuid in external_link_uuids:
                external_link_entity = Entity(entity_type=external_link_type,
                                              entity_id=entity_uuid,
                                              content=None,
                                              spreadsheet_location=entity.spreadsheet_location,
                                              is_linking_reference=True)
                external_link_entity.add_link(external_link_type, entity_uuid)
                self.entity_map.add_entity(external_link_entity)

    # direct_links maps to ingest db model
    def _generate_direct_links(self, entity: Entity):
        project = self.entity_map.get_project()

        self._link_entity_to_project(entity, project)
        self._link_supplementary_file_to_project(entity, project)

        external_links = entity.external_links or {}
        links_by_entity = entity.links_by_entity or {}

        input_biomaterial_ids = []
        input_biomaterial_ids.extend(external_links.get('biomaterial', []))
        input_biomaterial_ids.extend(links_by_entity.get('biomaterial', []))

        input_file_ids = []
        input_file_ids.extend(external_links.get('file', []))
        input_file_ids.extend(links_by_entity.get('file', []))

        if input_biomaterial_ids or input_file_ids:
            process = self._create_or_get_process(entity)
            self.entity_map.add_entity(process)
            self._link_process_to_project(process, project)
            self._link_entity_as_output_to_process(entity, process)
            self._link_protocols_to_process(entity, process)
            self._link_input_biomaterials_to_entity(input_biomaterial_ids, process)
            self._link_input_files_to_entity(input_file_ids, process)

    def _link_entity_as_output_to_process(self, entity: Entity, linking_process: Entity):
        entity.direct_links.append({
            'entity': linking_process.type,
            'id': linking_process.id,
            'relationship': 'derivedByProcesses'
        })

    def _link_input_files_to_entity(self, linked_file_ids: List[str], linking_process: Entity):
        for linked_file_id in linked_file_ids:
            linked_file_entity = self.entity_map.get_entity('file', linked_file_id)
            linked_file_entity.direct_links.append({
                'entity': linking_process.type,
                'id': linking_process.id,
                'relationship': 'inputToProcesses'
            })

    def _link_input_biomaterials_to_entity(self, linked_biomaterial_ids: List[str], linking_process: Entity):
        for linked_biomaterial_id in linked_biomaterial_ids:
            linked_biomaterial_entity = self.entity_map.get_entity('biomaterial', linked_biomaterial_id)
            linked_biomaterial_entity.direct_links.append({
                'entity': linking_process.type,
                'id': linking_process.id,
                'relationship': 'inputToProcesses'
            })

    def _link_protocols_to_process(self, entity: Entity, process: Entity):
        links_by_entity = entity.links_by_entity
        external_links = entity.external_links

        protocol_ids = []
        protocol_ids.extend(external_links.get('protocol', []))
        protocol_ids.extend(links_by_entity.get('protocol', []))

        for protocol_id in protocol_ids:
            process.direct_links.append({
                'entity': 'protocol',
                'id': protocol_id,
                'relationship': 'protocols'
            })

    def _link_process_to_project(self, linking_process: Entity, project: Entity):
        linking_process.direct_links.append({
            'entity': 'project',
            'id': project.id,
            'relationship': 'project',
            'is_collection': False
        })
        # TODO: Remove when process.projects is deprecated
        linking_process.direct_links.append({
            'entity': 'project',
            'id': project.id,
            'relationship': 'projects'
        })

    def _link_supplementary_file_to_project(self, entity: Entity, project: Entity):
        if project and entity.concrete_type == 'supplementary_file':
            project.direct_links.append({
                'entity': 'file',
                'id': entity.id,
                'relationship': 'supplementaryFiles'
            })

    def _link_entity_to_project(self, entity: Entity, project: Entity):
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

    def _validate_entity_links(self, entity: Entity):
        if entity.is_reference:
            return

        links_by_entity = entity.links_by_entity

        for link_entity_type, link_entity_ids in links_by_entity.items():
            for link_entity_id in link_entity_ids:
                if not link_entity_type == 'process':  # it is expected that no processes are defined in any tab,
                    # these will be created later
                    if not self._is_valid_spreadsheet_link(entity.type, link_entity_type):
                        raise InvalidLinkInSpreadsheet(entity, link_entity_type, link_entity_id)
                    if not self.entity_map.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)
                    if not self.entity_map.get_entity(link_entity_type, link_entity_id):
                        raise LinkedEntityNotFound(entity, link_entity_type, link_entity_id)

                if link_entity_type == 'process' and not len(link_entity_ids) == 1:
                    raise MultipleProcessesFound(entity, link_entity_ids)

    def _create_or_get_process(self, entity: Entity) -> Entity:
        external_links = entity.external_links
        processes = external_links.get('process', [])
        external_process_id = processes[0] if len(processes) > 0 else None

        links_by_entity = entity.links_by_entity
        process_id = links_by_entity['process'][0] if links_by_entity.get('process') else None

        if not process_id:
            process_id = self._generate_empty_process_id()

        linking_details = entity.linking_details

        process = self.entity_map.get_entity('process', process_id)

        if not process:
            process_json = self._create_process_json(process_id, linking_details)
            process = Entity(
                entity_type='process',
                entity_id=external_process_id or process_id,
                content=process_json,
                is_linking_reference=bool(external_process_id),
                is_reference=bool(external_process_id)
            )

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

    def _create_process_json(self, process_id: str, linking_details: dict):
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

        return linking_details

    def _generate_empty_process_id(self):
        self.process_id_ctr += 1
        return 'process_id_' + str(self.process_id_ctr)
