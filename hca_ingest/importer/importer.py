import logging
from typing import Tuple, List

import polling
from openpyxl import Workbook
from requests import HTTPError

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.importer.conversion import template_manager
from hca_ingest.importer.conversion.metadata_entity import MetadataEntity
from hca_ingest.importer.conversion.template_manager import TemplateManager
from hca_ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook
from hca_ingest.importer.spreadsheet.ingest_worksheet import IngestWorksheet
from hca_ingest.importer.submission.entity_linker import EntityLinker
from hca_ingest.importer.submission.entity_map import EntityMap
from hca_ingest.importer.submission.ingest_submitter import IngestSubmitter
from hca_ingest.importer.submission.submission import Submission
from hca_ingest.template.exceptions import UnknownKeySchemaException
from hca_ingest.utils.IngestError import ImporterError, ParserError

format = '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=format)


class XlsImporter:
    """
    XlsImporter is used to convert a contributor's spreadsheet into metadata json entities and to submit those to
    Ingest. Please see https://github.com/HumanCellAtlas/ingest-central/wiki/Data-Contributors-Spreadsheet-Quick-Guide
    for more information on the spreadsheet format.
    """

    def __init__(self, ingest_api: IngestApi):
        self.ingest_api = ingest_api
        self.logger = logging.getLogger(__name__)
        self.submitter = IngestSubmitter(self.ingest_api)

    def generate_json(self, file_path, is_update, project_uuid=None, update_project=False):
        ingest_workbook = IngestWorkbook.from_file(file_path)

        try:
            template_mgr = template_manager.build(ingest_workbook.get_schemas(), self.ingest_api)
        except Exception as e:
            raise SchemaRetrievalError(
                f'There was an error retrieving the schema information to process the spreadsheet. {str(e)}')

        workbook_importer = WorkbookImporter(template_mgr)
        spreadsheet_json, errors = workbook_importer.do_import(ingest_workbook, is_update, project_uuid, update_project)

        return spreadsheet_json, template_mgr, errors

    def dry_run_import_file(self, file_path, project_uuid=None):
        spreadsheet_json, template_mgr, errors = self.generate_json(file_path, project_uuid)

        if errors:
            return None, errors

        entity_map = EntityMap.load(spreadsheet_json)
        entity_linker = EntityLinker(template_mgr, entity_map)
        entity_linker.convert_spreadsheet_links_to_ingest_links()

        return entity_map, []

    def import_file(self, file_path, submission_url, is_update=False, project_uuid=None, update_project=False) -> Tuple[
        Submission, TemplateManager]:
        try:
            if project_uuid:
                self.submitter.link_submission_to_project(project_uuid, submission_url)

            submission = None
            template_mgr = None
            spreadsheet_json, template_mgr, errors = self.generate_json(file_path, is_update, project_uuid=project_uuid,
                                                                        update_project=update_project)
            entity_map = EntityMap.load(spreadsheet_json)
            self.ingest_api.delete_submission_errors(submission_url)

            if errors:
                self.report_errors(submission_url, errors)
            elif is_update:
                self.submitter.update_entities(entity_map)
            else:
                entity_linker = EntityLinker(template_mgr, entity_map)
                entity_linker.convert_spreadsheet_links_to_ingest_links()
                submission = self._submit_new_entities(entity_map, submission_url)

            project = entity_map.get_project()

            def is_editable(entity):
                return entity['editable'] is True

            if project and project_uuid and update_project:
                polling.poll(
                    lambda: self.ingest_api.get(submission_url).json(),
                    check_success=is_editable,
                    step=5,
                    timeout=90
                )
                self.submitter.update_entity(project)

        except HTTPError as httpError:
            self.logger.exception(httpError)
            status = httpError.response.status_code
            text = httpError.response.text
            importer_error = ImporterError(f'Received an HTTP {status} from  {httpError.request.url}: {text}')
            self.ingest_api.create_submission_error(submission_url, importer_error.getJSON())
            return None, template_mgr
        except Exception as e:
            self.ingest_api.create_submission_error(submission_url, ImporterError(str(e)).getJSON())
            self.logger.error(str(e), exc_info=True)
            return None, template_mgr
        finally:
            self.logger.info(f'Submission in {submission_url} is done!')
            return submission, template_mgr

    def _submit_new_entities(self, entity_map, submission_url):
        submission = self.submitter.add_entities(entity_map, submission_url)
        project = entity_map.get_project()
        if project and project.is_new:
            self.submitter.link_submission_to_project(project.uuid, submission_url)

        self.submitter.link_entities(entity_map, submission)
        return submission

    def report_errors(self, submission_url, errors):
        self.logger.info(f'Logged {len(errors)} ParsingErrors.', exc_info=False)
        for error in errors:
            self.ingest_api.create_submission_error(
                submission_url,
                ParserError(error["location"], error["type"], error["detail"]).getJSON()
            )

    @staticmethod
    def update_spreadsheet_with_uuids(submission: Submission, template_mgr: TemplateManager, file_path):
        if not submission:
            return
        wb = IngestWorkbook.from_file(file_path, read_only=False)
        wb.add_entity_uuids(submission)
        wb.add_schemas_worksheet(template_mgr.get_schemas())
        return wb.save(file_path)

    def import_project_from_workbook(self, workbook: Workbook, token: str) -> (str, List[dict]):
        project_metadata_json, errors = self._generate_project_json_from_workbook(workbook)

        if errors:
            return None, errors
        else:
            ingest_project = self.ingest_api.create_project(None, content=project_metadata_json, token=token)
            project_uuid = ingest_project['uuid']['uuid']
            return project_uuid, []

    def _generate_project_json_from_workbook(self, workbook):
        ingest_workbook = IngestWorkbook(workbook)
        template_mgr = self._setup_template_manager_for_project_import()
        workbook_importer = WorkbookImporter(template_mgr)
        spreadsheet_json, errors = workbook_importer.do_import(ingest_workbook, False, worksheet_titles=['Project'])

        if errors:
            return None, errors
        else:
            projects = list(spreadsheet_json.get('project').values())
            project = projects[0] if projects else None
            project_metadata = project.get('content')
            return project_metadata, []

    def _setup_template_manager_for_project_import(self):
        try:
            project_schema_url = self.ingest_api.get_latest_schema_url('type', 'project', 'project')
            template_mgr = template_manager.build([project_schema_url], self.ingest_api)
        except Exception as e:
            raise SchemaRetrievalError(
                f'There was an error retrieving the project schema information to import the project. {str(e)}')
        return template_mgr


_PROJECT_ID = 'project_0'
_PROJECT_TYPE = 'project'


class _ImportRegistry:
    """
    This is a helper class for managing metadata entities during Workbook import.
    """

    def __init__(self, template_mgr: TemplateManager):
        self.template_mgr = template_mgr
        self._submittable_registry = {}
        self._module_registry = {}
        self._module_list = []
        self.project_id = _PROJECT_ID

    def add_submittable(self, metadata: MetadataEntity):
        # TODO no test to check case sensitivity
        domain_type = metadata.domain_type.lower()
        type_map = self._submittable_registry.get(domain_type)
        if not type_map:
            type_map = {}
            self._submittable_registry[domain_type] = type_map
        if domain_type.lower() == _PROJECT_TYPE:
            if not type_map.get(self.project_id):
                metadata.object_id = metadata.object_id or self.project_id
                self.project_id = metadata.object_id
            else:
                raise MultipleProjectsFound()
        type_map[metadata.object_id] = metadata

    def add_submittables(self, metadata_entities):
        for entity in metadata_entities:
            self.add_submittable(entity)

    def add_project_reference(self, project_uuid, project: MetadataEntity = None):
        if project:
            project.object_id = project_uuid
            project.is_linking_reference = True
            project.is_reference = True
        else:
            project = MetadataEntity(domain_type=_PROJECT_TYPE,
                                     concrete_type=_PROJECT_TYPE,
                                     object_id=project_uuid,
                                     is_linking_reference=True)
        self.add_submittable(project)

    def add_module(self, metadata: MetadataEntity):
        if metadata.domain_type.lower() == 'project':
            metadata.object_id = self.project_id
        self._module_list.append(metadata)

    def add_modules(self, module_field_name, metadata_entities):
        all_removed_fields = []
        for entity in metadata_entities:
            removed_fields = entity.retain_fields(module_field_name)
            all_removed_fields.extend(removed_fields)
            self.add_module(entity)
        return all_removed_fields

    def import_modules(self):
        for module_entity in self._module_list:
            type_map = self._submittable_registry.get(module_entity.domain_type, {})
            submittable_entity = type_map.get(module_entity.object_id)
            if submittable_entity:
                submittable_entity.add_module_entity(module_entity)
            else:
                raise LinkToConcreteEntityNotFound(module_entity)

    def flatten(self):
        flat_map = {}
        for domain_type, type_map in self._submittable_registry.items():
            flat_type_map = {object_id: metadata.map_for_submission()
                             for object_id, metadata in type_map.items()}
            flat_map[domain_type] = flat_type_map
        return flat_map

    def has_project(self):
        project_registry = self._submittable_registry.get(_PROJECT_TYPE)
        return project_registry and project_registry.get(self.project_id)


class WorkbookImporter:
    def __init__(self, template_mgr):
        self.worksheet_importer = WorksheetImporter(template_mgr)
        self.template_mgr = template_mgr
        self.logger = logging.getLogger(__name__)

    def do_import(self, workbook: IngestWorkbook, is_update, project_uuid=None, update_project=False,
                  worksheet_titles: List[str] = None):
        registry = _ImportRegistry(self.template_mgr)

        if worksheet_titles:
            importable_worksheets = workbook.select_importable_worksheets(worksheet_titles)
        else:
            importable_worksheets = workbook.importable_worksheets()

        workbook_errors = self.validate_worksheets(is_update, importable_worksheets)

        importable_worksheets = [ws for ws in importable_worksheets]

        for worksheet in importable_worksheets:
            try:
                self._import_worksheet(project_uuid, registry, update_project, workbook_errors, worksheet)
            except Exception as e:
                workbook_errors.append(
                    {"location": f'sheet={worksheet.title}', "type": e.__class__.__name__, "detail": str(e)})

        if not registry.has_project() and not project_uuid:
            e = NoProjectFound()
            workbook_errors.append({"location": "File", "type": e.__class__.__name__, "detail": str(e)})

        if project_uuid and update_project and not registry.has_project():
            workbook_errors.append({"location": "File", "type": "NoProjectWorksheet",
                                    "detail": "The option to update the project was specified but there is no project "
                                              "worksheet found."})

        if project_uuid and not registry.has_project():
            registry.add_project_reference(project_uuid)

        self._import_modules(registry, workbook_errors)

        return registry.flatten(), workbook_errors

    def _import_worksheet(self, project_uuid, registry, update_project, workbook_errors, worksheet):
        self.sheet_in_schemas(worksheet)
        metadata_entities, worksheet_errors = self.worksheet_importer.do_import(worksheet)
        module_field_name = worksheet.get_module_field_name()
        workbook_errors.extend(worksheet_errors)

        if project_uuid and worksheet.is_project_module() and update_project:
            self._register_module(metadata_entities, module_field_name, registry, workbook_errors, worksheet)

        elif project_uuid and worksheet.is_project() and metadata_entities:
            if len(metadata_entities) > 1:
                raise MultipleProjectsFound()

            registry.add_project_reference(project_uuid, metadata_entities[0])

        elif worksheet.is_module_tab():
            self._register_module(metadata_entities, module_field_name, registry, workbook_errors, worksheet)

        else:
            registry.add_submittables(metadata_entities)

    def _register_module(self, metadata_entities, module_field_name, registry, workbook_errors, worksheet):
        removed_data = registry.add_modules(module_field_name, metadata_entities)
        workbook_errors.extend(self.list_data_removal_errors(worksheet.title, removed_data))

    def _import_modules(self, registry, workbook_errors):
        try:
            registry.import_modules()
        except LinkToConcreteEntityNotFound as error:
            location = error.module_entity.get_spreadsheet_location()
            workbook_errors.append({
                'location': f'sheet={location["worksheet_title"]} row_num={location["row_index"]}',
                'type': error.__class__.__name__, "detail": str(error)
            })

    def validate_worksheets(self, is_update, importable_worksheets):
        worksheets_with_uuid = []
        worksheets_without_uuid = []

        for worksheet in importable_worksheets:
            if worksheet.is_module_tab():
                continue
            concrete_type = self.template_mgr.get_concrete_type(worksheet.title)
            uuid_column = f'{concrete_type}.uuid'
            if worksheet.has_column(uuid_column):
                worksheets_with_uuid.append(worksheet.title)
            else:
                worksheets_without_uuid.append(worksheet.title)

        if is_update:
            errors = [MissingEntityUUIDFound(sheet_name) for sheet_name in worksheets_without_uuid]
        else:
            errors = [UnexpectedEntityUUIDFound(sheet_name) for sheet_name in worksheets_with_uuid]

        return [{"location": f'sheet={error.sheet_name}', "type": error.__class__.__name__, "detail": str(error)}
                for error in errors]

    def sheet_in_schemas(self, worksheet):
        schemas = self.template_mgr.template.json_schemas
        try:
            concrete_type = self.template_mgr.get_concrete_type(worksheet.title)
        except UnknownKeySchemaException as e:
            raise SheetNotFoundInSchemas(worksheet.title)
        module_field_name = worksheet.get_module_field_name()
        for schema in schemas:
            if 'name' in schema or 'title' in schema:
                schema_name = schema['name'] if 'name' in schema else schema['title']
                if schema_name == concrete_type:
                    if not worksheet.is_module_tab() or module_field_name in schema['properties']:
                        return True
                    raise SheetNotFoundInSchemas(worksheet.title)
        raise SheetNotFoundInSchemas(worksheet.title)

    @staticmethod
    def list_data_removal_errors(sheet, removed_data):
        errors = []
        for data in removed_data:
            e = DataRemoval(data['key'], data['value'])
            errors.append({"location": f'sheet={sheet}', "type": e.__class__.__name__, "detail": str(e)})
        return errors


class WorksheetImporter:
    KEY_HEADER_ROW_IDX = 4
    USER_FRIENDLY_HEADER_ROW_IDX = 2
    START_ROW_OFFSET = 5

    UNKNOWN_ID_PREFIX = '_unknown_'

    def __init__(self, template: TemplateManager):
        self.template = template
        self.unknown_id_ctr = 0
        self.logger = logging.getLogger(__name__)
        self.concrete_entity = None

    def do_import(self, ingest_worksheet: IngestWorksheet):
        records = []
        worksheet_errors = []
        try:
            row_template = self.template.create_row_template(ingest_worksheet)
            rows = ingest_worksheet.get_data_rows()
            for index, row in enumerate(rows):
                metadata, row_errors = row_template.do_import(row, ingest_worksheet.is_module_tab())
                for error in row_errors:
                    if 'location' in error:
                        error["location"] = f'sheet={ingest_worksheet.title} row={index}, {error["location"]}'
                    else:
                        error["location"] = f'sheet={ingest_worksheet.title} row={index}'
                    worksheet_errors.append(error)
                if not metadata.object_id:
                    metadata.object_id = self._generate_id()
                records.append(metadata)
        except Exception as e:
            worksheet_errors.append({
                "location": f'sheet={ingest_worksheet.title}',
                "type": e.__class__.__name__,
                "detail": str(e)
            })
        return records, worksheet_errors

    def _generate_id(self):
        self.unknown_id_ctr = self.unknown_id_ctr + 1
        return f'{self.UNKNOWN_ID_PREFIX}{self.unknown_id_ctr}'


class MultipleProjectsFound(Exception):
    def __init__(self):
        message = f'The spreadsheet should only be associated to a single project.'
        super(MultipleProjectsFound, self).__init__(message)


class NoProjectFound(Exception):
    def __init__(self):
        message = f'The spreadsheet should be associated to a project.'
        super(NoProjectFound, self).__init__(message)


class SheetNotFoundInSchemas(Exception):
    def __init__(self, sheet):
        message = f'The sheet named {sheet} was not found in the schema list.'
        super(SheetNotFoundInSchemas, self).__init__(message)
        self.sheet = sheet


class DataRemoval(Exception):
    def __init__(self, key, value):
        message = f'The column header [{key}] was not recognised, the following data has been removed: {value}.'
        super(DataRemoval, self).__init__(message)
        self.key = key
        self.value = value


class SchemaRetrievalError(Exception):
    pass


class UnexpectedEntityUUIDFound(Exception):
    def __init__(self, sheet_name):
        message = f'The {sheet_name} entities in the spreadsheet shouldn’t have UUIDs.'
        super(UnexpectedEntityUUIDFound, self).__init__(message)
        self.sheet_name = sheet_name


class MissingEntityUUIDFound(Exception):
    def __init__(self, sheet_name):
        message = f'The {sheet_name} entities in the spreadsheet should have UUIDs.'
        super(MissingEntityUUIDFound, self).__init__(message)
        self.sheet_name = sheet_name


class LinkToConcreteEntityNotFound(Exception):
    def __init__(self, module_entity: MetadataEntity):
        message = f'The module_entity is not linked to any concrete entity'
        super(LinkToConcreteEntityNotFound, self).__init__(message)
        self.module_entity = module_entity
