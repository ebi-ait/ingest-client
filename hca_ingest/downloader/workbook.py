from openpyxl.workbook import Workbook

from hca_ingest.api.ingestapi import IngestApi
from .data_collector import DataCollector
from .schema_collector import SchemaCollector
from .downloader import XlsDownloader
from .flattener import Flattener


class WorkbookDownloader:
    def __init__(self, api: IngestApi):
        self.data_collector = DataCollector(api)
        self.schema_collector = SchemaCollector()
        self.downloader = XlsDownloader()
        self.flattener = Flattener()

    def get_workbook_from_submission(self, submission_uuid: str) -> Workbook:
        submission_entities = self.data_collector.collect_data_by_submission_uuid(submission_uuid)
        entities_with_content = [entity for entity in submission_entities.values() if entity.content]
        schemas = self.schema_collector.get_schemas_for_entities(entities_with_content)
        flattened_json = self.flattener.flatten(entities_with_content, schemas)
        return self.downloader.create_workbook(flattened_json)
