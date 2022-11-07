from openpyxl.workbook import Workbook

from hca_ingest.api.ingestapi import IngestApi
from .data_collector import DataCollector
from .downloader import XlsDownloader
from .flattener import Flattener


class WorkbookDownloader:
    def __init__(self, api: IngestApi):
        self.collector = DataCollector(api)
        self.downloader = XlsDownloader()
        self.flattener = Flattener()

    def get_workbook_from_submission(self, submission_uuid: str) -> Workbook:
        entity_dict = self.collector.collect_data_by_submission_uuid(submission_uuid)
        filtered_list = [entity for entity in entity_dict.values() if entity.content]
        flattened_json = self.flattener.flatten(filtered_list)
        return self.downloader.create_workbook(flattened_json)
