from openpyxl.workbook import Workbook

from hca_ingest.api.ingestapi import IngestApi
from .data_collector import DataCollector
from .downloader import XlsDownloader


class WorkbookDownloader:
    def __init__(self, api: IngestApi):
        self.collector = DataCollector(api)
        self.downloader = XlsDownloader()

    def get_workbook_from_submission(self, submission_uuid: str) -> Workbook:
        entity_dict = self.collector.collect_data_by_submission_uuid(submission_uuid)
        entity_list = list(entity_dict.values())
        flattened_json = self.downloader.convert_json(entity_list)
        return self.downloader.create_workbook(flattened_json)
