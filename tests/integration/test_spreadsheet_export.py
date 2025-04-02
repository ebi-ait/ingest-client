import os
from collections import namedtuple
from typing import List
from unittest import TestCase

from dotenv import load_dotenv

from hca_ingest.downloader.workbook import WorkbookDownloader
from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.utils.s2s_token_client import S2STokenClient, ServiceCredential
from hca_ingest.utils.token_manager import TokenManager

Case = namedtuple("Case", 'project_uuid submission_uuid')
load_dotenv()  # take environment variables from .env.


class TestSpreadsheetExport(TestCase):
    def setUp(self) -> None:
        credential = ServiceCredential.from_env_var('GOOGLE_APPLICATION_CREDENTIALS')
        audience = os.environ.get('INGEST_API_JWT_AUDIENCE')
        s2s_token_client = S2STokenClient(credential, audience)
        token_manager = TokenManager(s2s_token_client)

        self.url = 'https://api.ingest.dev.archive.data.humancellatlas.org'
        self.api = IngestApi(self.url, token_manager=token_manager)

    def xtest_one_spreadsheet_export(self):
        test_case = next(self.get_test_cases())

        # When
        downloader = WorkbookDownloader(self.api)
        workbook = downloader.get_workbook_from_submission(test_case.submission_uuid)

        # Then
        self.workbook_only_contains_one_project(workbook, test_case.project_uuid)

    def xtest_multiple_spreadsheet_exports(self):
        test_cases = self.get_test_cases(5)
        # When
        downloader = WorkbookDownloader(self.api)
        for test_case in test_cases:
            workbook = downloader.get_workbook_from_submission(test_case.submission_uuid)
            # Then
            self.workbook_only_contains_one_project(workbook, test_case.project_uuid)

    def workbook_only_contains_one_project(self, workbook, project_uuid):
        # Then
        workbook_project_uuids = []
        for row in workbook['Project'].iter_rows(min_row=6, min_col=1, max_col=1):
            workbook_project_uuids.append(row[0].value)
        self.assertListEqual([project_uuid], workbook_project_uuids)

    def get_test_cases(self, number=1) -> List[Case]:
        entity_type = 'projects'
        url = f'{self.url}/{entity_type}/filter?wranglingState=SUBMITTED'
        last_url = self.api.get_link_from_resource_url(url, 'last', size=10)

        params = {'size': self.api.page_size}
        result = self.api.get(last_url, params=params).json()

        projects = result["_embedded"][entity_type] if '_embedded' in result else []
        test_cases_count = 0
        for case in self.get_uuids_from_projects(projects):
            test_cases_count = test_cases_count + 1
            yield case

        while "previous" in result["_links"] and test_cases_count < number:
            next_url = result["_links"]["previous"]["href"]
            result = self.api.get(next_url).json()
            projects = result["_embedded"][entity_type]
            for case in self.get_uuids_from_projects(projects):
                test_cases_count = test_cases_count + 1
                yield case

    def get_uuids_from_projects(self, projects) -> List[Case]:
        test_cases = []
        entity_type = 'submissionEnvelopes'
        for project in projects:
            project_uuid = project.get('uuid', {}).get('uuid', '')
            submissions_url = self.api.get_link_from_resource(project, entity_type)
            submissions = self.api.get_all(submissions_url, entity_type)
            for submission in submissions:
                submission_uuid = submission.get('uuid', {}).get('uuid', '')
                if submission_uuid:
                    test_case = Case(project_uuid, submission_uuid)
                    test_cases.append(test_case)
        return test_cases
