from ingest.api.ingestapi import IngestApi
from ingest.downloader.entity import Entity


class DataCollector:

    def __init__(self, ingest_api: IngestApi):
        self.api = ingest_api

    def collect_data_by_submission_uuid(self, submission_uuid):
        submission = self.api.get_submission_by_uuid(submission_uuid)
        submission_id = submission['_links']['self']['href'].split('/')[-1]
        project_json = self.api.get_related_project(submission_id)

        if project_json:
            data_by_submission = [
                project_json
            ]
        else:
            raise Exception('There should be a project')

        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'biomaterials')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'processes')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'protocols')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'files')

        return data_by_submission

    def collect_data_by_submission_uuid2(self, submission_uuid):
        submission = self.api.get_submission_by_uuid(submission_uuid)
        submission_id = submission['_links']['self']['href'].split('/')[-1]
        project_json = self.api.get_related_project(submission_id)

        if project_json:
            data_by_submission = [
                project_json
            ]
        else:
            raise Exception('There should be a project')

        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'biomaterials')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'processes')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'protocols')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'files')

        entity_list = []
        for entity_json in data_by_submission:
            entity = Entity.from_json(entity_json)
            entity_list.append(entity)

        return entity_list

    def __get_entities_by_submission_and_type(self, data_by_submission, submission, entity_type):
        entity_json = \
            self.api.get_related_entities(entity_type, submission, entity_type)
        if entity_json:
            data_by_submission.extend(list(entity_json))
