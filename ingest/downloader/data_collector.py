from ingest.api.ingestapi import IngestApi


class DataCollector:

    def __init__(self, ingest_api: IngestApi):
        self.api = ingest_api

    def collect_data_by_submission_uuid(self, submission_uuid):
        submission = self.api.get_submission_by_uuid(submission_uuid)
        project_json = self.api.get_related_projects(submission_uuid)

        data_by_submission = [
            project_json
        ]

        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'biomaterials')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'processes')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'protocols')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'files')

        return data_by_submission

    def __get_entities_by_submission_and_type(self, data_by_submission, submission, entity_type):
        entity_json = \
            self.api.get_related_entities(entity_type, submission, entity_type)
        if entity_json:
            data_by_submission.extend(list(entity_json))
