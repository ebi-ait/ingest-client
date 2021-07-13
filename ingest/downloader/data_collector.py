from ingest.api.ingestapi import IngestApi


class DataCollector:

    def __init__(self, ingest_api: IngestApi):
        self.api = ingest_api

    def collect_data_by_submission_uuid(self, submission_uuid):
        submission = self.api.get_submission_by_uuid(submission_uuid).json

        project_json = self.api.get_related_entities('relatedProjects', submission, 'project').json

        data_by_submission = [
            project_json
        ]
        return data_by_submission
