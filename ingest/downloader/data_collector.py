from ingest.api.ingestapi import IngestApi


class DataCollector:

    def __init__(self, ingest_api: IngestApi):
        self.api = ingest_api

    def collect_data_by_submission_uuid(self, submission_uuid):
        submission = self.api.get_submission_by_uuid(submission_uuid).json

        project_json = self.api.get_related_entities('relatedProjects', submission, 'projects').json
        data_by_submission = [
            project_json
        ]

        self.__get_biomaterials(data_by_submission, submission)

        return data_by_submission

    def __get_biomaterials(self, data_by_submission, submission):
        biomaterials_json = self.api.get_related_entities('biomaterials', submission, 'biomaterials').json
        if biomaterials_json:
            biomaterials_json = biomaterials_json['_embedded']['biomaterials']
            data_by_submission.extend(biomaterials_json)
