import json

import requests

from ingest.api.ingestapi import IngestApi
from ingest.importer.conversion import template_manager

TOKEN = ''
CELL_SUSPENSIONS_FILE = './carlos-ds-fix/cell_suspensions.json'
CWD = './carlos-ds-fix'
UPDATE_ASSAYS_FILE = f'{CWD}/assay_grouping_process_uuids.json'
INGEST_API = 'https://api.ingest.archive.data.humancellatlas.org'
SUBMISSION_URL = f'{INGEST_API}/submissionEnvelopes/5f1b0e98fe9c934c8b835c80'
PROJECT_UUID = 'ad98d3cd-26fb-4ee3-99c9-8a2ab085e737'
ingest_api = IngestApi(url=INGEST_API)
ingest_api.set_token(f'Bearer {TOKEN}')
template_mgr = template_manager.build(None, ingest_api)


def get_entity_by_uuid(entity_type, entity_uuid) -> dict:
    return ingest_api.get_entity_by_uuid(entity_type, entity_uuid)


if __name__ == '__main__':
    print('starting update')
    with open(UPDATE_ASSAYS_FILE, encoding='utf-8') as fh:
        UPDATE_ASSAYS = json.loads(fh.read())

    for cell_suspension_resource_0 in UPDATE_ASSAYS.values():
        process_uuid = cell_suspension_resource_0['assay_process']
        process_resource = get_entity_by_uuid('processes', process_uuid)
        project = get_entity_by_uuid('projects', PROJECT_UUID)
        ingest_api.link_entity(process_resource, project, 'projects')
