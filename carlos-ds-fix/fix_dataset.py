import json

import requests

from ingest.api.ingestapi import IngestApi
from ingest.importer.conversion import template_manager

TOKEN = ''
CELL_SUSPENSIONS_FILE = 'cell_suspensions.json'
NEW_ASSAYS_FILE = 'assay_grouping.json'
UPDATE_ASSAYS_FILE = 'assays_unlink_files.json'
SUBMISSION_URL = 'http://localhost:8080/submissionEnvelopes/5f1b0e98fe9c934c8b835c80'
ingest_api = IngestApi(url='http://localhost:8080')
ingest_api.set_token(TOKEN)
template_mgr = template_manager.build(None, ingest_api)


def get_id(type, content):
    if type == 'file':
        return content[f'{type}_core'][f'{type}_name']

    return content[f'{type}_core'][f'{type}_id']


def link_to_entity(from_entity, process, to_entity, protocols):
    ingest_api.link_entity(from_entity, process, 'inputToProcesses')
    ingest_api.link_entity(to_entity, process, 'derivedByProcesses')

    for protocol in protocols:
        ingest_api.link_entity(process, protocol, 'protocols')


def add_to_submission(entity_link: str, content: dict):
    resource = ingest_api.create_entity(SUBMISSION_URL, content, entity_link)
    return resource


def create_process_content(process_id: str, content: dict = None):
    schema_type = 'process'
    described_by = template_mgr.get_schema_url(schema_type)

    process_core = {'process_id': process_id}

    if not content:
        content = {}

    content.update({
        "process_core": process_core,
        "schema_type": schema_type,
        "describedBy": described_by
    })

    return content


def get_entity_by_uuid(entity_type, entity_uuid) -> dict:
    return ingest_api.get_entity_by_uuid(entity_type, entity_uuid)


def unlink_entities(from_entity, relationship, to_entity):
    from_uri = from_entity["_links"][relationship]["href"]
    to_uri = to_entity["_links"]['self']["href"]
    to_id = to_uri.split('/')[-1]

    delete_url = f'{from_uri}/{to_id}'

    headers = {
        'Content-type': 'text/uri-list',
        'Authorization': f'Bearer {TOKEN}'
    }

    print(f'removing file from process {delete_url}')
    r = requests.delete(delete_url, headers=headers)
    r.raise_for_status()


if __name__ == '__main__':
    print('starting update')
    with open(UPDATE_ASSAYS_FILE, encoding='utf-8') as fh:
        UPDATE_ASSAYS = json.loads(fh.read())

    for cell_suspension_resource_0 in UPDATE_ASSAYS.values():
        process_uuid = cell_suspension_resource_0['assay_process']
        process_resource = get_entity_by_uuid('processes', process_uuid)
        derived_files = list(ingest_api.get_related_entities('derivedFiles', process_resource, 'files'))
        derived_file_uuids = [d['uuid']['uuid'] for d in derived_files]

        output_file_uuids = cell_suspension_resource_0['files']

        for file_uuid in derived_file_uuids:
            if file_uuid not in output_file_uuids:
                file_resource = get_entity_by_uuid('files', file_uuid)
                unlink_entities(file_resource, 'derivedByProcesses', process_resource)

    with open(NEW_ASSAYS_FILE, encoding='utf-8') as fh:
        NEW_ASSAYS = json.loads(fh.read())

    with open(CELL_SUSPENSIONS_FILE, encoding='utf-8') as fh:
        cell_suspensions = json.loads(fh.read())

    for cell_suspension_resource_0 in cell_suspensions:
        cell_suspension_content = cell_suspension_resource_0['content']
        cell_suspension_id = get_id('biomaterial', cell_suspension_content)
        cell_suspension_resource = add_to_submission('biomaterials', cell_suspension_content)
        print(
            f'created {cell_suspension_resource["uuid"]["uuid"]} {cell_suspension_resource["_links"]["self"]["href"]} ')

        cell_suspension = NEW_ASSAYS[cell_suspension_id]
        input_biomaterial = cell_suspension['input_biomaterial']

        input_bio = input_biomaterial['uuid']
        input_biomaterial_resource = get_entity_by_uuid('biomaterials', input_bio)

        process_content = create_process_content(f'{cell_suspension_id}-input')
        process_resource = add_to_submission('processes', process_content)

        input_biomaterial['process'] = process_resource['uuid']['uuid']
        protocol_uuids = input_biomaterial['protocols']
        protocols = [get_entity_by_uuid('protocols', protocol_uuid) for protocol_uuid in protocol_uuids]
        link_to_entity(input_biomaterial_resource, process_resource, cell_suspension_resource, protocols)

        output_file_uuids = cell_suspension['files']

        process_id = cell_suspension['process_id']
        process_content = create_process_content(process_id)
        process_resource = add_to_submission('processes', process_content)
        cell_suspension['assay_process'] = process_resource['uuid']['uuid']

        for output_file_uuid in output_file_uuids:
            output_file_resource = get_entity_by_uuid('files', output_file_uuid)
            protocol_uuids = cell_suspension['protocols']
            protocols = [get_entity_by_uuid('protocols', protocol_uuid) for protocol_uuid in protocol_uuids]
            link_to_entity(cell_suspension_resource, process_resource, output_file_resource, protocols)
            print(f'linked {cell_suspension_id} to {output_file_uuid}')
            output_file_url = output_file_resource['_links']['self']['href']
            file_content = output_file_resource['content']
            file_content['library_prep_id'] = process_id
            ingest_api.patch(output_file_url, {'content': file_content})

    with open('../_local/assay_grouping_process_uuids.json', "w") as open_file:
        json.dump(NEW_ASSAYS, open_file, indent=4)
        open_file.close()
