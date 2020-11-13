import json

import requests

from ingest.api.ingestapi import IngestApi
from ingest.importer.conversion import template_manager

TOKEN = ''
CELL_SUSPENSIONS_FILE = './carlos-ds-fix/cell_suspensions.json'
CWD = './carlos-ds-fix'
NEW_ASSAYS_FILE = f'{CWD}/assay_grouping.json'
UPDATE_ASSAYS_FILE = f'{CWD}/assays_unlink_files.json'
UPDATE_DONORS_FILE = f'{CWD}/outdated_donors.json'
OUTPUT_FILE = f'{CWD}/assay_grouping_process_uuids.json'
INGEST_API = 'https://api.ingest.dev.archive.data.humancellatlas.org'
SUBMISSION_URL = f'{INGEST_API}/submissionEnvelopes/5f1b0e98fe9c934c8b835c80'
ingest_api = IngestApi(url=INGEST_API)
ingest_api.set_token(f'Bearer {TOKEN}')
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

    donors_to_update = [
        "bcc392bb-ca37-428e-8513-58c53a8116a0",
        "96dd85dd-6f2e-4b2a-947f-9a66ff2ba8f0",
        "f141b80f-70c3-42ac-83f9-f77af6dac71c",
        "73689cb0-8a83-4d84-a642-b3af2a72a086",
        "981597b0-882f-4797-8d31-a4353bc49371",
        "2ff6b64f-c1f3-413f-bf30-7a1b7a2ad62c",
        "4d72ef99-5ce9-43bd-9aed-b867d466eb84",
        "818a588a-50a6-429a-85a1-8e8d85b0ea37",
        "092c9a21-3e90-4e39-b7a8-294377bec28f",
        "c2723205-b835-4ca5-8ff1-d0e3989018f7",
        "f6a1741e-8a24-435f-b57e-c7897175bc0f",
        "5beade7e-dfeb-45db-9d97-afc594bc7ee2"
    ]

    for donor_uuid in donors_to_update:
        donor = get_entity_by_uuid('biomaterials', donor_uuid)
        donor_url = donor['_links']['self']['href']
        content = donor['content']
        content.update({"diseases": [{
            "text": "normal",
            "ontology_label": "normal",
            "ontology": "PATO:0000461"
        }]})
        ingest_api.patch(donor_url, {'content': content})

    with open(OUTPUT_FILE, "w") as open_file:
        json.dump(NEW_ASSAYS, open_file, indent=4)
        open_file.close()
