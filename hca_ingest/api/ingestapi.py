#!/usr/bin/env python
"""
desc goes here
"""
import json
import logging
import os
import time
from urllib.parse import urljoin

import requests

from hca_ingest.api.requests_utils import create_session_with_retry


class IngestApi:
    def __init__(self, url=None, token_manager=None):
        self.logger = logging.getLogger(__name__)
        self.session = create_session_with_retry()
        self.token_manager = token_manager

        if not url and 'INGEST_API' in os.environ:
            url = os.environ['INGEST_API']
            # expand interpolated env vars
            url = os.path.expandvars(url)
        self.url = url if url else "http://localhost:8080"
        self.headers = self.__get_basic_header()
        self.token = None
        self._submission_links = {}
        self.logger.info(f"using {self.url} for ingest API")
        self._ingest_links = self._get_ingest_links()

    def get_headers(self):
        # refresh token
        if self.token and not self.headers.get('Authorization'):
            self.set_token(f'Bearer {self.token}')
            print(f'Bearer {self.token}')

        if self.token_manager:
            self.set_token(f'Bearer {self.token_manager.get_token()}')
            self.logger.debug(f'Token refreshed!')

        return self.headers

    def set_token(self, token):
        self.token = token
        self.headers['Authorization'] = self.token
        self.logger.debug(f'Token set!')

        return self.headers

    # TODO think of a better way how to manage tokens in this module, allowing clients to unset token for now
    def unset_token(self):
        self.token = None
        self.headers.pop('Authorization', None)
        self.logger.debug(f'Token unset!')
        return self.headers

    def get(self, url, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = self.get_headers()
        r = self.session.get(url, **kwargs)
        r.raise_for_status()
        return r

    def patch(self, url, patch, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = self.get_headers()
        r = self.session.patch(url, json=patch, **kwargs)
        self.session.cache.delete_url(url)
        r.raise_for_status()
        return r

    def put(self, url, data, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = self.get_headers()

        r = self.session.put(url, json=data, **kwargs)
        self.session.cache.delete_url(url)
        r.raise_for_status()
        return r

    def post(self, url, data, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = self.get_headers()

        r = self.session.post(url, json=data, **kwargs)
        self.session.cache.delete_url(url)
        r.raise_for_status()
        return r

    def delete(self, url, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = self.get_headers()

        r = self.session.delete(url, **kwargs)
        self.session.cache.delete_url(url)
        r.raise_for_status()
        return r

    def _get_ingest_links(self):
        return self.get(self.url).json()["_links"]

    def get_link_from_resource_url(self, resource_url, link_name, headers=None):
        links = self.get(resource_url, headers=headers).json().get('_links', {})
        return links.get(link_name, {}).get('href')

    @staticmethod
    def get_link_from_resource(resource, link_name):
        links = resource.get('_links', {})
        return links.get(link_name, {}).get('href')

    def get_latest_schema_url(self, high_level_entity, domain_entity, concrete_entity):
        latest_schema = self.get_schemas(
            latest_only=True,
            high_level_entity=high_level_entity,
            domain_entity=domain_entity.split('/')[0],
            concrete_entity=concrete_entity
        )
        return latest_schema[0]['_links']['json-schema']['href'] if latest_schema else None

    def get_schemas(self, latest_only=True, high_level_entity=None, domain_entity=None, concrete_entity=None):
        schema_url = self.get_schemas_url()
        if latest_only:
            search_url = self.get_link_from_resource_url(schema_url, "search")
            response_j = self.get(search_url).json()
            all_schemas = list(self.get_related_entities("latestSchemas", response_j, "schemas"))
        else:
            all_schemas = list(self.get_entities(schema_url, "schemas"))

        if high_level_entity:
            all_schemas = list(filter(lambda schema: schema.get('highLevelEntity') == high_level_entity, all_schemas))

        if domain_entity:
            all_schemas = list(filter(lambda schema: schema.get('domainEntity') == domain_entity, all_schemas))

        if concrete_entity:
            all_schemas = list(filter(lambda schema: schema.get('concreteEntity') == concrete_entity, all_schemas))

        return all_schemas

    def get_schemas_url(self):
        return self.get_resource_repository_url("schemas")

    def get_staging_jobs_url(self):
        return self.get_resource_repository_url("stagingJobs")

    def get_resource_repository_url(self, repository_name: str):
        if repository_name in self._ingest_links:
            return self._ingest_links[repository_name]["href"].rsplit("{")[0]
        return None

    def get_submissions(self):
        params = {'sort': 'submissionDate,desc'}
        return self.get(
            self._ingest_links["submissionEnvelopes"]["href"].rsplit("{")[0],
            params=params
        ).json()["_embedded"]["submissionEnvelopes"]

    def get_projects(self, submission_id):
        return self.__get_projects_by_submission_id_and_type(submission_id, 'projects')

    def get_related_project(self, submission_id):
        projects = self.__get_projects_by_submission_id_and_type(submission_id, 'relatedProjects')
        return projects[0] if projects else None

    def __get_projects_by_submission_id_and_type(self, submission_id, project_type):
        submission_url = f'{self.url}/submissionEnvelopes/{submission_id}/{project_type}'
        return self.get(submission_url).json().get('_embedded', {}).get('projects', [])

    def get_project_by_id(self, project_id):
        project_url = self.url + '/projects/' + project_id
        return self.get(project_url).json()

    def get_project_by_uuid(self, uuid):
        return self.get_entity_by_uuid('projects', uuid)

    def get_entity_by_uuid(self, entity_type, uuid):
        url = self.url + f'/{entity_type}/search/findByUuid?uuid=' + uuid

        # TODO make the endpoint consistent
        if entity_type == 'submissionEnvelopes':
            url = self.url + f'/{entity_type}/search/findByUuidUuid?uuid=' + uuid

        return self.get(url).json()

    def get_entity_by_callback_link(self, callback_link):
        url = f'{self.url}{callback_link}'
        return self.get(url).json()

    def get_file_by_submission_url_and_filename(self, submission_url, filename):
        search_url = self.get_link_from_resource_url(self.url + '/files/search',
                                                     'findBySubmissionEnvelopeAndFileName')
        search_url = search_url.replace('{?submissionEnvelope,fileName}', '')
        params = {'submissionEnvelope': submission_url, 'fileName': filename}
        return self.get(search_url, params=params).json()

    def get_submission(self, submission_url):
        return self.get(submission_url).json()

    def get_submission_by_uuid(self, submission_uuid):
        headers = self.__get_basic_header()
        search_link = self.get_link_from_resource_url(self.url + '/submissionEnvelopes/search', 'findByUuid', headers)
        search_link = search_link.replace('{?uuid}', '')  # TODO: use a REST traverser instead of requests?
        return self.get(search_link, params={'uuid': submission_uuid}).json()

    def get_files(self, submission_id):
        submission_url = self.get_submission_url(submission_id)
        return self.get_entities(submission_url, 'files')

    def get_bundle_manifests(self, submission_id):
        submission_url = self.get_submission_url(submission_id)
        return self.get_entities(submission_url, "bundleManifests")

    def create_submission(self, update_submission=False):
        try:
            create_submission_url = self._ingest_links["submissionEnvelopes"]["href"].rsplit("{")[0]

            if update_submission:
                create_submission_url = f'{create_submission_url}/updateSubmissions'

            submission = self.post(create_submission_url, {}).json()
            submission_url = submission["_links"]["self"]["href"].rsplit("{")[0]
            self._submission_links[submission_url] = submission["_links"]
            return submission
        except requests.exceptions.RequestException as err:
            self.logger.error("Request failed: " + str(err))
            raise

    def get_submission_links(self, submission_url):
        if not self._submission_links.get(submission_url):
            self._submission_links[submission_url] = self.get(submission_url).json()["_links"]

        return self._submission_links.get(submission_url)

    def get_link_in_submission(self, submission_url, link_name):
        links = self.get_submission_links(submission_url)
        if link_name in links:
            link_obj = links.get(link_name)
            link = link_obj['href'].rsplit("{")[0]
            return link

        raise ValueError(f"{link_name} is not in submission resource links")

    def update_submission_state(self, submission_id, state):
        state_url = self.get_submission_state_url(submission_id, state)
        return self.put(state_url, data=None).json()

    def get_submission_state_url(self, submission_id, state):
        submission_url = self.get_submission_url(submission_id)
        return self.get_link_in_submission(submission_url, state)

    def get_submission_url(self, submission_id):
        return self._ingest_links["submissionEnvelopes"]["href"].rsplit("{")[0] + "/" + submission_id

    def get_full_url(self, callback_link):
        return urljoin(self.url, callback_link)

    def get_process(self, process_url):
        return self.get(process_url).json()

    def get_entities(self, submission_url, entity_type):
        submission = self.get(submission_url).json()
        if entity_type in submission["_links"]:
            yield from self.get_all(submission["_links"][entity_type]["href"], entity_type)

    def get_all(self, url, entity_type):
        result = self.get(url).json()

        entities = result["_embedded"][entity_type] if '_embedded' in result else []
        yield from entities

        while "next" in result["_links"]:
            next_url = result["_links"]["next"]["href"]
            result = self.get(next_url).json()
            entities = result["_embedded"][entity_type]
            yield from entities
            self.logger.info(f"GET {entity_type} {json.dumps(result['page'])}")

    def get_related_entities(self, relation, entity, entity_type):
        # get the self link from entity
        if relation in entity["_links"]:
            entity_uri = entity["_links"][relation]["href"]
            for entity in self.get_all(entity_uri, entity_type):
                yield entity

    def get_related_entities_count(self, relation, entity, entity_type):
        if relation in entity["_links"]:
            entity_uri = entity["_links"][relation]["href"]
            result = self.get(entity_uri).json()
            if 'page' in result:
                return result.get('page').get('totalElements')
            return len(result["_embedded"][entity_type])

    def create_project(self, submission_url: str, content: dict, uuid: str = None, token: str = None):
        if not submission_url:
            project = self._create_project_without_submission(content, token)
        else:
            project = self.create_entity(submission_url, {'content': content}, "projects", uuid)
        return project

    def _create_project_without_submission(self, content, token=None):
        headers = dict.copy(self.get_headers())
        if token:
            headers['Authorization'] = token
        return self.post(f'{self.url}/projects', {'content': content}, headers=headers).json()

    def create_biomaterial(self, submission_url, content, uuid=None):
        return self.create_entity(submission_url, {'content': content}, "biomaterials", uuid)

    def create_process(self, submission_url, content, uuid=None):
        return self.create_entity(submission_url, {'content': content}, "processes", uuid)

    def create_protocol(self, submission_url, content, uuid=None):
        return self.create_entity(submission_url, {'content': content}, "protocols", uuid)

    def create_file(self, submission_url, filename, content, uuid=None):
        submission_files_url = self.get_link_in_submission(submission_url, 'files')

        file_to_create_object = {
            "fileName": filename,
            "content": content
        }

        params = {}
        if uuid:
            params["updatingUuid"] = uuid

        time.sleep(0.001)

        # Do not use session retries here as it will throw requests.exceptions.RetryError for http 409 error
        # We want to retry that error when creating files by doing a subsequent PATCH requests to update the metadata
        r = requests.post(
            submission_files_url,
            json=file_to_create_object,
            params=params,
            headers=self.get_headers()
        )

        # TODO Investigate why core is returning internal server error
        if r.status_code == requests.codes.conflict or r.status_code == requests.codes.internal_server_error:
            search_files = self.get_file_by_submission_url_and_filename(submission_url, filename)

            if search_files and search_files.get('_embedded') and search_files['_embedded'].get('files'):
                file_in_ingest = search_files['_embedded'].get('files')[0]
                existing_content = file_in_ingest.get('content')
                new_content = existing_content

                if existing_content:
                    new_content.update(content)
                else:
                    new_content = content

                file_url = file_in_ingest['_links']['self']['href']
                time.sleep(0.001)
                r = self.patch(file_url, {'content': new_content})
                self.logger.debug(f'Updating existing content of file {file_url}.')

        r.raise_for_status()

        return r.json()

    def create_submission_manifest(self, submission_url, data):
        return self.create_entity(submission_url, data, 'submissionManifest')

    def create_submission_error(self, submission_url, data):
        return self.create_entity(submission_url, data, 'submissionEnvelopeErrors')

    def delete_submission_errors(self, submission_url: str):
        self.delete(f'{submission_url}/submissionErrors')

    def create_entity(self, submission_url, data, entity_type, uuid=None):
        params = {}
        if uuid:
            params["updatingUuid"] = uuid

        submission_url = self.get_link_in_submission(submission_url, entity_type)
        self.logger.debug(f"POST {submission_url} {json.dumps(data)}")

        return self.post(submission_url, data, params=params).json()

    def get_object_uuid(self, entity_uri):
        return self.get(entity_uri).json()["uuid"]["uuid"]

    def link_entity(self, from_entity, to_entity, relationship, is_collection=True):
        if not from_entity:
            raise ValueError("Error: from_entity is None")

        if not to_entity:
            raise ValueError("Error: to_entity is None")

        if not relationship:
            raise ValueError("Error: relationship is None")

        # check each dict in turn for non-None-ness

        from_entity_links = from_entity["_links"] if "_links" in from_entity else None
        if not from_entity_links:
            raise ValueError("Error: from_entity has no _links")

        from_entity_links_relationship = from_entity_links[relationship] if relationship in from_entity_links else None
        if not from_entity_links_relationship:
            raise ValueError("Error: from_entity_links has no {0} relationship".format(relationship))

        from_entity_links_relationship_href = from_entity_links_relationship[
            "href"] if "href" in from_entity_links_relationship else None
        if not from_entity_links_relationship_href:
            raise ValueError(
                "Error: from_entity_links_relationship for relationship {0} has no href".format(relationship))

        from_uri = from_entity["_links"][relationship]["href"]
        to_uri = self.get_link_from_resource(to_entity, 'self')

        self.logger.info('fromUri ' + from_uri + ' toUri:' + to_uri)

        headers = dict.copy(self.get_headers())
        headers['Content-type'] = 'text/uri-list'

        if is_collection:
            return self.post(
                from_uri.rsplit("{")[0],
                to_uri.rsplit("{")[0],
                headers=headers
            )
        return self.put(
            from_uri.rsplit("{")[0],
            to_uri.rsplit("{")[0],
            headers=headers
        )

    def create_bundle_manifest(self, bundle_manifest):
        url = self._ingest_links["bundleManifests"]["href"].rsplit("{")[0]
        return self.post(url, bundle_manifest.__dict__).json()

    def update_staging_details(self, submission_url, uuid, staging_area_location):
        staging_details = {
            "stagingDetails": {
                "stagingAreaUuid": {
                    "uuid": uuid
                },
                "stagingAreaLocation": {
                    "value": staging_area_location
                }
            }
        }
        self.patch(submission_url, staging_details)

    def delete_staging_jobs(self, staging_area_uuid):
        delete_jobs_url = f'{self.get_staging_jobs_url()}/delete'
        params = {"stagingAreaUuid": staging_area_uuid}
        return self.delete(delete_jobs_url, params=params).json()

    def delete_staging_job(self, staging_job_url):
        return self.delete(staging_job_url)

    def create_staging_job(self, staging_area_uuid, file_name, metadata_uuid):
        staging_info = {
            "stagingAreaUuid": staging_area_uuid,
            "stagingAreaFileName": file_name,
            "metadataUuid": metadata_uuid
        }
        return self.post(self.get_staging_jobs_url(), staging_info).json()

    def complete_staging_job(self, complete_url, upload_file_uri):
        staging_info = {
            "stagingAreaFileUri": upload_file_uri
        }
        return self.patch(complete_url, staging_info).json()

    def find_staging_job(self, upload_area_uuid, filename):
        search_params = {
            "stagingAreaUuid": upload_area_uuid,
            "stagingAreaFileName": filename
        }
        find_staging_job_suffix = '/search/findByStagingAreaUuidAndStagingAreaFileName'
        find_staging_job_url = self.get_staging_jobs_url() + find_staging_job_suffix
        return self.get(find_staging_job_url, params=search_params).json()

    @staticmethod
    def __get_basic_header():
        return {'Content-type': 'application/json'}
