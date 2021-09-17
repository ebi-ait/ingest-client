from ingest.downloader.flattener import Flattener
from tests.unit.downloader.test_flattener import FlattenerTest


class FlattenOntologyModuleTest(FlattenerTest):
    def test_flatten__has_ontology_property_with_multiple_elements(self):
        # given
        self.content.update({
            'organ_parts': [
                {
                    'ontology': 'UBERON:0000376',
                    'ontology_label': 'dummylabel1',
                    'text': 'dummytext1'
                },
                {
                    'ontology': 'UBERON:0002386',
                    'ontology_label': 'dummylabel2',
                    'text': 'dummytext2'
                }
            ]
        })

        self.metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [self.metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.organ_parts.ontology': 'UBERON:0000376||UBERON:0002386',
            'project.organ_parts.ontology_label': 'dummylabel1||dummylabel2',
            'project.organ_parts.text': 'dummytext1||dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.organ_parts.ontology',
                'project.organ_parts.ontology_label',
                'project.organ_parts.text'
            ]
        )

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_ontology_property_with_multiple_elements_but_inconsistent_columns(self):
        # given
        self.content.update(
            {'diseases': [
                {
                    'ontology': 'UBERON:0000376',
                    'ontology_label': 'dummylabel1',
                    'text': 'dummytext1'
                },
                {
                    'text': 'dummytext2'
                }
            ]})

        metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.diseases.ontology': 'UBERON:0000376',
            'project.diseases.ontology_label': 'dummylabel1',
            'project.diseases.text': 'dummytext1||dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.diseases.ontology',
                'project.diseases.ontology_label',
                'project.diseases.text'
            ]
        )

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_ontology_property_with_multiple_elements_but_with_empty_ontology_values(self):
        # given
        self.content.update(
            {'diseases': [
                {
                    'ontology': 'UBERON:0000376',
                    'ontology_label': 'dummylabel1',
                    'text': 'dummytext1'
                },
                {
                    'ontology': '',
                    'ontology_label': '',
                    'text': 'dummytext2'
                }
            ]})

        metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.diseases.ontology': 'UBERON:0000376',
            'project.diseases.ontology_label': 'dummylabel1',
            'project.diseases.text': 'dummytext1||dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.diseases.ontology',
                'project.diseases.ontology_label',
                'project.diseases.text'
            ]
        )

        # then
        self.assertEqual(self.flattened_metadata_entity, actual)

    def test_flatten__has_ontology_property_with_single_element_but_only_with_text_attr(self):
        # given
        self.content.update(
            {'diseases': [
                {
                    'text': 'dummytext2'
                }
            ]})

        metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.diseases.text': 'dummytext2',

        })
        self.flattened_metadata_entity['Project']['headers'].extend(
            [
                'project.diseases.text'
            ]
        )

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)

    def test_flatten__has_ontology_property_with_single_element(self):
        # given
        self.content.update({
            "organ_parts": [{
                "ontology": "UBERON:0000376",
                "ontology_label": "hindlimb stylopod",
                "text": "hindlimb stylopod"
            }]
        })

        metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(entity_list)

        self.flattened_metadata_entity['Project']['values'][0].update({
            'project.organ_parts.ontology': 'UBERON:0000376',
            'project.organ_parts.ontology_label': 'hindlimb stylopod',
            'project.organ_parts.text': 'hindlimb stylopod',

        })
        self.flattened_metadata_entity['Project']['headers'].extend([
            'project.organ_parts.ontology',
            'project.organ_parts.ontology_label',
            'project.organ_parts.text'
        ])

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)
