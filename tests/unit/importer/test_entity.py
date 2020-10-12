from unittest import TestCase

from ingest.importer.submission import Entity, EntityMap

PROJECT = 'project'
BIOMATERIAL = 'biomaterial'
PROTOCOL = 'protocol'


def test_entity(type: str, id: str, content: dict = {}):
    return Entity(type, id, content)


class EntityTest(TestCase):

    def test_link(self):
        # given:
        cell = test_entity(BIOMATERIAL, '456bad9')
        dissociation = test_entity(PROTOCOL, '900400')

        # and:
        self.assertNotIn(dissociation.id, cell.get_links(PROTOCOL))

        # when:
        cell.link(dissociation)

        # then:
        self.assertIn(dissociation.id, cell.get_links(PROTOCOL))

    def test_back_links(self):
        # given:
        project = test_entity(PROJECT, '6492bba')
        donor = test_entity(BIOMATERIAL, '5550101')
        specimen = test_entity(BIOMATERIAL, 'ab67891')

        # when:
        donor.link(project)
        specimen.link(project)
        specimen.link(donor)

        # then:
        for entity in [donor, specimen]:
            self.assertIn(entity, project.back_links)
        self.assertIn(specimen, donor.back_links)


class EntityMapTest(TestCase):

    def test_find_unlinked(self):
        # given:
        donor = test_entity(BIOMATERIAL, '89bce931')
        specimen_1 = test_entity(BIOMATERIAL, 'aab9013')
        specimen_2 = test_entity(BIOMATERIAL, '4471de1')
        cell_1 = test_entity(BIOMATERIAL, '1b7dcca')
        cell_2 = test_entity(BIOMATERIAL, 'f00c331')

        # and: link test data
        specimen_1.link(donor)
        specimen_2.link(donor)
        cell_1.link(specimen_1)

        # and:
        entity_map = EntityMap()
        for entity in [donor, specimen_1, specimen_2, cell_1, cell_2]:
            entity_map.add_entity(entity)

        # when:
        unlinked = entity_map.find_unlinked()

        # then:
        self.assertIn(cell_2, unlinked)
