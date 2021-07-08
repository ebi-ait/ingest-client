class Error(Exception):
    def __init__(self, code, message):
        super(Error, self).__init__(message)
        self.code = code
        self.message = message


class InvalidEntityIngestLink(Error):
    def __init__(self, from_entity, to_entity):
        message = f'It is not possible to link a {from_entity.type} to {to_entity.type} in ingest database.'
        super(InvalidEntityIngestLink, self).__init__('InvalidEntityIngestLink', message)
        self.from_entity = from_entity
        self.to_entity = to_entity


class InvalidLinkInSpreadsheet(Error):
    def __init__(self, from_entity, link_entity_type, link_entity_id):
        message = f'It is not possible to link a {from_entity.type} to {link_entity_type} in the spreadsheet.'
        super(InvalidLinkInSpreadsheet, self).__init__('InvalidLinkInSpreadsheet', message)
        self.from_entity = from_entity
        self.link_entity_type = link_entity_type
        self.link_entity_id = link_entity_id


class LinkedEntityNotFound(Error):
    def __init__(self, from_entity, entity_type, id):
        message = f'A link from a {from_entity.type} with id {from_entity.id} to a {entity_type} with id, ' \
                  f'"{id}", is not found in the spreadsheet.'
        super(LinkedEntityNotFound, self).__init__('LinkedEntityNotFound', message)
        self.entity = entity_type
        self.id = id


class MultipleProcessesFound(Error):
    def __init__(self, from_entity, process_ids):
        message = f'Multiple processes are linked {from_entity.type} in the spreadsheet: {process_ids}.'
        super(MultipleProcessesFound, self).__init__('MultipleProcessesFound', message)

        self.process_ids = process_ids
        self.from_entity = from_entity
