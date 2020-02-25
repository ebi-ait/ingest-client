class MultipleProjectsFound(Exception):
    def __init__(self):
        message = f'The spreadsheet should only be associated to a single project.'
        super(MultipleProjectsFound, self).__init__(message)


class NoProjectFound(Exception):
    def __init__(self):
        message = f'The spreadsheet should be associated to a project.'
        super(NoProjectFound, self).__init__(message)


class SheetNotFoundInSchemas(Exception):
    def __init__(self, sheet):
        message = f'The sheet named {sheet} was not found in the schema list.'
        super(SheetNotFoundInSchemas, self).__init__(message)
        self.sheet = sheet


class DataRemoval(Exception):
    def __init__(self, key, value):
        message = f'The column header [{key}] was not recognised, the following data has been removed: {value}.'
        super(DataRemoval, self).__init__(message)
        self.key = key
        self.value = value


class SchemaRetrievalError(Exception):
    pass


class MaxRowExceededError(Exception):
    pass