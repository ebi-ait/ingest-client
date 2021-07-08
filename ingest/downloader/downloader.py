EXCLUDE_KEYS = ['describedBy', 'schema_type']


class XlsDownloader:
    def convert_json(self, entity_list):
        workbook = {}

        for entity in entity_list:
            content = entity['content']
            concrete_type = self.get_concrete_entity(content)
            worksheet = workbook.get(concrete_type, [])
            workbook[concrete_type] = worksheet
            row = {f'{concrete_type}.uuid': entity['uuid']['uuid']}
            self.flatten(content, row, parent_key='project')
            worksheet.append(row)
        return workbook

    def flatten(self, content, output, parent_key=''):
        if isinstance(content, dict):
            for key in content:
                full_key = f'{parent_key}.{key}' if parent_key else key
                if key in EXCLUDE_KEYS:
                    continue
                value = content[key]

                if isinstance(value, dict) or isinstance(value, list):
                    self.flatten(value, output, parent_key=full_key)
                else:
                    output[full_key] = value
        elif isinstance(content, list):
            for elem in content:
                if isinstance(elem, dict):
                    self.flatten(elem, output, parent_key=parent_key)
                else:
                    stringified = [str(e) for e in content]
                    output[parent_key] = '||'.join(stringified)

    def get_concrete_entity(self, content):
        return content.get('describedBy').rsplit('/', 1)[-1]
