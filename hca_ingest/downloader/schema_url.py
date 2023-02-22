from dataclasses import dataclass, field
ACCEPTED_DOMAINS = ['biomaterial', 'protocol', 'process', 'file', 'project']

@dataclass(unsafe_hash=True)
class SchemaUrl:
    url: str = field(default='', hash=True)

    @property
    def concrete_type(self):
        return self.url.split('/')[-1] if self.url else ''

    @property
    def domain_type(self):
        try:
            domain = ACCEPTED_DOMAINS[[domain in self.url for domain in ACCEPTED_DOMAINS].index(True)]
            return domain
        except ValueError:
            return ''
