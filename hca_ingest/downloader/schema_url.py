from dataclasses import dataclass, field


@dataclass(unsafe_hash=True)
class SchemaUrl:
    url: str = field(default='', hash=True)

    @property
    def concrete_type(self):
        return self.url.split('/')[-1] if self.url else ''

    @property
    def domain_type(self):
        return self.url.split('/')[4] if self.url else ''
