import requests
from ruamel.yaml import YAML
from io import StringIO
from configparser import RawConfigParser

def get_config_as_str(path):
        if path.startswith('file://'):
            with open(path[7:], 'r') as f:
                return f.read()
        elif path.startswith('http://') or path.startswith('https://'):
            r = requests.get(path)
            if r.status_code != requests.codes.ok:
                raise Exception('Configure file "{}" fetch failed, statuscode: {}, message: {}'.format(path, r.status_code, r.content))
            return r.text
        else:
            raise Exception('Configure file protocal not supported: {}'.format(path))


class Configuration(object):
    def __init__(self, path):
        self.path = path
        self.config = self._parse(path)

    def _parse(self, path):
        config = RawConfigParser()
        content = get_config_as_str(path)

        if path.endswith('.ini'):
            config.read_string(content)
            return config
        elif path.endswith('.properties'):
            config.read_string('[properties]\n' + content)
            return config['properties']
        elif path.endswith('.yaml'):
            buffer = StringIO(content)
            return YAML(typ='safe').load(buffer)
        else:
            raise Exception('Configure file type not supported: {}'.format(path))

    def __getitem__(self, key):
        return self.config[key]

    def get(self, key, default=None):
        if key in self.config:
            return self.config[key]
        else:
            return default
