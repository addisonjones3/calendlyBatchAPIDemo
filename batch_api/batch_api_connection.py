import configparser
import os

# Default rate limits
request_rate_limits = {
    'prod': 100,
    'beta': 10,
}


def get_credential_values(section_name):
    credentials_filename = os.path.expanduser('~') + '/path/to/credentials.cfg'
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(credentials_filename)
    auth_credentials = {}

    if parser.has_section(section_name):
        items = parser.items(section_name)
        for item in items:
            auth_credentials[item[0]] = item[1]
    else:
        raise Exception(f"{section_name} not found in {credentials_filename}")

    return auth_credentials

# TODO: Move Custio Connection members here where possible
class BatchAPIConnection:
    def __init__(self, credential_section):
        self.credential_values = get_credential_values(credential_section)

    def get_auth_values(self, section_name):
        return
