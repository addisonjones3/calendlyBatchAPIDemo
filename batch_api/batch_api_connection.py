import configparser
import os

# Default rate limits
request_rate_limits = {
    'prod': 100,
    'beta': 10,
}


def get_credential_values(section_name):
    credentials_filename = os.path.expanduser('~') + '/.custio_creds/credentials.cfg'
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

# TODO: Figure out super
class BatchAPIConnection:
    def __init__(self, credential_section):
        self.credential_values = get_credential_values(credential_section)
        # Optional override of request rate limit in config file. Endpoint type required
        # self.request_rate_limit = endpoint_dict.get('request_rate_limit', request_rate_limits[endpoint_dict['endpoint_type']])
        # self.credential_values = get_config(self.endpoint_config_name)
        # self.auth_type = api_action_class.auth_type
        # self.auth_key = self.get_auth_key(self.credential_values, self.auth_type)

    def get_auth_values(self, section_name):
        return
