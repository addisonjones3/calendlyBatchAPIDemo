from batch_api.custio_api_actions import ModifyCustomer
from batch_api.custio_api_connection import CustIOAPIConnection, CustIOWorkspaces

import uuid
import random
import string


def generate_random_test_user(name):
    return {'id': str(uuid.uuid4()),
            'email': f'{name}@example.com',
            'name': name,
            'is_employee': random.choice([True, False])
            }


rand_users = list(map(generate_random_test_user,
                      [f'{random.choice(string.ascii_uppercase)}{"".join(random.choices(string.ascii_lowercase, k=8))}'
                       for i in range(0, 100)]))

reverse_sorted_users = sorted(rand_users, key=lambda x: x['id'], reverse=True)

custio_connection = CustIOAPIConnection(CustIOWorkspaces.demo_workspace)

custio_connection.send_requests(
    api_action_class=ModifyCustomer,
    uri_parameter_names=['id'],
    uri_param_list_remove_from_json=['id'],
    request_dict_list=reverse_sorted_users,
    use_batch_send=True
)
