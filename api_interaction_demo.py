from batch_api.custio_api_actions import ModifyCustomer
from batch_api.custio_api_connection import CustIOAPIConnection, CustIOWorkspaces

import uuid
import random
import string
import time
import datetime

'''

Example comparing batch vs. serial performance for Customer IO 
batch API framework.

Set num_requests_to_send to int of how many random profiles to create.

Script sends the requests to the /customers endpoint creating/updating
profiles. Passing an id to the endpoint with PUT will create or update
an existing profile. 

In this example, the batch section is creating the profiles and the serial
is updating them, but those sections can be reversed if one wants
to ensure the result is the same.

'''


def generate_random_test_user(name):
    return {'id': str(uuid.uuid4()),
            'email': f'{name}@example.com',
            'name': name,
            'is_employee': random.choice([True, False])
            }


def send_batch_requests(connection, users):
    print(f'Sending {len(users)} requests in batch')
    print('')
    batch_start_time = time.time()

    connection.send_requests(
        api_action_class=ModifyCustomer,
        request_dict_list=reverse_sorted_users,
        uri_parameter_names=['id'],
        uri_param_list_remove_from_json=['id'],
        use_batch_send=True
    )

    batch_end_time = time.time()
    print('')
    print(f'Batch send took {round(batch_end_time - batch_start_time, 2)} seconds')


def send_serial_requests(connection, users):
    print(f'Sending {len(users)} requests in serial')
    print('')
    serial_start_time = time.time()

    connection.send_requests(
        api_action_class=ModifyCustomer,
        request_dict_list=reverse_sorted_users,
        uri_parameter_names=['id'],
        uri_param_list_remove_from_json=['id'],
        use_batch_send=False
    )

    serial_end_time = time.time()
    print('')
    print(f'Serial send took {round(serial_end_time - serial_start_time, 2)} seconds')
    print('')


if __name__ == '__main__':
    print(f'Beginning example script at {datetime.datetime.now()}')
    print('')
    num_requests_to_send = 1000

    rand_users = list(map(generate_random_test_user,
                          [f'{random.choice(string.ascii_uppercase)}{"".join(random.choices(string.ascii_lowercase, k=8))}'
                           for i in range(0, num_requests_to_send)]))

    reverse_sorted_users = sorted(rand_users, key=lambda x: x['id'], reverse=True)
    print(f'Created reverse sorted list of {len(reverse_sorted_users)} users')
    print('')

    custio_connection = CustIOAPIConnection(CustIOWorkspaces.demo_workspace)

    send_batch_requests(custio_connection, reverse_sorted_users)
    send_serial_requests(custio_connection, reverse_sorted_users)
