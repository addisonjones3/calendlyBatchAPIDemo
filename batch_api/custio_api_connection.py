import base64
import asyncio
import aiohttp
import time
from math import ceil
from batch_api.batch_api_connection import BatchAPIConnection
from batch_api.custio_api_actions import CustIOAuthTypes


class CustIOWorkspaces:
    demo_workspace = 'calendly_demo_workspace'


class CustIOAPIConnection(BatchAPIConnection):

    def __init__(self, workspace_name):
        super().__init__(workspace_name)
        self.workspace_name = workspace_name
        self.connection_responses = []
        self.connection_errors = []

    def send_single_request(self, api_action_class, uri_parameter_names=None, uri_param_list_remove_from_json=None,
                            request_dict=None, **kwargs):

        headers = get_headers(self.credential_values, api_action_class)
        # print(headers)

        request = create_request(api_action_class,
                                 headers,
                                 request_dict=request_dict,
                                 uri_parameter_names=uri_parameter_names,
                                 uri_param_list_remove_from_json=uri_param_list_remove_from_json,
                                 **kwargs)

        response = request.send()

        self.connection_responses.append(response)

    def send_requests(self, api_action_class, request_dict_list=None, uri_parameter_names=None,
                      uri_param_list_remove_from_json=None, use_batch_send=False):
        """
        Parameters
        ----------
        :param api_action_class: Pass the class name of the action to be created
        :param request_dict_list: List of dictionaries representing the data to be sent
        :param uri_parameter_names: Names of dict key values to be passed into URI
        :param uri_param_list_remove_from_json: Names of dict members to be removed from dict after passed to URI
        :param use_batch_send: True/False flag to use aiohttp async send functionality
        :return: None
        """
        headers = get_headers(self.credential_values, api_action_class)
        # print(headers)

        chunk_count = ceil(len(request_dict_list) / api_action_class.request_rate_limit)

        for i, request_chunk in enumerate(_chunk_rate_limited_requests(request_dict_list, api_action_class.request_rate_limit)):
            print(f'Beginning chunk {i + 1} of {chunk_count}')
            request_list = []
            for request_dict in request_chunk:

                request = create_request(api_action_class,
                                         headers,
                                         uri_parameter_names=uri_parameter_names,
                                         uri_param_list_remove_from_json=uri_param_list_remove_from_json,
                                         request_dict=request_dict
                                         )

                request_list.append(request)

            begin_tick = time.time()
            end_tick = begin_tick + 1

            chunk_responses = []
            chunk_errors = []

            if use_batch_send:
                responses = asyncio.run(_send_batch_requests(request_list))
                chunk_responses.extend([res for res in responses])
                chunk_errors.extend([res for res in responses if res.status != 200])

            else:
                for req in request_list:
                    resp = req.send()
                    chunk_responses.append(resp)
                    if resp.status_code != 200:
                        chunk_errors.append(resp)

            self.connection_responses.extend(chunk_responses)
            self.connection_errors.extend(chunk_errors)

            finished_time = time.time()
            print(f'{len(chunk_responses)} requests complete in {round(finished_time - begin_tick, 4)}')

            # Sleep until beginning of next second
            if end_tick - finished_time > 0:
                print(f'Sleeping {round(end_tick - time.time(), 4)}')
                time.sleep(end_tick - time.time())


def get_headers(credential_values, api_action_class):
    return {
        "Authorization": _get_auth_string(credential_values, api_action_class),
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
    }


def create_request(api_action_class, headers, uri_parameter_names=None, uri_param_list_remove_from_json=None,
                   request_dict=None, **kwargs):
    try:
        uri_values = [request_dict[id_name] for id_name in uri_parameter_names] if uri_parameter_names else None

    except KeyError as ke:
        print(f'{ke} from uri_parameter_names not found in request_dict')
        raise ke

    # Pass dict removing keys from removal list
    try:
        if uri_param_list_remove_from_json:
            json_payload = {k: v for k, v in request_dict.items() if k not in uri_param_list_remove_from_json}
        else:
            json_payload = request_dict

    except KeyError as ke:
        print(f'{ke} from uri_param_list_remove_from_json not found in request_dict')
        raise ke

    request = api_action_class(
        headers=headers,
        uri_values=uri_values,
        json=json_payload if len(json_payload) > 0 else None,
        **kwargs
    )

    return request


def _get_auth_string(credential_values, api_action_class):
    if api_action_class.auth_type == CustIOAuthTypes.app:
        key_string = credential_values['app_api_key']
        return f'Bearer {key_string}'

    elif api_action_class.auth_type == CustIOAuthTypes.track:
        key_string = base64.b64encode(
            f"{credential_values['track_site_id']}:{credential_values['track_api_key']}".encode('ascii')).decode('ascii')
        return f'Basic {key_string}'


def _chunk_rate_limited_requests(json, rate_limit=100):
    for i in range(0, len(json), rate_limit):
        yield json[i:i + rate_limit]


async def _send_batch_requests(batch_requests):
    async with aiohttp.ClientSession() as session:
        responses = await asyncio.gather(*[batch_request.send_batch(session) for batch_request in batch_requests])
        return responses
