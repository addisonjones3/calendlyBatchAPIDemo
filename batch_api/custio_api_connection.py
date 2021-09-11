import base64
import asyncio
import aiohttp
import time
import logging
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
        print(headers)

        request = create_request(api_action_class,
                                 headers,
                                 uri_parameter_names=uri_parameter_names,
                                 uri_param_list_remove_from_json=uri_param_list_remove_from_json,
                                 request_dict=request_dict,
                                 **kwargs)

        response = request.send()

        self.connection_responses.append(response)

    def send_requests(self, api_action_class, uri_parameter_names=None, uri_param_list_remove_from_json=None,
                      request_dict_list=None, use_batch_send=False):
        headers = get_headers(self.credential_values, api_action_class)
        print(headers)

        for i, request_chunk in enumerate(_chunk_rate_limited_requests(request_dict_list, api_action_class.request_rate_limit)):
            print(f'Beginning chunk {i + 1} of {int(len(request_dict_list) / api_action_class.request_rate_limit)}')
            request_list = []
            for request in request_chunk:

                request = create_request(api_action_class,
                                         headers,
                                         uri_parameter_names=uri_parameter_names,
                                         uri_param_list_remove_from_json=uri_param_list_remove_from_json,
                                         request_dict=request
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
            print(f'{len(responses)} requests complete in {round(finished_time - begin_tick, 4)}')

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

    identifier_values = [request_dict.get(id_name, None) for id_name in uri_parameter_names] if uri_parameter_names else None

    # Remove member of dict if not wanted in json payload
    if uri_param_list_remove_from_json:
        for param in uri_param_list_remove_from_json:
            request_dict.pop(param)

    if request_dict:
        json_payload = None if len(request_dict) == 0 else request_dict

    request = api_action_class(
        headers=headers,
        identifier_values=identifier_values,
        json=json_payload,
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
