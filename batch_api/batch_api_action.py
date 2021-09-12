import requests


class BatchAPIMethods:
    GET = 'GET'
    PUT = 'PUT'
    POST = 'POST'


class BatchAPIAction:

    def __init__(self, url=None, headers=None, uri_values=None, json=None, method=None, **kwargs):
        self.method = method
        self.json = json
        self.request_url = url.format(*uri_values) if uri_values else url
        self.headers = headers

    def send(self):
        try:
            resp = requests.request(self.method, self.request_url, headers=self.headers, json=self.json)
            # print(f'{resp.status_code} {self.method} {self.request_url}: {self.json}')
            # Raise 401 immediately
            if resp.status_code == 401:
                raise Exception('401 status returned. Check credentials')

        except Exception as e:
            raise e

        return resp

    async def send_batch(self, session):
        try:
            async with await session.request(
                    method=self.method,
                    url=self.request_url,
                    json=self.json,
                    headers=self.headers,
            ) as resp:

                # Raise 401 immediately
                if resp.status == 401:
                    raise Exception('401 status returned. Check credentials')
                # print(f'{resp.status} {self.method} {self.request_url}: {self.json}')
                return resp

        except Exception as e:
            raise e
