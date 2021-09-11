import requests
import configparser
import os
import logging
import asyncio
import aiohttp


class BatchAPIMethods:
    GET = 'GET'
    PUT = 'PUT'
    POST = 'POST'


class BatchAPIAction:

    def __init__(self, url=None, headers=None, identifier_values=None, json=None, method=None, **kwargs):
        self.method = method
        self.json = json
        self.request_url = url.format(*identifier_values) if identifier_values else url
        self.headers = headers

    def send(self):
        resp = requests.request(self.method, self.request_url, headers=self.headers, json=self.json)
        logging.info(f'{resp.status_code} {self.method} {self.request_url}: {self.json}')
        return resp

    async def send_batch(self, session, verbose_logging=False):
        try:
            async with await session.request(
                    method=self.method,
                    url=self.request_url,
                    json=self.json,
                    headers=self.headers,
            ) as resp:

                # print(f'{resp.status} {self.method} {self.request_url}: {self.json}')
                return resp

        except Exception as e:
            print(e)
            raise e
