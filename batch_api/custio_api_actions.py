from datetime import datetime
from batch_api.batch_api_action import BatchAPIAction, BatchAPIMethods


class CustIOAPITypes:
    prod = 'prod'
    beta = 'beta'


class CustIOBaseURLs:
    track = 'https://track.customer.io/api/v1'
    beta = 'https://beta-api.customer.io/v1/api'


class CustIOAuthTypes:
    track = 'track'
    app = 'app'


def get_request_rate_limit(api_type):
    if api_type == CustIOAPITypes.prod:
        return 100

    elif api_type == CustIOAPITypes.beta:
        return 5


class ModifyCustomer(BatchAPIAction):
    api_type = CustIOAPITypes.prod
    auth_type = CustIOAuthTypes.track
    request_rate_limit = get_request_rate_limit(api_type)

    def __init__(self, **kwargs):
        super().__init__(url=f'{CustIOBaseURLs.track}/customers/' + '{0}',
                         method=BatchAPIMethods.PUT,
                         **kwargs
                         )
        self.json['ts'] = int(datetime.now().timestamp())


class CreateExport(BatchAPIAction):
    api_type = CustIOAPITypes.beta
    auth_type = CustIOAuthTypes.app
    request_rate_limit = get_request_rate_limit(api_type)

    def __init__(self, filters, **kwargs):
        """

        Parameters
        ----------
        filters - Required json dictionary containing filters for an export. Documentation found here: https://customer.io/docs/api/#operation/exportPeopleData
        Example:
        filters = {
            "filters": {
                "attribute": {
                    "field": "my_attribute_tag",
                    "operator": "exists"
                }
            }
        }
        """
        super().__init__(url=f'{CustIOBaseURLs.beta}/exports/customers',
                         method=BatchAPIMethods.POST,
                         json=filters,
                         headers=kwargs['headers'],
                         )


class GetExport(BatchAPIAction):
    api_type = CustIOAPITypes.beta
    auth_type = CustIOAuthTypes.app
    request_rate_limit = get_request_rate_limit(api_type)

    def __init__(self, **kwargs):
        super().__init__(url=f'{CustIOBaseURLs.beta}/exports/' + '{0}',
                         method=BatchAPIMethods.GET,
                         **kwargs
                         )


class GetExportDownloadUrl(BatchAPIAction):
    api_type = CustIOAPITypes.beta
    auth_type = CustIOAuthTypes.app
    request_rate_limit = get_request_rate_limit(api_type)

    def __init__(self, **kwargs):
        super().__init__(url=f'{CustIOBaseURLs.beta}/exports/' + '{0}' + '/download',
                         method=BatchAPIMethods.GET,
                         **kwargs
                         )
