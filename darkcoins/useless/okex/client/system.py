from typing import Optional

from darkcoins.okex.client.base import BaseClient
from darkcoins.okex.consts import GET


class SystemClient(BaseClient):

    def __init__(self, *args, **kwargs):
        super(SystemClient, self).__init__(*args, **kwargs)

    def status(self, state: Optional[str] = None):
        uri = '/api/v5/system/status'
        params = {'state': state}
        data = self._request_with_params(GET, uri, params)
        return data

