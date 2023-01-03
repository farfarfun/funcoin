from notecoin.okex.client.base import BaseClient
from notecoin.okex.consts import GET, POST


class SubAccountClient(BaseClient):
    def __init__(self, *args, **kwargs):
        super(SubAccountClient, self).__init__(*args, **kwargs)

    def balances(self, subAcct):
        uri = '/api/v5/account/subaccount/balances'
        params = {"subAcct": subAcct}
        return self._request_with_params(GET, uri, params)

    def bills(self, ccy=None, type=None, subAcct=None, after=None, before=None, limit=None):
        uri = '/api/v5/asset/subaccount/bills'
        params = {"ccy": ccy, 'type': type, 'subAcct': subAcct, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def delete(self, pwd, subAcct, apiKey):
        uri = '/api/v5/users/subaccount/delete-apikey'
        params = {'pwd': pwd, 'subAcct': subAcct, 'apiKey': apiKey}
        return self._request_with_params(POST, uri, params)

    def reset(self, pwd, subAcct, label, apiKey, perm, ip=None):
        uri = '/api/v5/users/subaccount/modify-apikey'
        params = {'pwd': pwd, 'subAcct': subAcct, 'label': label, 'apiKey': apiKey, 'perm': perm, 'ip': ip}
        return self._request_with_params(POST, uri, params)

    def create(self, pwd, subAcct, label, Passphrase, perm=None, ip=None):
        uri = '/api/v5/users/subaccount/apikey'
        params = {'pwd': pwd, 'subAcct': subAcct, 'label': label, 'Passphrase': Passphrase, 'perm': perm, 'ip': ip}
        return self._request_with_params(POST, uri, params)

    def view_list(self, enable=None, subAcct=None, after=None, before=None, limit=None):
        uri = '/api/v5/users/subaccount/list'
        params = {'enable': enable, 'subAcct': subAcct, 'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def control_transfer(self, ccy, amt, froms, to, fromSubAccount, toSubAccount):
        uri = '/api/v5/asset/subaccount/transfer'
        params = {'ccy': ccy, 'amt': amt, 'from': froms, 'to': to, 'fromSubAccount': fromSubAccount,
                  'toSubAccount': toSubAccount}
        return self._request_with_params(POST, uri, params)
