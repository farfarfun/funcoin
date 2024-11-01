from funcoin.huobi.connection import (RestApiSyncClient, SubscribeClient,
                                       WebSocketReqClient)
from funcoin.huobi.constant import (AccountType, HttpMethod,
                                     SubuserTradePrivilegeType,
                                     SubUserTradeStatus)
from funcoin.huobi.utils.input_checker import (check_in_list,
                                                check_should_not_none)


class SubuserClient(object):
    def __init__(self, *args, **kwargs):
        """
        Create the request client instance.
        :param kwargs: The option of request connection.
            api_key: The public key applied from Huobi.
            secret_key: The private key applied from Huobi.
            url: The URL name like "https://api.huobi.pro".
            init_log: Init logger, default is False, True will init logger handler
        """
        self.__kwargs = kwargs
        self.rest_api_sync_client = RestApiSyncClient(*args, **kwargs)
        self.web_socket_req_client = WebSocketReqClient(*args, **kwargs)
        self.sub_socket_req_client = SubscribeClient(*args, **kwargs)

    def post_create_subuser(self, user_list):
        check_should_not_none(user_list, 'userList')

        params = user_list
        channel = "/v2/sub-user/creation"

        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def post_set_tradable_market(self, sub_uids, account_type: 'SubuserTradePrivilegeType',
                                 activation: 'SubUserTradeStatus'):
        check_should_not_none(sub_uids, 'subUids')
        check_should_not_none(account_type, 'accountType')
        check_should_not_none(activation, 'activation')

        check_in_list(account_type,
                      [SubuserTradePrivilegeType.MARGIN, SubuserTradePrivilegeType.SUPER_MARGIN], "accountType")
        check_in_list(activation, [SubUserTradeStatus.ACTIVATED, SubUserTradeStatus.DEACTIVATED], "activation")

        params = {
            'subUids': sub_uids,
            'accountType': account_type,
            'activation': activation
        }
        channel = "/v2/sub-user/tradable-market"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def post_set_subuser_transferability(self, sub_uids: 'str', transferrable: 'bool',
                                         account_type: 'AccountType' = AccountType.SPOT):
        check_should_not_none(sub_uids, 'subUids')
        check_should_not_none(transferrable, 'transferrable')
        check_in_list(account_type, [AccountType.SPOT], 'accountType')

        params = {
            "subUids": sub_uids,
            "accountType": account_type,
            "transferrable": transferrable
        }
        channel = "/v2/sub-user/transferability"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def post_subuser_apikey_generate(self, otp_token: 'str', sub_uid: 'int', note: 'str', permission: 'bool',
                                     ip_addresses: 'str' = None):
        check_should_not_none(otp_token, 'otpToken')
        check_should_not_none(sub_uid, 'subUid')
        check_should_not_none(note, 'note')
        check_should_not_none(permission, 'permission')
        # check_in_list(permission, [AccountType.SPOT], 'accountType')

        params = {
            "otpToken": otp_token,
            "subUid": sub_uid,
            "note": note,
            "permission": permission,
            "ipAddresses": ip_addresses
        }
        channel = "/v2/sub-user/api-key-generation"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def get_user_apikey_info(self, uid: 'str', access_key: 'str' = None):
        check_should_not_none(uid, 'uid')

        params = {
            "uid": uid,
            "accessKey": access_key
        }
        channel = "/v2/user/api-key"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def post_subuser_apikey_modification(self, sub_uid: 'str', access_key: 'str', note: 'str' = None,
                                         permission: 'str' = None, ip_addresses: 'str' = None):
        check_should_not_none(sub_uid, 'subUid')
        check_should_not_none(access_key, 'accessKey')

        params = {
            "subUid": sub_uid,
            "accessKey": access_key,
            "note": note,
            "permission": permission,
            "ipAddresses": ip_addresses
        }
        channel = "/v2/sub-user/api-key-modification"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def post_subuser_apikey_deletion(self, sub_uid: 'str', access_key: 'str'):
        check_should_not_none(sub_uid, 'subUid')
        check_should_not_none(access_key, 'accessKey')

        params = {
            "subUid": sub_uid,
            "accessKey": access_key
        }
        channel = "/v2/sub-user/api-key-deletion"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def get_uid(self):
        params = {
        }
        channel = "/v2/user/uid"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)
