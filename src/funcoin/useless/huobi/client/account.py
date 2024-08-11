import asyncio
import json

import aiohttp
from funcoin.huobi.connection import (RestApiSyncClient, SubscribeClient,
                                       WebSocketReqClient)
from funcoin.huobi.constant import (AccountBalanceMode, AccountType,
                                     HttpMethod, TransferFuturesPro,
                                     TransferMasterType,
                                     get_default_server_url)
from funcoin.huobi.utils import get_current_timestamp
from funcoin.huobi.utils.input_checker import (check_currency, check_in_list,
                                                check_should_not_none)


class AccountClient(object):

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

    def get_accounts(self):
        """
        Get the account list.
        :return: The list of accounts data.
        """
        channel = "/v1/account/accounts"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, {})

    def get_balance(self, account_id: int):
        """
        Get the account list.
        :return: The list of accounts data.
        """
        check_should_not_none(account_id, "account-id")
        params = {
            "account-id": account_id
        }

        def get_channel():
            path = "/v1/account/accounts/{}/balance"
            return path.format(account_id)

        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, get_channel(), params)

    def get_account_by_type_and_symbol(self, account_type, symbol):
        accounts = self.get_accounts()
        if accounts and len(accounts):
            for account_obj in accounts:
                if account_obj.type == account_type:
                    if account_type == AccountType.MARGIN:
                        if symbol == account_obj.subtype:
                            return account_obj
                    else:
                        return account_obj

        return None

    @staticmethod
    async def async_get_account_balance(balance_full_url, account_id, ret_map):
        async with aiohttp.ClientSession() as session:
            async with session.get(balance_full_url) as resp:
                _json = await resp.json()
                ret_map[account_id] = _json
                return _json

    """
    (SDK encapsulated api) to easily use but not recommend for low performance and frequence limitation
    """

    def get_account_balance(self) -> list:
        """
        Get the balance of a all accounts.

        :return: The information of all account balance.
        """
        server_url = get_default_server_url(self.__kwargs.get("url"))
        tasks = []
        account_obj_map = {}
        accounts = self.get_accounts()
        account_balance_list = []
        account_balance_json_map = {}
        for account_item in accounts:
            account_obj_map[account_item.id] = account_item
            params = {"account-id": account_item.id}

            def get_channel():
                path = "/v1/account/accounts/{}/balance"
                return path.format(account_item.id)

            balance_request = self.rest_api_sync_client.create_request(HttpMethod.GET_SIGN, get_channel(), params)

            balance_url = server_url + balance_request.url
            tasks.append(asyncio.ensure_future(
                self.async_get_account_balance(balance_url, account_item.id, account_balance_json_map)))

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(asyncio.wait(tasks))
        except Exception as ee:
            print(ee)
        finally:
            # loop.close()  #for thread safe, the event loop can't be closed
            pass

        for account_id, account_balance_json in account_balance_json_map.items():
            account_balance_list.append(account_balance_json.get("data", {}))

        del account_balance_json_map
        del tasks
        return account_balance_list

    def get_account_balance_by_subuid(self, sub_uid):
        """
        Get account balance of a sub-account.

        :param sub_uid: the specified sub account id to get balance for.
        :return: the balance of a sub-account specified by sub-account uid.
        """
        check_should_not_none(sub_uid, "sub-uid")
        params = {
            "sub-uid": sub_uid
        }

        def get_channel():
            path = "/v1/account/accounts/{}"
            return path.format(sub_uid)

        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, get_channel(), params)

    def get_aggregated_subuser_balance(self):
        """
        Get the aggregated balance of all sub-accounts of the current user.

        :return: The balance of all the sub-account aggregated.
        """
        params = {}
        channel = "/v1/subuser/aggregate-balance"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def transfer_between_parent_and_subuser(self, sub_uid: int, currency: str, amount: float,
                                            transfer_type: TransferMasterType):
        """
        Transfer Asset between Parent and Sub Account.

        :param sub_uid: The target sub account uid to transfer to or from. (mandatory)
        :param currency: The crypto currency to transfer. (mandatory)
        :param amount: The amount of asset to transfer. (mandatory)
        :param transfer_type: The type of transfer, see {@link TransferMasterType} (mandatory)
        :return: The order id.
        """
        check_currency(currency)
        check_should_not_none(sub_uid, "sub-uid")
        check_should_not_none(amount, "amount")
        check_should_not_none(transfer_type, "type")

        params = {
            "sub-uid": sub_uid,
            "currency": currency,
            "amount": amount,
            "type": transfer_type
        }
        channel = "/v1/subuser/transfer"

        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def sub_account_update(self, mode: AccountBalanceMode, callback, error_handler=None):
        """
        Subscribe accounts update

        :param mode: subscribe mode
                "0" : for balance
                "1" : for available and balance
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(price_depth_event: 'PriceDepthEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass

        :return:  No return
        """

        check_should_not_none(callback, "callback")
        if str(mode) == AccountBalanceMode.TOTAL:
            mode = AccountBalanceMode.TOTAL
        else:
            mode = AccountBalanceMode.BALANCE

        def accounts_update_channel(_mode=0):
            channel = dict()
            channel["action"] = "sub"
            if _mode is None:
                channel["ch"] = "accounts.update"
            else:
                channel["ch"] = "accounts.update#{mode}".format(mode=_mode)
            return json.dumps(channel)

        def subscription(connection):
            connection.send(accounts_update_channel(mode))

        self.sub_socket_req_client.execute_subscribe_v2(subscription, callback, error_handler, is_trade=True)

    def req_account_balance(self, callback, client_req_id=None, error_handler=None):
        """
        Subscribe account changing event. If the balance is updated,
        server will send the data to client and onReceive in callback will be called.

        :param client_req_id: client request ID
        :param callback: The implementation is required. onReceive will be called if receive server's update.
            example: def callback(account_event: 'AccountEvent'):
                        pass
        :param error_handler: The error handler will be called if subscription failed or error happen
                              between client and Huobi server
            example: def error_handler(exception: 'HuobiApiException')
                        pass
        :return:  No return
        """

        check_should_not_none(callback, "callback")

        def request_account_list_channel(_client_req_id=None):
            channel = dict()
            channel["op"] = "req"
            channel["topic"] = "accounts.list"
            channel["cid"] = str(_client_req_id) if _client_req_id else str(get_current_timestamp())
            return json.dumps(channel)

        def subscription(connection):
            connection.send(request_account_list_channel(client_req_id))

        self.web_socket_req_client.execute_subscribe_v1(subscription, callback, error_handler, is_trade=True)

    def transfer_between_futures_and_pro(self, currency: str, amount: float,
                                         transfer_type: TransferFuturesPro) -> int:
        """
        Transfer Asset between Futures and Contract.

        :param currency: The crypto currency to transfer. (mandatory)
        :param amount: The amount of asset to transfer. (mandatory)
        :param transfer_type: The type of transfer, need be "futures-to-pro" or "pro-to-futures" (mandatory)
        :return: The order id.
        """

        check_currency(currency)
        check_should_not_none(currency, "currency")
        check_should_not_none(amount, "amount")
        check_should_not_none(transfer_type, "transfer_type")
        params = {
            "currency": currency,
            "amount": amount,
            "type": transfer_type
        }
        channel = "/v1/futures/transfer"

        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def get_account_history(self, account_id: int, currency: str = None,
                            transact_types: str = None, start_time: int = None, end_time: int = None,
                            sort: str = None, size: int = None):
        """
        get account change record
        :param account_id: account id (mandatory)
        :param currency: currency as "btc,eth" (optional)
        :param transact_types: see AccountTransactType, the value can be
                              "trade" (交易),"etf"（ETF申购）, "transact-fee"（交易手续费）,
                              "deduction"（手续费抵扣）, "transfer"（划转）, "credit"（借币）,
                              "liquidation"（清仓）, "interest"（币息）, "deposit"（充币），
                              "withdraw"（提币）, "withdraw-fee"（提币手续费）, "exchange"（兑换）,
                              "other-types"（其他） (optional)
        :param start_time
        :param end_time: for time range to search (optional)
        :param sort: see SortDesc, "asc" or "desc" (optional)
        :param size: page size (optional)

        :return: account change record list.
        """
        check_should_not_none(account_id, "account-id")
        params = {
            "account-id": account_id,
            "currency": currency,
            "transact-types": transact_types,
            "start-time": start_time,
            "end-time": end_time,
            "sort": sort,
            "size": size
        }
        channel = "/v1/account/history"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def post_sub_uid_management(self, sub_uid: int, action: str):
        """
        use to freeze or unfreeze the sub uid

        :return: user and status.
        """

        check_should_not_none(sub_uid, "subUid")
        check_should_not_none(action, "action")

        params = {
            "subUid": sub_uid,
            "action": action
        }
        channel = "/v2/sub-user/management"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def get_account_ledger(self, account_id: int, currency: str = None, transact_types: str = None,
                           start_time: int = None, end_time: int = None, sort: str = None, limit: int = None,
                           from_id: int = None) -> list:
        """
        get account ledger
        :param from_id:  from_id:
        :param account_id: account id (mandatory)
        :param currency: currency as "btc,eth" (optional)
        :param transact_types: see AccountTransactType, the value can be "trade" (交易),"etf"（ETF申购）,
                                "transact-fee"（交易手续费）, "deduction"（手续费抵扣）, "transfer"（划转）,
                                "credit"（借币）, "liquidation"（清仓）, "interest"（币息）,
                                "deposit"（充币），"withdraw"（提币）, "withdraw-fee"（提币手续费）,
                                "exchange"（兑换）, "other-types"（其他） (optional)
        :param start_time
        :param end_time: for time range to search (optional)
        :param sort: see SortDesc, "asc" or "desc" (optional)
        :param limit: page size (optional)
        :return: account ledger list.
        """

        check_should_not_none(account_id, "accountId")

        params = {
            "accountId": account_id,
            "currency": currency,
            "transactTypes": transact_types,
            "startTime": start_time,
            "endTime": end_time,
            "sort": sort,
            "limit": limit,
            "fromId": from_id
        }
        channel = "/v2/account/ledger"
        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def post_account_transfer(self, from_user: int, from_account_type: str, from_account: int, to_user: int,
                              to_account_type: str, to_account: int, currency: str, amount: str):
        check_should_not_none(from_user, "from-user")
        check_should_not_none(from_account_type, "from-account-type")
        check_should_not_none(from_account, "from_account")
        check_should_not_none(to_user, "to-user")
        check_should_not_none(to_account, "to-account")
        check_should_not_none(to_account_type, "to-account")
        check_should_not_none(currency, "currency")

        check_in_list(from_account_type, [AccountType.SPOT], "from_account_type")
        check_in_list(to_account_type, [AccountType.SPOT], "to_account_type")

        params = {
            "from-user": from_user,
            "from-account-type": from_account_type,
            "from-account": from_account,
            "to-user": to_user,
            "to-account-type": to_account_type,
            "to-account": to_account,
            "currency": currency,
            "amount": amount
        }
        channel = "/v1/account/transfer"
        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)

    def get_account_asset_valuation(self, account_type, valuation_currency: str = None, sub_uid: str = None):
        check_should_not_none(account_type, "account-type")

        params = {
            "accountType": account_type,
            "valuationCurrency": valuation_currency.upper(),
            "subUid": sub_uid
        }
        channel = "/v2/account/asset-valuation"

        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def get_account_point(self, sub_uid: str = None):
        params = {
            "subUid": sub_uid
        }
        channel = "/v2/point/account"

        return self.rest_api_sync_client.request_process(HttpMethod.GET_SIGN, channel, params)

    def post_point_transfer(self, from_uid: str, to_uid: str, group_id: str, amount: str):
        channel = "/v2/point/transfer"
        params = {
            "fromUid": from_uid,
            "toUid": to_uid,
            "groupId": group_id,
            "amount": amount
        }

        return self.rest_api_sync_client.request_process(HttpMethod.POST_SIGN, channel, params)
