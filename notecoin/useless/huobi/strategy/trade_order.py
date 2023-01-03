from notecoin.huobi.client import (AccountClient, GenericClient, MarketClient,
                                   TradeClient)
from notecoin.huobi.constant import OrderType
from notetool.tool.secret import read_secret


class TradeOrder:
    def __init__(self):
        self.trade: TradeClient = None
        self.market: MarketClient = None
        self.generic: GenericClient = None
        self.account: AccountClient = None
        self.account_id = None

        self.init()

    def init(self):
        api_key = read_secret(cate1='coin', cate2='huobi', cate3='api_key')
        secret_key = read_secret(cate1='coin', cate2='huobi', cate3='secret_key')
        self.market = MarketClient(api_key=api_key, secret_key=secret_key)
        self.generic = GenericClient(api_key=api_key, secret_key=secret_key)
        self.trade = TradeClient(api_key=api_key, secret_key=secret_key)

        self.account = AccountClient(api_key=api_key, secret_key=secret_key)

        account_dict = self.account.get_accounts()
        # print(account_dict.dict_data['data'][0]['id'])
        self.account_id = account_dict.dict_data['data'][0]['id']

    def buy(self, symbol, amount):
        source = 'spot-api'
        return self.trade.create_order(symbol=symbol,
                                       account_id=self.account_id,
                                       order_type=OrderType.BUY_MARKET,
                                       amount=amount,
                                       source=source,
                                       price=0)


