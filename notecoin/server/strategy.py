from fastapi import APIRouter
from notebuild.tool.fastapi import add_api_routes, api_route
from notecoin.strategy.binance import Strategy

path_root = '/home/bingtao/workspace/tmp'


class StrategyServer(APIRouter):
    def __init__(self, prefix='/strategy', *args, **kwargs):
        super(StrategyServer, self).__init__(prefix=prefix, *args, **kwargs)
        self.running = False
        add_api_routes(self)
        self.strategy = Strategy()

    @api_route('/refresh_account', description="get value")
    def refresh_account(self):
        self.strategy.refresh_markets()
        self.strategy.refresh_account()

    @api_route('/buy_market', description="get value")
    def buy_market(self, symbol):
        self.strategy.buy_market(symbol)

    @api_route('/auto_oco', description="get value")
    def auto_oco(self):
        if self.running:
            return {"status": "false", "msg": "is running"}
        self.running = True
        self.strategy.auto_oco()
        self.running = False
