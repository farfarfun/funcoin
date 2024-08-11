import pandas as pd
from darkbuild.tool.fastapi import add_api_routes, api_route
from funcoin.database.connect import RedisConnect
from funcoin.okex.client.const import market_api


class MarketTickers(RedisConnect):
    def __init__(self, prefix='/market', cache_prefix='okex_market_tickets', *args, **kwargs):
        super(MarketTickers, self).__init__(prefix=prefix, cache_prefix=cache_prefix, *args, **kwargs)
        add_api_routes(self)

    @api_route('/update', description="update market tickers")
    def update_value(self, suffix=""):
        data = pd.DataFrame(market_api.get_tickers(instType='SPOT').data)
        self.put_value(self.get_key(suffix=suffix), data)
        return {"success": len(data)}

    @api_route('/read', description="read market tickers")
    def get_value(self, suffix=""):
        return super(MarketTickers, self).get_value(suffix=suffix)
