from .account import AccountAccount
from .base import BaseAPI
from .market import MarketTickers
from .websocket_service import WebsocketService

market_tickers = MarketTickers()
account_account = AccountAccount()
base_api = BaseAPI()
websocket_api = WebsocketService()
