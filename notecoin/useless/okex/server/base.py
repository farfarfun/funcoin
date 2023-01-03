from fastapi import APIRouter
from notebuild.tool.fastapi import add_api_routes, api_route
from notecoin.okex.database.client import OkexClientAccountBalance


class BaseAPI(APIRouter):
    def __init__(self, prefix='/base', *args, **kwargs):
        super(BaseAPI, self).__init__(prefix=prefix, *args, **kwargs)
        add_api_routes(self)

    @api_route('/account/balance/update', description="AccountBalance")
    def account_balance_update(self):
        return OkexClientAccountBalance.update()
