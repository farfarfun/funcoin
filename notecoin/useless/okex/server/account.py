from notebuild.tool.fastapi import add_api_routes, api_route
from notecoin.database.base import create_all, create_session
from notecoin.database.connect import RedisConnect
from notecoin.okex.client.const import account_api
from notecoin.okex.database.client import OkexClientAccountBalance


class AccountAccount(RedisConnect):
    def __init__(self, prefix="/account", cache_prefix='okex_account_balance', *args, **kwargs):
        super(AccountAccount, self).__init__(prefix=prefix, cache_prefix=cache_prefix, *args, **kwargs)
        add_api_routes(self)
        self.session = create_session()
        create_all()

    @api_route('/update', description="update market tickers")
    def update_value(self, suffix=""):
        try:
            data = account_api.get_account().data[0]['details']
            self.put_value(self.get_key(suffix=suffix), data)
            for detail in data:
                print(detail)
                create_all()
                self.session.merge(OkexClientAccountBalance(**detail))
            self.session.commit()
            create_all()
            return {"success": len(data)}
        except Exception as e:
            self.session.rollback()
            return {"failed": str(e)}

    def get_value(self, suffix=""):
        return super(AccountAccount, self).get_value(suffix=suffix)
