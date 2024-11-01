import pickle
from typing import Any

import redis
import six
from fastapi import APIRouter
from darkbuild.tool.fastapi import api_route


class BaseConnect(APIRouter):
    def __init__(self, cache_prefix='cache', *args, **kwargs):
        self.cache_prefix = cache_prefix
        super(BaseConnect, self).__init__(*args, **kwargs)

    @api_route('/get', description="get value")
    def get_key(self, suffix=""):
        return f"{self.cache_prefix}_{suffix}"

    @api_route('/update', description="update value")
    def update_value(self, suffix=""):
        raise Exception("not implements")

    def get_value(self, suffix=""):
        raise Exception("not implements")

    def put_value(self, key, value: Any):
        raise Exception("not implements")


class SqlalchemyConnect(BaseConnect):
    def __init__(self, *args, **kwargs):
        super(SqlalchemyConnect, self).__init__(*args, **kwargs)
