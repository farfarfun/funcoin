from typing import Dict, Iterable, Optional, Union

from funcoin.okex.client.base import BaseClient
from funcoin.okex.consts import GET, POST
from funcoin.okex.types import *
from funcoin.okex.utils import enum_to_str


class Order(object):
    def __init__(self, inst_id: str, td_mode: Union[TdMode, str], ord_type: Union[OrderType, str],
                 sz: Union[float, int, str], ccy: Optional[Union[CcyType, str]] = None,
                 cl_ord_id: Optional[str] = None, tag: Optional[str] = None,
                 pos_side: Optional[Union[PosSide, str]] = None,
                 reduce_only: Optional[Union[str, bool]] = None,
                 tgt_ccy: Optional[Union[TrgCCY, str]] = None) -> None:
        super(Order, self).__init__()
        self.instId = inst_id
        self.tdMode = td_mode
        self.ordType = ord_type
        self.sz = sz
        self.ccy = ccy
        self.clOrdId = cl_ord_id
        self.tag = tag
        self.posSide = pos_side
        self.reduceOnly = reduce_only
        self.tgtCcy = tgt_ccy


class CancelOrder(object):
    def __init__(self, inst_id: str, ord_id: Optional[str] = None, cl_ord_id: Optional[str] = None) -> None:
        super(CancelOrder, self).__init__()
        self.instId = inst_id
        self.ordId = ord_id
        self.clOrdId = cl_ord_id


class TradeClient(BaseClient):

    def __init__(self, *args, **kwargs):
        super(TradeClient, self).__init__(*args, **kwargs)

    def order(self, inst_id: str, td_mode: Union[TdMode, str], ord_type: Union[OrderType, str],
              sz: Union[float, int, str], ccy: Optional[Union[CcyType, str]] = None, cl_ord_id: Optional[str] = None,
              tag: Optional[str] = None, pos_side: Optional[Union[PosSide, str]] = None,
              px: Optional[Union[float, int, str]] = None, reduce_only: Optional[Union[str, bool]] = None) -> Dict:
        uri = '/api/v5/trade/order'
        params = {}
        if inst_id is not None:
            params['instId'] = str(inst_id)
        if td_mode is not None:
            params['tdMode'] = enum_to_str(td_mode)
        if ord_type is not None:
            params['ordType'] = enum_to_str(ord_type)
        if sz is not None:
            params['sz'] = str(abs(sz))
            if sz >= 0:
                params['side'] = 'buy'
            else:
                params['side'] = 'sell'
        if ccy is not None:
            params['ccy'] = enum_to_str(ccy)
        if cl_ord_id is not None:
            params['clOrdId'] = str(cl_ord_id)
        if tag is not None:
            params['tag'] = str(tag)
        if pos_side is not None:
            params['posSide'] = enum_to_str(pos_side)
        if px is not None:
            params['px'] = str(px)
        if reduce_only is not None:
            if isinstance(reduce_only, bool):
                if reduce_only:
                    params['reduceOnly'] = 'true'
                else:
                    params['reduceOnly'] = 'false'
            else:
                params['reduceOnly'] = str(reduce_only)
        data = self._request_with_params(POST, uri, params)["data"]

        return data

    def batch_orders(self, orders: Union[Order, Iterable[Order]]) -> Dict:
        uri = '/api/v5/trade/batch-orders'

        orders_list = []
        if isinstance(orders, Order):
            orders_list.append(orders)
        else:
            orders_list = orders
        params = []

        for order in orders_list:
            param = {}
            if order.instId is not None:
                param['instId'] = str(order.instId)
            if order.tdMode is not None:
                param['tdMode'] = enum_to_str(order.tdMode)
            if order.ordType is not None:
                param['ordType'] = enum_to_str(order.ordType)
            if order.sz is not None:
                param['sz'] = str(abs(order.sz))
                if order.sz >= 0:
                    param['side'] = 'buy'
                else:
                    param['side'] = 'sell'
            if order.ccy is not None:
                param['ccy'] = enum_to_str(order.ccy)
            if order.clOrdId is not None:
                param['clOrdId'] = str(order.clOrdId)
            if order.tag is not None:
                param['tag'] = str(order.tag)
            if order.posSide is not None:
                param['posSide'] = enum_to_str(order.posSide)

            if order.reduceOnly is not None:
                if isinstance(order.reduceOnly, bool):
                    if order.reduceOnly:
                        param['reduceOnly'] = 'true'
                    else:
                        param['reduceOnly'] = 'false'
                else:
                    param['reduceOnly'] = str(order.reduceOnly)
            if order.tgtCcy is not None:
                param['tgtCcy'] = enum_to_str(order.tgtCcy)
            params.append(param)

        data = self._request_with_params(POST, uri, params)["data"]

        return data

    def cancel_order(self, instId, ordId=None, clOrdId=None):
        uri = '/api/v5/trade/cancel-order'
        params = {'instId': instId, 'ordId': ordId, 'clOrdId': clOrdId}
        return self._request_with_params(POST, uri, params)

    def cancel_batch_orders(self, orders: Union[CancelOrder, Iterable[CancelOrder]]):
        uri = '/api/v5/trade/cancel-batch-orders'
        orders_list = []
        if isinstance(orders, Order):
            orders_list.append(orders)
        else:
            orders_list = orders
        params = []

        for order in orders_list:
            param = dict()
            if order.instId is not None:
                param["instId"] = str(order.instId)
            if order.ordId is not None:
                param["ordId"] = str(order.ordId)
            if order.clOrdId is not None:
                param["clOrdId"] = str(order.clOrdId)
            params.append(param)

        data = self._request_with_params(POST, uri, params)["data"]
        return data

    def get_order(self, inst_id: str, ord_id: Optional[str] = None, cl_ord_id: Optional[str] = None) -> Dict:
        uri = '/api/v5/trade/order'
        params = {}
        if inst_id is not None:
            params['instId'] = str(inst_id)
        if ord_id is not None:
            params['ordId'] = str(ord_id)
        if cl_ord_id is not None:
            params['clOrdId'] = str(cl_ord_id)

        data = self._request_with_params(GET, uri, params)["data"]

        return data

    def place_order(self, instId, tdMode, side, ordType, sz, ccy=None, clOrdId=None, tag=None, posSide=None, px=None,
                    reduceOnly=None):
        uri = '/api/v5/trade/order'
        params = {'instId': instId, 'tdMode': tdMode, 'side': side,
                  'ordType': ordType, 'sz': sz, 'ccy': ccy,
                  'clOrdId': clOrdId, 'tag': tag, 'posSide': posSide, 'px': px, 'reduceOnly': reduceOnly}
        return self._request_with_params(POST, uri, params)

    def place_multiple_orders(self, orders_data):
        uri = '/api/v5/trade/batch-orders'
        return self._request_with_params(POST, uri, orders_data)

    def cancel_multiple_orders(self, orders_data):
        uri = '/api/v5/trade/cancel-batch-orders'
        return self._request_with_params(POST, uri, orders_data)

    def amend_order(self, instId, cxlOnFail=None, ordId=None, clOrdId=None, reqId=None, newSz=None, newPx=None):
        uri = '/api/v5/trade/amend-order'
        params = {'instId': instId, 'cxlOnFailc': cxlOnFail, 'ordId': ordId, 'clOrdId': clOrdId, 'reqId': reqId,
                  'newSz': newSz, 'newPx': newPx}

        return self._request_with_params(POST, uri, params)

    def amend_multiple_orders(self, orders_data):
        uri = '/api/v5/trade/amend-batch-orders'
        return self._request_with_params(POST, uri, orders_data)

    def close_positions(self, instId, mgnMode, posSide=None, ccy=None):
        uri = '/api/v5/trade/close-position'
        params = {'instId': instId, 'mgnMode': mgnMode, 'posSide': posSide, 'ccy': ccy}
        return self._request_with_params(POST, uri, params)

    def get_orders(self, instId, ordId=None, clOrdId=None):
        uri = '/api/v5/trade/order'
        params = {'instId': instId, 'ordId': ordId, 'clOrdId': clOrdId}
        return self._request_with_params(GET, uri, params)

    def get_order_list(self, instType=None, uly=None, instId=None, ordType=None, state=None, after=None, before=None,
                       limit=None):
        uri = '/api/v5/trade/orders-pending'
        params = {'instType': instType, 'uly': uly, 'instId': instId, 'ordType': ordType, 'state': state,
                  'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_orders_history(self, instType, uly=None, instId=None, ordType=None, state=None, after=None, before=None,
                           limit=None):
        uri = '/api/v5/trade/orders-history'
        params = {'instType': instType, 'uly': uly, 'instId': instId, 'ordType': ordType, 'state': state,
                  'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def orders_history_archive(self, instType, uly=None, instId=None, ordType=None, state=None, after=None, before=None,
                               limit=None):
        uri = '/api/v5/trade/orders-history-archive'
        params = {'instType': instType, 'uly': uly, 'instId': instId, 'ordType': ordType, 'state': state,
                  'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def get_fills(self, instType=None, uly=None, instId=None, ordId=None, after=None, before=None, limit=None):
        uri = '/api/v5/trade/fills'
        params = {'instType': instType, 'uly': uly, 'instId': instId, 'ordId': ordId, 'after': after, 'before': before,
                  'limit': limit}
        return self._request_with_params(GET, uri, params)

    def place_algo_order(self, instId, tdMode, side, ordType, sz, ccy=None, posSide=None, reduceOnly=None,
                         tpTriggerPx=None,
                         tpOrdPx=None, slTriggerPx=None, slOrdPx=None, triggerPx=None, orderPx=None):
        uri = '/api/v5/trade/order-algo'
        params = {'instId': instId, 'tdMode': tdMode, 'side': side, 'ordType': ordType, 'sz': sz, 'ccy': ccy,
                  'posSide': posSide, 'reduceOnly': reduceOnly, 'tpTriggerPx': tpTriggerPx, 'tpOrdPx': tpOrdPx,
                  'slTriggerPx': slTriggerPx, 'slOrdPx': slOrdPx, 'triggerPx': triggerPx, 'orderPx': orderPx}
        return self._request_with_params(POST, uri, params)

    def cancel_algo_order(self, params):
        uri = '/api/v5/trade/cancel-algos'
        return self._request_with_params(POST, uri, params)

    def order_algos_list(self, ordType, algoId=None, instType=None, instId=None, after=None, before=None, limit=None):
        uri = '/api/v5/trade/orders-algo-pending'
        params = {'ordType': ordType, 'algoId': algoId, 'instType': instType, 'instId': instId, 'after': after,
                  'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)

    def order_algos_history(self, ordType, state=None, algoId=None, instType=None, instId=None, after=None, before=None,
                            limit=None):
        uri = '/api/v5/trade/orders-algo-history'
        params = {'ordType': ordType, 'state': state, 'algoId': algoId, 'instType': instType, 'instId': instId,
                  'after': after, 'before': before, 'limit': limit}
        return self._request_with_params(GET, uri, params)
