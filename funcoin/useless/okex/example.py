from funcoin.okex.client import (
    AccountClient,
    AssetClient,
    MarketClient,
    PublicClient,
    SubAccountClient,
    SystemClient,
    TradeClient,
)
from funtool.secret import read_secret

api_key = read_secret(cate1="coin", cate2="okex", cate3="api_key")
secret_key = read_secret(cate1="coin", cate2="okex", cate3="secret_key")
passphrase = read_secret(cate1="coin", cate2="okex", cate3="passphrase")

# flag是实盘与模拟盘的切换参数 flag is the key parameter which can help you to change between demo and real trading.
# flag = True  # 模拟盘 demo trading
flag = False  # 实盘 real trading
account = AccountClient(api_key, secret_key, passphrase, False, flag)
fundingAPI = AssetClient(api_key, secret_key, passphrase, False, flag)
market = MarketClient(api_key, secret_key, passphrase, False, flag)
publicAPI = PublicClient(api_key, secret_key, passphrase, False, flag)


def test_account():
    print("查看账户持仓风险", account.get_position_risk("SWAP").data_size())
    print("查看账户余额", account.get_account().data_size())
    print("查看持仓信息", account.get_positions("FUTURES", "BTC-USD-210402").data_size())
    # print('账单流水查询',account.get_bills_detail('FUTURES', 'BTC', 'cross').data_size())
    print("账单流水查询", account.get_bills_details("FUTURES", "BTC", "cross").data_size())
    print("查看账户配置", account.get_account_config().data_size())
    print("设置持仓模式", account.get_position_mode("long_short_mode").data_size())
    print("设置杠杆倍数", account.set_leverage(instId="BTC-USD-210402", lever="10", mgnMode="cross").data_size())
    # 获取最大可交易数量  Get Maximum Tradable Size For Instrument
    # result = account.get_maximum_trade_size('BTC-USDT-210402', 'cross', 'USDT')
    # 获取最大可用数量  Get Maximum Available Tradable Amount
    # result = account.get_max_avail_size('BTC-USDT-210402', 'isolated', 'BTC')
    # 调整保证金  Increase/Decrease margint
    # result = account.Adjustment_margin('BTC-USDT-210409', 'long', 'add', '100')
    # 获取杠杆倍数 Get Leverage
    # result = account.get_leverage('BTC-USDT-210409', 'isolated')
    # 获取币币逐仓杠杆最大可借  Get the maximum loan of isolated MARGIN
    # result = account.get_max_load('BTC-USDT', 'cross', 'BTC')

    print("获取当前账户交易手续费费率", account.get_fee_rates("FUTURES", "", category="1").data_size())
    # 获取计息记录  Get interest-accrued
    # result = account.get_interest_accrued('BTC-USDT', 'BTC', 'isolated', '', '', '10')
    # 获取用户当前杠杆借币利率 Get Interest-accrued
    # result = account.get_interest_rate()
    # 期权希腊字母PA / BS切换  Set Greeks (PA/BS)
    # result = account.set_greeks('BS')
    print("查看账户最大可转余额", account.get_max_withdrawal("").data_size())


def test_assert():
    # 获取充值地址信息  Get Deposit Address
    # result = fundingAPI.get_deposit_address('')
    # 获取资金账户余额信息  Get Balance
    # result = fundingAPI.get_balances('BTC')
    # 资金划转  Funds Transfer
    # result = fundingAPI.funds_transfer(ccy='', amt='', type='1', froms="", to="",subAcct='')
    # 提币  Withdrawal
    # result = fundingAPI.coin_withdraw('usdt', '2', '3', '', '', '0')
    print("充值记录", fundingAPI.get_deposit_history().data_size())
    print("提币记录", fundingAPI.get_withdrawal_history().data_size())
    print("获取币种列表 ", fundingAPI.get_currency().data_size())
    # 余币宝申购/赎回  PiggyBank Purchase/Redemption
    # result = fundingAPI.purchase_redempt('BTC', '1', 'purchase')

    print("资金流水查询", fundingAPI.get_bills().data_size())


def test_market():
    print("获取所有产品行情信息", market.get_tickers("SPOT").data_size())
    print("获取单个产品行情信息", market.get_ticker("BTC-USDT").data_size())
    print("获取指数行情", market.get_index_ticker("BTC", "BTC-USD").data_size())
    print("获取产品深度", market.get_orderbook("BTC-USDT-210402", "400").data_size())
    print("获取所有交易产品K线数据", market.get_candlesticks("BTC-USDT-210924", bar="1m").data_size())
    print("获取交易产品历史K线数据（仅主流币实盘数据）", market.get_history_candlesticks("BTC-USDT").data_size())
    print("获取指数K线数据", market.get_index_candlesticks("BTC-USDT").data_size())
    print("获取标记价格K线数据", market.get_mark_price_candlesticks("BTC-USDT").data_size())
    print("获取交易产品公共成交数据", market.get_trades("BTC-USDT", "400").data_size())
    print("获取平台24小时成交总量", market.get_volume().data_size())
    print("Oracle 上链交易数据", market.get_oracle().data_size())


def test_public():
    print("获取交易产品基础信息", publicAPI.get_instruments("FUTURES", "BTC-USDT"))
    print("获取交割和行权记录", publicAPI.get_deliver_history("FUTURES", "BTC-USD"))
    print("获取持仓总量", publicAPI.get_open_interest("SWAP").data_size())
    print("获取永续合约当前资金费率", publicAPI.get_funding_rate("BTC-USD-SWAP"))
    print("获取永续合约历史资金费率", publicAPI.funding_rate_history("BTC-USD-SWAP"))
    print("获取限价", publicAPI.get_price_limit("BTC-USD-210402"))
    print("获取期权定价", publicAPI.get_opt_summary("BTC-USD"))
    print("获取预估交割/行权价格", publicAPI.get_estimated_price("ETH-USD-210326"))
    print("获取免息额度和币种折算率", publicAPI.discount_interest_free_quota(""))
    print("获取系统时间", publicAPI.get_system_time())
    print(
        "获取平台公共爆仓单信息", publicAPI.get_liquidation_orders("FUTURES", uly="BTC-USDT", alias="next_quarter", state="filled")
    )
    print("获取标记价格", publicAPI.get_mark_price("FUTURES"))
    print("获取合约衍生品仓位档位", publicAPI.get_tier(instType="MARGIN", instId="BTC-USDT", tdMode="cross"))


def test_trade():
    tradeAPI = TradeClient(api_key, secret_key, passphrase, False, flag)
    # 下单  Place Order
    # result = tradeAPI.place_order(instId='BTC-USDT-210326', tdMode='cross', side='sell', posSide='short',
    #                               ordType='market', sz='100')
    # 批量下单  Place Multiple Orders
    # result = tradeAPI.place_multiple_orders([
    #     {'instId': 'BTC-USD-210402', 'tdMode': 'isolated', 'side': 'buy', 'ordType': 'limit', 'sz': '1', 'px': '17400',
    #      'posSide': 'long',
    #      'clOrdId': 'a12344', 'tag': 'test1210'},
    #     {'instId': 'BTC-USD-210409', 'tdMode': 'isolated', 'side': 'buy', 'ordType': 'limit', 'sz': '1', 'px': '17359',
    #      'posSide': 'long',
    #      'clOrdId': 'a12344444', 'tag': 'test1211'}
    # ])

    # 撤单  Cancel Order
    # result = tradeAPI.cancel_order('BTC-USD-201225', '257164323454332928')
    # 批量撤单  Cancel Multiple Orders
    # result = tradeAPI.cancel_multiple_orders([
    #     {"instId": "BTC-USD-210402", "ordId": "297389358169071616"},
    #     {"instId": "BTC-USD-210409", "ordId": "297389358169071617"}
    # ])

    # 修改订单  Amend Order
    # result = tradeAPI.amend_order()
    # 批量修改订单  Amend Multiple Orders
    # result = tradeAPI.amend_multiple_orders(
    #     [{'instId': 'BTC-USD-201225', 'cxlOnFail': 'false', 'ordId': '257551616434384896', 'newPx': '17880'},
    #      {'instId': 'BTC-USD-201225', 'cxlOnFail': 'false', 'ordId': '257551616652488704', 'newPx': '17882'}
    #      ])

    # 市价仓位全平  Close Positions
    # result = tradeAPI.close_positions('BTC-USDT-210409', 'isolated', 'long', '')
    # 获取订单信息  Get Order Details
    # result = tradeAPI.get_orders('BTC-USD-201225', '257173039968825345')

    print("获取未成交订单列表", tradeAPI.get_order_list().data_size())
    print("获取历史订单记录（近七天", tradeAPI.get_orders_history("FUTURES").data_size())
    print("获取历史订单记录（近三个月）", tradeAPI.orders_history_archive("FUTURES").data_size())
    # 获取成交明细  Get Transaction Details
    # result = tradeAPI.get_fills()
    # 策略委托下单  Place Algo Order
    # result = tradeAPI.place_algo_order('BTC-USDT-210409', 'isolated', 'buy', ordType='conditional',
    #                                    sz='100',posSide='long', tpTriggerPx='60000', tpOrdPx='59999')
    # 撤销策略委托订单  Cancel Algo Order
    # result = tradeAPI.cancel_algo_order([{'algoId': '297394002194735104', 'instId': 'BTC-USDT-210409'}])
    # 获取未完成策略委托单列表  Get Algo Order List
    # result = tradeAPI.order_algos_list('conditional', instType='FUTURES')
    # 获取历史策略委托单列表  Get Algo Order History
    # result = tradeAPI.order_algos_history('conditional', 'canceled', instType='FUTURES')


def test_sub_account():
    subAccountAPI = SubAccountClient(api_key, secret_key, passphrase, False, flag)
    print("查询子账户的交易账户余额(适用于母账户)", subAccountAPI.balances(subAcct="").data_size())
    print("查询子账户转账记录(仅适用于母账户)", subAccountAPI.bills().data_size())
    print("删除子账户APIKey(仅适用于母账户)", subAccountAPI.delete(pwd="", subAcct="", apiKey="").data_size())
    print("重置子账户的APIKey(仅适用于母账户)", subAccountAPI.reset(pwd="", subAcct="", label="", apiKey="", perm="").data_size())
    print("创建子账户的APIKey(仅适用于母账户)", subAccountAPI.create(pwd="123456", subAcct="", label="", Passphrase="").data_size())
    print("查看子账户列表(仅适用于母账户)", subAccountAPI.view_list().data_size())
    print(
        "母账户控制子账户与子账户之间划转（仅适用于母账户）",
        subAccountAPI.control_transfer(ccy="", amt="", froms="", to="", fromSubAccount="", toSubAccount="").data_size(),
    )


def test_system():
    # 系统状态API(仅适用于实盘) system status
    status = SystemClient(api_key, secret_key, passphrase, False, flag)
    print("查看系统的升级状态", status.status())


if __name__ == "__main__":
    test_account()
    test_assert()
    test_public()
    test_market()
