import calendar
import datetime
import os
import time
from datetime import timedelta

from notedrive.lanzou import LanZouCloud
from notedrive.tables import SqliteTable


class AutoSaveDetail(SqliteTable):
    def __init__(self,
                 db_path=None,
                 save_freq='d',
                 fid=-1,
                 ms=False,
                 save_key=None,
                 save_with_clear=True,
                 *args,
                 **kwargs):
        if db_path is None:
            db_path = os.path.abspath(os.path.dirname(__file__)) + '/data/coin.db'

        super(AutoSaveDetail, self).__init__(db_path=db_path, *args, **kwargs)

        self.ms = ms
        self.fid = fid
        self.save_key = save_key
        self.save_freq = save_freq
        self.save_with_clear = save_with_clear
        self.last_finish_save_unix = datetime.datetime.now()
        self.last_temp_save_unix = datetime.datetime.now()

        if self.save_freq in ('d', 'D', 'Day', 'day'):
            self.save_freq = 'd'
        elif self.save_freq in ('m', 'M', 'mon', 'Mon', 'month', 'Month'):
            self.save_freq = 'm'
        elif self.save_freq in ('week', 'Week'):
            self.save_freq = 'w'
        elif self.save_freq in ('Y', 'y', 'year', 'Year'):
            self.save_freq = 'y'
        else:
            self.save_freq = 'd'

        self.path_root = os.path.join(os.path.dirname(self.db_path), self.table_name)
        if not os.path.exists(self.path_root):
            os.makedirs(self.path_root)

        self.downer = LanZouCloud()
        self.downer.ignore_limits()
        self.downer.login_by_cookie()

    def auto_save(self):
        """
        数据保存，分两种，
        一种是数据已经准备完成，上传云端,每天凌晨1点以后保存一次
        另一种是保存中间结果，每隔3个小时保存一次
        """
        time_unix = datetime.datetime.now()

        is_finish_save = False

        if self.save_freq in ('d', 'D', 'Day', 'day'):
            is_finish_save = self.last_finish_save_unix.day != time_unix.day and time_unix.hour >= 1
        elif self.save_freq in ('m', 'M', 'mon', 'Mon', 'month', 'Month'):
            is_finish_save = self.last_finish_save_unix.month != time_unix.month and time_unix.hour >= 1
        elif self.save_freq in ('m', 'M', 'mon', 'Mon', 'month', 'Month'):
            is_finish_save = self.last_finish_save_unix.year != time_unix.year and time_unix.hour >= 1

        is_temp_save = abs(self.last_temp_save_unix.hour - time_unix.hour) >= 3
        if is_finish_save:
            self._save(now=self.last_finish_save_unix, pop=self.save_with_clear)
            self.last_finish_save_unix = time_unix
            self.downer.sync_files(self.path_root, folder_id=self.fid, overwrite=True, remove_local=True)
        elif is_temp_save:
            self._save(pop=False)
            self.last_temp_save_unix = time_unix

    def _save(self, pop=None, now=None):
        now = now or datetime.datetime.now()

        name = self.table_name
        if self.save_freq in ('d', 'D', 'Day', 'day'):
            fmt = "%Y%m%d"
        elif self.save_freq in ('m', 'M', 'mon', 'Mon', 'month', 'Month'):
            fmt = "%Y%m"
        elif self.save_freq in ('week', 'Week'):
            fmt = "%Y%U"
        elif self.save_freq in ('Y', 'y', 'year', 'Year'):
            fmt = "%Y"
        else:
            fmt = "%Y%m%d"
        if self.save_key is not None:
            start, end = self.between_unix_ms(now)
            condition = '{key}>={start} and {key}<={end}'.format(key=self.save_key, start=start, end=end)
            condition2 = '{key}<={end}'.format(key=self.save_key, end=end)
        else:
            condition = None
            condition2 = None

        path = '{}/{}-{}.csv'.format(self.path_root, name, self.last_finish_save_unix.strftime(fmt))

        self.to_csv(condition=condition, path=path, index=None)
        if pop:
            self.delete(condition2)

    def between_unix_ms(self, now):
        now = now or datetime.datetime.now()

        this_day_start = now.date()
        this_day_end = this_day_start + timedelta(days=1)
        this_week_start = (now - timedelta(days=now.weekday())).date()
        this_week_end = (this_week_start + timedelta(days=7))
        this_month_start = datetime.datetime(now.year, now.month, 1).date()
        this_month_end = this_month_start + timedelta(days=calendar.monthrange(now.year, now.month)[1])
        this_year_start = datetime.datetime(now.year, 1, 1).date()
        this_year_end = datetime.datetime(now.year + 1, 1, 1).date()

        this_day_start = self.date2unix(this_day_start.timetuple())
        this_day_end = self.date2unix(this_day_end.timetuple()) - 1
        this_week_start = self.date2unix(this_week_start.timetuple())
        this_week_end = self.date2unix(this_week_end.timetuple()) - 1
        this_month_start = self.date2unix(this_month_start.timetuple())
        this_month_end = self.date2unix(this_month_end.timetuple()) - 1
        this_year_start = self.date2unix(this_year_start.timetuple())
        this_year_end = self.date2unix(this_year_end.timetuple()) - 1

        if self.save_freq in ('d', 'D', 'Day', 'day'):
            return this_day_start, this_day_end
        elif self.save_freq in ('m', 'M', 'mon', 'Mon', 'month', 'Month'):
            return this_month_start, this_month_end
        elif self.save_freq in ('week', 'Week'):
            return this_week_start, this_week_end
        elif self.save_freq in ('Y', 'y', 'year', 'Year'):
            return this_year_start, this_year_end
        else:
            return this_day_start, this_day_end

    def date2unix(self, t):
        if self.ms:
            return round(time.mktime(t) * 1000)
        return round(time.mktime(t))


class TradeDetail(AutoSaveDetail):
    def __init__(self, table_name='trade_detail', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/coin.db'

        super(TradeDetail, self).__init__(db_path=db_path, ms=True,
                                          fid=3359101,
                                          save_key='ts',
                                          table_name=table_name, *args, **kwargs)
        self.columns = ['trade_id', 'amount', 'price', 'ts', 'direction']

    def create(self):
        self.execute("""
            create table if not exists {} (
               trade_id       BIGINT         
              ,amount         FLOAT 
              ,price          FLOAT
              ,ts             BIGINT
              ,direction      VARCHAR(5)
              ,primary key (trade_id)           
              )
            """.format(self.table_name))


class SymbolInfo(AutoSaveDetail):
    def __init__(self, table_name='symbol_info', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/coin.db'

        super(SymbolInfo, self).__init__(db_path=db_path,
                                         table_name=table_name,
                                         save_freq='y',
                                         fid=3360754,
                                         save_key='',
                                         save_with_clear=False,
                                         *args, **kwargs)
        self.columns = ['symbol', 'base_currency', 'quote_currency', 'price_precision', 'amount_precision',
                        'symbol_partition',
                        'state', 'value_precision',
                        'min_order_amt', 'max_order_amt',
                        'limit_order_min_order_amt', 'limit_order_max_order_amt',
                        'sell_market_min_order_amt', 'sell_market_max_order_amt',
                        'buy_market_max_order_value',
                        'min_order_value', 'max_order_value',
                        'leverage_ratio', 'underlying', 'mgmt_fee_rate', 'charge_time',
                        'rebal_time', 'rebal_threshold',
                        'init_nav', 'api_trading'
                        ]

    def create(self):
        self.execute("""
                create table if not exists {} (
                symbol		                    VARCHAR	-- 交易对
                ,base_currency                  VARCHAR -- 交易对中的基础币种
                ,quote_currency		            VARCHAR	-- 交易对中的报价币种
                ,price_precision		        integer	-- 交易对报价的精度（小数点后位数）
                ,amount_precision		        integer	-- 交易对基础币种计数精度（小数点后位数）
                ,symbol_partition		        VARCHAR	-- 交易区，可能值: [main，innovation]
                ,`state`		                VARCHAR	-- 交易对状态；可能值: [online，offline,suspend] online _ 已上线；offline _ 交易对已下线，不可交易；suspend -- 交易暂停；pre_online _ 即将上线
                ,value_precision		        integer	-- 交易对交易金额的精度（小数点后位数）
                ,min_order_amt		            float	-- 交易对限价单最小下单量 ，以基础币种为单位（即将废弃）
                ,max_order_amt		            float	-- 交易对限价单最大下单量 ，以基础币种为单位（即将废弃）
                ,limit_order_min_order_amt		float	-- 交易对限价单最小下单量 ，以基础币种为单位（NEW）
                ,limit_order_max_order_amt		float	-- 交易对限价单最大下单量 ，以基础币种为单位（NEW）
                ,sell_market_min_order_amt		float	-- 交易对市价卖单最小下单量，以基础币种为单位（NEW）
                ,sell_market_max_order_amt		float	-- 交易对市价卖单最大下单量，以基础币种为单位（NEW）
                ,buy_market_max_order_value		float	-- 交易对市价买单最大下单金额，以计价币种为单位（NEW）
                ,min_order_value		        float	-- 交易对限价单和市价买单最小下单金额 ，以计价币种为单位
                ,max_order_value		        float	-- 交易对限价单和市价买单最大下单金额 ，以折算后的USDT为单位（NEW）
                ,leverage_ratio		            float	-- 交易对杠杆最大倍数(仅对逐仓杠杆交易对、全仓杠杆交易对、杠杆ETP交易对有效）
                ,underlying		                VARCHAR	-- 标的交易代码 (仅对杠杆ETP交易对有效)
                ,mgmt_fee_rate		            float	-- 持仓管理费费率 (仅对杠杆ETP交易对有效)
                ,charge_time		            VARCHAR	-- 持仓管理费收取时间 (24小时制，GMT+8，格式：HH:MM:SS，仅对杠杆ETP交易对有效)
                ,rebal_time		                VARCHAR	-- 每日调仓时间 (24小时制，GMT+8，格式：HH:MM:SS，仅对杠杆ETP交易对有效)
                ,rebal_threshold		        float	-- 临时调仓阈值 (以实际杠杆率计，仅对杠杆ETP交易对有效)
                ,init_nav		                float	-- 初始净值（仅对杠杆ETP交易对有效）
                ,api_trading		            VARCHAR	-- API交易使能标记（有效值：enabled, disabled）
                ,PRIMARY KEY (symbol)
                )
                """.format(self.table_name))


class KlineDetail(AutoSaveDetail):
    def __init__(self, table_name='kline', db_path=None, *args, **kwargs):
        if db_path is None:
            db_path = os.path.abspath(
                os.path.dirname(__file__)) + '/data/coin.db'

        super(KlineDetail, self).__init__(db_path=db_path,
                                          save_key='id',
                                          table_name=table_name, *args, **kwargs)
        self.columns = ['symbol', 'id', 'amount', 'count', 'open', 'close', 'low', 'high', 'vol']

    def create(self):
        self.execute("""
            create table if not exists {} (
               symbol	VARCHAR	-- 交易对
              ,id	    bigint	-- 调整为新加坡时间的时间戳，单位秒，并以此作为此K线柱的id
              ,amount	float	-- 以基础币种计量的交易量
              ,`count`	integer	-- 交易次数
              ,`open`   float	-- 本阶段开盘价
              ,`close`	float	-- 本阶段收盘价
              ,low	    float	-- 本阶段最低价
              ,high	    float	-- 本阶段最高价
              ,vol	    float	-- 以报价币种计量的交易量
              ,primary key (symbol,id)           
              )
            """.format(self.table_name))


class Kline1MinDetail(KlineDetail):
    def __init__(self, table_name='kline_1min', db_path=None, *args, **kwargs):
        super(Kline1MinDetail, self).__init__(table_name=table_name, fid=3359096, db_path=db_path, *args, **kwargs)


class Kline5MinDetail(KlineDetail):
    def __init__(self, table_name='kline_5min', db_path=None, *args, **kwargs):
        super(Kline5MinDetail, self).__init__(table_name=table_name, fid=3359092, db_path=db_path, *args, **kwargs)


class Kline15MinDetail(KlineDetail):
    def __init__(self, table_name='kline_15min', db_path=None, *args, **kwargs):
        super(Kline15MinDetail, self).__init__(table_name=table_name,
                                               fid=3359095,
                                               db_path=db_path,
                                               *args, **kwargs)


class Kline30MinDetail(KlineDetail):
    def __init__(self, table_name='kline_30min', db_path=None, *args, **kwargs):
        super(Kline30MinDetail, self).__init__(table_name=table_name,
                                               db_path=db_path,
                                               fid=3359097,
                                               save_freq='m', *args, **kwargs)


class Kline60MinDetail(KlineDetail):
    def __init__(self, table_name='kline_60min', db_path=None, *args, **kwargs):
        super(Kline60MinDetail, self).__init__(table_name=table_name,
                                               fid=3360759,
                                               db_path=db_path,
                                               save_freq='m', *args, **kwargs)


class Kline4HourDetail(KlineDetail):
    def __init__(self, table_name='kline_4hour', db_path=None, *args, **kwargs):
        super(Kline4HourDetail, self).__init__(table_name=table_name,
                                               save_freq='m',
                                               fid=3359094,
                                               db_path=db_path, *args, **kwargs)


class Kline1DayDetail(KlineDetail):
    def __init__(self, table_name='kline_1day', db_path=None, *args, **kwargs):
        super(Kline1DayDetail, self).__init__(table_name=table_name,
                                              db_path=db_path,
                                              fid=3359093,
                                              save_freq='y', *args, **kwargs)


class Kline1MonDetail(KlineDetail):
    def __init__(self, table_name='kline_1mon', db_path=None, *args, **kwargs):
        super(Kline1MonDetail, self).__init__(table_name=table_name,
                                              db_path=db_path,
                                              save_freq='y',
                                              fid=3359099,
                                              save_with_clear=False,
                                              *args, **kwargs)


class Kline1WeekDetail(KlineDetail):
    def __init__(self, table_name='kline_1week', db_path=None, *args, **kwargs):
        super(Kline1WeekDetail, self).__init__(table_name=table_name,
                                               db_path=db_path,
                                               save_freq='y',
                                               fid=3359098,
                                               save_with_clear=False,
                                               *args, **kwargs)


class Kline1YearDetail(KlineDetail):
    def __init__(self, table_name='kline_1year', db_path=None, *args, **kwargs):
        super(Kline1YearDetail, self).__init__(table_name=table_name,
                                               db_path=db_path,
                                               save_freq='y',
                                               fid=3359091,
                                               save_with_clear=False,
                                               *args, **kwargs)
