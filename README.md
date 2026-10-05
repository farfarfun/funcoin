# funcoin

`funcoin` 是一个基于 [ccxt](https://github.com/ccxt/ccxt) 的加密货币行情采集工具：按天（UTC 日）拉取交易所的 K 线（Kline）与逐笔成交（Trade）数据，打包压缩后通过 [funtable](https://github.com/farfarfun/funtable) / [fundrive](https://github.com/farfarfun/fundrive) 上传到云存储（默认阿里云 OSS），并提供一个按固定间隔循环下载的常驻服务入口。

## 安装

```bash
pip install funcoin
```

需要 Python 3.12+（传递依赖 `fundrive[oss]` 自 2.0.84 起要求 3.12，详见 CHANGELOG）。

## 最小示例

### 0. 不需要任何凭据的最小可运行示例

下面这段用替身交易所跑通「拉取 → 写 CSV」的完整链路，复制粘贴即可运行：不联网、
不需要 Binance / OSS 凭据，可用来确认装完包之后能不能正常用。

```python
from unittest.mock import MagicMock

from funcoin.coins.base.loader import KlineLoder

exchange = MagicMock()
# ccxt 的 fetch_ohlcv 返回 [timestamp, open, high, low, close, volume]；
# 第二次返回空列表表示这一段已经拉完。
exchange.fetch_ohlcv.side_effect = [[[1700000000000, 10.0, 30.0, 5.0, 20.0, 99.0]], []]
exchange.sort_by.side_effect = lambda data, key: data

loader = KlineLoder(
    exchange,
    csv_path="demo.csv",
    unix_start=1700000000000,
    unix_end=1700000600000,
)
loader.load_symbol("BTC/USDT")  # 内部已负责 flush 与关闭文件

print(open("demo.csv").read())
# symbol,timestamp,open,high,low,close,vol
# BTC/USDT,1700000000000,10.0,30.0,5.0,20.0,99.0
```

### 1. 配置凭据（跑真实下载前必做）

真实下载需要一把 Binance API Key 和一套阿里云 OSS 凭据，统一通过
[funsecret](https://github.com/farfarfun/funsecret) 存取，**不要写进代码或提交进仓库**。
装 funcoin 时会一并装上 `funsecret` 命令，下面几条执行一次即可（凭据加密落在本机
`~/.secret/`，不会随项目走）：

```bash
# Binance API Key / Secret。只拉公开行情的话，建议在 Binance 后台创建
# 「只读（Enable Reading）」权限的 Key，不要开启交易和提现权限。
funsecret write '<your-binance-api-key>'    coin binance api_key
funsecret write '<your-binance-secret-key>' coin binance secret_key

# 阿里云 OSS 访问凭据（用于上传打包好的行情文件）
funsecret write '<your-oss-access-key>'        fundrive oss farfarfun access_key
funsecret write '<your-oss-access-secret>'     fundrive oss farfarfun access_secret
funsecret write 'oss-cn-hangzhou.aliyuncs.com' fundrive oss farfarfun endpoint

# 核对是否写进去了（会把明文打到终端，注意共享终端的回滚缓冲）
funsecret read coin binance api_key
```

`download_daily()` 读取的就是上面这 5 个键：

| funsecret 键 | 用途 | 是否必需 |
| --- | --- | --- |
| `coin.binance.api_key` | Binance API Key | 是 |
| `coin.binance.secret_key` | Binance API Secret | 是 |
| `fundrive.oss.farfarfun.access_key` | OSS AccessKeyId | 是 |
| `fundrive.oss.farfarfun.access_secret` | OSS AccessKeySecret | 是 |
| `fundrive.oss.farfarfun.endpoint` | OSS Endpoint | 是 |

目标 bucket 当前固定为 `farfarfun`、表目录固定为 `funcoin/binance_kline_daily_1m/`
（见 `funcoin/coins/task/download.py`），`download_daily()` 暂不支持参数化；
要换 bucket、换交易所请照下一节自己组装 `LoadTask`。

### 2. 作为库调用

```python
import ccxt
from fundrive.drives import OSSDrive
from funsecret import read_secret
from funtable.table import DriveTable

from funcoin.coins.table.load import LoadTask

exchange = ccxt.binance(
    {
        "apiKey": read_secret("coin", "binance", "api_key"),
        "secret": read_secret("coin", "binance", "secret_key"),
    }
)

drive = OSSDrive()
drive.login(
    access_key=read_secret("fundrive", "oss", "farfarfun", "access_key"),
    access_secret=read_secret("fundrive", "oss", "farfarfun", "access_secret"),
    endpoint=read_secret("fundrive", "oss", "farfarfun", "endpoint"),
    bucket_name="farfarfun",
)

table = DriveTable(table_fid="funcoin/binance_kline_daily_1m/", drive=drive)
task = LoadTask(table=table, exchange=exchange)
task.run(days=30)  # 从昨天（UTC 日）往前回补 30 天，已存在的分区会跳过
```

凭据配好之后，也可以直接调用封装好的入口函数：

```python
from funcoin.coins.task.download import download_daily

download_daily(days=30)
```

### 3. 作为命令行 / 服务运行

```bash
# 一次性任务：立即下载一轮后退出（--days 默认 800，与 download_daily() 一致）
funcoin download --days 30

# 常驻服务：按固定间隔循环执行每日下载，收到 SIGTERM/SIGINT 时当轮结束后优雅退出
funcoin-download run
```

常驻行为由三个环境变量控制：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `FUNCOIN_DOWNLOAD_INTERVAL` | `86400` | 两轮下载之间的间隔秒数，下限 60 秒 |
| `FUNCOIN_DOWNLOAD_DAYS` | `800` | 每轮从昨天往前回补的天数，已上传的分区会跳过 |
| `FUNCOIN_RUN_ONCE` | 未设置 | 设为 `1`/`true` 时跑完一轮就退出，等价于一次性任务 |

常驻模式下单轮失败只记日志、等下一轮重试，不会让服务退出；`FUNCOIN_RUN_ONCE=1` 时异常
会原样抛出，便于 CI / 人工排查。

用脚本管理生命周期（`dev` 跑本仓库源码，`prod` 只跑已安装的正式发布包）：

```bash
bash scripts/setup.sh run dev       # 前台运行
bash scripts/setup.sh start prod    # 后台启动
bash scripts/setup.sh status        # 同时查看 dev/prod
bash scripts/setup.sh stop prod
bash scripts/setup.sh restart dev
```

脚本在 `.run/` 下维护 `*.pid` + `*.meta`（记录进程启动时刻与命令特征串）：
进程已退出的陈旧 pid 文件会被明确报告并清理；PID 还活着但身份对不上（已被别的进程复用）
时拒绝操作并提示人工确认，不会误杀无关进程；服务确在运行时 `start` 直接拒绝重复启动。
`prod` 启动前会用隔离的 `python3 -I` + 空 `PYTHONPATH` 校验 `funcoin` 确实解析到
site-packages 下、且有对应的 distribution 元数据，editable 安装与源码树都会被拒绝。

## 核心组件

- `funcoin.coins.base.loader.BaseLoader`：数据加载器基类，定义 `_open`/`_write`/`_close`/`_load_symbols`/`_load_symbol` 等 hook。
- `KlineLoder` / `TradeLoader`：分别拉取 K 线 / 逐笔成交数据的具体加载器，均先写本地 CSV 再由 `LoadTask` 打包上传。
- `funcoin.coins.table.load.LoadTask`：按日编排「拉取 → 压缩 → 上传 → 清理本地文件」的完整流程。
- `funcoin.coins.task.download.download_daily`：面向 Binance 的开箱即用下载入口，供 CLI / 服务复用。

## 已知局限

- 目前仅内置了 Binance 交易所的开箱即用下载入口（`download_daily`），其余 ccxt 支持的交易所需要自行组装 `LoadTask`。
- 云存储上传默认使用阿里云 OSS（`fundrive.drives.OSSDrive`），切换其他云存储需自行替换 `Drive` 实现。

---

## 关于 farfarfun

[farfarfun](https://github.com/farfarfun) 是一个专注于实用工具库的开源组织，
涵盖云存储、数据处理、AI、多媒体与开发工具链等方向。

- 🏠 组织主页：<https://github.com/farfarfun>
- 📦 PyPI：<https://pypi.org/user/niuliangtao/>
- 📧 联系：farfarfun@qq.com

本项目基于 [MIT](LICENSE) 协议开源。
