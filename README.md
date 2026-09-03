# funcoin

`funcoin` 是一个基于 [ccxt](https://github.com/ccxt/ccxt) 的加密货币行情采集工具：按天拉取交易所的 K 线（Kline）与逐笔成交（Trade）数据，打包压缩后通过 [funtable](https://github.com/farfarfun/funtable) / [fundrive](https://github.com/farfarfun/fundrive) 上传到云存储（默认阿里云 OSS），并提供一个可长期运行的下载服务入口。

## 安装

```bash
pip install funcoin
```

## 最小示例

### 1. 作为库调用

```python
import ccxt
from fundrive.drives import OSSDrive
from funtable.table import DriveTable

from funcoin.coins.table.load import LoadTask

exchange = ccxt.binance({"apiKey": "your-api-key", "secret": "your-secret-key"})

drive = OSSDrive()
drive.login(
    access_key="your-oss-access-key",
    access_secret="your-oss-access-secret",
    endpoint="your-oss-endpoint",
    bucket_name="your-bucket",
)

table = DriveTable(table_fid="funcoin/binance_kline_daily_1m/", drive=drive)
task = LoadTask(table=table, exchange=exchange)
task.run(days=30)  # 从昨天开始，往前回补 30 天的 K 线数据，已存在的分区会跳过
```

也可以直接调用封装好的入口函数：

```python
from funcoin.coins.task.download import download_daily

download_daily(days=30)
```

`download_daily` 会通过 [funsecret](https://github.com/farfarfun/funsecret) 读取以下密钥：

- `coin.binance.api_key` / `coin.binance.secret_key`：Binance API Key/Secret
- `fundrive.oss.farfarfun.access_key` / `access_secret` / `endpoint`：OSS 访问凭据

### 2. 作为命令行 / 服务运行

```bash
# 立即执行一次下载（默认回补 365 天）
funcoin download --days 30

# 以服务方式运行，内部同样调用 download_daily
funcoin-download
```

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
