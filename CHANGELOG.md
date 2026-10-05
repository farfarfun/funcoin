# 更新日志

本文件记录 `funcoin` 的版本变更，按版本倒序排列。

## [未发布]

### 破坏性变更

- **K 线字段顺序修正，旧数据需要重新下载。** `KlineLoder` 此前把 ccxt `fetch_ohlcv()`
  返回的 6 列按 `["timestamp", "open", "high", "low", "vol", "close"]` 命名，而 ccxt 的
  返回契约固定是 `[timestamp, open, high, low, close, volume]`（见各交易所的
  `parse_ohlcv()`）。也就是说**此前所有已上传文件里的 `close` 列装的是成交量、`vol` 列
  装的是收盘价**。现已改为正确的 `["timestamp", "open", "high", "low", "close", "vol"]`。
  迁移：已落在云存储上的历史分区全部是错的，需要删除后重新回补；任何基于旧文件
  `close`/`vol` 做出的计算都要作废重算。若暂时无法重跑，可先把旧文件的这两列对调回来
  再使用。
- **按日切分改为显式 UTC 口径。** `FileProperty.daily()` 此前用 naive
  `datetime.strptime()` + `.timestamp()`，等价于按**运行机器的本地时区**切天；在 UTC+8
  的机器上，`20260101` 这个文件实际覆盖的是 `2025-12-31 16:00Z ~ 2026-01-01 16:00Z`，
  同名文件换台机器跑出来内容还不一样。现统一按 UTC 日切分。迁移：此前在非 UTC 机器上
  生成的分区边界都是偏的，需要重新回补。
- **浮点精度不再被截断。** `BaseLoader.write_data` 此前走
  `orjson.loads(df.to_json(orient="records"))`，而 pandas 的 `to_json` 默认
  `double_precision=10`，会把价格/成交量静默截断到 10 位小数——SHIB/PEPE 这类单价在
  `1e-8` 量级的币种直接丢有效数字（`8.123456789e-06` 被写成 `8.1235e-06`）。改用
  `to_dict(orient="records")` 后保留完整精度。迁移：新旧文件的数值位数会有差异，
  做数据比对时需注意。

### 修复

- `BaseLoader.load_symbol` 调用的是 `self._load_symbol(symbol=symbol, pbr=pbr, *args)`：
  `*args` 会先按位置绑到形参 `symbol`/`pbr` 上，只要 `args` 非空就必定
  `TypeError: got multiple values for argument 'symbol'`。改为按位置传参。
- `KlineLoder._load_symbol` 的游标推进用 `result[-1][0]`（上一批最后一根 K 线的时间戳），
  而 ccxt 的 `since` 是**闭区间**下界，于是每轮都会把最后一根重新拉回来；一旦某轮只返回
  一根，游标原地不动，循环永不退出。改为 `result[-1][0] + 1`，并补 `MAX_KLINE_PAGES` /
  `MAX_TRADE_PAGES` 轮数上限兜底。
- `KlineLoder`/`TradeLoader` 的失败重试此前没有次数上限，网络异常时会无限重试。现在连续
  失败 `MAX_RETRIES`（3）次后抛 `DataLoadError`；未拉完就退出循环时补 WARNING 日志，
  不再静默产出残缺文件。
- `TradeLoader._load_symbol` 的 `retries` 归零逻辑写在 `try/else` 里，而循环体内有
  `continue`——Python 中 `continue` 会跳过 `else` 子句，导致成功一次也不清零，偶发失败
  会被当成连续失败累计。改为成功后立即归零。
- `TradeLoader._load_symbol` 在 `pbr=None`（不带进度条调用）时直接 `AttributeError`，
  现已判空。
- `LoadTask.run()` 在循环里「先减一天再使用」，`days=1` 实际处理的是**前天**而不是昨天，
  最近的一天永远补不上。改为从昨天开始、往前数 `days` 天。
- `LoadTask.download_trade()` 不再向 `TradeLoader` 传 `timeframe`：逐笔成交没有周期概念，
  这个参数只会被 `**kwargs` 静默吞掉，容易让人误以为生效。
- `BaseLoader.load_symbol()` 收尾时补上强制 flush。`write_data()` 默认攒够 10000 条才
  落盘，而单个 symbol 一天的数据远不到这个量，原先直接 `_close()` 会把整批缓存连同
  文件句柄一起丢掉，只留下一个**只有表头的空 CSV**（批量入口 `load_symbols()` 因为
  `_load_symbols()` 末尾有 flush 而不受影响）。
- `CSVLoader` 打开 CSV 时补 `newline=""` 与 `encoding="utf-8"`，避免 Windows 下多出空行、
  以及依赖系统默认编码。

### 变更

- **服务入口实现真正的常驻行为。** `funserver` 的 `_start()` 是用 `nohup` 把 `run()` 丢到
  后台，而 `FunCoin.run()` / `FunCoinDownload.run()` 过去只是调一次 `download_daily()`
  就返回——进程随即退出，`setup.sh status` 永远显示「未运行」。新增
  `funcoin.server.scheduler.run_download_loop()`：按固定间隔循环执行，支持
  SIGTERM/SIGINT 优雅退出（睡眠按 1 秒切片，可中断），常驻模式下单轮失败只记日志、
  等下一轮重试。行为由 `FUNCOIN_DOWNLOAD_INTERVAL`（默认 86400，下限 60）、
  `FUNCOIN_DOWNLOAD_DAYS`（默认 800）、`FUNCOIN_RUN_ONCE` 三个环境变量控制；
  一次性下载仍可用 `funcoin download --days N`。
- 回补天数默认值统一到 `funcoin.coins.task.download.DEFAULT_DAYS = 800`，消除此前
  「README 写 365、`funcoin download` 默认 365、库函数默认 800」三处互相矛盾的默认值。
- `scripts/setup.sh` 重写：用 `.run/*.pid` + `.run/*.meta`（进程启动时刻 + 命令特征串）
  区分 `missing`/`invalid`/`stale`/`mismatch`/`running` 五种状态，PID 被复用时拒绝操作
  而不是误杀无关进程；`prod` 启动前用隔离的 `python3 -I` + 空 `PYTHONPATH` 校验
  `funcoin` 确实解析到 site-packages 且有对应的 distribution 元数据，editable 安装与
  源码树一律拒绝。
- `pyproject.toml` 显式声明 `[tool.ruff]` 规则集（`E,F,W,I,UP,B,SIM,C4,DTZ,RUF`）与
  `[tool.pytest.ini_options]`，使 lint/测试发现结果不再依赖上层目录的配置。
- README 重写：补充**无需任何凭据的 mock 最小示例**、`funsecret write` 的逐条凭据初始化
  命令与键位表（明确哪些步骤需要真实 Binance/OSS 凭据）、常驻服务的环境变量表，并说明
  bucket `farfarfun` 与表目录 `funcoin/binance_kline_daily_1m/` 当前是写死的。
- `.gitignore` 补充 `*.rar`/`*.zip`/`*.7z`，与已有的 `*.tar`/`*.csv` 一起覆盖 `LoadTask`
  产生的本地临时产物。

### 新增

- 单元测试由 27 个扩充到 43 个，覆盖 OHLCV 列序、浮点精度、游标推进、重试上限、
  `pbr=None`、`*args` 透传、`LoadTask.run()` 的起始日、UTC 日边界、调度循环的
  once/常驻/环境变量读取，以及「服务入口必须是阻塞的」这条回归约束。

## [1.0.57]（当前版本）

### 破坏性变更

- **最低 Python 版本由 3.10 提升到 3.12。** 1.0.56 之前 `requires-python` 声明
  `>=3.10`，但那是一个**从未成立过**的声明（见下方「修复」），3.10/3.11 上实际装不上。
  迁移：仍在 3.10/3.11 的使用方需要升级解释器到 3.12+，或继续停留在依赖
  `fundrive[oss]<2.0.84` 的旧版本 funcoin。该版本下限由传递依赖
  `fundrive[oss]` 决定，若组织层面决定下调，需要先降 fundrive 的 Python 下限。

### 修复

- `requires-python` 由 `>=3.10` 更正为 `>=3.12`。原声明与实际依赖矛盾：依赖
  `fundrive[oss]>=2.0.86`，而 fundrive 自 2.0.84 起全部要求 `>=3.12`，因此已发布的
  1.0.55 在 Python 3.10/3.11 上**根本装不上**——解析器只会报 `fundrive[oss]` 无解，
  而不是明确告知 Python 版本不满足。同一矛盾也让 `uv lock` 在解析 win32 + 3.10/3.11
  的组合时直接失败，导致锁文件无法重新生成（urllib3 因此卡在有漏洞的 2.7.0）。

### 变更

- 重新生成 `uv.lock`：传递依赖 urllib3 由 2.7.0 升到 2.8.0，修掉 GitHub dependabot 报出的
  3 个漏洞（2 个 HIGH：`HTTPResponse.stream()/read_chunked()` 无界缓冲、HTTPS 代理的 TLS
  配置可能被忽略；1 个 MEDIUM：chunked deflate 可进入无限循环）。同批次另有若干依赖的
  小版本更新，均未跨大版本。

### 新增

- 补充 `pyproject.toml` 中缺失的直接依赖 `farlog`、`tqdm`，并为全部依赖补上版本下限。
- `[project]` 显式声明 `license = "MIT"` 与 `license-files = ["LICENSE"]`。
- 新增 `scripts/setup.sh` 作为服务的统一生命周期管理入口（`run`/`start`/`stop`/`restart`/`status`，区分 `dev`/`prod`）。
- 生成并提交 `uv.lock` 以保证可复现构建。
- 补充完整 README（简介、安装命令、最小可运行示例、核心组件说明）。

### 修复

- `BaseLoader.__exit__` 不再无条件返回 `True` 吞掉 with 块内的异常，清理资源后改为返回 `False`，让异常正常向上传播。
- `KlineLoder`/`TradeLoader` 拉取失败时的日志补充交易所、symbol、时间范围等定位上下文。
- `FunCoin.run`（`funcoin run`/`start`/`restart` 的服务入口）不再是空壳 `pass`，改为执行一次每日行情下载。

### 变更

- 日志统一改用 `farlog.getLogger`，移除对 `logging`/`funutil` 的直接依赖（`coins/base/loader.py`、`coins/table/load.py`）。
- 公开类/方法补充中文 docstring 与 Python 3.10 风格类型标注（`str | None` 等）。
- 补齐 `src/funcoin/server/__init__.py`，让 `funcoin.server` 与 `funcoin.coins` 一样是常规包而非隐式命名空间包。
- 按 `ruff format` 默认风格重排 `coins/table/load.py` 与 `tests/test_smoke.py`（此前 `ruff format --check` 不通过）。

### 移除

- 删除 `useless/` 下 113 个历史遗留 Python 文件（约 1.1 万行）。这套平行源码树引用的
  `funcoin.base.db`、`funcoin.huobi.*`、`funcoin.okex.*`、`funcoin.server.strategy` 等模块
  在当前 `src/funcoin/` 中均不存在，整体无法导入；其中还残留已被组织现行包取代的
  `funtool.time`/`funtool.log`/`funtool.secret` 入口、裸 `logging` 调用、大量调试 `print`
  以及吞掉异常的宽泛 `except Exception`。
- 删除 `example/` 下 5 个同样不可运行的示例文件（`example_v3.py`、`example_v5.py`、
  `market_e.py`、`okex_exmple.py`、`okex-v5.html`）。它们引用 `funcoin.okex.v5.client`、
  `funcoin.huobi.client.market`、`funcoin.task.load`、`funcoin.coins.base.file` 等不存在的
  模块，`example_v5.py` 还在用 `funtool.secret`。可运行的最小示例见 README。

### 废弃

（无）
