# 更新日志

本文件记录 `funcoin` 的版本变更，按版本倒序排列。

## [1.0.57]（当前版本）

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
