# 更新日志

本文件记录 `funcoin` 的版本变更，按版本倒序排列。

## [1.0.57]（当前版本）

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

### 废弃

（无）
