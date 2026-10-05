#!/usr/bin/env bash
# funcoin 每日行情下载服务的统一生命周期管理入口。
#
# dev  —— 强制跑本仓库 src/ 下的源码，用于本地调试。
# prod —— 强制跑已安装的正式发布包，拒绝 editable / 源码树 / PYTHONPATH 注入。
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
RUN_DIR="$ROOT_DIR/.run"
NAME="funcoin-download"

# 进程命令行里的特征串，用于确认某个 PID 确实是本服务（见 service_state）。
IDENTITY="funcoin.server.download"
START_SNIPPET="from funcoin.server.download import FunCoinDownload; FunCoinDownload().run()"

usage() {
    echo "用法: $0 {start|stop|restart|run} {dev|prod}; $0 status [dev|prod]" >&2
    exit 1
}

pid_file() {
    printf '%s/%s-%s.pid' "$RUN_DIR" "$NAME" "$1"
}

# 与 pid 文件配套的身份文件，记录「进程启动时刻 + 命令特征串」。
# 只有 pid 文件时无法区分「还是我们那个进程」和「PID 回绕后被复用给了别人」。
meta_file() {
    printf '%s/%s-%s.meta' "$RUN_DIR" "$NAME" "$1"
}

log_file() {
    printf '%s/%s-%s.log' "$RUN_DIR" "$NAME" "$1"
}

# 进程启动时刻：Linux 读 /proc/<pid>/stat 的第 22 个字段 starttime，
# 其他平台回落到 ps lstart。进程不存在时输出空串。
proc_starttime() {
    local pid="$1"
    if [ -r "/proc/${pid}/stat" ]; then
        # 第 2 字段 comm 可能含空格和右括号，先截到最后一个 ')'，
        # 余下部分从 state 开始，starttime 即余下部分的第 20 个字段。
        sed 's/^.*) //' "/proc/${pid}/stat" 2>/dev/null | awk '{print $20}'
        return 0
    fi
    ps -o lstart= -p "$pid" 2>/dev/null | tr -s ' '
}

proc_cmdline() {
    local pid="$1"
    if [ -r "/proc/${pid}/cmdline" ]; then
        tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null
        return 0
    fi
    ps -o args= -p "$pid" 2>/dev/null
}

record_process() {
    local env="$1" pid="$2"
    printf '%s\n' "$pid" >"$(pid_file "$env")"
    {
        printf 'identity=%s\n' "$IDENTITY"
        printf 'starttime=%s\n' "$(proc_starttime "$pid")"
    } >"$(meta_file "$env")"
}

# 输出下列状态之一：
#   missing  —— 没有 pid 文件，服务未启动
#   invalid  —— pid 文件内容不是合法 PID
#   stale    —— pid 文件存在但进程已退出（陈旧残留）
#   mismatch —— PID 存活但身份校验不过，说明该 PID 已被别的进程复用
#   running  —— PID 存活且启动时刻 + 命令特征都对得上，确认是本服务
service_state() {
    local env="$1" pid_path meta_path pid
    pid_path=$(pid_file "$env")
    meta_path=$(meta_file "$env")

    if [ ! -f "$pid_path" ]; then
        echo missing
        return 0
    fi

    pid=$(tr -d '[:space:]' <"$pid_path")
    case "$pid" in
        '' | *[!0-9]*)
            echo invalid
            return 0
            ;;
    esac

    if ! kill -0 "$pid" 2>/dev/null; then
        echo stale
        return 0
    fi

    # 没有 meta 文件（例如旧版脚本留下的 pid 文件）时无法确认身份：
    # 宁可报错让人工确认，也不要贸然把一个未知进程当成本服务 kill 掉。
    if [ ! -f "$meta_path" ]; then
        echo mismatch
        return 0
    fi

    local recorded_identity='' recorded_starttime=''
    while IFS='=' read -r key value; do
        case "$key" in
            identity) recorded_identity="$value" ;;
            starttime) recorded_starttime="$value" ;;
        esac
    done <"$meta_path"

    if [ -z "$recorded_starttime" ] || [ "$recorded_starttime" != "$(proc_starttime "$pid")" ]; then
        echo mismatch
        return 0
    fi

    local token="${recorded_identity:-$IDENTITY}"
    case "$(proc_cmdline "$pid")" in
        *"$token"*) echo running ;;
        *) echo mismatch ;;
    esac
}

# stale/invalid 明确报告并清理；mismatch 报错返回 1，由调用方中止操作。
clear_stale() {
    local env="$1" state="$2"
    case "$state" in
        stale)
            echo "提示: ${NAME} (env=${env}) 的 pid 文件是陈旧残留（进程已退出），已清理 $(pid_file "$env")" >&2
            rm -f "$(pid_file "$env")" "$(meta_file "$env")"
            ;;
        invalid)
            echo "提示: ${NAME} (env=${env}) 的 pid 文件内容不是合法 PID，已清理 $(pid_file "$env")" >&2
            rm -f "$(pid_file "$env")" "$(meta_file "$env")"
            ;;
        mismatch)
            echo "错误: ${NAME} (env=${env}) 的 pid 文件记录的 PID $(cat "$(pid_file "$env")" 2>/dev/null) 当前属于另一个进程" >&2
            echo "      （启动时刻 / 命令特征校验不通过），拒绝把它当作本服务操作。" >&2
            echo "      确认无误后手工删除 $(pid_file "$env") 与 $(meta_file "$env") 再重试。" >&2
            return 1
            ;;
    esac
    return 0
}

# prod 启动前的严格校验：确认 funcoin 解析到的是**已安装的正式发布包**。
#
# 为什么只比对「路径不在本仓库目录下」不够：
#   1) 另一个源码 checkout 的 editable 安装同样不在本仓库目录下；
#   2) `.pth` 指回源码的 editable 安装，import 永远成功；
#   3) PYTHONPATH 注入的任意目录也能冒充。
# 所以这里四道一起上：清空 PYTHONPATH、用 `python3 -I`（隔离模式，CWD 与 user
# site 都不进 sys.path）、切到不含 Python 模块的 .run/ 目录执行、最后断言模块文件
# 落在 site-packages / dist-packages 下，并用 importlib.metadata 确认这个
# distribution 真的被安装过（同时拒绝 __editable__ 之流的 editable 垫片）。
# 注意不要用 `cd /` 跑校验：farlog 会在当前目录建相对 logs/，在 / 下会 PermissionError。
check_prod_installed() {
    mkdir -p "$RUN_DIR"
    if ! (
        cd "$RUN_DIR" || exit 1
        PYTHONPATH="" python3 -I - <<'PYCHECK'
import sys
from importlib import import_module
from importlib.metadata import PackageNotFoundError, distribution, version
from pathlib import Path

NAME = "funcoin"

try:
    module = import_module(NAME)
except Exception as exc:
    print(f"import {NAME} 失败: {exc.__class__.__name__}: {exc}", file=sys.stderr)
    raise SystemExit(1) from None

origin = getattr(module, "__file__", None)
if origin is None:
    print(f"{NAME} 没有 __file__，无法确认安装来源", file=sys.stderr)
    raise SystemExit(1)

resolved = Path(origin).resolve()
if not any(part in ("site-packages", "dist-packages") for part in resolved.parts):
    print(f"{NAME} 解析到 {resolved}，不在 site-packages/dist-packages 下", file=sys.stderr)
    print("（典型原因：editable 安装，或直接从源码工作树导入）", file=sys.stderr)
    raise SystemExit(1)

try:
    dist = distribution(NAME)
except PackageNotFoundError:
    print(f"{resolved} 存在，但查不到 {NAME} 的 distribution 元数据", file=sys.stderr)
    raise SystemExit(1) from None

# editable 安装会在 site-packages 留下 __editable__*.pth / __editable___*_finder.py，
# 真正的代码仍在源码树里；把记录的文件逐个过一遍，发现垫片就判不合格。
for file in dist.files or ():
    if Path(file).name.startswith("__editable__"):
        print(f"{NAME} 是 editable 安装（{file}），prod 不接受", file=sys.stderr)
        raise SystemExit(1)

print(f"{NAME} {version(NAME)} -> {resolved}")
PYCHECK
    ); then
        echo "错误: prod 要求运行已安装的 funcoin 正式发布包，当前校验未通过。" >&2
        echo "      请执行 pip install funcoin（不要用 -e）后再启动 prod；" >&2
        echo "      本地源码调试请改用 dev 环境。" >&2
        return 1
    fi
    return 0
}

# 前台运行。dev 下把本仓库 src/ 顶到 sys.path 最前，保证跑的就是本地改动。
start_cmd() {
    local env="$1"
    if [ "$env" = "prod" ]; then
        check_prod_installed
        cd "$RUN_DIR"
        PYTHONPATH="" exec python3 -c "$START_SNIPPET"
    else
        cd "$ROOT_DIR"
        export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
        exec python3 -c "$START_SNIPPET"
    fi
}

do_start() {
    local env="$1" state pid
    mkdir -p "$RUN_DIR"

    state=$(service_state "$env")
    if [ "$state" = "running" ]; then
        echo "错误: ${NAME} 已在运行 (env=${env}, pid $(cat "$(pid_file "$env")"))，拒绝重复启动" >&2
        exit 1
    fi
    clear_stale "$env" "$state"

    if [ "$env" = "prod" ]; then
        check_prod_installed
    fi

    if [ "$env" = "dev" ]; then
        (
            cd "$ROOT_DIR" || exit 1
            PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
                nohup python3 -c "$START_SNIPPET" >"$(log_file "$env")" 2>&1 &
            echo $! >"$RUN_DIR/.last.pid"
        )
    else
        (
            cd "$RUN_DIR" || exit 1
            PYTHONPATH="" nohup python3 -c "$START_SNIPPET" >"$(log_file "$env")" 2>&1 &
            echo $! >"$RUN_DIR/.last.pid"
        )
    fi
    pid=$(cat "$RUN_DIR/.last.pid")
    rm -f "$RUN_DIR/.last.pid"
    record_process "$env" "$pid"
    echo "${NAME} 已在后台启动 (env=${env}, pid ${pid})，日志: $(log_file "$env")"
}

do_stop() {
    local env="$1" state pid
    state=$(service_state "$env")
    if [ "$state" != "running" ]; then
        # mismatch 由 clear_stale 报错并返回 1（set -e 会据此中止），
        # 不能先打印「未在运行」误导使用者。
        clear_stale "$env" "$state"
        echo "${NAME} 未在运行 (env=${env})" >&2
        return 0
    fi

    pid=$(cat "$(pid_file "$env")")
    kill "$pid"
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "提示: ${NAME} (env=${env}, pid ${pid}) 在 10 秒内没有退出，发送 SIGKILL" >&2
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$(pid_file "$env")" "$(meta_file "$env")"
    echo "${NAME} 已停止 (env=${env})"
}

do_run() {
    mkdir -p "$RUN_DIR"
    start_cmd "$1"
}

do_status() {
    local env="$1"
    case "$(service_state "$env")" in
        running) echo "${NAME} 运行中 (env=${env}, pid $(cat "$(pid_file "$env")"))" ;;
        stale | invalid) echo "${NAME} 未运行 (env=${env}，存在陈旧 pid 文件 $(pid_file "$env"))" ;;
        mismatch) echo "${NAME} 状态未知 (env=${env}): pid 文件记录的 PID 已被其他进程复用，需人工确认" ;;
        *) echo "${NAME} 未运行 (env=${env})" ;;
    esac
}

action="${1:-}"
env="${2:-}"

case "$action" in
    start | stop | restart | run)
        [ "$env" = "dev" ] || [ "$env" = "prod" ] || usage
        ;;
esac

case "$action" in
    start) do_start "$env" ;;
    stop) do_stop "$env" ;;
    restart)
        do_stop "$env"
        do_start "$env"
        ;;
    run) do_run "$env" ;;
    status)
        if [ -n "$env" ]; then
            [ "$env" = "dev" ] || [ "$env" = "prod" ] || usage
            do_status "$env"
        else
            do_status dev
            do_status prod
        fi
        ;;
    *) usage ;;
esac
