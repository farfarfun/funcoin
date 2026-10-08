#!/usr/bin/env bash
# funcoin 每日行情下载服务的统一安装与生命周期管理入口。
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
RUN_DIR="$ROOT_DIR/.run"
NAME="funcoin-download"
PID_FILE="$RUN_DIR/$NAME.pid"
META_FILE="$RUN_DIR/$NAME.meta"
LOG_FILE="$RUN_DIR/$NAME.log"

usage() {
    echo "用法: $0 {start|stop|restart|run|status}" >&2
    echo "      $0 {install-dev|publish}" >&2
    echo "      $0 install-prod [version]" >&2
    exit 1
}

require_funbuild() {
    command -v funbuild >/dev/null 2>&1 || {
        echo "错误: 需要先安装 funbuild" >&2
        exit 1
    }
}

installed_cli() {
    command -v "$NAME" || {
        echo "错误: 未安装 $NAME，请先执行 install-dev 或 install-prod" >&2
        return 1
    }
}

# 运行期只接受非 editable 的已安装包，不能从当前源码树或 PYTHONPATH 回退。
check_installed() {
    mkdir -p "$RUN_DIR"
    installed_cli >/dev/null
    if ! (
        cd "$RUN_DIR" || exit 1
        PYTHONPATH="" python3 -I - <<'PYCHECK'
import sys
from importlib import import_module
from importlib.metadata import distribution
from pathlib import Path

name = "funcoin"
try:
    module = import_module(name)
    dist = distribution(name)
except Exception as exc:
    print(f"无法加载已安装的 {name}: {exc}", file=sys.stderr)
    raise SystemExit(1) from None

module_file = getattr(module, "__file__", None)
if module_file is None:
    print(f"{name} 没有 __file__，无法确认安装来源", file=sys.stderr)
    raise SystemExit(1)
origin = Path(module_file).resolve()
if not any(part in ("site-packages", "dist-packages") for part in origin.parts):
    print(f"{name} 解析到 {origin}，不是已安装包", file=sys.stderr)
    raise SystemExit(1)
if any(Path(file).name.startswith("__editable__") for file in dist.files or ()):
    print(f"{name} 是 editable 安装，运行期不接受", file=sys.stderr)
    raise SystemExit(1)
PYCHECK
    ); then
        echo "错误: 请先执行 install-dev 或 install-prod，运行命令不会回退到源码目录" >&2
        return 1
    fi
}

installed_version() {
    mkdir -p "$RUN_DIR"
    (
        cd "$RUN_DIR" || exit 1
        PYTHONPATH="" python3 -I -c \
            'from importlib.metadata import version; print(version("funcoin"))' \
            2>/dev/null
    ) || echo "未安装"
}

proc_starttime() {
    local pid="$1"
    if [ -r "/proc/${pid}/stat" ]; then
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
    local pid="$1" identity="$2"
    printf '%s\n' "$pid" >"$PID_FILE"
    {
        printf 'identity=%s\n' "$identity"
        printf 'starttime=%s\n' "$(proc_starttime "$pid")"
    } >"$META_FILE"
}

service_state() {
    local pid recorded_identity='' recorded_starttime=''
    [ -f "$PID_FILE" ] || {
        echo missing
        return 0
    }

    pid=$(tr -d '[:space:]' <"$PID_FILE")
    case "$pid" in
        '' | *[!0-9]*)
            echo invalid
            return 0
            ;;
    esac
    kill -0 "$pid" 2>/dev/null || {
        echo stale
        return 0
    }
    [ -f "$META_FILE" ] || {
        echo mismatch
        return 0
    }

    while IFS='=' read -r key value; do
        case "$key" in
            identity) recorded_identity="$value" ;;
            starttime) recorded_starttime="$value" ;;
        esac
    done <"$META_FILE"

    if [ -z "$recorded_identity" ] || [ -z "$recorded_starttime" ] || \
        [ "$recorded_starttime" != "$(proc_starttime "$pid")" ]; then
        echo mismatch
        return 0
    fi
    case "$(proc_cmdline "$pid")" in
        *"$recorded_identity"*) echo running ;;
        *) echo mismatch ;;
    esac
}

clear_stale() {
    local state="$1"
    case "$state" in
        stale | invalid)
            echo "提示: $NAME 的 pid 文件已失效，现已清理" >&2
            rm -f "$PID_FILE" "$META_FILE"
            ;;
        mismatch)
            echo "错误: $PID_FILE 指向的进程不属于 $NAME，拒绝操作" >&2
            echo "      请人工确认后删除 $PID_FILE 与 $META_FILE" >&2
            return 1
            ;;
    esac
}

guard_legacy_pid_files() {
    local legacy
    for legacy in "$RUN_DIR/$NAME-dev.pid" "$RUN_DIR/$NAME-prod.pid"; do
        if [ -e "$legacy" ]; then
            echo "错误: 发现旧版环境 PID 文件 $legacy" >&2
            echo "      请用旧版脚本停止对应服务，或确认进程已退出后删除旧 PID/meta 文件" >&2
            return 1
        fi
    done
}

do_start() {
    local state pid cli
    mkdir -p "$RUN_DIR"
    check_installed
    cli=$(installed_cli)
    state=$(service_state)
    if [ "$state" = "running" ]; then
        echo "错误: $NAME 已在运行 (pid $(cat "$PID_FILE"))，拒绝重复启动" >&2
        return 1
    fi
    clear_stale "$state"

    (
        cd "$RUN_DIR" || exit 1
        PYTHONPATH="" nohup "$cli" run >"$LOG_FILE" 2>&1 &
        echo $! >"$RUN_DIR/.last.pid"
    )
    pid=$(cat "$RUN_DIR/.last.pid")
    rm -f "$RUN_DIR/.last.pid"
    record_process "$pid" "$cli"
    echo "$NAME 已在后台启动 (version=$(installed_version), pid $pid)，日志: $LOG_FILE"
}

do_stop() {
    local state pid
    state=$(service_state)
    if [ "$state" != "running" ]; then
        clear_stale "$state"
        echo "$NAME 未在运行"
        return 0
    fi

    pid=$(cat "$PID_FILE")
    kill "$pid"
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "提示: $NAME (pid $pid) 在 10 秒内没有退出，发送 SIGKILL" >&2
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE" "$META_FILE"
    echo "$NAME 已停止"
}

do_run() {
    local cli
    check_installed
    cli=$(installed_cli)
    cd "$RUN_DIR"
    PYTHONPATH="" exec "$cli" run
}

do_status() {
    case "$(service_state)" in
        running) echo "$NAME $(installed_version): 运行中 (pid $(cat "$PID_FILE"))" ;;
        stale | invalid) echo "$NAME $(installed_version): 未运行（存在陈旧 pid 文件 $PID_FILE）" ;;
        mismatch) echo "$NAME $(installed_version): 状态未知，pid 文件对应进程身份不匹配" ;;
        *) echo "$NAME $(installed_version): 未运行" ;;
    esac
}

action="${1:-}"
version="${2:-}"

case "$action" in
    install-dev)
        [ "$#" -eq 1 ] || usage
        require_funbuild
        cd "$ROOT_DIR"
        exec funbuild install
        ;;
    install-prod)
        [ "$#" -le 2 ] || usage
        exec python3 -m pip install "funcoin${version:+==$version}"
        ;;
    publish)
        [ "$#" -eq 1 ] || usage
        require_funbuild
        cd "$ROOT_DIR"
        exec funbuild build
        ;;
    start | stop | restart | run | status)
        [ "$#" -eq 1 ] || usage
        guard_legacy_pid_files
        ;;
    *) usage ;;
esac

case "$action" in
    start) do_start ;;
    stop) do_stop ;;
    restart)
        do_stop
        do_start
        ;;
    run) do_run ;;
    status) do_status ;;
esac
