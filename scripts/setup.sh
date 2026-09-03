#!/bin/bash
# funcoin 每日行情下载服务的统一生命周期管理入口。
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
RUN_DIR="$ROOT_DIR/.run"
PID_FILE="$RUN_DIR/funcoin-download.pid"
LOG_FILE="$RUN_DIR/funcoin-download.log"

usage() {
    echo "用法: $0 {start|stop|restart|run|status} {dev|prod}" >&2
    exit 1
}

is_running() {
    [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

check_prod_installed() {
    # prod 只能跑已安装的正式包，不能回退到本仓库源码；
    # 通过比对 funcoin 包的实际加载路径是否落在本仓库目录内来判断。
    python3 - "$ROOT_DIR" <<'PYEOF'
import os
import sys

root_dir = os.path.realpath(sys.argv[1])
try:
    import funcoin
except ImportError:
    print("error: 未安装 funcoin 正式包，请先 pip install funcoin（或 uv pip install funcoin）", file=sys.stderr)
    sys.exit(1)

pkg_path = os.path.realpath(funcoin.__file__)
if pkg_path.startswith(root_dir + os.sep):
    print(
        "error: 当前 funcoin 是从本仓库源码目录加载的（{}），".format(pkg_path)
        + "prod 模式禁止直接跑源码，请先安装正式发布包",
        file=sys.stderr,
    )
    sys.exit(1)
PYEOF
}

start_cmd() {
    local env="$1"
    if [ "$env" = "prod" ]; then
        check_prod_installed
        python3 -c "from funcoin.server.download import FunCoinDownload; FunCoinDownload().run()"
    else
        # dev 模式强制优先加载本仓库 src/ 下的源码，避免被系统/全局环境里
        # 恰好装着的其它 funcoin 版本掩盖，保证跑的就是本地改动。
        (cd "$ROOT_DIR" && PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" python3 -c "from funcoin.server.download import FunCoinDownload; FunCoinDownload().run()")
    fi
}

do_start() {
    local env="$1"
    mkdir -p "$RUN_DIR"
    if is_running; then
        echo "funcoin-download 已在运行 (pid $(cat "$PID_FILE"))" >&2
        exit 1
    fi
    # nohup 起的是一个全新的 bash 进程，不会继承当前 shell 里的变量/函数，
    # 必须显式 export，否则子进程里 ROOT_DIR 为空。
    export ROOT_DIR
    export -f start_cmd check_prod_installed
    nohup bash -c "start_cmd '$env'" >"$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "funcoin-download 已在后台启动 (env=$env, pid $(cat "$PID_FILE"))"
}

do_stop() {
    if ! is_running; then
        echo "funcoin-download 未在运行" >&2
        rm -f "$PID_FILE"
        return
    fi
    kill "$(cat "$PID_FILE")"
    rm -f "$PID_FILE"
    echo "funcoin-download 已停止"
}

do_run() {
    local env="$1"
    mkdir -p "$RUN_DIR"
    start_cmd "$env"
}

do_status() {
    if is_running; then
        echo "funcoin-download 运行中 (pid $(cat "$PID_FILE"))"
    else
        echo "funcoin-download 未运行"
    fi
}

action="${1:-}"
env="${2:-}"

case "$action" in
    start|stop|restart|run)
        [ "$env" = "dev" ] || [ "$env" = "prod" ] || usage
        ;;
esac

case "$action" in
    start)
        do_start "$env"
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_stop || true
        do_start "$env"
        ;;
    run)
        do_run "$env"
        ;;
    status)
        do_status
        ;;
    *)
        usage
        ;;
esac
