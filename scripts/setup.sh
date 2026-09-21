#!/bin/bash
# funcoin 每日行情下载服务的统一生命周期管理入口。
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
RUN_DIR="$ROOT_DIR/.run"

usage() {
    echo "用法: $0 {start|stop|restart|run} {dev|prod}; $0 status [dev|prod]" >&2
    exit 1
}

pid_file() {
    printf '%s/funcoin-download-%s.pid' "$RUN_DIR" "$1"
}

log_file() {
    printf '%s/funcoin-download-%s.log' "$RUN_DIR" "$1"
}

is_running() {
    local env="$1" pid_file
    pid_file=$(pid_file "$env")
    [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null
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
        exec python3 -c "from funcoin.server.download import FunCoinDownload; FunCoinDownload().run()"
    else
        # dev 模式强制优先加载本仓库 src/ 下的源码，避免被系统/全局环境里
        # 恰好装着的其它 funcoin 版本掩盖，保证跑的就是本地改动。
        cd "$ROOT_DIR"
        export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
        exec python3 -c "from funcoin.server.download import FunCoinDownload; FunCoinDownload().run()"
    fi
}

do_start() {
    local env="$1"
    local pid_file log_file
    pid_file=$(pid_file "$env")
    log_file=$(log_file "$env")
    mkdir -p "$RUN_DIR"
    if is_running "$env"; then
        echo "funcoin-download 已在运行 (env=$env, pid $(cat "$pid_file"))" >&2
        exit 1
    fi
    [ "$env" = "prod" ] && check_prod_installed
    if [ "$env" = "dev" ]; then
        (cd "$ROOT_DIR" && PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" \
            nohup python3 -c "from funcoin.server.download import FunCoinDownload; FunCoinDownload().run()" \
            >"$log_file" 2>&1 & echo $! > "$pid_file")
    else
        nohup python3 -c "from funcoin.server.download import FunCoinDownload; FunCoinDownload().run()" \
            >"$log_file" 2>&1 &
        echo $! > "$pid_file"
    fi
    echo "funcoin-download 已在后台启动 (env=$env, pid $(cat "$pid_file"))"
}

do_stop() {
    local env="$1" pid_file
    pid_file=$(pid_file "$env")
    if ! is_running "$env"; then
        echo "funcoin-download 未在运行 (env=$env)" >&2
        rm -f "$pid_file"
        return
    fi
    kill "$(cat "$pid_file")"
    for _ in 1 2 3 4 5; do
        is_running "$env" || break
        sleep 1
    done
    rm -f "$pid_file"
    echo "funcoin-download 已停止 (env=$env)"
}

do_run() {
    local env="$1"
    mkdir -p "$RUN_DIR"
    start_cmd "$env"
}

do_status() {
    local env="$1" pid_file
    pid_file=$(pid_file "$env")
    if is_running "$env"; then
        echo "funcoin-download 运行中 (env=$env, pid $(cat "$pid_file"))"
    else
        echo "funcoin-download 未运行 (env=$env)"
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
        do_stop "$env"
        ;;
    restart)
        do_stop "$env" || true
        do_start "$env"
        ;;
    run)
        do_run "$env"
        ;;
    status)
        if [ -n "$env" ]; then
            [ "$env" = "dev" ] || [ "$env" = "prod" ] || usage
            do_status "$env"
        else
            do_status dev
            do_status prod
        fi
        ;;
    *)
        usage
        ;;
esac
