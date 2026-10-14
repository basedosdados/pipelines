#!/usr/bin/env bash
# Sobe/derruba a infra local do Prefect 3 pra testar flows sem precisar
# fazer deploy em dev na nuvem. Ver CONTRIBUTING.md, seção "Testar uma
# pipeline localmente".
#
# Uso:
#   scripts/dev_local.sh up       # sobe servidor + work pool + worker
#   scripts/dev_local.sh down     # derruba tudo
#   scripts/dev_local.sh status   # mostra se está rodando
#
# Depois de `up`, exporte a variável impressa na tela antes de rodar
# `deploy_flows.py --local` ou qualquer comando `prefect` manual:
#   export PREFECT_API_URL=http://127.0.0.1:4200/api
set -euo pipefail

cd "$(dirname "$0")/.."

# PREFECT_HOME escopado ao projeto, não ~/.prefect global — evita colidir
# com a config real (que aponta pro Prefect de verdade da BD) e evita um
# bug real já visto: ~/.prefect pertencer a outro usuário/root em algumas
# máquinas, quebrando o SQLite com "unable to open database file".
export PREFECT_HOME="$PWD/.prefect-local"
PORT="${PREFECT_LOCAL_PORT:-4200}"
POOL_NAME="local-dev"
PID_DIR="$PREFECT_HOME/pids"
SERVER_LOG="$PREFECT_HOME/server.log"
WORKER_LOG="$PREFECT_HOME/worker.log"

mkdir -p "$PID_DIR"

_is_running() {
    local pid_file="$1"
    [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

cmd_up() {
    export PREFECT_API_URL="http://127.0.0.1:${PORT}/api"

    if _is_running "$PID_DIR/server.pid"; then
        echo "Servidor já rodando (PID $(cat "$PID_DIR/server.pid"))."
    else
        echo "Subindo servidor Prefect local em http://127.0.0.1:${PORT} ..."
        nohup uv run prefect server start --host 127.0.0.1 --port "$PORT" \
            > "$SERVER_LOG" 2>&1 &
        echo $! > "$PID_DIR/server.pid"
        disown
        sleep 8
    fi

    if ! uv run prefect work-pool inspect "$POOL_NAME" > /dev/null 2>&1; then
        echo "Criando work pool '${POOL_NAME}' (tipo process)..."
        uv run prefect work-pool create "$POOL_NAME" --type process
    fi

    if _is_running "$PID_DIR/worker.pid"; then
        echo "Worker já rodando (PID $(cat "$PID_DIR/worker.pid"))."
    else
        echo "Subindo worker pro pool '${POOL_NAME}' ..."
        nohup uv run prefect worker start --pool "$POOL_NAME" \
            > "$WORKER_LOG" 2>&1 &
        echo $! > "$PID_DIR/worker.pid"
        disown
        sleep 5
    fi

    cat << EOF

Infra local pronta.

    export PREFECT_API_URL="http://127.0.0.1:${PORT}/api"

UI/dashboard: http://127.0.0.1:${PORT}
Logs: ${SERVER_LOG} / ${WORKER_LOG}

Pra deployar um flow pro pool local:
    uv run .github/workflows/scripts/deploy_flows.py --local --files pipelines/datasets/<dataset>/flows.py

Pra derrubar tudo: scripts/dev_local.sh down
EOF
}

cmd_down() {
    # `uv run prefect ... &` deixa o processo real (filho do `uv run`)
    # órfão se só o PID do `uv run` for morto — `pkill -f` com um padrão
    # específico pega pai e filho de uma vez, sem depender da relação de
    # parentesco entre eles.
    local patterns=(
        "prefect server start --host 127.0.0.1 --port ${PORT}"
        "prefect worker start --pool ${POOL_NAME}"
    )
    local found=0
    for pattern in "${patterns[@]}"; do
        if pkill -f -- "$pattern" 2>/dev/null; then
            found=1
        fi
    done
    if [ "$found" -eq 1 ]; then
        echo "Infra local derrubada (servidor + worker)."
    else
        echo "Nada rodando."
    fi
    rm -f "$PID_DIR/server.pid" "$PID_DIR/worker.pid"
}

cmd_status() {
    for name in server worker; do
        if _is_running "$PID_DIR/$name.pid"; then
            echo "$name: rodando (PID $(cat "$PID_DIR/$name.pid"))"
        else
            echo "$name: parado"
        fi
    done
}

case "${1:-}" in
    up) cmd_up ;;
    down) cmd_down ;;
    status) cmd_status ;;
    *)
        echo "Uso: $0 {up|down|status}" >&2
        exit 1
        ;;
esac
