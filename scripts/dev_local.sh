#!/usr/bin/env bash
# Sobe/derruba a infra local do Prefect 3 pra testar flows sem precisar
# fazer deploy em dev na nuvem. Ver CONTRIBUTING.md, seção "Testar uma
# pipeline localmente".
#
# Uso:
#   scripts/dev_local.sh up       # carrega .env (Vault), sobe servidor +
#                                  # work pool + worker, e já deploya os
#                                  # flows dos arquivos .py alterados em
#                                  # relação a origin/main (commitados ou não)
#   scripts/dev_local.sh up --all # idem, mas deploya TODOS os flows do repo
#                                  # (mais lento, ~330 deployments)
#   scripts/dev_local.sh down     # derruba tudo
#   scripts/dev_local.sh status   # mostra se está rodando
#
# Depois de `up`, exporte a variável impressa na tela antes de rodar
# qualquer comando `prefect` manual (ou um redeploy incremental de um
# único dataset depois de editar algo):
#   export PREFECT_API_URL=http://127.0.0.1:4200/api
set -euo pipefail

cd "$(dirname "$0")/.."

# Falha rápido e claro em vez de uma cascata de erros confusos (servidor
# e worker rodam em `nohup ... &`, então um `uv` não encontrado neles
# falha em silêncio, só visível no log — e o script seguiria até o fim
# imprimindo "Infra local pronta" mesmo sem nada ter funcionado de
# verdade). Causa comum: terminal aberto antes do PATH do `uv` ser
# configurado — abra um terminal novo (ou rode `uv --version` aqui pra
# confirmar) antes de tentar de novo.
if ! command -v uv > /dev/null 2>&1; then
    echo "ERRO: 'uv' não encontrado no PATH deste terminal." >&2
    echo "Confirme rodando 'uv --version' aqui — se também falhar, abra um terminal novo (ou reinstale/reconfigure o uv) antes de rodar este script." >&2
    exit 1
fi

# PREFECT_HOME escopado ao projeto, não ~/.prefect global — evita colidir
# com a config real (que aponta pro Prefect de verdade da BD) e evita um
# bug real já visto: ~/.prefect pertencer a outro usuário/root em algumas
# máquinas, quebrando o SQLite com "unable to open database file".
export PREFECT_HOME="$PWD/.prefect-local"
# Se customizar via PREFECT_LOCAL_PORT, exporte o mesmo valor em todo
# comando desta sessão (up/down/status) — down identifica o processo
# pela porta, então um valor diferente do usado no up não o encontra.
PORT="${PREFECT_LOCAL_PORT:-4200}"
POOL_NAME="local-dev"
PID_DIR="$PREFECT_HOME/pids"
SERVER_LOG="$PREFECT_HOME/server.log"
WORKER_LOG="$PREFECT_HOME/worker.log"
DEPLOY_LOG="$PREFECT_HOME/last_deploy.log"

# Falha rápido e claro em vez de "Permission denied" sem contexto ao
# gravar server.pid/server.log — acontece se uma execução anterior rodou
# como outro usuário (ex. sudo por engano), deixando .prefect-local/ de
# propriedade dele.
if [ -e "$PREFECT_HOME" ] && [ ! -w "$PREFECT_HOME" ]; then
    echo "ERRO: '$PREFECT_HOME' existe mas não é gravável por você." >&2
    echo "Causa provável: uma execução anterior rodou como outro usuário (ex. sudo por engano)." >&2
    echo "Resolva com: sudo chown -R \"\$USER\" $PREFECT_HOME" >&2
    echo "ou, mais simples (o estado ali é descartável): rm -rf $PREFECT_HOME" >&2
    exit 1
fi

# Flows genéricos/compartilhados (pipelines/utils/) que qualquer dataset
# migrado pode disparar via run_deployment() no meio da cadeia
# (build_and_promote, run_dbt_model, transferência pra prod) — sempre
# deployados no modo padrão do `up`, mesmo sem mudança neles, porque sem
# isso a cadeia check_update -> extract_and_load -> build_and_promote
# quebra com "deployment não encontrado" assim que chega nesse estágio.
# Via --files (não --changed): --changed escalaria pra --all sozinho,
# já que esses caminhos começam com o prefixo compartilhado
# pipelines/utils/ (ver SHARED_PREFIXES em deploy_flows.py).
ALWAYS_DEPLOY_FILES=(
    pipelines/utils/metadata/flows.py
    pipelines/utils/execute_dbt_model/flows.py
    pipelines/utils/materialize_prod/flows.py
)

mkdir -p "$PID_DIR"

_is_running() {
    local pid_file="$1"
    [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null
}

# Confere que o processo subido em `nohup ... &` continua vivo depois do
# `sleep` — sem isso, uma falha silenciosa em background (ex. `uv` não
# encontrado nesse subprocesso específico) passa despercebida e o script
# segue até o fim dizendo "Infra local pronta" mesmo sem nada no ar.
_require_alive() {
    local pid_file="$1" log_file="$2" label="$3"
    if ! _is_running "$pid_file"; then
        echo "ERRO: $label caiu logo depois de subir. Veja o motivo em: $log_file" >&2
        tail -n 20 "$log_file" >&2 2>/dev/null || true
        exit 1
    fi
}

# Roda um deploy mostrando a saída normalmente (`tee`) e, se falhar,
# acrescenta as linhas "✗ Falha ao registrar ..." no acumulador
# `failures` da função que chamou (variável `local` do bash é visível
# pra quem ela chama) — pra listar no aviso final em vez de só dizer
# "1+ flow falhou" sem dizer quais.
_run_deploy() {
    if uv run .github/workflows/scripts/deploy_flows.py "$@" --workers 4 2>&1 | tee "$DEPLOY_LOG"; then
        return 0
    fi
    failures+="$(grep '✗ Falha' "$DEPLOY_LOG" || true)"$'\n'
    return 1
}

# Lista os .py alterados em relação a origin/main — commitados nesta branch
# ou ainda só no working tree (inclusive arquivo novo não rastreado). Base
# pro deploy seletivo do `up`: reaproveita a mesma lógica de --changed que
# o deploy de CI/prod já usa (expand_changed_files() em deploy_flows.py),
# que expande pra pasta do dataset inteira e escala sozinho pra --all se a
# mudança for em infra compartilhada (pipelines/utils/, pipelines/crawler/).
_changed_python_files() {
    git fetch origin main --quiet 2>/dev/null || true

    local base
    base="$(git merge-base origin/main HEAD 2>/dev/null)" \
        || base="$(git merge-base main HEAD 2>/dev/null)" \
        || return 0

    {
        git diff --name-only "$base" -- '*.py'
        git ls-files --others --exclude-standard -- '*.py'
    } | sort -u
}

cmd_up() {
    local deploy_mode="${1:-}"
    export PREFECT_API_URL="http://127.0.0.1:${PORT}/api"

    # `check_update`/`extract_and_load` precisam do que vem do .env —
    # Vault (VAULT_ADDRESS/VAULT_TOKEN, pra autenticar no backend) e
    # GOOGLE_APPLICATION_CREDENTIALS (pra escrever de verdade no GCS/
    # BigQuery dev). Sourcear o arquivo inteiro é o jeito robusto de
    # pegar tudo isso sem parsing manual frágil.
    if [ -f .env ]; then
        set -a
        # shellcheck disable=SC1091
        source .env
        set +a
        echo "Credenciais de .env carregadas."
    else
        echo "Aviso: .env não encontrado — check_update vai falhar sem VAULT_ADDRESS/VAULT_TOKEN (ver CONTRIBUTING.md)."
    fi
    # CRÍTICO, incondicional, sempre por último: `.env` também seta
    # PREFECT_API_URL/PREFECT_API_KEY pra produção — sourcear o arquivo
    # sem isso faria servidor/worker/deploy abaixo falarem com o Prefect
    # de produção em vez do local.
    export PREFECT_API_URL="http://127.0.0.1:${PORT}/api"
    unset PREFECT_API_KEY

    # Trava de segurança (defesa em profundidade, não deveria nunca disparar
    # dado o reforço acima): se por algum motivo PREFECT_API_URL não for
    # local, aborta em vez de arriscar mandar deploy/schedule pra produção.
    case "$PREFECT_API_URL" in
        http://127.0.0.1:*|http://localhost:*) ;;
        *)
            echo "ERRO: PREFECT_API_URL não é local (${PREFECT_API_URL}) — abortando." >&2
            exit 1
            ;;
    esac

    if _is_running "$PID_DIR/server.pid"; then
        echo "Servidor já rodando (PID $(cat "$PID_DIR/server.pid"))."
    else
        echo "Subindo servidor Prefect local em http://127.0.0.1:${PORT} ..."
        nohup uv run prefect server start --host 127.0.0.1 --port "$PORT" \
            > "$SERVER_LOG" 2>&1 &
        echo $! > "$PID_DIR/server.pid"
        disown
        sleep 8
        _require_alive "$PID_DIR/server.pid" "$SERVER_LOG" "Servidor Prefect"
    fi

    if ! uv run prefect work-pool inspect "$POOL_NAME" > /dev/null 2>&1; then
        echo "Criando work pool '${POOL_NAME}' (tipo process)..."
        # `|| true`: se o servidor ainda não respondia quando o `inspect`
        # acima rodou (ex. subiu agora mesmo, `sleep 8` nem sempre é
        # suficiente numa máquina mais carregada), ele falha achando que
        # o pool não existe — mas o pool JÁ existe de verdade (persistiu
        # em .prefect-local/), então o `create` abaixo falha com "already
        # exists". Sem o `|| true`, o `set -e` do topo do arquivo aborta o
        # `up` inteiro aqui, antes até de subir o worker.
        uv run prefect work-pool create "$POOL_NAME" --type process || true
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
        _require_alive "$PID_DIR/worker.pid" "$WORKER_LOG" "Worker"
    fi

    # --workers baixo (default do script é 8): o backend aqui é SQLite via
    # um único servidor local, não a API real — muita escrita concorrente
    # já causou 503 (Service Unavailable) em alguns deployments ao testar
    # com 8. `if !` em vez de deixar o `set -e` do topo do arquivo abortar
    # o `up` inteiro por causa de 1-2 flows que falharem — o resto já foi
    # registrado, e dá pra redeployar só o que faltou com --files depois.
    local deployed=1 failures=""
    if [ "$deploy_mode" = "--all" ]; then
        echo "Deployando TODOS os flows pro pool local (--all) ..."
        _run_deploy --local --all || deployed=0
    else
        echo "Deployando flows genéricos compartilhados (build_and_promote, run_dbt_model, transfer pra prod) ..."
        _run_deploy --local --files "${ALWAYS_DEPLOY_FILES[@]}" || deployed=0

        local changed
        changed="$(_changed_python_files)"
        if [ -z "$changed" ]; then
            echo "Nenhum .py alterado em relação a origin/main — nenhum dataset deployado automaticamente."
            echo "Deploye o que precisar na mão (ver dicas abaixo), ou rode 'scripts/dev_local.sh up --all' pra deployar tudo."
        else
            echo "Deployando flows dos arquivos alterados em relação a origin/main:"
            echo "$changed" | sed 's/^/  /'
            # shellcheck disable=SC2086
            _run_deploy --local --changed $changed || deployed=0
        fi
    fi

    if [ "$deployed" -eq 0 ]; then
        echo
        echo "Aviso: 1+ flow falhou ao deployar (motivo exato abaixo — às vezes é 503 por concorrência no SQLite local, mas nem sempre):"
        echo "$failures" | sed '/^$/d'
        echo "Redeploye só ele com: uv run .github/workflows/scripts/deploy_flows.py --local --files <arquivo>"
    fi

    cat << EOF

Infra local pronta (ver aviso acima se algum deploy falhou).

    export PREFECT_API_URL="http://127.0.0.1:${PORT}/api"

UI/dashboard: http://127.0.0.1:${PORT}
Logs: ${SERVER_LOG} / ${WORKER_LOG}

Editou um dataset e quer só redeployar ele (mais rápido que rodar tudo de novo):
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
    up)
        shift
        cmd_up "${1:-}"
        ;;
    down) cmd_down ;;
    status) cmd_status ;;
    *)
        echo "Uso: $0 {up [--all]|down|status}" >&2
        exit 1
        ;;
esac
