#!/bin/sh
# Decode GCP credentials before starting the flow.
# basedosdados lib reads BASEDOSDADOS_CREDENTIALS_* directly (base64).
# dbt reads /credentials-dev/dev.json and /credentials-prod/prod.json.
# If DBT_SERVICE_ACCOUNT is set, use it for both dbt targets (dbt-rpc@ SA).
# Otherwise fall back to decoding BASEDOSDADOS_CREDENTIALS_* (dev worker behaviour).
set -eu

umask 077
mkdir -p /credentials-dev /credentials-prod

decode() {
    src="$1" dst="$2"
    printf '%s' "$src" | base64 -d > "$dst" || {
        echo "Erro ao decodificar credenciais para $dst" >&2
        rm -f "$dst"
        exit 1
    }
}

if [ -n "${DBT_SERVICE_ACCOUNT:-}" ]; then
    decode "$DBT_SERVICE_ACCOUNT" /credentials-dev/dev.json
    decode "$DBT_SERVICE_ACCOUNT" /credentials-prod/prod.json
else
    [ -n "${BASEDOSDADOS_CREDENTIALS_STAGING:-}" ] && \
        decode "$BASEDOSDADOS_CREDENTIALS_STAGING" /credentials-dev/dev.json
    [ -n "${BASEDOSDADOS_CREDENTIALS_PROD:-}" ] && \
        decode "$BASEDOSDADOS_CREDENTIALS_PROD" /credentials-prod/prod.json
fi

exec "$@"
