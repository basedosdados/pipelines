#!/bin/sh
# Decode GCP credentials before starting the flow.
# basedosdados lib reads BASEDOSDADOS_CREDENTIALS_* directly (base64).
# dbt reads /credentials-dev/dev.json and /credentials-prod/prod.json.
# If DBT_SERVICE_ACCOUNT is set, use it for both dbt targets (dbt-rpc@ SA).
# Otherwise fall back to decoding BASEDOSDADOS_CREDENTIALS_* (dev worker behaviour).

mkdir -p /credentials-dev /credentials-prod

if [ -n "$DBT_SERVICE_ACCOUNT" ]; then
    echo "$DBT_SERVICE_ACCOUNT" | base64 -d > /credentials-dev/dev.json
    echo "$DBT_SERVICE_ACCOUNT" | base64 -d > /credentials-prod/prod.json
else
    [ -n "$BASEDOSDADOS_CREDENTIALS_STAGING" ] && \
        echo "$BASEDOSDADOS_CREDENTIALS_STAGING" | base64 -d > /credentials-dev/dev.json
    [ -n "$BASEDOSDADOS_CREDENTIALS_PROD" ] && \
        echo "$BASEDOSDADOS_CREDENTIALS_PROD" | base64 -d > /credentials-prod/prod.json
fi

exec "$@"
