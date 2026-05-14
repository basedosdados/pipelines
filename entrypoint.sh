#!/bin/sh
# Decode BASEDOSDADOS_CREDENTIALS_STAGING into the file that dbt expects.
# basedosdados lib stores credentials as base64; we decode once to get the raw JSON.
if [ -n "$BASEDOSDADOS_CREDENTIALS_STAGING" ]; then
    mkdir -p /credentials-dev
    echo "$BASEDOSDADOS_CREDENTIALS_STAGING" | base64 -d > /credentials-dev/dev.json
fi

if [ -n "$BASEDOSDADOS_CREDENTIALS_PROD" ]; then
    mkdir -p /credentials-prod
    echo "$BASEDOSDADOS_CREDENTIALS_PROD" | base64 -d > /credentials-prod/prod.json
fi

exec "$@"
