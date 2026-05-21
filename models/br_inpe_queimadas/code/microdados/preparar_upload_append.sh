#!/usr/bin/env bash
# Copia particoes novas (mar/2025+) de output/fire_data para output/upload_append.
# Rode a partir de models/br_inpe_queimadas/code/microdados apos o processing.py.

set -euo pipefail

BASE="./output/fire_data"
DEST="./output/upload_append"

echo "Limpando pasta de upload anterior (se existir)..."
rm -rf "${DEST}"
mkdir -p "${DEST}/ano=2025" "${DEST}/ano=2026"

echo "Copiando 2025: meses 3 a 12..."
for mes in 3 4 5 6 7 8 9 10 11 12; do
  if [[ -d "${BASE}/ano=2025/mes=${mes}" ]]; then
    cp -r "${BASE}/ano=2025/mes=${mes}" "${DEST}/ano=2025/"
    echo "  ok: ano=2025/mes=${mes}"
  else
    echo "  aviso: nao encontrado ano=2025/mes=${mes}"
  fi
done

echo "Copiando 2026: meses 1 a 5..."
for mes in 1 2 3 4 5; do
  if [[ -d "${BASE}/ano=2026/mes=${mes}" ]]; then
    cp -r "${BASE}/ano=2026/mes=${mes}" "${DEST}/ano=2026/"
    echo "  ok: ano=2026/mes=${mes}"
  else
    echo "  aviso: nao encontrado ano=2026/mes=${mes}"
  fi
done

echo ""
echo "Particoes prontas em: ${DEST}"
du -sh "${DEST}" 2>/dev/null || true
find "${DEST}" -type d -name 'mes=*' 2>/dev/null | sort
