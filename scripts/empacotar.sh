#!/usr/bin/env bash
# Gera o pacote de entrega (sem as bases grandes). Uso: ./scripts/empacotar.sh [nome]
set -euo pipefail
RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
NOME="${1:-entrega_logs_hadoop_spark}"
cd "$(dirname "$RAIZ")"
tar czf "$NOME.tgz" \
  --exclude="$(basename "$RAIZ")/dados" \
  --exclude="$(basename "$RAIZ")/hadoop/build" \
  --exclude="$(basename "$RAIZ")/hadoop/logs-mr.jar" \
  --exclude="*.tgz" --exclude="__pycache__" \
  "$(basename "$RAIZ")"
echo "Pacote: $(pwd)/$NOME.tgz"; tar tzf "$NOME.tgz" | head -50
