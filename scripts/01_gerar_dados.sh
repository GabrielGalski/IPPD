#!/usr/bin/env bash
# Gera as bases experimentais no disco local do master (o tempo de geracao NAO entra nas medicoes).
source "$(dirname "$0")/comum.sh"
mkdir -p "$RAIZ/dados"
cp -f "$RAIZ/logs_exemplo.csv" "$RAIZ/dados/logs_exemplo.csv"
for spec in "10000 logs_10k" "1000000 logs_1m" "10000000 logs_10m"; do
  set -- $spec
  if [[ -f "$RAIZ/dados/$2.csv" ]]; then echo "ja existe: dados/$2.csv"; continue; fi
  # parametros padrao: --semente 42 --ips 500 (registrar no relatorio se alterar)
  $PYTHON "$RAIZ/gerar_logs.py" --registros "$1" --saida "$RAIZ/dados/$2.csv"
done
ls -lh "$RAIZ/dados"
wc -l "$RAIZ"/dados/*.csv   # = registros + 1 (cabecalho)
