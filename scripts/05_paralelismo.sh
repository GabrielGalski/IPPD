#!/usr/bin/env bash
# Experimento adicional - varia o paralelismo configurado (P tarefas/executores) para a Q2.
# Uso: PS="1 2 4 8" BASES="logs_1m logs_10m" RUNS=3 ./scripts/05_paralelismo.sh
source "$(dirname "$0")/comum.sh"
RUNS="${RUNS:-3}"
PS="${PS:-1 2 4 8}"
BASES="${BASES:-logs_1m logs_10m}"

for base in $BASES; do
  for p in $PS; do
    for run in $(seq 1 "$RUNS"); do
      if (( run % 2 )); then ordem="hadoop spark"; else ordem="spark hadoop"; fi
      for fw in $ordem; do
        rodar_q2 "$fw" "$base" "$p" "$run" paralelismo
      done
    done
  done
done
echo "Tempos em $CSV_TEMPOS. Gere tabelas/graficos com: $PYTHON scripts/analisar.py"
