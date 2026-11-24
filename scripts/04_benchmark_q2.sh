#!/usr/bin/env bash
# Secao 5 - tempo da Q2 em Hadoop e Spark para 10 mil, 1 milhao e 10 milhoes de registros (>= 3 execucoes).
# Uso: P=8 RUNS=3 ./scripts/04_benchmark_q2.sh
source "$(dirname "$0")/comum.sh"
RUNS="${RUNS:-3}"
P="${P:-8}"                                  # paralelismo fixo neste experimento
BASES="${BASES:-logs_10k logs_1m logs_10m}"
AQUECER="${AQUECER:-1}"                      # 1 = execucao de aquecimento descartada por framework

if [[ "$AQUECER" == 1 ]]; then
  echo ">> aquecimento (descartado)"
  CSV_TEMPOS="$RAIZ/resultados/aquecimento.csv" rodar_q2 hadoop logs_10k "$P" 0 aquecimento
  CSV_TEMPOS="$RAIZ/resultados/aquecimento.csv" rodar_q2 spark  logs_10k "$P" 0 aquecimento
fi

for base in $BASES; do
  for run in $(seq 1 "$RUNS"); do
    # alterna a ordem dos frameworks para nao favorecer sistematicamente nenhum deles (cache, carga)
    if (( run % 2 )); then ordem="hadoop spark"; else ordem="spark hadoop"; fi
    for fw in $ordem; do
      rodar_q2 "$fw" "$base" "$P" "$run" volume
    done
  done
done
echo "Tempos em $CSV_TEMPOS. Gere tabelas/graficos com: $PYTHON scripts/analisar.py"
