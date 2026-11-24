#!/usr/bin/env bash
# Configuracao e funcoes compartilhadas. Use: source scripts/comum.sh
# Todas as variaveis podem ser sobrescritas pelo ambiente, ex.: SPARK_MASTER=spark://master:7077 ./scripts/04_benchmark_q2.sh
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HDFS_DADOS="${HDFS_DADOS:-/dados/logs}"     # onde ficam os CSV no HDFS
HDFS_SAIDA="${HDFS_SAIDA:-/saida}"          # onde os jobs gravam resultados
JAR="${JAR:-$RAIZ/hadoop/logs-mr.jar}"
SPARK_MASTER="${SPARK_MASTER:-yarn}"        # yarn | spark://<master>:7077 | local
SPLIT_MB="${SPLIT_MB:-32}"                  # tamanho maximo de split/particao de entrada (igual nos 2 frameworks)
EXEC_MEM="${EXEC_MEM:-1g}"
DRIVER_MEM="${DRIVER_MEM:-1g}"
PYTHON="${PYTHON:-python3}"
SPLIT_BYTES=$((SPLIT_MB * 1024 * 1024))
CSV_TEMPOS="${CSV_TEMPOS:-$RAIZ/resultados/tempos_q2.csv}"

agora()  { date +%s.%N; }
diff_s() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", b-a}'; }

registros_de() {
  case "$1" in
    logs_exemplo) echo 100 ;; logs_10k) echo 10000 ;; logs_1m) echo 1000000 ;; logs_10m) echo 10000000 ;; *) echo NA ;;
  esac
}

# hadoop_q <Classe> <P> <entrada> <saida> [-D extras...]
# P = paralelismo: no maximo P maps e P reduces simultaneos, e P redutores.
hadoop_q() {
  local classe="$1" p="$2" entrada="$3" saida="$4"; shift 4
  hadoop jar "$JAR" "logs.$classe" \
    -D mapreduce.job.reduces="$p" \
    -D mapreduce.job.running.map.limit="$p" \
    -D mapreduce.job.running.reduce.limit="$p" \
    -D mapreduce.input.fileinputformat.split.maxsize="$SPLIT_BYTES" \
    "$@" "$entrada" "$saida"
}

# spark_q <P> <entrada> <saida_base> <consultas> [args extras de consultas.py]
# P = paralelismo: P executores x 1 nucleo (YARN) / P nucleos totais (standalone) / local[P].
spark_q() {
  local p="$1" entrada="$2" saida="$3" consultas="$4"; shift 4
  local master="$SPARK_MASTER"
  local recursos=()
  case "$SPARK_MASTER" in
    yarn)      recursos=(--deploy-mode client --num-executors "$p" --executor-cores 1 --executor-memory "$EXEC_MEM") ;;
    spark://*) recursos=(--deploy-mode client --total-executor-cores "$p" --executor-cores 1 --executor-memory "$EXEC_MEM") ;;
    local*)    master="local[$p]" ;;
  esac
  spark-submit --master "$master" --driver-memory "$DRIVER_MEM" \
    ${recursos[@]+"${recursos[@]}"} \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.sql.shuffle.partitions="$p" \
    --conf spark.default.parallelism="$p" \
    --conf spark.sql.files.maxPartitionBytes="$SPLIT_BYTES" \
    "$RAIZ/spark/consultas.py" --entrada "$entrada" --saida "$saida" --consultas "$consultas" "$@"
}

# medir <log> <comando...> -> imprime segundos de parede (ou NA se falhar)
medir() {
  local log="$1"; shift
  local t0 t1
  t0=$(agora)
  if "$@" >"$log" 2>&1; then t1=$(agora); diff_s "$t0" "$t1"; else echo "NA"; fi
}

extrair() { sed -n "s/.*$1=\([0-9.]*\).*/\1/p" "$2" | tail -1; }

# rodar_q2 <hadoop|spark> <base> <P> <execucao> <experimento>
# Mede de ponta a ponta: submissao/inicializacao + leitura do HDFS + processamento + escrita no HDFS.
rodar_q2() {
  local fw="$1" base="$2" p="$3" run="$4" exp="$5"
  local entrada="$HDFS_DADOS/$base.csv"
  local saida="$HDFS_SAIDA/$exp/$fw/${base}_p${p}"
  local log="$RAIZ/resultados/logs/${exp}_${fw}_${base}_p${p}_r${run}.log"
  mkdir -p "$RAIZ/resultados/logs"
  [[ -f "$CSV_TEMPOS" ]] || echo "experimento,framework,base,registros,paralelismo,execucao,tempo_total_s,tempo_interno_s,data" > "$CSV_TEMPOS"

  hdfs dfs -rm -r -f -skipTrash "$saida" >/dev/null 2>&1 || true   # fora da medicao
  local t interno
  if [[ "$fw" == hadoop ]]; then
    t=$(medir "$log" hadoop_q Q2AcessosPorIP "$p" "$entrada" "$saida")
    interno=$(extrair TEMPO_JOB_S "$log" || true)
  else
    t=$(medir "$log" spark_q "$p" "$entrada" "$saida" Q2)
    interno=$(extrair TEMPO_TOTAL_INTERNO_S "$log" || true)
  fi
  echo "$exp,$fw,$base,$(registros_de "$base"),$p,$run,$t,${interno:-NA},$(date -Iseconds)" >> "$CSV_TEMPOS"
  echo "[$exp] $fw $base P=$p exec=$run -> total ${t}s | interno ${interno:-NA}s  (log: $log)"
  [[ "$t" != NA ]] || echo "  !! FALHOU - veja o log acima"
}
