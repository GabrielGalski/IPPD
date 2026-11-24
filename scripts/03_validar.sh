#!/usr/bin/env bash
# Etapa D - executa Q1-Q5 no Hadoop e no Spark sobre logs_exemplo.csv e compara com resultados_esperados.txt.
source "$(dirname "$0")/comum.sh"
P="${P:-2}"
ENT="$HDFS_DADOS/logs_exemplo.csv"
LOCAL="$RAIZ/resultados/validacao"
rm -rf "$LOCAL"; mkdir -p "$LOCAL/hadoop" "$LOCAL/spark" "$LOCAL/spark_rdd"

echo "==================== Hadoop MapReduce"
for classe in Q1TotalAcessos Q2AcessosPorIP Q3AcessosPorStatus Q4BytesPorRecurso Q5TopRecursos; do
  id="${classe:0:2}"; id="${id,,}"
  hadoop_q "$classe" "$P" "$ENT" "$HDFS_SAIDA/validacao/hadoop/$id" > "$LOCAL/hadoop/$id.log" 2>&1 \
    || { echo "FALHA em $classe (veja $LOCAL/hadoop/$id.log)"; continue; }
  hdfs dfs -getmerge "$HDFS_SAIDA/validacao/hadoop/$id" "$LOCAL/hadoop/$id.txt"
  echo "$classe ok"
done

echo "==================== Spark (DataFrame)"
spark_q "$P" "$ENT" "$HDFS_SAIDA/validacao/spark" Q1,Q2,Q3,Q4,Q5 --mostrar 2>"$LOCAL/spark/spark.log" | tee "$LOCAL/spark/stdout.txt"
for id in q1 q2 q3 q4 q5; do
  hdfs dfs -getmerge "$HDFS_SAIDA/validacao/spark/$id" "$LOCAL/spark/$id.txt" || true
done

echo "==================== Spark (RDD, apenas Q2)"
spark_q "$P" "$ENT" "$HDFS_SAIDA/validacao/spark_rdd" Q2 --api rdd 2>"$LOCAL/spark_rdd/spark.log" >/dev/null
hdfs dfs -getmerge "$HDFS_SAIDA/validacao/spark_rdd/q2" "$LOCAL/spark_rdd/q2.txt" || true

echo "==================== Comparacao com resultados_esperados.txt"
st=0
for impl in hadoop spark spark_rdd; do
  $PYTHON "$RAIZ/scripts/validar.py" --esperado "$RAIZ/resultados_esperados.txt" --dir "$LOCAL/$impl" --nome "$impl" || st=1
done
exit $st
