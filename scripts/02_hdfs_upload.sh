#!/usr/bin/env bash
# Etapa A - envia as bases ao HDFS (os blocos sao distribuidos e replicados nos DataNodes/workers).
source "$(dirname "$0")/comum.sh"
hdfs dfs -mkdir -p "$HDFS_DADOS" "$HDFS_SAIDA"
for f in logs_exemplo logs_10k logs_1m logs_10m; do
  echo ">> put dados/$f.csv -> $HDFS_DADOS/"
  hdfs dfs -put -f "$RAIZ/dados/$f.csv" "$HDFS_DADOS/"
done
hdfs dfs -ls -h "$HDFS_DADOS"
echo; echo ">> Blocos e localizacao (DataNodes) da base de 10 milhoes:"
hdfs fsck "$HDFS_DADOS/logs_10m.csv" -files -blocks -locations | grep -E "blk_|Total blocks|Average block replication" || true
