#!/usr/bin/env bash
# Registra a configuracao do cluster (para o relatorio). Executar no master.
source "$(dirname "$0")/comum.sh"
mkdir -p "$RAIZ/resultados"
OUT="$RAIZ/resultados/ambiente.txt"
{
  echo "=== Data: $(date -Iseconds)   Host: $(hostname)"
  echo; echo "=== Versoes"; hadoop version | head -1; spark-submit --version 2>&1 | grep -m1 -i "version"; java -version 2>&1 | head -1; $PYTHON --version
  echo; echo "=== Master: CPU/memoria"; nproc; free -h 2>/dev/null || true
  echo; echo "=== HDFS (DataNodes, capacidade, replicacao)"; hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|Name:|Configured Capacity|DFS Used%" || true
  hdfs getconf -confKey dfs.replication; hdfs getconf -confKey dfs.blocksize
  echo; echo "=== YARN (NodeManagers, vcores e memoria por no)"; yarn node -list -all -showDetails 2>/dev/null || yarn node -list -all
  echo; echo "=== Configuracao usada nos experimentos"
  echo "SPARK_MASTER=$SPARK_MASTER SPLIT_MB=$SPLIT_MB EXEC_MEM=$EXEC_MEM DRIVER_MEM=$DRIVER_MEM"
} 2>&1 | tee "$OUT"
echo "Salvo em $OUT"
