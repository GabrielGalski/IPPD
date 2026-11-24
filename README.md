# Análise distribuída de logs de acesso com Hadoop MapReduce e Apache Spark

Todos os comandos abaixo são executados **no nó master** do cluster (1 master + até 9 workers).
Os workers executam apenas os daemons (DataNode/NodeManager ou Spark Worker): você não precisa entrar neles.

## Estrutura

```
gerar_logs.py, logs_exemplo.csv, resultados_esperados.txt   (fornecidos)
hadoop/src/logs/      Q1..Q5 em Java (API MapReduce) + LogParser, SomaReducer, JobUtil
hadoop/build.sh       compila e gera hadoop/logs-mr.jar
spark/consultas.py    Q1..Q5 em PySpark (DataFrame; Q2 também em RDD com --api rdd)
scripts/              automação: info do cluster, geração, HDFS, validação, benchmark, análise, pacote
resultados/           ambiente.txt, validacao/, tempos_q2.csv, tabelas.md, gráficos, logs/
relatorio/            relatório (até 4 páginas)
```

## 0. Preparação (no master)

```bash
# copiar o projeto para o master (ex.: scp/docker cp) e entrar na pasta
sed -i 's/\r$//' scripts/*.sh hadoop/build.sh    # remove CRLF se os arquivos vieram do Windows
chmod +x scripts/*.sh hadoop/build.sh
./scripts/00_info_cluster.sh                     # registra versões, DataNodes, NodeManagers -> resultados/ambiente.txt
```

Variáveis configuráveis (ver `scripts/comum.sh`): `SPARK_MASTER` (padrão `yarn`; use `spark://master:7077`
se o cluster for Spark standalone), `HDFS_DADOS=/dados/logs`, `HDFS_SAIDA=/saida`, `SPLIT_MB=32`,
`EXEC_MEM=1g`, `DRIVER_MEM=1g`, `PYTHON=python3`.

## 1. Gerar dados e enviar ao HDFS (Etapa A)

```bash
./scripts/01_gerar_dados.sh      # python3 gerar_logs.py --registros {10000|1000000|10000000} --saida dados/logs_*.csv
./scripts/02_hdfs_upload.sh      # hdfs dfs -mkdir -p /dados/logs ; hdfs dfs -put -f dados/*.csv /dados/logs/
```

Parâmetros do gerador: semente 42, 500 IPs (padrão). `logs_exemplo.csv` foi gerado com `--ips 12` (ver resultados_esperados.txt).

## 2. Compilar o MapReduce (Etapa B)

```bash
./hadoop/build.sh                # javac -cp $(hadoop classpath) ... ; jar cf hadoop/logs-mr.jar
# execução manual de uma consulta:
hadoop jar hadoop/logs-mr.jar logs.Q2AcessosPorIP -D mapreduce.job.reduces=4 \
       /dados/logs/logs_exemplo.csv /saida/manual/q2
hdfs dfs -cat /saida/manual/q2/part-r-*
```

Classes: `logs.Q1TotalAcessos`, `logs.Q2AcessosPorIP`, `logs.Q3AcessosPorStatus`, `logs.Q4BytesPorRecurso` (opcional),
`logs.Q5TopRecursos` (opcional). Combinador ligado por padrão (`-D logs.combinador=false` para desligar).

## 3. Spark (Etapa C)

```bash
spark-submit --master yarn spark/consultas.py \
  --entrada /dados/logs/logs_exemplo.csv --saida /saida/manual/spark --consultas Q1,Q2,Q3,Q4,Q5 --mostrar
```

## 4. Validação (Etapa D)

```bash
./scripts/03_validar.sh          # roda Q1..Q5 nos dois frameworks e compara com resultados_esperados.txt
```

Saídas em `resultados/validacao/{hadoop,spark,spark_rdd}/qN.txt`; o script imprime OK/FALHA por consulta.

## 5. Experimentos de desempenho (Q2)

```bash
P=8 RUNS=3 ./scripts/04_benchmark_q2.sh                       # 10k, 1M, 10M; Hadoop e Spark; 3 execuções + aquecimento
PS="1 2 4 8" BASES="logs_1m logs_10m" RUNS=3 ./scripts/05_paralelismo.sh
python3 scripts/analisar.py                                   # resultados/tabelas.md + gráficos (requer matplotlib)
```

Critério de medição (igual para os dois frameworks):
- **Tempo total** (usado nas tabelas): tempo de parede do comando `hadoop jar` / `spark-submit`, incluindo
  inicialização da JVM/job/sessão, alocação de containers no YARN, leitura do HDFS, processamento e escrita do resultado no HDFS.
  A remoção do diretório de saída e a geração dos dados ficam fora da medição.
- **Tempo interno** (complementar): Hadoop = submissão → término do job; Spark = após criar a SparkSession → fim da escrita.
- **Paralelismo P**: Hadoop = no máximo P maps e P reduces simultâneos (`mapreduce.job.running.{map,reduce}.limit`)
  e P redutores; Spark = P executores × 1 núcleo, `spark.sql.shuffle.partitions=P`, alocação dinâmica e AQE desligados.
- Splits/partições de entrada de 32 MB nos dois frameworks (`split.maxsize` / `spark.sql.files.maxPartitionBytes`).
- Ordem dos frameworks alternada a cada execução; uma execução de aquecimento descartada.

## 6. Pacote de entrega

```bash
./scripts/empacotar.sh           # .tgz sem as bases grandes (dados/) e sem binários
```
