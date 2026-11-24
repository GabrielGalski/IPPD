#!/usr/bin/env python3
"""Consultas Q1-Q5 sobre logs de acesso em PySpark, lendo do HDFS.

API principal: DataFrame (Q2 tambem disponivel em RDD com --api rdd, para discutir expressividade).

Exemplo:
  spark-submit --master yarn spark/consultas.py \
      --entrada hdfs:///dados/logs/logs_exemplo.csv --saida /saida/spark --consultas Q1,Q2,Q3,Q4,Q5 --mostrar
Cada consulta Qn grava o resultado (CSV sem cabecalho) em <saida>/qn.
"""
import argparse
import time
from operator import add

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

# Esquema explicito: evita uma passada extra sobre os dados (inferSchema) e fixa os tipos.
SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("recurso", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("bytes", LongType(), True),
])


def ler(spark, entrada):
    # header=True descarta o cabecalho; linhas malformadas sao descartadas
    return spark.read.csv(entrada, header=True, schema=SCHEMA, mode="DROPMALFORMED")


# ---------------------------------------------------------------- consultas (DataFrame)
def q1(spark, df):
    # count() = contagens parciais por particao + soma no driver
    return spark.createDataFrame([(df.count(),)], "total long")


def q2(spark, df):
    # groupBy + count: agregacao parcial por particao (como um combinador),
    # shuffle por hash de ip e agregacao final
    return df.groupBy("ip").agg(F.count(F.lit(1)).alias("acessos"))


def q3(spark, df):
    return df.groupBy("status").agg(F.count(F.lit(1)).alias("acessos"))


def q4(spark, df):
    return df.groupBy("recurso").agg(F.sum("bytes").cast("long").alias("bytes"))


def q5(spark, df):
    # orderBy + limit: top-k global (TakeOrderedAndProject); coalesce(1) preserva a ordem na escrita
    return (df.groupBy("recurso").agg(F.count(F.lit(1)).alias("acessos"))
              .orderBy(F.desc("acessos"), F.asc("recurso"))
              .limit(10)
              .coalesce(1))


CONSULTAS = {"Q1": q1, "Q2": q2, "Q3": q3, "Q4": q4, "Q5": q5}
ORDEM_EXIBICAO = {"Q1": None, "Q2": "ip", "Q3": "status", "Q4": "recurso", "Q5": None}


# ---------------------------------------------------------------- Q2 em RDD (MapReduce explicito)
def q2_rdd(spark, entrada, destino):
    linhas = spark.sparkContext.textFile(entrada)
    pares = (linhas.filter(lambda l: l and not l.startswith("timestamp,"))
                   .map(lambda l: l.split(","))
                   .filter(lambda c: len(c) == 5)
                   .map(lambda c: (c[1], 1)))          # map: (ip, 1)
    contagens = pares.reduceByKey(add)                  # combinacao local + shuffle + soma
    contagens.map(lambda kv: f"{kv[0]},{kv[1]}").saveAsTextFile(destino)
    return contagens


def remover_hdfs(spark, caminho):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(caminho)
    fs = p.getFileSystem(conf)
    if fs.exists(p):
        fs.delete(p, True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entrada", required=True, help="arquivo/diretorio CSV no HDFS")
    ap.add_argument("--saida", required=True, help="diretorio base de saida no HDFS")
    ap.add_argument("--consultas", default="Q1,Q2,Q3,Q4,Q5", help="lista separada por virgula")
    ap.add_argument("--api", choices=["df", "rdd"], default="df", help="API para Q2 (padrao: df)")
    ap.add_argument("--mostrar", action="store_true", help="imprime os resultados (apenas bases pequenas)")
    args = ap.parse_args()

    consultas = [c.strip().upper() for c in args.consultas.split(",") if c.strip()]
    t_sessao = time.time()
    spark = SparkSession.builder.appName(f"logs-{'-'.join(consultas)}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    t_ini = time.time()
    print(f"TEMPO_SESSAO_S={t_ini - t_sessao:.3f}")

    df = ler(spark, args.entrada)
    for q in consultas:
        destino = f"{args.saida.rstrip('/')}/{q.lower()}"
        t0 = time.time()
        if q == "Q2" and args.api == "rdd":
            remover_hdfs(spark, destino)
            res_rdd = q2_rdd(spark, args.entrada, destino)
            res = None
        else:
            res = CONSULTAS[q](spark, df)
            res.write.mode("overwrite").csv(destino)
        print(f"TEMPO_CONSULTA_{q}_S={time.time() - t0:.3f}")

        if args.mostrar:
            print(f"## {q} -> {destino}")
            if res is None:
                linhas = sorted(res_rdd.collect())
            else:
                ordem = ORDEM_EXIBICAO[q]
                linhas = [tuple(r) for r in (res.orderBy(ordem) if ordem else res).collect()]
            for linha in linhas:
                print(",".join(str(x) for x in linha))

    print(f"TEMPO_TOTAL_INTERNO_S={time.time() - t_ini:.3f}")
    spark.stop()


if __name__ == "__main__":
    main()
