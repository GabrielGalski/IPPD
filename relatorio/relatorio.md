# Análise distribuída de registros de acesso com Hadoop e Spark

**Integrantes:** … **Disciplina:** … **Data:** …

> Limite: 4 páginas. Exporte para PDF (ex.: `pandoc relatorio.md -o relatorio.pdf`) e coloque os gráficos de `resultados/`.

## 1. Ambiente e configuração
- Cluster: 1 master (NameNode, ResourceManager, driver Spark) + N workers (DataNode + NodeManager). Copiar de `resultados/ambiente.txt`: versões de Hadoop/Spark/Java, vCores e memória por NodeManager, `dfs.replication`, `dfs.blocksize`.
- Bases: 10 mil / 1 milhão / 10 milhões de registros, semente 42, 500 IPs (tamanho em MB e nº de blocos no HDFS).
- Gerenciador do Spark (YARN ou standalone), modo `client`, memória de executor.
- **Paralelismo configurado × recursos físicos:** P executores/tarefas vs. núcleos efetivamente disponíveis (somatório de vCores do YARN; quantos nós receberam tarefas — conferir na UI do YARN/Spark).

## 2. Abordagem
### 2.1 MapReduce (Q1–Q5)
- Q2: `IpMapper` emite (IP, 1); `SomaReducer` como combinador e redutor (soma é associativa/comutativa). Fluxo: split → map → combine → partition (hash) → shuffle/sort → reduce → HDFS.
- Q1 (chave única, 1 redutor), Q3 (status), Q4 (bytes por recurso), Q5 (redutor único com ordenação no `cleanup`).
- Tratamento do cabeçalho e linhas malformadas.

### 2.2 Spark (Q1–Q5)
- API escolhida: DataFrame (esquema explícito). Q2 também em RDD (`map` + `reduceByKey`) para comparação.
- Operações distribuídas: leitura particionada do HDFS, `groupBy().agg()` (agregação parcial + *exchange*/shuffle + agregação final), `orderBy().limit()` (top‑k), escrita paralela por partição. Citar o plano (`explain()`) se útil.

### 2.3 Expressividade
- Linhas de código / número de classes por consulta; Q5 em MapReduce exige redutor único/estado em memória vs. 1 linha em Spark.

## 3. Validação
Tabela Q1–Q5 × {Hadoop, Spark DF, Spark RDD} com OK/FALHA (saída de `scripts/03_validar.sh`).

## 4. Resultados
### 4.1 Tempo × volume (Q2, P = …)
(tabela de `resultados/tabelas.md` + `grafico_volume.png`)

### 4.2 Paralelismo (Q2, P = 1, 2, 4, 8)
(tabela + `grafico_paralelismo.png` + `grafico_speedup.png`)

## 5. Discussão
- **Custos fixos:** na base de 10 mil o tempo é dominado por inicialização (JVM, ApplicationMaster/containers no YARN, SparkSession/executores). Comparar tempo total × interno.
- **Variação com o volume:** a partir de qual tamanho o processamento passa a dominar; crescimento ~linear?
- **Paralelismo:** speedup/eficiência; limites (nº de splits: base de 10M ≈ … splits de 32 MB; shuffle; overhead de tarefas; base pequena não escala).
- **Hadoop × Spark:** materialização em disco entre map e reduce vs. execução em memória/pipeline; JVM por tarefa vs. executores reaproveitados; Python (DataFrame executa na JVM, RDD serializa para Python).
- Não assumir que um é sempre mais rápido: apontar cenários em que cada um se saiu melhor.
- Ameaças à validade: carga compartilhada do cluster, cache do SO/HDFS, variação entre execuções.

## 6. Conclusões
Resposta à questão orientadora: expressividade e tempo de processamento em volumes crescentes.
