package logs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Q5 (opcional no Hadoop) — Dez recursos mais acessados.
 * Map: (recurso, 1); Combiner: soma parcial; Reduce (único): acumula as contagens
 * e, no cleanup, ordena por contagem decrescente e nome crescente, emitindo os N primeiros.
 * N configurável com -D logs.topn=10.
 */
public class Q5TopRecursos extends Configured implements Tool {

    public static class RecursoMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable UM = new LongWritable(1);
        private final Text recurso = new Text();

        @Override
        protected void map(LongWritable offset, Text linha, Context ctx)
                throws IOException, InterruptedException {
            String[] c = LogParser.parse(linha.toString());
            if (c == null) return;
            recurso.set(c[LogParser.RECURSO]);
            ctx.write(recurso, UM);
        }
    }

    public static class TopNReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final Map<String, Long> contagens = new HashMap<>();

        @Override
        protected void reduce(Text chave, Iterable<LongWritable> valores, Context ctx) {
            long soma = 0;
            for (LongWritable v : valores) soma += v.get();
            contagens.put(chave.toString(), soma);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            int n = ctx.getConfiguration().getInt("logs.topn", 10);
            List<Map.Entry<String, Long>> lista = new ArrayList<>(contagens.entrySet());
            lista.sort((a, b) -> {
                int cmp = Long.compare(b.getValue(), a.getValue()); // contagem decrescente
                return cmp != 0 ? cmp : a.getKey().compareTo(b.getKey()); // empate: nome crescente
            });
            for (int i = 0; i < Math.min(n, lista.size()); i++) {
                ctx.write(new Text(lista.get(i).getKey()), new LongWritable(lista.get(i).getValue()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { JobUtil.uso("Q5TopRecursos"); return 2; }
        Job job = JobUtil.criarJob(getConf(), "Q5 - top 10 recursos", Q5TopRecursos.class, args[0], args[1]);
        job.setMapperClass(RecursoMapper.class);
        job.setCombinerClass(SomaReducer.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1); // ordenação global exige um único redutor
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return JobUtil.executar(job);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Q5TopRecursos(), args));
    }
}
