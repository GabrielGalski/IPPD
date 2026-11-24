package logs;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Q1 — Quantidade total de acessos. Map: ("total", 1); Combiner/Reduce: soma. */
public class Q1TotalAcessos extends Configured implements Tool {

    public static class TotalMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final Text TOTAL = new Text("total");
        private static final LongWritable UM = new LongWritable(1);

        @Override
        protected void map(LongWritable offset, Text linha, Context ctx)
                throws IOException, InterruptedException {
            if (LogParser.parse(linha.toString()) != null) {
                ctx.write(TOTAL, UM);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { JobUtil.uso("Q1TotalAcessos"); return 2; }
        Job job = JobUtil.criarJob(getConf(), "Q1 - total de acessos", Q1TotalAcessos.class, args[0], args[1]);
        job.setMapperClass(TotalMapper.class);
        if (JobUtil.usarCombinador(job.getConfiguration())) job.setCombinerClass(SomaReducer.class);
        job.setReducerClass(SomaReducer.class);
        job.setNumReduceTasks(1); // uma única chave
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return JobUtil.executar(job);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Q1TotalAcessos(), args));
    }
}
