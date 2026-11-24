package logs;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Q3 — Quantidade de acessos por status HTTP. Map: (status, 1); Combiner/Reduce: soma. */
public class Q3AcessosPorStatus extends Configured implements Tool {

    public static class StatusMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable UM = new LongWritable(1);
        private final Text status = new Text();

        @Override
        protected void map(LongWritable offset, Text linha, Context ctx)
                throws IOException, InterruptedException {
            String[] c = LogParser.parse(linha.toString());
            if (c == null) return;
            status.set(c[LogParser.STATUS]);
            ctx.write(status, UM);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { JobUtil.uso("Q3AcessosPorStatus"); return 2; }
        Job job = JobUtil.criarJob(getConf(), "Q3 - acessos por status", Q3AcessosPorStatus.class, args[0], args[1]);
        job.setMapperClass(StatusMapper.class);
        if (JobUtil.usarCombinador(job.getConfiguration())) job.setCombinerClass(SomaReducer.class);
        job.setReducerClass(SomaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return JobUtil.executar(job);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Q3AcessosPorStatus(), args));
    }
}
