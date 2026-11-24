package logs;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Q2 — Quantidade de acessos por IP.
 * Map: (IP, 1); Combiner (opcional): soma parcial; Reduce: soma por IP.
 * Número de redutores: -D mapreduce.job.reduces=N.
 */
public class Q2AcessosPorIP extends Configured implements Tool {

    public static class IpMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable UM = new LongWritable(1);
        private final Text ip = new Text();

        @Override
        protected void map(LongWritable offset, Text linha, Context ctx)
                throws IOException, InterruptedException {
            String[] c = LogParser.parse(linha.toString());
            if (c == null) return;
            ip.set(c[LogParser.IP]);
            ctx.write(ip, UM);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { JobUtil.uso("Q2AcessosPorIP"); return 2; }
        Job job = JobUtil.criarJob(getConf(), "Q2 - acessos por IP", Q2AcessosPorIP.class, args[0], args[1]);
        job.setMapperClass(IpMapper.class);
        if (JobUtil.usarCombinador(job.getConfiguration())) job.setCombinerClass(SomaReducer.class);
        job.setReducerClass(SomaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return JobUtil.executar(job);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Q2AcessosPorIP(), args));
    }
}
