package logs;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Q4 (opcional no Hadoop) — Soma dos bytes por recurso. Map: (recurso, bytes); Combiner/Reduce: soma. */
public class Q4BytesPorRecurso extends Configured implements Tool {

    public static class BytesMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text recurso = new Text();
        private final LongWritable bytes = new LongWritable();

        @Override
        protected void map(LongWritable offset, Text linha, Context ctx)
                throws IOException, InterruptedException {
            String[] c = LogParser.parse(linha.toString());
            if (c == null) return;
            try {
                bytes.set(Long.parseLong(c[LogParser.BYTES].trim()));
            } catch (NumberFormatException e) {
                return;
            }
            recurso.set(c[LogParser.RECURSO]);
            ctx.write(recurso, bytes);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) { JobUtil.uso("Q4BytesPorRecurso"); return 2; }
        Job job = JobUtil.criarJob(getConf(), "Q4 - bytes por recurso", Q4BytesPorRecurso.class, args[0], args[1]);
        job.setMapperClass(BytesMapper.class);
        if (JobUtil.usarCombinador(job.getConfiguration())) job.setCombinerClass(SomaReducer.class);
        job.setReducerClass(SomaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return JobUtil.executar(job);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Q4BytesPorRecurso(), args));
    }
}
