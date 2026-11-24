package logs;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/** Configuração comum a todos os jobs (entrada/saída no HDFS, separador CSV, medição de tempo). */
public final class JobUtil {
    private JobUtil() {}

    public static Job criarJob(Configuration conf, String nome, Class<?> classeJar,
                               String entrada, String saida) throws IOException {
        // saída no formato "chave,valor" (padrão do Hadoop seria TAB)
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, nome);
        job.setJarByClass(classeJar);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(entrada));

        Path out = new Path(saida);
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true); // equivalente ao mode("overwrite") do Spark
        }
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }

    /** Executa o job e imprime o tempo entre a submissão e o término (TEMPO_JOB_S). */
    public static int executar(Job job) throws Exception {
        long t0 = System.nanoTime();
        boolean ok = job.waitForCompletion(true);
        double s = (System.nanoTime() - t0) / 1e9;
        System.out.printf(Locale.ROOT, "TEMPO_JOB_S=%.3f%n", s);
        return ok ? 0 : 1;
    }

    /** Permite desligar o combinador com -D logs.combinador=false (experimento opcional). */
    public static boolean usarCombinador(Configuration conf) {
        return conf.getBoolean("logs.combinador", true);
    }

    public static void uso(String classe) {
        System.err.println("Uso: hadoop jar logs-mr.jar logs." + classe
                + " [-D prop=valor ...] <entrada_hdfs> <saida_hdfs>");
    }
}
