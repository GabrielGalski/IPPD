package logs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Soma os valores de cada chave. Como a soma é associativa e comutativa,
 * a mesma classe serve de combinador (agregação parcial no lado do map) e de redutor.
 */
public class SomaReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable total = new LongWritable();

    @Override
    protected void reduce(Text chave, Iterable<LongWritable> valores, Context ctx)
            throws IOException, InterruptedException {
        long soma = 0;
        for (LongWritable v : valores) {
            soma += v.get();
        }
        total.set(soma);
        ctx.write(chave, total);
    }
}
