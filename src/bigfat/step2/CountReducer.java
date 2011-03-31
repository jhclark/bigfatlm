package bigfat.step2;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import bigfat.datastructs.Ngram;

public class CountReducer extends Reducer<Ngram, VLongWritable, Ngram, VLongWritable> {

	private VLongWritable writable = new VLongWritable();
//	private Counter[] ngramCounts;
	
//	@Option(shortName = "N", longName = "order", usage = "order of the language model")
//	private int maxOrder;
//	
//	@Override
//	public void setup(Context context) {
//		this.ngramCounts = new Counter[maxOrder];
//		for (int i = 0; i < maxOrder; i++) {
//			ngramCounts[i] = context.getCounter(BigFatLM.PROGRAM_NAME, (i + 1) + "-grams");
//		}
//	}

	/**
	 * This reducer is only useful to Kneser-Ney language models as a combiner.
	 * It is included for later implementation of techniques such as stupid
	 * backoff.
	 */
	@Override
	public void reduce(Ngram key, Iterable<VLongWritable> values, Context context) {
		try {
			long sum = 0;
			for (VLongWritable count : values) {
				sum += count.get();
			}
			writable.set(sum);
			context.write(key, writable);
//			ngramCounts[reversedIds.size() - 1].increment(1);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
