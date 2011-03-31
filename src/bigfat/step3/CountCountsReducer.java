package bigfat.step3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class CountCountsReducer extends
		Reducer<CountOfCountsInfo, VLongWritable, CountOfCountsInfo, VLongWritable> {

	private VLongWritable writable = new VLongWritable();

	/**
	 * This reducer is only useful to Kneser-Ney language models as a combiner.
	 * It is included for later implementation of techniques such as stupid
	 * backoff.
	 */
	@Override
	public void reduce(CountOfCountsInfo key, Iterable<VLongWritable> values, Context context) {
		try {
			long sum = 0;
			for (VLongWritable count : values) {
				sum += count.get();
			}
			writable.set(sum);
			context.write(key, writable);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
