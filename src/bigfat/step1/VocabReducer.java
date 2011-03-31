package bigfat.step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import bigfat.BigFatLM;

public class VocabReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable writable = new LongWritable();
	private Counter typeCount;
	
	@Override
	public void setup(Context context) {
		this.typeCount = context.getCounter(BigFatLM.PROGRAM_NAME, "Types");
	}

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) {
		try {
			long sum = 0;
			for(LongWritable count : values) {
				sum += count.get();
			}
			typeCount.increment(1);
			writable.set(sum);
			context.write(key, writable);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
