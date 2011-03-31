package bigfat.step1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;

public class VocabMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private final static LongWritable ONE = new LongWritable(1);
	private Text word = new Text();
	
	private Counter sentCount;
	private Counter tokenCount;
	
	@Override
	public void setup(Context context) {
		this.sentCount = context.getCounter(BigFatLM.PROGRAM_NAME, "Sentences");
		this.tokenCount = context.getCounter(BigFatLM.PROGRAM_NAME, "Tokens");
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		try {
			sentCount.increment(1);
			StringTokenizer tok = new StringTokenizer(value.toString());
			while (tok.hasMoreTokens()) {
				word.set(tok.nextToken());
				context.write(word, ONE);
				tokenCount.increment(1);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
