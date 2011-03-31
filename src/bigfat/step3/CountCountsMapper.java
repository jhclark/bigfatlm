package bigfat.step3;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;

public class CountCountsMapper extends
		Mapper<Ngram, VLongWritable, CountOfCountsInfo, VLongWritable> {

	private final FastIntArrayList ids = new FastIntArrayList(5);
	private static final VLongWritable ONE = new VLongWritable(1);
	private final CountOfCountsInfo info = new CountOfCountsInfo();
	private Counter[] ngramsRead;
	private Counter[] ngramsWritten;

	@Option(shortName = "N", longName = "order", usage = "order of the language model")
	private int maxOrder;

	public static final int MAX_MOD_KN_SPECIAL_COUNT = 3;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);

		this.ngramsRead = new Counter[maxOrder];
		this.ngramsWritten = new Counter[maxOrder];
		for (int i = 0; i < maxOrder; i++) {
			ngramsRead[i] = context.getCounter(BigFatLM.PROGRAM_NAME, (i + 1) + "-grams read");
			ngramsWritten[i] =
					context.getCounter(BigFatLM.PROGRAM_NAME, (i + 1) + "-grams written");
		}
	}

	@Override
	public void map(Ngram key, VLongWritable value, Context context) {
		try {

			key.getAsIds(ids);
			int order = ids.size();
			long count = value.get();

			// we need one above the maximum discount due to the discounting
			// function D
			if (order > 0 && count <= MAX_MOD_KN_SPECIAL_COUNT + 1) {
				ngramsRead[order - 1].increment(1);
				info.setInfo(order, (int) count);
				context.write(info, ONE);
				ngramsWritten[order - 1].increment(1);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
