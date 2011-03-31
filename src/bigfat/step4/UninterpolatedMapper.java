package bigfat.step4;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;

public class UninterpolatedMapper
		extends
		Mapper<Ngram, VLongWritable, Ngram, UninterpolatedIntermediateValue> {

	private final UninterpolatedIntermediateValue intermediate = new UninterpolatedIntermediateValue();
	private final Ngram ngramHistory = new Ngram();
	private final FastIntArrayList ids = new FastIntArrayList(5);
	private static final FastIntArrayList ZERO_WORDS = new FastIntArrayList(0);

	@Option(shortName = "N", longName = "order", usage = "order of the language model")
	private int maxOrder;

	@Option(shortName = "M", longName = "minCountsPre", usage = "minimum count for any n-gram to be included in an uninterpolated probability, comma-delimited", arrayDelim = ",", defaultValue = "1,1,2,2,2,2,2,2,2,2")
	private Integer[] minCountsPre;

	private Counter[] ngramsRead;
	private Counter[] ngramsDiscarded;
	private Counter[] ngramsWritten;
	private Counter[] ngramHistoriesWritten;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);

		this.ngramsRead = new Counter[maxOrder + 1];
		this.ngramsDiscarded = new Counter[maxOrder + 1];
		this.ngramsWritten = new Counter[maxOrder + 1];
		this.ngramHistoriesWritten = new Counter[maxOrder + 1];
		if(minCountsPre.length < maxOrder) {
			throw new RuntimeException("minCountsPre array must be at least as long as maxOrder");
		}
		for (int i = 0; i <= maxOrder; i++) {
			String adjusted = (i == maxOrder) ? "" : "adjusted ";
			ngramsRead[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram " + adjusted + "counts read");
			ngramsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram " + adjusted + "counts written");
			ngramHistoriesWritten[i] = context.getCounter(
					BigFatLM.PROGRAM_NAME, i + "-gram " + adjusted
							+ "counts written as histories");
			ngramsDiscarded[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-grams pruned before contributing to denominator (" + adjusted + "count < "
					+ (i == 0 ? 0 : minCountsPre[i - 1]) + ")");
		}
	}

	@Override
	public void map(Ngram key, VLongWritable value, Context hadoopContext) {
		try {
			// the (possibly adjusted) count
			long count = value.get();

			// these ngrams are NOT reversed
			key.getAsIds(ids);
			int order = ids.size();

			ngramsRead[order].increment(1);

			if (order > 0 && count < minCountsPre[order - 1]) {
				ngramsDiscarded[order].increment(1);
			} else {

				FastIntArrayList history;
				FastIntArrayList word;

				// calling reduce() for a m-gram
				// requires a denominator from the (m-1)-gram
				if (order < maxOrder) {
					// these are histories ONLY
					history = ids;
					word = ZERO_WORDS;
					emit(hadoopContext, count, history, word);
					ngramHistoriesWritten[order].increment(1);
				}

				if (order > 0) {
					// these are NOT just histories
					history = ids.subarray(0, ids.size() - 1);
					word = ids.subarray(ids.size() - 1, ids.size());
					emit(hadoopContext, count, history, word);
					ngramsWritten[order].increment(1);
				} else {
					// TODO: Special case for zerotons
				}
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void emit(Context hadoopContext, long count,
			final FastIntArrayList history, final FastIntArrayList word) throws IOException, InterruptedException {

		ngramHistory.setFromIds(history, false);
		intermediate.setInfo(word, count);
		hadoopContext.write(ngramHistory, intermediate);
	}
}
