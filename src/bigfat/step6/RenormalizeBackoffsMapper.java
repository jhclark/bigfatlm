package bigfat.step6;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step5.InterpolateOrdersInfo;

public class RenormalizeBackoffsMapper
		extends
		Mapper<Ngram, InterpolateOrdersInfo, RenormalizeBackoffsKey, RenormalizeBackoffsInfo> {

	private final FastIntArrayList reversedIds = new FastIntArrayList(5);
	private final RenormalizeBackoffsKey outputKey = new RenormalizeBackoffsKey();
	private final RenormalizeBackoffsInfo outputValue = new RenormalizeBackoffsInfo();

	@Option(shortName = "N", longName = "order", usage = "order of the language model")
	private int maxOrder;

	private Counter[] ngramsRead;
	private Counter[] ngramsWrittenForNumer;
	private Counter[] ngramsWrittenForDenom;
	private Counter[] ngramsWrittenForMerging;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);

		this.ngramsRead = new Counter[maxOrder + 1];
		this.ngramsWrittenForNumer = new Counter[maxOrder + 1];
		this.ngramsWrittenForDenom = new Counter[maxOrder + 1];
		this.ngramsWrittenForMerging = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			ngramsRead[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram probabilities with unnormalized backoffs read");
			ngramsWrittenForNumer[i] = context.getCounter(
					BigFatLM.PROGRAM_NAME, i
							+ "-grams written for backoff numerators");
			ngramsWrittenForDenom[i] = context.getCounter(
					BigFatLM.PROGRAM_NAME, i
							+ "-grams written for backoff denominators");
			ngramsWrittenForMerging[i] = context.getCounter(
					BigFatLM.PROGRAM_NAME, i + "-grams written for merging");
		}
	}

	@Override
	public void map(Ngram ngram, InterpolateOrdersInfo value, Context hadoopContext) {
		try {

			ngram.getAsIds(reversedIds, true);
			int order = reversedIds.size();
			this.ngramsRead[order].increment(1);			

			// TODO: Special handling for zero-ton?

			// send full n-gram corresponding to a history to the same
			// shard so that it can be united with its backoff weight
			// that is, the n-gram for P(a b c)" needs the backoff weight
			// for BOW(a b c), so it needs to go to the same partition
			// as P(a b c *) and P(b c *)
			// NOTE: This is the only place where the "context ngram"
			// actually isn't a context -- it's just going to the same
			// reduce partition as other matching contexts
			boolean LAST = true;
			outputKey.setInfo(order, reversedIds, LAST, false);
			outputValue.setInfo(BigFatLM.ZEROTON_ID,
					value.getInterpolatedProb());
			hadoopContext.write(outputKey, outputValue);
			this.ngramsWrittenForMerging[order].increment(1);

			// max-order N-grams do not need to accumulate
			// statistics for a numerator/denominator
			// since they will never have a backoff weight
			// however (N-1)-grams need N-gram statistics
			// for their numerators
			if (order > 0 && order <= maxOrder) {
				boolean NOT_LAST = false;

				FastIntArrayList reversedContextIds = reversedIds.subarray(1,
						reversedIds.size());
				int word = reversedIds.get(0);
				outputValue.setInfo(word, value.getInterpolatedProb());
				
				
				// first write full context (minus current word) for numerator
				// -- the current word will live in the value
				//
				// n-gram was of length order, now we take off the word
				// we're marginalizing to make it length order-1; it will
				// server as a numerator in calculating a backoff for
				// a n-gram of length order-1
				//
				if(order > 1) {
					outputKey.setInfo(order - 1, reversedContextIds, NOT_LAST,
							false);
					hadoopContext.write(outputKey, outputValue);
					this.ngramsWrittenForNumer[order].increment(1);
				}
				
				// then write out the context/history minus its oldest word for
				// the denominator
				// (max order N-grams will never be used as a denominator for a
				// higher order)
				if (order < maxOrder) {
					// ngram was of length order, we took off the current word
					// that we're marginalizing, and it will eventually
					// be used to calculate the backoff for a n-gram of length
					// order
					outputKey.setInfo(order, reversedContextIds, NOT_LAST,
							false);
					hadoopContext.write(outputKey, outputValue);
					this.ngramsWrittenForDenom[order].increment(1);
				}
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
