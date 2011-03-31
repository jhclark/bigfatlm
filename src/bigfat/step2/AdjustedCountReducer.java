package bigfat.step2;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;

public class AdjustedCountReducer extends
		Reducer<Ngram, VLongWritable, Ngram, VLongWritable> {

	private Ngram ngram = new Ngram();
	private VLongWritable writable = new VLongWritable();
	private FastIntArrayList reversedIds;

	// we will actually need these for each order
	private FastIntArrayList[] prevReversedContext;
	private long[] contextsSum;

	@Option(shortName = "N", longName = "maxOrder", usage = "order of the language model")
	private int maxOrder;

	private Counter[] ngramCountsWritten;

	public static final boolean UNREVERSE_OUTPUT_KEYS = true;

	@Override
	public void setup(Context context) {

		HadoopUtils.configure(context.getConfiguration(), this);

		reversedIds = new FastIntArrayList(maxOrder);

		// keep running sum and context for lower orders (0 through N-1)
		prevReversedContext = new FastIntArrayList[maxOrder + 1];
		contextsSum = new long[maxOrder + 1];
		this.ngramCountsWritten = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			String adjusted = (i == maxOrder) ? "" : "adjusted ";
			ngramCountsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram " + adjusted + "count types written by reducer");
		}
	}

	/**
	 * For use with AdjustedCountPartitioner. Both classes expect ngrams to be
	 * reversed. We will emit summed counts for the high order ngrams and counts
	 * of number of unique contexts (the number of unique words that we saw
	 * prefixing the ngram) for lower order ngrams.
	 */
	@Override
	public void reduce(Ngram reversedNgram, Iterable<VLongWritable> values,
			Context hadoopContext) {

		try {
			reversedNgram.getAsIds(reversedIds);

			// sum the counts and emit immediately
			long countSum = 0;
			for (VLongWritable value : values) {
				countSum += value.get();
			}

			// we do not need to adjust highest order counts
			// --
			// we cannot adjust counts for n-grams that start with the BOS
			// <s> token, so we just return the unadjusted counts
			boolean hasBosTag = (reversedIds.size() > 0 && reversedIds
					.get(reversedIds.size() - 1) == BigFatLM.BOS_ID);
			if (reversedIds.size() == maxOrder || hasBosTag) {

				int order = reversedIds.size();
				writable.set(countSum);
				ngram.setFromIds(reversedIds, UNREVERSE_OUTPUT_KEYS);
				hadoopContext.write(ngram, writable);
				ngramCountsWritten[order].increment(1);
			}

			// sum the number of keys (unique contexts) over multiple calls
			// to this reducer -- emit sum when context changes
			//
			// we need context counts for all orders
			// (order 5 ngrams are suff stats for order 4 adjusted/context
			// counts)
			// (unigrams are suff stats for zero-ton context counts)
			FastIntArrayList reversedContext = reversedIds.subarray(0,
					reversedIds.size() - 1);
			int contextOrder = reversedContext.size();
			if (prevReversedContext[contextOrder] != null) {
				if (!reversedContext.equals(prevReversedContext[contextOrder])) {
					// System.err.println("Reversed context "
					// + prevReversedContext[contextOrder].toString()
					// + " NOT same as new reversed context " +
					// reversedContext.toString());

					emitContext(hadoopContext, contextOrder);
					prevReversedContext[contextOrder] = reversedContext.copy();

				} else {
					// System.err.println("Reversed context "
					// + prevReversedContext[contextOrder].toString()
					// + " same as new reversed context " +
					// reversedContext.toString());
				}
			} else {
				// initialize context (done once per context order per reducer)
				prevReversedContext[contextOrder] = reversedContext.copy();

				// System.err.println("Initialized reversed context "
				// + prevReversedContext[contextOrder].toString());
			}

			// for lower order adjusted counts, we don't care how many times
			// they
			// occurred only how many contexts occurred at least once
			contextsSum[contextOrder] += 1;

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void emitContext(Context hadoopContext, int contextOrder)
			throws IOException, InterruptedException {

		// System.err.println(prevReversedContext[contextOrder] +
		// " != " + reversedContext);

		// we have a new context now

		long adjustedCount = contextsSum[contextOrder];
		ngram.setFromIds(prevReversedContext[contextOrder],
				UNREVERSE_OUTPUT_KEYS);
		writable.set(adjustedCount);
		hadoopContext.write(ngram, writable);
		// System.err.println("Write context: " + ngram.toString() +
		// " -- "
		// +
		// writable.toString());
		ngramCountsWritten[contextOrder].increment(1);

		contextsSum[contextOrder] = 0;
	}

	@Override
	public void cleanup(Context hadoopContext) {
		try {
			for (int contextOrder = 0; contextOrder < prevReversedContext.length; contextOrder++) {
				if (contextsSum[contextOrder] > 0) {
					emitContext(hadoopContext, contextOrder);
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
