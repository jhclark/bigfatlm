package bigfat.step4;

import jannopts.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.diskcollections.QueueBuilder;
import bigfat.diskcollections.QueueBuilder.DiskQueueListener;
import bigfat.hadoop.HadoopUtils;
import bigfat.step3.CountCountsMapper;
import bigfat.step3.Discounts;
import bigfat.util.Sanity;

public class UninterpolatedReducer
		extends
		Reducer<Ngram, UninterpolatedIntermediateValue, Ngram, UninterpolatedInfo> {

	private final Ngram historyNgram = new Ngram();
	private final FastIntArrayList historyIds = new FastIntArrayList(5);
	private final FastIntArrayList wordIds = new FastIntArrayList(1);
	private final UninterpolatedInfo info = new UninterpolatedInfo();

	@Option(shortName = "n", longName = "maxOrder", usage = "order of the language model")
	private int maxOrder;

	@Option(shortName = "D", longName = "debug", usage = "show debugging output?")
	private boolean debug;

	@Option(shortName = "m", longName = "minCountsPost", usage = "minimum count for any n-gram to be included in the model AFTER it has been included in an uninterpolated probability, comma-delimited", arrayDelim = ",", defaultValue = "1,1,2,2,2,2,2,2,2,2")
	private Integer[] minCountsPost;

	private Discounts discounts;
	private Counter[] ngramProbsWritten;
	private Counter[] ngramHistoriesRead;
	private Counter[] ngramExtensionsRead;
	private Counter[] ngramsDiscarded;
	private static final int MAX_VALUES_IN_MEM = 10000000;
	private final QueueBuilder<UninterpolatedIntermediateValue> extensions = new QueueBuilder<UninterpolatedIntermediateValue>(
			MAX_VALUES_IN_MEM);
	private Counter diskHits;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);

		try {
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			Path[] cache = HadoopUtils.getLocalCacheFiles(context
					.getConfiguration());
			if (cache == null || cache.length != 1) {
				throw new RuntimeException(
						"Did not find exactly 1 distributed cache file");
			}
			Path vocabFile = cache[0];
			BufferedReader discountsIn = new BufferedReader(
					new InputStreamReader(fs.open(vocabFile)));

			this.discounts = Discounts.load(discountsIn);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		this.ngramHistoriesRead = new Counter[maxOrder + 1];
		this.ngramExtensionsRead = new Counter[maxOrder + 1];
		this.ngramProbsWritten = new Counter[maxOrder + 1];
		this.ngramsDiscarded = new Counter[maxOrder + 1];
		if(minCountsPost.length < maxOrder) {
			throw new RuntimeException("minCountsPre array must be at least as long as maxOrder");
		}
		for (int i = 0; i <= maxOrder; i++) {
			String adjusted = (i == maxOrder) ? "" : "adjusted ";
			ngramHistoriesRead[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram histories adjusted counts read");

			ngramExtensionsRead[i] = context.getCounter(BigFatLM.PROGRAM_NAME,
					i + "-gram extension adjusted counts read");

			ngramProbsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram uninterpolated probabilities written");

			ngramsDiscarded[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-grams pruned after contributing to denominator ("
					+ adjusted + "count < "
					+ (i == 0 ? 0 : minCountsPost[i - 1]) + ")");
		}

		this.diskHits = context.getCounter(BigFatLM.PROGRAM_NAME,
				"N-grams exceeding " + MAX_VALUES_IN_MEM
						+ " threshold (temporarily dumped to disk)");
		this.extensions.setListener(new DiskQueueListener() {
			@Override
			public void notifyOfDiskHit() {
				diskHits.increment(1);
			}
		});
	}

	private static boolean shouldIncludeInDenom(FastIntArrayList wordIds) {
		// <s> token can never suffix a unigram in a bigram, etc.
		return wordIds.size() > 0 && wordIds.get(0) != BigFatLM.BOS_ID;
	}


	@Override
	public void reduce(Ngram history,
			Iterable<UninterpolatedIntermediateValue> values, Context context) {
		
		try {
			history.getAsIds(historyIds);
			historyNgram.setFromIds(historyIds, false);
			long backoffDenom = 0;
			int lowerOrder = historyIds.size();
			ngramHistoriesRead[lowerOrder].increment(1);

			// TODO: Reuse arrays
			// see accompanying tech paper for meanings of f and g
			long lowerOrderF = 0;
			long[] g = new long[CountCountsMapper.MAX_MOD_KN_SPECIAL_COUNT];
			long fSum = 0;

			// during the first pass over the values, we will accumulate the
			// fSum
			// and pick out the lowerOrderF, which will be used later
			long numValues = 0;
			long numExtensions = 0;
			extensions.clear();
			for (UninterpolatedIntermediateValue value : values) {
				numValues++;
				// there are zero or one words in wordIds
				value.getWord(wordIds);

				// TODO: Output highest-order n-grams during first pass through
				// data

				// TODO: Remove unused lowerOrderF from mapper
				if (wordIds.size() == 0) {
					lowerOrderF = value.getAdjustedCount();

				} else if (shouldIncludeInDenom(wordIds)) {
					numExtensions++;
					ngramHistoriesRead[lowerOrder + 1].increment(1);
					// this is the NON-adjusted count for the highest order
					long f = value.getAdjustedCount();
					fSum += f;
					extensions.add(value.copy());

					// accumulate sufficient status for the "g()" function
					// a count of extensions
					// (see accompanying tech paper)
					int countCateg = getCountCategory(f);
					g[countCateg - 1]++;

					// do summation over f() for backoff denom
					backoffDenom += f;
					
				} else if (wordIds.size() == 1 && wordIds.get(0) == BigFatLM.BOS_ID) {
					// always assign zero prob to <s> and don't interpolate it
					// with lower orders
					info.setValues(wordIds.get(0), 0.0f, 0.0f);
					context.write(historyNgram, info);
					ngramProbsWritten[1].increment(1);
					
				}
			}

			final double interpolationWeight = calculateInterpolationWeight(
					history, backoffDenom, lowerOrder, g, numValues,
					numExtensions);

			// calculate zeroton uniform distribution
			if (lowerOrder == 0) {
				// we are missing </s> from extensions
				long vocabSize = numExtensions + 1;
				
				double zerotonProb = 1.0 / (double) (vocabSize);
				// double zerotonLogProb = -Math.log10(vocabSize);
				// TODO: Are we emitting multiple zero-tons?
				int wordId = BigFatLM.ZEROTON_ID;
				info.setValues(wordId, zerotonProb, interpolationWeight);

				if (debug) {
					System.err.println("uniform log prob for zerotons = "
							+ zerotonProb + " :: from vocabSize = " + vocabSize);
				}
				context.write(historyNgram, info);
			}

			for (UninterpolatedIntermediateValue value : extensions.reread()) {

				// when history + word gives us a highest-order n-gram,
				// then we already have the suff stats to emit the
				// uninterpolated prob
				//
				// there is no backoff weight for highest-order ngrams
				// if (lowerOrderF == 0) {
				// throw new
				// RuntimeException("lowerOrderF for lower order (history order) "
				// + lowerOrder + " and ngram history \"" + history.toString()
				// + "\" not found");
				// }

				final long denom = fSum;
				long f = value.getAdjustedCount();
				int ngramOrder = historyIds.size() + 1;
				int countCateg = getCountCategory(f);
				double d = discounts.getDiscount(ngramOrder, countCateg);
				double uninterpProb = ((double)f - d) / (double) denom;
				Sanity.checkValidProb(uninterpProb,"Invalid value for uninterpolated probability: %f; "
											+ "stats were d=%f f=%d ngramOrder=%d countCateg=%d lowerOrderF=%d",
									uninterpProb, d, f, ngramOrder, countCateg,
									lowerOrderF);
				value.getWord(wordIds);
				if (debug) {
					System.out.printf(
							"%s %d\tNUMER %d DENOM %d DISC %f UIP %f\n",
							history.toString(), wordIds.get(0), f, denom, d,
							uninterpProb);
				}

				int order = lowerOrder + 1;
				if (order > 0 && f < minCountsPost[order - 1]) {
					ngramsDiscarded[order].increment(1);

				} else {
					int wordId = wordIds.get(0);
					info.setValues(wordId, uninterpProb, interpolationWeight);
					context.write(historyNgram, info);
					ngramProbsWritten[order].increment(1);
				}
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private double calculateInterpolationWeight(
			Ngram history, long interpDenom,
			int lowerOrder, long[] g, long numValues, long numExtensions) {
		
		// for lower order ngrams, we also calculate a backoff
		// weight
		double numerator = 0;
		int discountOrder = lowerOrder + 1;
		for (int iCountCateg = 1; iCountCateg <= CountCountsMapper.MAX_MOD_KN_SPECIAL_COUNT; iCountCateg++) {
			numerator += discounts.getDiscount(discountOrder, iCountCateg)
					* g[iCountCateg - 1];
		}
		final double interpWeight;
		if (numExtensions == 0) {
			interpWeight = 1.0;
		} else {
			interpWeight = numerator / (double) interpDenom;
			Sanity.checkValidBackoff(interpWeight, "Invalid value for backoff weight: %f"
								+ "; stats were numerator=%f backoffDenom=%d;"
								+ "calculated for history %s of order %d "
								+ "after reading %d values and %d extensions",
						interpWeight, numerator, interpDenom, history.toString(),
						lowerOrder, numValues, numExtensions);
		}

		if (debug) {
			System.out.printf("%s\tLOW %f LOW.TC %d C1 %d C2 %d C3 %d\n",
					history.toString(), interpWeight, interpDenom, g[0], g[1],
					g[2]);
		}
		return interpWeight;
	}

	private int getCountCategory(long adjustedCount) {
		return (int) Math.min(adjustedCount, CountCountsMapper.MAX_MOD_KN_SPECIAL_COUNT);
	}
}
