package bigfat.step6;

import gnu.trove.TIntFloatHashMap;
import jannopts.Option;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.diskcollections.DiskFloatArray;
import bigfat.diskcollections.QueueBuilder;
import bigfat.diskcollections.QueueBuilder.DiskQueueListener;
import bigfat.hadoop.HadoopUtils;
import bigfat.step5.InterpolateOrdersInfo;
import bigfat.util.Sanity;

public class RenormalizeBackoffsReducer
		extends
		Reducer<RenormalizeBackoffsKey, RenormalizeBackoffsInfo, Ngram, InterpolateOrdersInfo> {

	private Ngram outputNgram = new Ngram();
	private InterpolateOrdersInfo outputInfo = new InterpolateOrdersInfo();
	private FastIntArrayList reversedContextIds = new FastIntArrayList(5);

	@Option(shortName = "n", longName = "maxOrder", usage = "order of the language model")
	private int maxOrder;

	@Option(shortName = "D", longName = "debug", usage = "show debugging output?")
	private boolean debug;

	private Counter[] ngramsWritten;
	private Counter[] lowerOrderContextsRead;
	private Counter[] higherOrderContextsRead;
	private Counter[] notHashable;

	// TODO: We should really expose these as tunable config params
	// it's less critical to fit everything in memory the first time,
	// as long as we can hash it later
	private static final int MAX_VALUES_IN_MEM = 10000000;
	private static final int MAX_VALUES_IN_HASH = 12000000;
	private TIntFloatHashMap denomHash;
	private final QueueBuilder<RenormalizeBackoffsInfo> denomQueue = new QueueBuilder<RenormalizeBackoffsInfo>(
			MAX_VALUES_IN_MEM);
	private Counter diskHits;

	private FastIntArrayList denomLowerOrderContext = new FastIntArrayList(5);
	private FastIntArrayList backoffContext = new FastIntArrayList(5);
	private double backoff;
	// this will only be used for the unigram reduce() call
	private double zerotonProb = 1.0;
	private DiskFloatArray diskFloatArray;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);

		this.lowerOrderContextsRead = new Counter[maxOrder + 1];
		this.higherOrderContextsRead = new Counter[maxOrder + 1];
		this.ngramsWritten = new Counter[maxOrder + 1];
		this.notHashable = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			lowerOrderContextsRead[i] = context.getCounter(
					BigFatLM.PROGRAM_NAME, i
							+ "-gram lower order contexts read");

			higherOrderContextsRead[i] = context.getCounter(
					BigFatLM.PROGRAM_NAME, i
							+ "-gram higher order contexts read");

			ngramsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-grams renormalized written");

			notHashable[i] = context
					.getCounter(
							BigFatLM.PROGRAM_NAME,
							i
									+ "-grams not hashable (for suffixing words -- used linear search instead)");
		}

		this.diskHits = context.getCounter(BigFatLM.PROGRAM_NAME,
				"N-grams exceeding " + MAX_VALUES_IN_MEM
						+ " threshold (temporarily dumped to disk)");
		this.denomQueue.setListener(new DiskQueueListener() {
			@Override
			public void notifyOfDiskHit() {
				diskHits.increment(1);
			}
		});
	}
	
	@Override
	public void reduce(RenormalizeBackoffsKey key,
			Iterable<RenormalizeBackoffsInfo> values, Context context) {

		try {
			key.getAsIds(reversedContextIds);

			// we will read in 3 types of records here, in the following order:
			// w = word; c = context; c_2 = context with oldest word removed
			// 1) lower order contexts of the form "w c_2" for the denominator
			//    [these lower order contexts will be valid for several
			//     iterations of steps 2-3 below]
			// 2) current order contexts of the form "w c" for the numerator
			// 3) the context whose backoff we are calculating c
			final boolean isLowerOrderContext = (reversedContextIds.size() < key
					.getTargetOrder() && !key.shouldBeLastInSort());
			final boolean isNumeratorKey = (!key.shouldBeLastInSort()
					&& !isLowerOrderContext);

			if (debug) {
				System.out.printf(
						"NEW KEY: %s isLower=%s idSize=%d tgtOrder=%d\n",
						key.toString(), (isLowerOrderContext ? "T" : "F"),
						reversedContextIds.size(), key.getTargetOrder());
			}

			if (isLowerOrderContext) {
				
				// we will never emit anything due to these contexts,
				// but they are sufficient statistics for the denominator
				// of the backoff renormalization formula
				//
				// we also calculate the zeroton (unk) probability here
				// and then emit it
				readAndStoreLowerOrderContexts(key, values);

			} else {
				if (isNumeratorKey) {
					backoff = calcBackoffFromNumerContexts(key, values);
				} else {
					// this is the history plus extending word
					writeBackoff(key, values, context);
				}
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isNewLowerOrderContext(FastIntArrayList reversedContextIds) {
		FastIntArrayList myLowerOrderContext;
		if (reversedContextIds.size() > 0) {
			myLowerOrderContext = reversedContextIds.subarray(0,
					reversedContextIds.size() - 1);
		} else {
			myLowerOrderContext = reversedContextIds;
		}
		boolean result = !denomLowerOrderContext.equals(myLowerOrderContext);
		if (debug && result) {
			System.out.printf("Encountered new reversed lower order context "
					+ myLowerOrderContext + "within: " + reversedContextIds
					+ " verus the old reversed lower order context "
					+ denomLowerOrderContext + "\n");
		}
		return result;
	}

	private void writeBackoff(RenormalizeBackoffsKey key,
			Iterable<RenormalizeBackoffsInfo> values, Context context) throws IOException, InterruptedException {
		
		// we're in a new backoff context, but never saw
		// that context -- reset to default value
		if (!backoffContext.equals(reversedContextIds)) {
			backoff = 1.0;
		}

		RenormalizeBackoffsInfo value;
		try {
			value = Iterables.getOnlyElement(values);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(
					"Got multiple values when expecting one for key "
							+ key.toString()
							+ " after recalculating backoff weight",
					e);
			
		}
	
		double prob = value.getProb();	
		
		if (reversedContextIds.size() == 1 && reversedContextIds.get(0) == BigFatLM.ZEROTON_ID) {
			System.err.printf("Setting zeroton probability %e\n", zerotonProb);
			
		    // we no longer need the same precision as when we were dealing with all the tiny values
		    prob = zerotonProb;
		}
		
		Sanity.checkValidProb(prob);
		Sanity.checkValidBackoff(backoff);

		outputNgram.setFromIds(reversedContextIds, true);
		outputInfo.setInfo(prob, backoff);
		context.write(outputNgram, outputInfo);
		ngramsWritten[key.getTargetOrder()].increment(1);

		if (debug) {
			System.out.printf("Writing %s P=%f BO=%f\n",
					reversedContextIds.toString(), prob, backoff);
		}
	}

	private double calcBackoffFromNumerContexts(RenormalizeBackoffsKey key,
			Iterable<RenormalizeBackoffsInfo> values) throws IOException {
		
		if (isNewLowerOrderContext(reversedContextIds)) {
			if (debug) {
				System.out
						.printf("Clearing denomQueue -- encountered new lower order context\n");
			}
			denomQueue.clear();
			denomHash = null;
			if(diskFloatArray != null) {
				diskFloatArray.dispose();
				diskFloatArray = null;
			}
			// TODO: Throw error in this case since it means we will
			// never get matches for the denominator
		}

		double denom = 1.0;
		double numer = 1.0;
		for (RenormalizeBackoffsInfo numerValue : values) {
			higherOrderContextsRead[key.getTargetOrder()]
					.increment(1);

			numer -= numerValue.getProb();

			if (debug) {
				System.out.printf("RCONTEXT " + key.toString()
						+ " WORD " + numerValue.getWord()
						+ " NUMERMINUS " + numerValue.getProb()
						+ " NUMERNOW " + numer + "\n");
			}

			// find the shorter history version of this
			// n-gram, if any (there should be), and subtract
			// it from the denominator

			int numerWord = numerValue.getWord();
			if (denomQueue.size() <= MAX_VALUES_IN_HASH
					&& denomHash != null) {
				// look up in hash
				double prob = denomHash.get(numerWord);
				Sanity.checkValidProb(prob);
				// if (prob == null) {
				// throw new RuntimeException(
				// "No word found in shorter context for denominator: "
				// + numerWord);
				// }
				denom -= prob;
				if (debug) {
					System.out.printf("RCONTEXT " + key.toString()
							+ " WORD " + numerWord + " DENOMMINUS "
							+ prob + " DENOMNOW " + denom + "\n");
				}
			} else {

				// there were too many values to hash, so we have to
				// perform the much on-disk seek
				// TODO: This will need to be fixed for truly huge
				double denomProb = diskFloatArray.get(numerWord);
				Sanity.checkValidProb(denomProb);
				denom -= denomProb;
				if (debug) {
					System.out.printf("RCONTEXT "
							+ key.toString() + " WORD "
							+ numerWord
							+ " DENOMMINUS "
							+ denomProb
							+ " DENOMNOW " + numer + "\n");
				}
			}

		}

		if (numer < 0) {
			System.out
					.println("Setting negative numerator to zero...");
			numer = 0.0;
		}

		if (denom < 0) {
			denom = 0.0;
		}

		// we already know the value of the denominator because we
		// have
		// already seen all words that can come after the shorter
		// version of this context thanks to those coming first in
		// the
		// sort order

		// positive backoffs are okay
		final double bo;
		if (denom > 0) {
			bo = (numer / denom);
			Sanity.checkValidBackoff(backoff, "Invalid backoff from numer %e / denom %e", numer, denom);
		} else {
			bo = 1.0;
		}

		// for deficient models (such as our interpolation
		// model), cap backoff at 1.0 since we could actually give more mass
		// to higher orders than is possible in a proper probability
		// model

		// do NOT round backoffs greater than 1 since this is
		// legitimately a possible normalization factor (these
		// aren't probabilities) when the higher order distribution
		// gives a lower probability than the lower order
		// distribution

		// TODO: Read in context n-gram first so that we know it's
		// missing
		backoffContext = reversedContextIds.copy();

		if (debug) {
			System.out.printf("Calculating backoff: REVCONTEXT "
					+ reversedContextIds.toString() + " NUMER "
					+ numer + " DENOM " + denom + " BACKOFF "
					+ backoff + "\n");
		}
		
		return bo;
	}

	private void readAndStoreLowerOrderContexts(RenormalizeBackoffsKey key,
			Iterable<RenormalizeBackoffsInfo> values)
			throws IOException {
		
		denomLowerOrderContext = reversedContextIds.copy();
		denomQueue.clear();
		
		// numerical precision trick -- don't mix numbers of very different magnitudes
		double subtractBuffer = 0.0;
		double BUFFER_MAGNITUDE = 1e-3;

		// we have to read all values in first
		// since we can't get the size of the iterator
		for (RenormalizeBackoffsInfo value : values) {
			denomQueue.add(value.copy());
			if (debug) {
				System.out.printf(
						"Received value for denom WORD %d PROB %f\n",
						value.getWord(), value.getProb());
			}

			if (key.getTargetOrder() == 1
					&& isZeroton(value)) {

			    double prob = value.getProb();
			    if(prob > BUFFER_MAGNITUDE) {
			    	zerotonProb -= prob;	
			    } else {
			    	subtractBuffer += prob;						
			    }

			    if(subtractBuffer > BUFFER_MAGNITUDE) {
			    	zerotonProb -= subtractBuffer;
			    	subtractBuffer = 0.0;
			    }

			    if (debug) {
					System.out.printf("Zeroton prob now %e; subtract buffer now %e\n",
							  zerotonProb, subtractBuffer);
			    }
			}
			lowerOrderContextsRead[key.getTargetOrder() - 1]
					.increment(1);
		}

		if (denomQueue.size() <= MAX_VALUES_IN_HASH) {
			// we expect this to fit in memory in a hash
			// this will give us O(1) lookups instead of O(n)
			int capacity = (int) (denomQueue.size() * 1.5f);
			denomHash = new TIntFloatHashMap(capacity);
			for (RenormalizeBackoffsInfo value : denomQueue.reread()) {
				denomHash.put(value.getWord(), (float) value.getProb());
			}
			denomQueue.clear();
		} else {
			notHashable[key.getTargetOrder() - 1].increment(1);
			
			for(int i=0;i<10;i++) {System.gc();}
			long heapAvail = Runtime.getRuntime().totalMemory();
			long heapUsed = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

			if(notHashable[key.getTargetOrder() - 1].getValue() < 1000) {
			    System.err.println("Not enough memory to hash all values for key: " + key.toString() + "; left " + denomQueue.getDiskEntryCount() + " entries on disk with heap usage " + heapUsed + "/" + heapAvail);
			}
			
			diskFloatArray = new DiskFloatArray(File.createTempFile("bigfatlm", "denom"));
			for (RenormalizeBackoffsInfo value : denomQueue.reread()) {
				diskFloatArray.set(value.getWord(), (float) value.getProb());
			}
		}
		
		// finally flush out the zerotonProb subtract buffer
		zerotonProb -= subtractBuffer;
		subtractBuffer = 0.0;
		if(debug) {
			System.out.printf(" prob now %e; subtract buffer now %e\n",
					  zerotonProb, subtractBuffer);
		}
	}

	private static boolean isZeroton(RenormalizeBackoffsInfo value) {
		return value.getWord() != BigFatLM.ZEROTON_ID;
	}
}
