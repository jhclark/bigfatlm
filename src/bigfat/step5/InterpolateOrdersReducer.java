package bigfat.step5;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;

public class InterpolateOrdersReducer
		extends
		Reducer<DuplicatableNgram, InterpolateOrdersIntermediateInfo, Ngram, InterpolateOrdersInfo> {

	private Ngram ngram = new Ngram();
	private InterpolateOrdersInfo info = new InterpolateOrdersInfo();
	private FastIntArrayList ids = new FastIntArrayList(5);
	private double[] probs;

	@Option(shortName = "n", longName = "maxOrder", usage = "order of the language model")
	private int maxOrder;

	@Option(shortName = "D", longName = "debug", usage = "show debugging output?")
	private boolean debug;

	private Counter[] ngramsWritten;
	private Counter[] ngramsRead;
	private boolean unigramAnointedOne;
	private int prevOrder = -1;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);
		this.ngramsRead = new Counter[maxOrder + 1];
		this.ngramsWritten = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			ngramsRead[i] = context.getCounter(BigFatLM.PROGRAM_NAME,
					i + "-gram extension adjusted counts read by reducer");

			ngramsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram uninterpolated probabilities written by reducer");
		}

		probs = new double[maxOrder + 1];
		
		int reducerId = HadoopUtils.getReducerId(context);
		this.unigramAnointedOne = (reducerId == 0);
		System.err.println("REDUCER ID = " + reducerId + "; ANOINTED TO EMIT UNIGRAMS = " + this.unigramAnointedOne);
	}

	@Override
	public void reduce(DuplicatableNgram reversedNgram,
			Iterable<InterpolateOrdersIntermediateInfo> values, Context context) {

		try {
			reversedNgram.getAsIds(ids);
			int order = reversedNgram.getOrder();
			
			InterpolateOrdersIntermediateInfo value = Iterables.getOnlyElement(values);
			
			if (order == 0) {
				if(prevOrder != -1) {
					throw new RuntimeException("Zeroton must come first in sort order!");
				}
				if(debug) {
					System.err.printf("Found zeroton with prob: %e\n", value.getUninterpolatedProb());
				}
				prevOrder = 0;
			}
			
			ngramsRead[order].increment(1);
			
			// reset so that we don't use a previous entry's values
			// backoffWeights[order] = 0;
			// probs[order] = 0;

			ngram.setFromIds(ids, true);

			double interpWeight = value.getInterpolationWeight();
			probs[order] = value.getUninterpolatedProb();

			// interpolate with previous order
			// (all lower orders are included by recursive definition)
			if (order > 0) {
				double uninterpProb = probs[order];
				probs[order] = uninterpProb + interpWeight * probs[order - 1];

				if (debug) {
					System.out.printf("%s\tUIP %e IP %e LOW %e LOIP %e\n",
							ngram.toString(), uninterpProb, probs[order],
							interpWeight, probs[order - 1]);
				}
			}

			ngramsWritten[order].increment(1);
			info.setInfo(probs[order], interpWeight);

			// Unigrams are duplicated across all reducers,
			// but only one needs to emit them
			if(order > 1 || this.unigramAnointedOne) {
				context.write(ngram, info);
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
