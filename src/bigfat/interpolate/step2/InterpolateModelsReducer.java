package bigfat.interpolate.step2;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step5.DuplicatableNgram;
import bigfat.step5.InterpolateOrdersInfo;

import com.google.common.collect.Iterables;

public class InterpolateModelsReducer
		extends
		Reducer<DuplicatableNgram, InterpolateModelsVector, Ngram, InterpolateOrdersInfo> {

	// need a map from filenames to weights
	@Option(shortName = "w", longName = "weights", usage = "Interpolation weights in the format lmName1=weight1;lmName2=weight2...", arrayDelim = ";")
	private String[] weights;

	private double[] weightsArr;

	private int numModels;

	private Ngram ngram = new Ngram();
	private InterpolateOrdersInfo writable = new InterpolateOrdersInfo();
	private FastIntArrayList reversedIds;

	// [modelId][order]
	private double[][] probs;
	private double[][] backoffs;

	@Option(shortName = "N", longName = "maxOrder", usage = "order of the language model")
	private int maxOrder;

	private Counter[] ngramCountsWritten;

	public static final boolean UNREVERSE_OUTPUT_KEYS = true;

	@Override
	public void setup(Context context) {

		HadoopUtils.configure(context.getConfiguration(), this);

		reversedIds = new FastIntArrayList(maxOrder);

		this.numModels = weights.length;
		this.probs = new double[numModels][maxOrder+1];
		this.backoffs = new double[numModels][maxOrder+1];

		this.weightsArr = new double[weights.length];
		for (int i = 0; i < weights.length; i++) {
			if (!weights[i].trim().isEmpty()) {
				this.weightsArr[i] = Double.parseDouble(weights[i]);
			}
		}

		this.ngramCountsWritten = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			String adjusted = (i == maxOrder) ? "" : "adjusted ";
			ngramCountsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram " + adjusted + "count types written by reducer");
		}
	}

	/**
	 * 
	 */
	@Override
	public void reduce(DuplicatableNgram reversedNgram,
			Iterable<InterpolateModelsVector> values, Context hadoopContext) {

		try {
			reversedNgram.getAsIds(reversedIds);
			int order = reversedIds.size();

			InterpolateModelsVector value = Iterables.getOnlyElement(values);
			double[] valueBackoffs = value.getBackoffWeights();
			double[] valueProbs = value.getProbs();
			for (int iModel = 0; iModel < valueBackoffs.length; iModel++) {
				backoffs[iModel][order] = valueBackoffs[iModel];
				probs[iModel][order] = valueProbs[iModel];
			}

			double interpolatedProb = interpolateModels(probs, backoffs,
					weightsArr, order);
			ngram.setFromIds(reversedIds, true);
			writable.setInfo(interpolatedProb, 0.0);
			hadoopContext.write(ngram, writable);

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static double interpolateModels(double[][] probs, double[][] backoffs,
			double[] weights, int curOrder) {

		double sum = 0.0f;
		for (int iModel = 0; iModel < probs.length; iModel++) {
			sum += weights[iModel]
					* getModelProb(probs[iModel], backoffs[iModel],
							curOrder);
		}
		return sum;
	}

	private static double getModelProb(double[] probs, double[] backoffs,
			int queryOrder) {

		double prob = 1.0;

		if (queryOrder == 0) {
			throw new Error("Why are we querying for a zeroton?");
		}

		int iOrder = queryOrder - 1;
		while (iOrder > 0 && probs[iOrder] == 0.0) {
			// no prob for this order? backoff.
			prob *= backoffs[iOrder];
			iOrder--;
		}

		// this also includes order zero as the unk token
		prob *= probs[iOrder];
		return prob;
	}
}
