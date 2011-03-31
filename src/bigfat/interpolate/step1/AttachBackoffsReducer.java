package bigfat.interpolate.step1;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.interpolate.step2.InterpolateModelsVector;

public class AttachBackoffsReducer extends
		Reducer<Ngram, InterpolateModelsInfo, Ngram, InterpolateModelsVector> {

	private InterpolateModelsVector writable = new InterpolateModelsVector();
	private FastIntArrayList ids;

	// [modelId]
	private double[] probs;

	// [order][modelId]
	private double[][] backoffs;
	private double[] NO_BACKOFF;

	private int numModels;

	@Option(shortName = "N", longName = "maxOrder", usage = "order of the language model")
	private int maxOrder;

	// need a map from filenames to weights
	@Option(shortName = "w", longName = "weights", usage = "Interpolation weights in the format lmName1=weight1;lmName2=weight2...", arrayDelim = ";")
	private String[] weights;

	private Counter[] ngramCountsWritten;

	@Override
	public void setup(Context context) {

		HadoopUtils.configure(context.getConfiguration(), this);

		this.numModels = weights.length;

		ids = new FastIntArrayList(maxOrder);

		// keep running sum and context for lower orders (0 through N-1)
		probs = new double[numModels];
		backoffs = new double[maxOrder][numModels];
		NO_BACKOFF = new double[numModels];
		for (int i = 0; i < numModels; i++) {
			NO_BACKOFF[i] = 1.0f;
		}
		this.ngramCountsWritten = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			String adjusted = (i == maxOrder) ? "" : "adjusted ";
			ngramCountsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram " + adjusted + "count types written by reducer");
		}
	}

	@Override
	public void reduce(Ngram ngram, Iterable<InterpolateModelsInfo> values,
			Context hadoopContext) {

		try {
			ngram.getAsIds(ids);
			int order = ids.size();

			for (int iModel = 0; iModel < numModels; iModel++) {
				backoffs[order - 1][iModel] = 1.0;
				probs[iModel] = 0.0;
			}

			// read information for this ngram
			for (InterpolateModelsInfo value : values) {
				int modelId = value.getModelNumber();

				if (value.hasBackoff()) {
					// we'll emit this backoff in a later call to reduce()
					backoffs[order - 1][modelId] = value.getBackoffWeight();
				}

				if (value.hasProb()) {
					// we'll emit this prob immediately
					probs[modelId] = value.getInterpolatedProb();
				}
			}

			// in a previous iteration, we've gathered
			// backoff information for this ngram
			double[] backoff = NO_BACKOFF;
			if (order > 1) {
				backoff = backoffs[order - 2];
			}
			writable.setInfo(probs, backoff);
			hadoopContext.write(ngram, writable);

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
