package bigfat.step7;

import jannopts.Option;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.hadoop.HadoopUtils;
import bigfat.step1.Vocabulary;
import bigfat.step5.InterpolateOrdersInfo;
import bigfat.util.Sanity;

// TODO: Special class for InterpolatedIntermediateValue?
public class DArpaMapper extends
		Mapper<Ngram, InterpolateOrdersInfo, Text, Text> {

	@Option(shortName = "N", longName = "order", usage = "order of the language model")
	private int maxOrder;

	private Vocabulary vocab;
	private final Text outputKey = new Text();
	private final Text outputValue = new Text();
	private final StringBuilder builder = new StringBuilder();
	private final FastIntArrayList ids = new FastIntArrayList(5);

	private Counter[] ngramsRead;
	private Counter[] zeroBackoffs;
	private Counter[] ngramsWritten;

	@Override
	public void setup(Context context) {
		HadoopUtils.configure(context.getConfiguration(), this);

		this.ngramsRead = new Counter[maxOrder + 1];
		this.ngramsWritten = new Counter[maxOrder + 1];
		this.zeroBackoffs = new Counter[maxOrder + 1];
		for (int i = 0; i <= maxOrder; i++) {
			ngramsRead[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram unformatted entries read");

			ngramsWritten[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram dARPA entries written");

			zeroBackoffs[i] = context.getCounter(BigFatLM.PROGRAM_NAME, i
					+ "-gram zero backoffs found");
		}

		try {
			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			Path[] cache = HadoopUtils.getLocalCacheFiles(context
					.getConfiguration());
			if (cache == null || cache.length != 1) {
				throw new RuntimeException(
						"Did not find exactly 1 distributed cache file");
			}
			Path vocabFile = cache[0];
			vocab = Vocabulary.load(vocabFile, fs);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void map(Ngram ngram, InterpolateOrdersInfo value,
			Context hadoopContext) {
		try {

			ngram.getAsIds(ids);
			int order = ids.size();
			ngramsRead[order].increment(1);

			// convert IDs to strings
			stringify(ids, vocab, builder);

			outputKey.set(builder.toString());

			double prob = value.getInterpolatedProb();
			Sanity.checkValidProb(prob, "Invalid probability from previous step");
			double logProb = Math.log10(prob);
			if (logProb == Float.NEGATIVE_INFINITY) {
				logProb = BigFatLM.LOG_PROB_OF_ZERO;
			}

			// get from zeroton ID
			if (ids.size() == 1 && ids.get(0) == BigFatLM.UNK_ID
					|| ids.get(0) == BigFatLM.ZEROTON_ID) {
				System.err.println("Found <unk>: " + logProb);
			}

			double bo = value.getBackoffWeight();
			Sanity.checkValidBackoff(bo, "Invalid backoff from previous step");
			double logBackoff = Math.log10(bo);
			if (bo == 0) {
				logBackoff = BigFatLM.LOG_PROB_OF_ZERO;
				zeroBackoffs[order].increment(1);
//				 throw new
//				 RuntimeException("Should never have zero backoff weight;
//				 this could happen if we pruned a required shorter context
//				 (lower order counts may fall below threshold when parents
//				 don't due to Kneser-Ney adjusted counts)");
			}
			
			Sanity.checkValidLogProb(logProb, "Invalid log prob %e from prob %e", logProb, prob);
			Sanity.checkValidReal(logBackoff, "Invalid logBackoff %e from backoff %e", logBackoff, bo);

			builder.setLength(0);
			builder.append(String.format("%.6e", logProb));
			if (logBackoff != 0.0f) {
				builder.append('\t');
				builder.append(String.format("%.6e", logBackoff));
			}
			outputValue.set(builder.toString());

			ngramsWritten[order].increment(1);
			hadoopContext.write(outputKey, outputValue);

		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void stringify(FastIntArrayList ids, Vocabulary vocab,
			StringBuilder builder) {
		
		builder.setLength(0);
		for (int i = 0; i < ids.size(); i++) {
			String tok = vocab.toWord(ids.get(i));
			builder.append(tok);
			if (i < ids.size() - 1) {
				builder.append(' ');
			}
		}
	}
}
