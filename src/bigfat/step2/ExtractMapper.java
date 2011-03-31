package bigfat.step2;

import jannopts.Option;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import bigfat.BigFatLM;
import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.filtering.NgramFilter;
import bigfat.filtering.NullFilter;
import bigfat.hadoop.HadoopUtils;
import bigfat.util.StringUtils;

public class ExtractMapper extends
		Mapper<LongWritable, Text, Ngram, VLongWritable> {

	@Option(shortName = "N", longName = "order", usage = "Order of the language model")
	int maxOrder;

	// TODO: This should be a program internal option,
	// not exposed to the user
	@Option(shortName = "r", longName = "reverseKeys", usage = "reverse intermediate keys? true for Kneser-Ney smoothing; false for stupid backoff")
	boolean reverseIntermediateKeys;

	@Option(shortName = "a", longName = "noBosAndEos", usage = "don't append the beginning and end of sentence markers <s> and </s>?", defaultValue = "false", required = false)
	boolean noAppendBosEos;

	private final ArrayList<String> tokens = new ArrayList<String>(100);
	private final FastIntArrayList ids = new FastIntArrayList(5);
	private final Ngram ngram = new Ngram();
	private static final VLongWritable ONE = new VLongWritable(1);
	private Map<String, Integer> vocab;

	private NgramFilter filter = new NullFilter();

	private Counter sentCount;
	private Counter[] ngramCount;

	@Override
	public void setup(Context context) {

		HadoopUtils.configure(context.getConfiguration(), this);

		try {

			Path[] cache = HadoopUtils.getLocalCacheFiles(context
					.getConfiguration());
			if (cache == null || cache.length != 1) {
				throw new RuntimeException(
						"Did not find exactly 1 distributed cache file. Found "
								+ (cache == null ? "no cache" : cache.length)
								+ ": " + Arrays.toString(cache));
			}
			Path vocabFile = cache[0];

			FileSystem fs = FileSystem.getLocal(context.getConfiguration());
			context.setStatus("Reading vocabulary");
			vocab = readVocabFromFile(vocabFile, fs);
			context.setStatus("Vocabulary read done.");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		this.sentCount = context.getCounter(BigFatLM.PROGRAM_NAME, "Sentences");
		this.ngramCount = new Counter[maxOrder];
		for (int i = 0; i < maxOrder; i++) {
			this.ngramCount[i] = context.getCounter(BigFatLM.PROGRAM_NAME,
					(i + 1) + "-gram instances written by mapper");
		}

//		int mapperId = HadoopUtils.getMapperId(context);
//		boolean bosAnointedOne = (mapperId == 0);

//		// emit <s> if we are the chosen one (chosen map task)
//		if (bosAnointedOne && !noAppendBosEos) {
//			try {
//				FastIntArrayList tok = new FastIntArrayList(1);
//				tok.add(BigFatLM.BOS_ID);
//				ngram.setFromIds(tok, reverseIntermediateKeys);
//				context.write(ngram, ONE);
//				ngramCount[tok.size()].increment(1);
//			} catch (IOException e) {
//				throw new RuntimeException(e);
//			} catch (InterruptedException e) {
//				throw new RuntimeException(e);
//			}
//		}
	}

	private Map<String, Integer> readVocabFromFile(Path vocabFile, FileSystem fs)
			throws IOException {

		int vocabSize = 0;
		BufferedReader vocabIn = new BufferedReader(new InputStreamReader(
				fs.open(vocabFile)));
		String line;
		while ((line = vocabIn.readLine()) != null) {
			vocabSize++;
		}
		vocabIn.close();

		float load = 1.25f;
		int capacity = (int) (vocabSize * load) + 1;
		Map<String, Integer> vocab = new HashMap<String, Integer>(capacity,
				load);

		vocabIn = new BufferedReader(new InputStreamReader(fs.open(vocabFile)));
		while ((line = vocabIn.readLine()) != null) {
			String[] split = StringUtils.tokenize(line, "\t");
			if (split.length != 2) {
				throw new RuntimeException("Malformed vocabId file");
			}
			String word = split[0];
			int id = Integer.parseInt(split[1]);
			vocab.put(word, id);
		}
		vocabIn.close();
		return vocab;
	}

	private void tokenize(String sent, ArrayList<String> out) {
		out.clear();
		String[] toks = sent.split("\\s+");
		out.ensureCapacity(toks.length + 2);
		if (!noAppendBosEos) {
			out.add(BigFatLM.BOS);
		}
		for (int i = 0; i < toks.length; i++) {
			String tok = toks[i].trim();
			if (tok.length() > 0) {
				out.add(tok);
			}
		}
		if (!noAppendBosEos) {
			out.add(BigFatLM.EOS);
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context hadoopContext) {
		try {
			sentCount.increment(1);
			tokenize(value.toString(), tokens);
			convertToIds(tokens, ids);

			for (int i = 0; i < ids.size(); i++) {
				for (int j = i + 1; j <= ids.size() && (j - i) <= maxOrder; j++) {
					FastIntArrayList subarray = ids.subarray(i, j);
					FastIntArrayList context = ids.subarray(i + 1, j);
					// TODO: Filter using vocab file and don't loop over
					// non-required n-grams in the first place
					if (filter.isRequired(context, false)) {
						ngram.setFromIds(subarray, reverseIntermediateKeys);
						hadoopContext.write(ngram, ONE);
						ngramCount[subarray.size() - 1].increment(1);
					} else {
						// TODO: Increment filtered counter
					}
				}
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void convertToIds(List<String> toks, FastIntArrayList arr) {
		arr.clear();
		for (String tok : toks) {
			Integer id = vocab.get(tok);
			if (id == null) {
				throw new RuntimeException(
						"No vocabulary ID found for token \"" + tok + "\"");
			}
			arr.add(id);
		}
	}
}
