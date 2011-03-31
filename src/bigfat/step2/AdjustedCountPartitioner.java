package bigfat.step2;

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.util.NgramUtils;

public class AdjustedCountPartitioner extends Partitioner<Ngram, VLongWritable> {

	private final FastIntArrayList arr = new FastIntArrayList(5);

	/**
	 * Ngrams must be reversed. We will partition by all but the leading word
	 * according to the true ngram, which corresponds to the first n-1 tokens of
	 * the reversed ngram.
	 */
	@Override
	public int getPartition(Ngram key, VLongWritable value, int numReduceTasks) {

		// partition by first two words
		key.getAsIds(arr);

		// do not include "last" token (last of reversed ngram; first of
		// non-reversed)
		final boolean includeLastToken = false;
		int hashCode = NgramUtils.hashNgram(arr, includeLastToken);
		
		// we use same strategy as HashPartitioner here
		int which = (hashCode & Integer.MAX_VALUE) % numReduceTasks;
		return which;
	}

}
