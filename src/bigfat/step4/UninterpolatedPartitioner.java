package bigfat.step4;

import org.apache.hadoop.mapreduce.Partitioner;

import bigfat.datastructs.FastIntArrayList;
import bigfat.datastructs.Ngram;
import bigfat.util.NgramUtils;

public class UninterpolatedPartitioner extends
		Partitioner<Ngram, UninterpolatedIntermediateValue> {

	private final FastIntArrayList arr = new FastIntArrayList(5);

	@Override
	public int getPartition(Ngram key,
			UninterpolatedIntermediateValue value, int numReduceTasks) {

		// partition by ngram (not history/non-history indicator)
		key.getAsIds(arr);
		final boolean includeLastToken = true;
		int hashCode = NgramUtils.hashNgram(arr, includeLastToken);
		
		// we use same strategy as HashPartitioner here
		int which = (hashCode & Integer.MAX_VALUE) % numReduceTasks;
		return which;
	}

}
