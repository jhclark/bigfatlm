package bigfat.step5;

import org.apache.hadoop.mapreduce.Partitioner;


import bigfat.datastructs.FastIntArrayList;
import bigfat.util.NgramUtils;

public class InterpolateOrdersPartitioner<V> extends
		Partitioner<DuplicatableNgram, V> {

	@Override
	public int getPartition(DuplicatableNgram key, V value, int numReduceTasks) {
		return key.getDestinationReducer();
	}

	public int getNonUnigramPartition(FastIntArrayList nonReversedIds, int numReduceTasks) {
		// partition by first two words
		if(nonReversedIds.size() < 2) {
			throw new IllegalArgumentException("You should be duplicating 1-grams and zero-grams");
		}
		FastIntArrayList prefixBigram = nonReversedIds.subarray(0, 2);
		
		int hashCode = NgramUtils.hashNgram(prefixBigram, true);
		
		// we use same strategy as HashPartitioner here
		int which = (hashCode & Integer.MAX_VALUE) % numReduceTasks;
		return which;
	}

}
