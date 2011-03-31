package bigfat.step6;

import org.apache.hadoop.mapreduce.Partitioner;

import bigfat.datastructs.FastIntArrayList;
import bigfat.util.Hashing;
import bigfat.util.NgramUtils;

public class RenormalizeBackoffsPartitioner extends Partitioner<RenormalizeBackoffsKey, RenormalizeBackoffsInfo> {

	private final FastIntArrayList arr = new FastIntArrayList(5);

	@Override
	public int getPartition(RenormalizeBackoffsKey key, RenormalizeBackoffsInfo value, int numReduceTasks) {

	    // partition by the shorter n-gram history
	    // this means we should leave off the last token
	    // of the full reversed history, but not for shorter
	    // histories
	    // also, keys to be emitted as final n-grams belong
	    // in the same partition as the shorter history
	    key.getAsIds(arr);
	    final boolean includeLastToken = (arr.size() < key.getTargetOrder());

	    int hashCode = Hashing.smear(key.getTargetOrder()) 
	    	^ NgramUtils.hashNgram(arr, includeLastToken);
	
	    // we use same strategy as HashPartitioner here
	    int which = (hashCode & Integer.MAX_VALUE) % numReduceTasks;
	    return which;
	}

}
