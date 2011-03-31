package bigfat.hadoop;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class DuplicatorPartitioner<K extends DuplicatableWriteable, V> extends HashPartitioner<K, V> {

	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		if(true/*key.isDuplicated()*/) {
			// we need to have exactly numReduceTasks duplicated keys!
			return 0;
		} else {
			return super.getPartition(key, value, numReduceTasks);
		}
	}

}
