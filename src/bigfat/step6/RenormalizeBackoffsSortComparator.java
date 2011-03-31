package bigfat.step6;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import bigfat.datastructs.NgramComparator;

public class RenormalizeBackoffsSortComparator extends WritableComparator {

	public RenormalizeBackoffsSortComparator() {
		super(RenormalizeBackoffsKey.class);
	}

	public int compare(byte[] buf1, int start1, int len1, byte[] buf2,
			int start2, int len2) {
		try { 
			
			int order1 = readVInt(buf1, start1);
			int order2 = readVInt(buf2, start2);

			int result;
			if (order1 < order2) {
				result = -1; // in order
			} else if (order1 > order2) {
				result = 1; // flip
			} else {

				int ngramStart1 = WritableUtils.decodeVIntSize(buf1[start1]) + start1;
				int ngramStart2 = WritableUtils.decodeVIntSize(buf2[start2]) + start2;
				int ngramResult = NgramComparator.compareNgramsRaw(buf1,
						ngramStart1, buf2, ngramStart2);
				if (ngramResult != 0) {
					result = ngramResult;
				} else {
					// lastFlag is always last byte
					boolean isLast1 = buf1[start1 + len1 - 1] == 1;
					boolean isLast2 = buf2[start2 + len2 - 1] == 1;
					if (isLast1 == false && isLast2 == true) {
						result = -1; // in order
					} else if (isLast1 == true && isLast2 == false) {
						result = 1; // flip
					} else {
						result = 0; // equal
					}
				}
			}
			
			// if debug...
//			int debugResult = debugCompare(buf1, start1, len1, buf2, start2, len2); 
//			if(debugResult != result) {
//				throw new RuntimeException("Bad comparator!");
//			}
			return result;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private int debugCompare(byte[] buf1, int start1, int len1, byte[] buf2,
			int start2, int len2) throws IOException {
		
		DataInputBuffer buffer = new DataInputBuffer();
		  RenormalizeBackoffsKey key1 = new RenormalizeBackoffsKey();
		  RenormalizeBackoffsKey key2 = new RenormalizeBackoffsKey();
		  buffer.reset(buf1, start1, len1);                   // parse key1
		  key1.readFields(buffer);
		  
		  buffer.reset(buf2, start2, len2);                   // parse key2
		  key2.readFields(buffer);
		
		return compare(key1, key2);
	}
}
