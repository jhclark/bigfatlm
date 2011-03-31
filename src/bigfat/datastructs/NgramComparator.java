package bigfat.datastructs;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class NgramComparator extends WritableComparator {
	public NgramComparator() {
		super(Ngram.class);
	}

	public int compare(byte[] buf1, int start1, int len1, byte[] buf2,
			int start2, int len2) {

		return compareNgramsRaw(buf1, start1, buf2, start2);
	}

	public static int compareNgramsRaw(byte[] buf1, int start1, byte[] buf2,
			int start2) {
		
		try {
			// determine order of ngrams (size of lists)
			int size1 = readVInt(buf1, start1);
			int size2 = readVInt(buf2, start2);
			
			// skip over list sizes
			int curWordOffset1 = WritableUtils.decodeVIntSize(buf1[start1])
					+ start1;
			int curWordOffset2 = WritableUtils.decodeVIntSize(buf2[start2])
					+ start2;
			
			for (int i = 0; i < size1 || i < size2; i++) {
				if (i >= size1) {
					// equal up to this point, but 2 is longer -- we're in
					// order
					return -1;
				} else if (i >= size2) {
					// equal up to this point, but 1 is longer -- swap so that 1
					// comes after 2
					return 1;
				} 
				// TODO: Zeros come first!
				int wordSize1 = WritableUtils
						.decodeVIntSize(buf1[curWordOffset1]);
				int wordSize2 = WritableUtils
						.decodeVIntSize(buf2[curWordOffset2]);
				int resultAtI = compareBytes(buf1, curWordOffset1,
						wordSize1, buf2, curWordOffset2, wordSize2);
				if (resultAtI != 0) {
					return resultAtI;
				}
				curWordOffset1 += wordSize1;
				curWordOffset2 += wordSize2;
			}
			// same length, same content
			// (we would have detected 2 longer than 1 in the loop)
			return 0;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}